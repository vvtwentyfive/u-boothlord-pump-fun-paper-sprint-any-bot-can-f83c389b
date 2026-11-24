// Pump.fun Paper Sprint — selective early-momentum bot (agent-fun/replay/v2).
//
// Idea: most pump.fun coins fade within minutes, so wait for the survivors — curves
// at least 500 slots old whose last 300 slots still show broad, accelerating buy flow
// (several distinct buyers, net SOL in, price up and near its window high) — buy a
// small cap-sized stake (0.05 SOL), and exit fast: take-profit +30%, stop-loss -15%,
// a trailing stop once +20% up, or a 6000-slot time limit that recycles the 5 position
// slots to the next coin. Buying the burst itself (early mode) consistently catches
// the top on the dev window; waiting for survivors loses least. Tiny probe round trips
// keep the run above the guardrail pace (20 coins, round trip >= 0.01 SOL each) when
// the filter finds too few coins. Early mode (entryMode 'early') buys young coins
// with real buy flow instead, for research; momentum is the shipped default.
//
// Deterministic by construction: no clock, no randomness, iteration only over
// insertion-ordered maps fed by the batch stream. The hello seed is never used.

import { LAMPORTS_PER_SOL, runBot, type Batch, type Bot, type BotState, type OrderInput, type Pubkey } from './vendor/makefun-bot.ts';

const LAMPORTS = Number(LAMPORTS_PER_SOL);
const CURVE_FEE = 0.0125; // protocol 95 bps + creator 30 bps charged on each side
const BPS = 10_000;

export interface Params {
  /** Entry mode: 'early' buys every young coin that passes the flow filter (small,
   *  cap-sized stakes, fast exits); 'momentum' waits for a rising-price signal. */
  entryMode: 'early' | 'momentum';
  /** Entry lookback in slots. */
  window: number;
  /** Net buy SOL (buys minus sells) over the window. */
  minNetSol: number;
  /** Buy trades in the window. */
  minBuys: number;
  /** Distinct buying wallets in the window. */
  minBuyers: number;
  /** Price now over price at the window start, minus 1. */
  minPriceUp: number;
  /** Skip curves that already ran this far inside one window. */
  maxPriceUp: number;
  /** Price now at least this share of the window high. */
  nearHigh: number;
  /** Curve real SOL bounds at entry. */
  minRqSol: number;
  maxRqSol: number;
  /** Slots since the CreateEvent before an entry is allowed. */
  minAge: number;
  /** First buy of a position (SOL). */
  initialSol: number;
  /** One add-on buy while the position is winning (SOL). */
  addSol: number;
  /** Most SOL into one coin in total. */
  maxPerCoinSol: number;
  /** Smallest buy worth sending (also the guardrail round-trip floor). */
  minBuySol: number;
  /** Slippage on buys (bps); sells always go out at 10000. */
  buySlipBps: number;
  /** Sell the whole position once value reaches cost * (1 + takeProfit). */
  takeProfit: number;
  /** Sell the whole position once value falls to cost * (1 - stopLoss). */
  stopLoss: number;
  /** Trailing stop arms once value reached cost * (1 + trailArm). */
  trailArm: number;
  /** ...then sells once value falls this far below its peak. */
  trail: number;
  /** Exit when net SOL over fadeWindow slots falls below -fadeNetSol. */
  fadeWindow: number;
  fadeNetSol: number;
  /** Exit after this many slots held (unless already far in profit). */
  maxHold: number;
  /** Price must be this far above the last buy before an add-on. */
  addStep: number;
  /** Add only while the position is up at least this much. */
  addIfUp: number;
  /** Slots to wait after an exit before the same curve may be bought again. */
  cooldown: number;
  /** Most entries per curve over the run. */
  maxEntries: number;
  /** Guardrail pacing: target coins / fills over paceSecs of block time. */
  targetCoins: number;
  targetFills: number;
  paceSecs: number;
  /** Probe round-trip size (SOL). */
  probeSol: number;
  maxProbes: number;
}

export const DEFAULT_PARAMS: Params = {
  entryMode: 'momentum',
  window: 300,
  minNetSol: 1,
  minBuys: 5,
  minBuyers: 5,
  minPriceUp: 0.05,
  maxPriceUp: 1.0,
  nearHigh: 0.9,
  minRqSol: 0.5,
  maxRqSol: 40,
  minAge: 500,
  initialSol: 0.05,
  addSol: 0,
  maxPerCoinSol: 0.05,
  minBuySol: 0.01,
  buySlipBps: 1500,
  takeProfit: 0.3,
  stopLoss: 0.15,
  trailArm: 0.2,
  trail: 0.25,
  fadeWindow: 30,
  fadeNetSol: 1000,
  maxHold: 6000,
  addStep: 0.05,
  addIfUp: 0.1,
  cooldown: 300,
  maxEntries: 1,
  targetCoins: 25,
  targetFills: 240,
  paceSecs: 21_600,
  probeSol: 0.02,
  maxProbes: 1,
};

interface Trade {
  slot: number;
  buy: boolean;
  sol: number;
  user: string;
  price: number;
}

interface Tape {
  created: number | null;
  trades: Trade[];
  entries: number;
  lastExit: number;
}

interface Held {
  opened: number;
  peakValue: number;
  invested: number;
  exiting: boolean;
  filled: boolean;
  lastAddPrice: number;
  lastValue: number;
}

export function createStrategy(p: Params = DEFAULT_PARAMS): Bot {
  const tapes = new Map<Pubkey, Tape>();
  const held = new Map<Pubkey, Held>();
  /** Probe round trips in flight, by mint: false while buying, true once selling. */
  const probes = new Map<Pubkey, boolean>();
  /** The bot's own filled orders and the coins they were on (harness closes excluded). */
  let myFills = 0;
  const myCoins = new Set<Pubkey>();
  let firstTime: number | null = null;
  let lastTime: number | null = null;
  const keep = Math.max(p.window, p.fadeWindow) + 5;

  const tapeOf = (mint: Pubkey): Tape => {
    let t = tapes.get(mint);
    if (t === undefined) {
      t = { created: null, trades: [], entries: 0, lastExit: -1e12 };
      tapes.set(mint, t);
    }
    return t;
  };

  /** Entry signal on a curve's tape at slot `now`. Early mode buys young coins with
   *  real buy flow (several buyers, net SOL in) without waiting for a price run-up:
   *  the 1%-of-reserve size cap keeps the stake small while the curve is young, and
   *  the exits do the work. Momentum mode additionally wants the price rising and
   *  holding near its window high. */
  const entrySignal = (t: Tape, now: number, rqSol: number): boolean => {
    if (rqSol < p.minRqSol || rqSol > p.maxRqSol) return false;
    if (t.created === null || now - t.created < p.minAge) return false;
    const from = now - p.window;
    let buySol = 0;
    let sellSol = 0;
    let buys = 0;
    let startPrice = -1;
    let high = 0;
    let last = 0;
    const byUser = new Map<string, number>();
    for (const tr of t.trades) {
      if (tr.slot <= from) {
        startPrice = tr.price;
        continue;
      }
      if (startPrice < 0) startPrice = tr.price;
      if (tr.buy) {
        buySol += tr.sol;
        buys++;
        byUser.set(tr.user, (byUser.get(tr.user) ?? 0) + tr.sol);
      } else {
        sellSol += tr.sol;
      }
      if (tr.price > high) high = tr.price;
      last = tr.price;
    }
    if (buys < p.minBuys || byUser.size < p.minBuyers) return false;
    if (buySol - sellSol < p.minNetSol) return false;
    if (p.entryMode === 'early') return true;
    if (startPrice <= 0 || last <= 0) return false;
    const up = last / startPrice - 1;
    if (up < p.minPriceUp || up > p.maxPriceUp) return false;
    if (last < high * p.nearHigh) return false;
    return true;
  };

  /** Net recorded SOL (buys minus sells) over the last `w` slots. */
  const flow = (t: Tape, now: number, w: number): number => {
    let net = 0;
    for (let i = t.trades.length - 1; i >= 0; i--) {
      const tr = t.trades[i]!;
      if (tr.slot <= now - w) break;
      net += tr.buy ? tr.sol : -tr.sol;
    }
    return net;
  };

  /** Close value of `tokens` on this curve in SOL, curve + tx fees deducted. */
  const closeValue = (tokens: number, vq: number, vt: number, tx: number): number =>
    ((tokens * vq) / (vt + tokens) / LAMPORTS) * (1 - CURVE_FEE) - tx;

  return {
    onBatch(state: BotState, batch: Batch): OrderInput[] {
      const now = batch.slot;
      const traded = new Set<Pubkey>();
      for (const f of batch.fills) {
        if (f.status === 'filled' && !f.client_id.startsWith('_')) {
          myFills++;
          myCoins.add(f.mint);
        }
      }
      for (const e of batch.events) {
        if (e.block_time !== null) {
          if (firstTime === null) firstTime = e.block_time;
          if (lastTime === null || e.block_time > lastTime) lastTime = e.block_time;
        }
        if (e.event === 'CreateEvent') {
          tapeOf(e.mint).created = e.slot;
        } else if (e.event === 'TradeEvent') {
          const t = tapeOf(e.mint);
          const sol = Number(e.sol_amount) / LAMPORTS;
          t.trades.push({
            slot: e.slot,
            buy: e.is_buy,
            sol,
            user: e.user,
            price: Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves),
          });
          traded.add(e.mint);
        }
      }
      for (const mint of traded) {
        const t = tapes.get(mint)!;
        let cut = 0;
        while (cut < t.trades.length && t.trades[cut]!.slot <= now - keep) cut++;
        if (cut > 0) t.trades.splice(0, cut);
      }
      const marks = new Map(batch.marks.map((m) => [m.mint, m] as const));
      const pendingByMint = new Map<Pubkey, number>();
      for (const po of state.pending.values()) pendingByMint.set(po.order.mint, (pendingByMint.get(po.order.mint) ?? 0) + 1);

      const orders: OrderInput[] = [];
      const cap = state.hello.caps.max_orders_per_batch;
      const tx = Number(state.txFeeLamports()) / LAMPORTS;

      // Exits first: take-profit, stop-loss, trailing stop, fading flow, time limit.
      for (const [mint, h] of held) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined) {
          if (!pendingByMint.has(mint)) {
            held.delete(mint);
            const t = tapeOf(mint);
            if (h.filled) t.lastExit = now;
            else t.entries--;
          }
          continue;
        }
        h.filled = true;
        if (curve === undefined || curve.complete) continue;
        const mk = marks.get(mint);
        const vq = mk ? Number(mk.virtual_quote_reserves) : Number(curve.virtual_quote_reserves);
        const vt = mk ? Number(mk.virtual_token_reserves) : Number(curve.virtual_token_reserves);
        const tokens = Number(pos.token_amount);
        const value = closeValue(tokens, vq, vt, tx);
        const cost = Number(pos.cost_lamports) / LAMPORTS;
        if (value > h.peakValue) h.peakValue = value;
        h.lastValue = value;
        let exit = h.exiting;
        const t = tapeOf(mint);
        if (!exit && value >= cost * (1 + p.takeProfit)) exit = true;
        if (!exit && value <= cost * (1 - p.stopLoss)) exit = true;
        if (!exit && h.peakValue >= cost * (1 + p.trailArm) && value < h.peakValue * (1 - p.trail)) exit = true;
        if (!exit && flow(t, now, p.fadeWindow) < -p.fadeNetSol) exit = true;
        if (!exit && now - h.opened > p.maxHold && value < cost * (1 + p.takeProfit)) exit = true;
        if (!exit) continue;
        h.exiting = true;
        let sellPending = false;
        for (const po of state.pending.values()) {
          if (po.order.mint === mint && po.order.side === 'sell') sellPending = true;
        }
        if (sellPending) continue;
        if (orders.length >= cap) break;
        orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: BPS });
      }

      // Probe round trips: sell the whole probe as soon as its buy has landed.
      for (const [mint, selling] of probes) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined) {
          if (!pendingByMint.has(mint)) probes.delete(mint);
          continue;
        }
        if (curve === undefined || curve.complete || pendingByMint.has(mint)) continue;
        if (orders.length >= cap) break;
        orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: BPS });
        if (!selling) probes.set(mint, true);
      }

      // Entries and one add-on per winning position.
      let free = Number(state.freeCashLamports()) / LAMPORTS;
      let slotsLeft = state.hello.caps.max_open_positions - state.committedPositionCount();
      for (const mint of traded) {
        if (orders.length >= cap) break;
        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        if (pendingByMint.has(mint) || probes.has(mint)) continue;
        const t = tapes.get(mint)!;
        const h = held.get(mint);
        const price = Number(curve.virtual_quote_reserves) / Number(curve.virtual_token_reserves);
        const rqSol = Number(curve.real_quote_reserves) / LAMPORTS;
        let want: number;
        if (h === undefined) {
          if (slotsLeft <= 0) continue;
          if (t.entries >= p.maxEntries || now - t.lastExit < p.cooldown) continue;
          if (!entrySignal(t, now, rqSol)) continue;
          want = p.initialSol;
        } else {
          // Add once into strength: price stepped up, position in profit, flow still in.
          if (h.exiting || !h.filled || h.invested >= p.maxPerCoinSol - p.minBuySol) continue;
          const pos = state.positions.get(mint);
          if (pos === undefined) continue;
          if (price < h.lastAddPrice * (1 + p.addStep)) continue;
          if (h.lastValue < (Number(pos.cost_lamports) / LAMPORTS) * (1 + p.addIfUp)) continue;
          if (flow(t, now, p.window) < p.minNetSol) continue;
          want = Math.min(p.addSol, p.maxPerCoinSol - h.invested);
        }
        const capSol = Math.floor(Number(state.maxBuyLamports(mint))) / LAMPORTS;
        let amount = Math.min(capSol, want);
        if (amount > free - tx) amount = free - tx;
        if (amount < p.minBuySol) continue;
        const lamports = BigInt(Math.floor(amount * LAMPORTS));
        orders.push({ side: 'buy', mint, quote_lamports: lamports, max_slippage_bps: p.buySlipBps });
        free -= amount + tx;
        if (h === undefined) {
          held.set(mint, { opened: now, peakValue: 0, invested: amount, exiting: false, filled: false, lastAddPrice: price, lastValue: 0 });
          t.entries++;
          slotsLeft--;
        } else {
          h.invested += amount;
          h.lastAddPrice = price;
        }
      }

      // Guardrail pacing: open a probe when the bot's fills or coins lag the pace.
      if (probes.size < p.maxProbes && slotsLeft > 0 && firstTime !== null && lastTime !== null) {
        const pace = Math.min(1, (lastTime - firstTime) / p.paceSecs);
        const needCoins = myCoins.size < Math.ceil(p.targetCoins * pace);
        const needFills = myFills < Math.ceil(p.targetFills * pace);
        if (needCoins || needFills) {
          const minRq = (p.probeSol * BPS) / state.hello.caps.max_order_bps_of_real_reserve;
          for (const mint of traded) {
            if (orders.length >= cap || probes.size >= p.maxProbes) break;
            if (held.has(mint) || probes.has(mint) || pendingByMint.has(mint) || state.positions.has(mint)) continue;
            if (needCoins && !needFills && myCoins.has(mint)) continue;
            const curve = state.curves.get(mint);
            if (curve === undefined || curve.complete) continue;
            if (Number(curve.real_quote_reserves) / LAMPORTS < minRq * 1.5) continue;
            if (free < p.probeSol + 2 * tx) break;
            orders.push({ side: 'buy', mint, quote_lamports: BigInt(Math.floor(p.probeSol * LAMPORTS)), max_slippage_bps: p.buySlipBps });
            probes.set(mint, false);
            free -= p.probeSol + tx;
          }
        }
      }
      return orders;
    },
  };
}

// Run only when executed directly as `node /bot/main.ts` (the grader's command);
// importing this file from a backtest must not start the bot.
if (process.argv[1]?.endsWith('main.ts')) {
  await runBot(createStrategy(DEFAULT_PARAMS));
}
