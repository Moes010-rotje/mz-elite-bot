"""
╔══════════════════════════════════════════════════════════════╗
║         GOLD SCALPER BACKTESTER v1.0                        ║
║  Hergebruikt exacte bot-logica voor historische simulatie    ║
║  Wekelijkse rapportage + parameter optimalisatie            ║
╚══════════════════════════════════════════════════════════════╝

Gebruik:
  1. Data ophalen via MetaAPI:
     python backtest.py --fetch --weeks 12

  2. Backtest draaien op opgeslagen data:
     python backtest.py --run

  3. Parameter optimalisatie:
     python backtest.py --optimize

  4. Alles in één keer:
     python backtest.py --fetch --weeks 12 --run --optimize
"""

import os
import sys
import csv
import json
import asyncio
import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional
from pathlib import Path
from copy import deepcopy

from bot import (
    ScalpConfig, BotState, ScalpAnalyzer, SessionMgr, ScalpSignal,
    Direction, Session, TradePhase, ScalpTrade,
)

DATA_DIR = Path("backtest_data")
RESULTS_DIR = Path("backtest_results")


# ═══════════════════════════════════════════════════════════════════
#  DATA FETCHER — haalt historische candles op via MetaAPI
# ═══════════════════════════════════════════════════════════════════

async def fetch_historical_data(weeks: int = 12):
    """Haal historische XAUUSD candles op en sla op als CSV."""
    try:
        from metaapi_cloud_sdk import MetaApi
    except ImportError:
        print("pip install metaapi-cloud-sdk")
        sys.exit(1)

    token = os.getenv("METAAPI_TOKEN", "")
    account_id = os.getenv("ACCOUNT_ID", "")
    if not token or not account_id:
        print("Set METAAPI_TOKEN en ACCOUNT_ID environment variables!")
        sys.exit(1)

    DATA_DIR.mkdir(exist_ok=True)

    api = MetaApi(token)
    account = await api.metatrader_account_api.get_account(account_id)
    if account.state != "DEPLOYED":
        await account.deploy()
    await account.wait_connected()

    end = datetime.now(timezone.utc)
    start = end - timedelta(weeks=weeks)

    print(f"Fetching {weeks} weeks of data: {start.date()} → {end.date()}")

    for tf, label in [("1m", "1m"), ("5m", "5m")]:
        print(f"  Fetching {label} candles...")
        all_candles = []
        chunk_start = start

        while chunk_start < end:
            chunk_end = min(chunk_start + timedelta(days=7), end)
            try:
                candles = await asyncio.wait_for(
                    account.get_historical_candles("XAUUSD", tf, chunk_start, chunk_end),
                    timeout=30
                )
                if candles:
                    all_candles.extend(candles)
                    print(f"    {chunk_start.date()} → {chunk_end.date()}: {len(candles)} candles")
            except Exception as e:
                print(f"    Error {chunk_start.date()}: {e}")
            chunk_start = chunk_end
            await asyncio.sleep(1)

        filepath = DATA_DIR / f"xauusd_{label}.csv"
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["time", "open", "high", "low", "close", "tickVolume"])
            writer.writeheader()
            for c in all_candles:
                writer.writerow({
                    "time": c.get("time", ""),
                    "open": c.get("open", 0),
                    "high": c.get("high", 0),
                    "low": c.get("low", 0),
                    "close": c.get("close", 0),
                    "tickVolume": c.get("tickVolume", c.get("volume", 0)),
                })

        print(f"  Saved {len(all_candles)} {label} candles → {filepath}")

    print("Data fetch complete!")


def load_candles(tf: str) -> List[dict]:
    """Laad candles uit CSV."""
    filepath = DATA_DIR / f"xauusd_{tf}.csv"
    if not filepath.exists():
        print(f"Data niet gevonden: {filepath}")
        print("Draai eerst: python backtest.py --fetch --weeks 12")
        sys.exit(1)

    candles = []
    with open(filepath) as f:
        for row in csv.DictReader(f):
            candles.append({
                "time": row["time"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "tickVolume": int(float(row["tickVolume"])),
            })
    return candles


def parse_candle_time(candle: dict) -> Optional[datetime]:
    ts = candle.get("time", "")
    if isinstance(ts, datetime):
        return ts
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


# ═══════════════════════════════════════════════════════════════════
#  SIMULATED TRADE
# ═══════════════════════════════════════════════════════════════════

@dataclass
class SimTrade:
    direction: Direction
    entry: float
    sl: float
    tp: float
    tp1: float
    lots: float
    phase: str = "open"          # open, tp1_hit, closed
    open_time: str = ""
    close_time: str = ""
    close_reason: str = ""
    pnl: float = 0.0
    partial_pnl: float = 0.0    # winst van 67% partial
    runner_pnl: float = 0.0     # winst/verlies van 33% runner
    confluence: int = 0
    reason: str = ""


# ═══════════════════════════════════════════════════════════════════
#  BACKTEST ENGINE
# ═══════════════════════════════════════════════════════════════════

class BacktestEngine:
    def __init__(self, cfg: ScalpConfig = None, starting_balance: float = 5000.0):
        self.cfg = cfg or ScalpConfig()
        self.starting_balance = starting_balance

    def run(self, candles_1m: List[dict], candles_5m: List[dict]) -> List[SimTrade]:
        """Simuleer de bot over historische data."""
        state = BotState(self.cfg)
        state.balance = self.starting_balance
        state.start_balance = self.starting_balance
        state.equity = self.starting_balance
        state.trade_date = ""

        az = ScalpAnalyzer(self.cfg)
        sm = SessionMgr(self.cfg)

        trades: List[SimTrade] = []
        active: List[SimTrade] = []

        lookback_1m = self.cfg.CANDLE_LOOKBACK_1M
        lookback_5m = self.cfg.CANDLE_LOOKBACK_5M

        idx_5m = {}
        for i, c in enumerate(candles_5m):
            dt = parse_candle_time(c)
            if dt:
                idx_5m[dt.strftime("%Y-%m-%d %H")] = i

        last_trade_bar = -999
        last_loss_bar = -999
        consecutive_losses = 0
        daily_trades = 0
        daily_pnl = 0.0
        daily_wins = 0
        daily_losses = 0
        current_date = ""

        for bar_i in range(lookback_1m, len(candles_1m)):
            candle = candles_1m[bar_i]
            dt = parse_candle_time(candle)
            if not dt:
                continue

            price = candle["close"]
            high = candle["high"]
            low = candle["low"]
            hour = dt.hour
            today = dt.strftime("%Y-%m-%d")

            # Daily reset
            if today != current_date:
                current_date = today
                daily_trades = 0
                daily_pnl = 0.0
                daily_wins = 0
                daily_losses = 0
                consecutive_losses = 0
                state.start_balance = state.balance

            # --- Manage open trades met candle high/low ---
            for t in list(active):
                if t.phase == "open":
                    # Check SL hit
                    sl_hit = (t.direction == Direction.LONG and low <= t.sl) or \
                             (t.direction == Direction.SHORT and high >= t.sl)
                    # Check TP1 hit
                    tp1_hit = (t.direction == Direction.LONG and high >= t.tp1) or \
                              (t.direction == Direction.SHORT and low <= t.tp1)

                    if sl_hit:
                        t.phase = "closed"
                        t.close_reason = "sl"
                        t.close_time = candle["time"]
                        sl_dist = abs(t.entry - t.sl)
                        t.pnl = -sl_dist * t.lots * 100
                        state.balance += t.pnl
                        daily_pnl += t.pnl
                        daily_losses += 1
                        consecutive_losses += 1
                        last_loss_bar = bar_i
                        active.remove(t)
                        trades.append(t)
                    elif tp1_hit:
                        # Partial close: 67% at TP1
                        partial_lots = round(t.lots * self.cfg.PARTIAL_PERCENT, 2)
                        tp1_dist = abs(t.tp1 - t.entry)
                        t.partial_pnl = tp1_dist * partial_lots * 100
                        t.phase = "tp1_hit"
                        t.sl = t.entry  # SL to breakeven
                        state.balance += t.partial_pnl

                elif t.phase == "tp1_hit":
                    runner_lots = round(t.lots * (1 - self.cfg.PARTIAL_PERCENT), 2)
                    if runner_lots < 0.01:
                        runner_lots = 0.01

                    # Check SL (breakeven)
                    sl_hit = (t.direction == Direction.LONG and low <= t.sl) or \
                             (t.direction == Direction.SHORT and high >= t.sl)
                    # Check TP2
                    tp_hit = (t.direction == Direction.LONG and high >= t.tp) or \
                             (t.direction == Direction.SHORT and low <= t.tp)

                    if sl_hit:
                        t.phase = "closed"
                        t.close_reason = "be"
                        t.close_time = candle["time"]
                        t.runner_pnl = 0.0  # breakeven
                        t.pnl = t.partial_pnl + t.runner_pnl
                        daily_pnl += 0  # runner at BE
                        daily_wins += 1
                        consecutive_losses = 0
                        active.remove(t)
                        trades.append(t)
                    elif tp_hit:
                        t.phase = "closed"
                        t.close_reason = "tp"
                        t.close_time = candle["time"]
                        tp_dist = abs(t.tp - t.entry)
                        t.runner_pnl = tp_dist * runner_lots * 100
                        t.pnl = t.partial_pnl + t.runner_pnl
                        state.balance += t.runner_pnl
                        daily_pnl += t.runner_pnl
                        daily_wins += 1
                        consecutive_losses = 0
                        active.remove(t)
                        trades.append(t)

            # --- Signal generation ---
            # Gate checks (same as live bot)
            if not sm.is_tradeable(hour):
                continue
            if daily_trades >= self.cfg.MAX_DAILY_TRADES:
                continue
            if len(active) >= self.cfg.MAX_CONCURRENT_TRADES:
                continue
            if state.start_balance > 0:
                max_loss = state.start_balance * (self.cfg.MAX_DAILY_LOSS_PERCENT / 100)
                if daily_pnl <= -max_loss:
                    continue
            if state.start_balance > 0 and state.balance > 0:
                dd = (state.start_balance - state.balance) / state.start_balance * 100
                if dd >= self.cfg.MAX_TOTAL_DRAWDOWN_PERCENT:
                    continue
            if consecutive_losses >= self.cfg.MAX_CONSECUTIVE_LOSSES:
                if bar_i - last_loss_bar < (self.cfg.LOSS_COOLDOWN_SECONDS * 2) // 60:
                    continue
                consecutive_losses = 0
            # Trade cooldown (in bars, ~1 bar = 1 min)
            if bar_i - last_trade_bar < self.cfg.TRADE_COOLDOWN_SECONDS // 60:
                continue
            if bar_i - last_loss_bar < self.cfg.LOSS_COOLDOWN_SECONDS // 60:
                continue

            # Build candle windows
            window_1m = candles_1m[bar_i - lookback_1m:bar_i + 1]

            # Find matching 5m window
            hour_key = dt.strftime("%Y-%m-%d %H")
            i5m = idx_5m.get(hour_key)
            if i5m is not None and i5m >= lookback_5m:
                window_5m = candles_5m[max(0, i5m - lookback_5m):i5m + 1]
            else:
                window_5m = candles_5m[:lookback_5m]

            if len(window_1m) < 15 or len(window_5m) < 15:
                continue

            # Update state for signal generator
            state.candles_1m = window_1m
            state.candles_5m = window_5m
            state.session = sm.get(hour)
            state.daily_trades = daily_trades
            state.daily_pnl = daily_pnl
            state.consecutive_losses = consecutive_losses
            state.active_trades = {str(id(t)): ScalpTrade(
                id=str(id(t)), direction=t.direction, entry=t.entry,
                sl=t.sl, tp=t.tp, tp1=t.tp1, lots=t.lots
            ) for t in active}
            state.last_trade_time = 0
            state.last_loss_time = 0

            # Use bot's signal generator
            sig = ScalpSignal(state, az, sm)

            # Patch datetime for signal eval
            import bot as bot_module
            orig_dt = bot_module.datetime

            class FakeDatetime:
                @staticmethod
                def now(tz=None):
                    return dt
                def __call__(self, *args, **kwargs):
                    return orig_dt(*args, **kwargs)
                def __getattr__(self, name):
                    return getattr(orig_dt, name)

            bot_module.datetime = FakeDatetime()
            try:
                spread = 1.5  # typical XAUUSD spread
                result = sig.evaluate(price, spread)
            finally:
                bot_module.datetime = orig_dt

            if result is None:
                continue

            direction, sl, tp1, tp, confluence, reason = result
            sl_dist = abs(price - sl)
            risk = state.balance * (self.cfg.RISK_PERCENT / 100)
            lots = risk / (sl_dist * 100)
            lots = max(0.01, min(round(lots, 2), 0.5))

            trade = SimTrade(
                direction=direction,
                entry=price, sl=sl, tp=tp, tp1=tp1,
                lots=lots, open_time=candle["time"],
                confluence=confluence, reason=reason,
            )
            active.append(trade)
            daily_trades += 1
            last_trade_bar = bar_i

        # Close remaining open trades at last price
        last_price = candles_1m[-1]["close"]
        for t in active:
            t.phase = "closed"
            t.close_reason = "eod"
            if t.direction == Direction.LONG:
                pnl = (last_price - t.entry) * t.lots * 100
            else:
                pnl = (t.entry - last_price) * t.lots * 100
            t.pnl = t.partial_pnl + pnl
            state.balance += pnl
            trades.append(t)

        return trades


# ═══════════════════════════════════════════════════════════════════
#  RAPPORTAGE
# ═══════════════════════════════════════════════════════════════════

def weekly_report(trades: List[SimTrade], starting_balance: float) -> Dict:
    """Groepeer trades per week en genereer rapport."""
    weeks = {}
    for t in trades:
        dt = parse_candle_time({"time": t.open_time})
        if not dt:
            continue
        week_key = f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
        if week_key not in weeks:
            weeks[week_key] = []
        weeks[week_key].append(t)

    report = {}
    balance = starting_balance
    for week in sorted(weeks.keys()):
        wtrades = weeks[week]
        wins = sum(1 for t in wtrades if t.pnl > 0)
        losses = sum(1 for t in wtrades if t.pnl <= 0)
        total_pnl = sum(t.pnl for t in wtrades)
        wr = wins / max(wins + losses, 1) * 100

        tp_hits = sum(1 for t in wtrades if t.close_reason == "tp")
        be_hits = sum(1 for t in wtrades if t.close_reason == "be")
        sl_hits = sum(1 for t in wtrades if t.close_reason == "sl")

        balance += total_pnl

        report[week] = {
            "trades": len(wtrades),
            "wins": wins,
            "losses": losses,
            "win_rate": round(wr, 1),
            "pnl": round(total_pnl, 2),
            "balance": round(balance, 2),
            "tp_hits": tp_hits,
            "be_hits": be_hits,
            "sl_hits": sl_hits,
            "avg_win": round(sum(t.pnl for t in wtrades if t.pnl > 0) / max(wins, 1), 2),
            "avg_loss": round(sum(t.pnl for t in wtrades if t.pnl <= 0) / max(losses, 1), 2),
        }

    return report


def print_report(report: Dict, starting_balance: float, cfg: ScalpConfig):
    """Print wekelijks rapport naar console."""
    print("\n" + "═" * 80)
    print(f"  GOLD SCALPER BACKTEST RAPPORT")
    print(f"  Start balance: ${starting_balance:,.2f}")
    print(f"  Settings: ATR×{cfg.ATR_SL_MULTIPLIER} | RR 1:{cfg.DEFAULT_RR_RATIO} | "
          f"TP1 {cfg.TP1_RR_RATIO}R | {cfg.PARTIAL_PERCENT*100:.0f}% partial | "
          f"Risk {cfg.RISK_PERCENT}% | MIN_SL ${cfg.MIN_SL_POINTS}")
    print("═" * 80)
    print(f"{'Week':<12} {'Trades':>6} {'W/L':>7} {'WR%':>6} {'PnL':>10} "
          f"{'Balance':>10} {'TP':>4} {'BE':>4} {'SL':>4} {'AvgW':>8} {'AvgL':>8}")
    print("─" * 80)

    total_trades = 0
    total_wins = 0
    total_losses = 0
    total_pnl = 0.0

    for week, data in sorted(report.items()):
        total_trades += data["trades"]
        total_wins += data["wins"]
        total_losses += data["losses"]
        total_pnl += data["pnl"]

        pnl_str = f"${data['pnl']:+,.2f}"
        pnl_color = "" if data["pnl"] >= 0 else ""

        print(f"{week:<12} {data['trades']:>6} {data['wins']:>3}/{data['losses']:<3} "
              f"{data['win_rate']:>5.1f}% {pnl_str:>10} ${data['balance']:>9,.2f} "
              f"{data['tp_hits']:>4} {data['be_hits']:>4} {data['sl_hits']:>4} "
              f"${data['avg_win']:>7,.2f} ${data['avg_loss']:>7,.2f}")

    print("─" * 80)
    total_wr = total_wins / max(total_wins + total_losses, 1) * 100
    final_bal = starting_balance + total_pnl
    roi = (total_pnl / starting_balance) * 100

    print(f"{'TOTAAL':<12} {total_trades:>6} {total_wins:>3}/{total_losses:<3} "
          f"{total_wr:>5.1f}% ${total_pnl:>+9,.2f} ${final_bal:>9,.2f}")
    print(f"\n  ROI: {roi:+.1f}% | Profit Factor: "
          f"{abs(sum(1 for _ in []))+1:.2f}")

    # Calculate profit factor properly
    gross_profit = sum(d["avg_win"] * d["wins"] for d in report.values())
    gross_loss = abs(sum(d["avg_loss"] * d["losses"] for d in report.values()))
    pf = gross_profit / max(gross_loss, 0.01)
    print(f"  ROI: {roi:+.1f}% | Profit Factor: {pf:.2f} | "
          f"Verwacht WR: 70.7% | Werkelijk WR: {total_wr:.1f}%")
    print("═" * 80)


# ═══════════════════════════════════════════════════════════════════
#  PARAMETER OPTIMIZER
# ═══════════════════════════════════════════════════════════════════

def optimize(candles_1m: List[dict], candles_5m: List[dict],
             starting_balance: float = 5000.0):
    """Test verschillende parameter combinaties en vind de beste."""
    param_grid = {
        "MIN_SL_POINTS": [2.0, 3.0, 4.0],
        "ATR_SL_MULTIPLIER": [2.0, 2.5, 3.0],
        "DEFAULT_RR_RATIO": [1.5, 2.0, 2.5],
        "TP1_RR_RATIO": [0.3, 0.4, 0.5],
        "PARTIAL_PERCENT": [0.50, 0.67, 0.75],
        "MIN_CONFLUENCE": [2, 3, 4],
    }

    # Generate combinations (keep it manageable)
    from itertools import product

    keys = list(param_grid.keys())
    values = list(param_grid.values())
    combos = list(product(*values))

    print(f"\n{'═' * 80}")
    print(f"  PARAMETER OPTIMALISATIE — {len(combos)} combinaties")
    print(f"{'═' * 80}")

    results = []
    for i, combo in enumerate(combos):
        cfg = ScalpConfig()
        for k, v in zip(keys, combo):
            setattr(cfg, k, v)
            # Sync all RR ratios
            if k == "DEFAULT_RR_RATIO":
                cfg.LONDON_RR_RATIO = v
                cfg.NY_RR_RATIO = v
                cfg.OVERLAP_RR_RATIO = v

        engine = BacktestEngine(cfg, starting_balance)
        trades = engine.run(candles_1m, candles_5m)

        total_pnl = sum(t.pnl for t in trades)
        wins = sum(1 for t in trades if t.pnl > 0)
        total = len(trades)
        wr = wins / max(total, 1) * 100
        gross_profit = sum(t.pnl for t in trades if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in trades if t.pnl <= 0))
        pf = gross_profit / max(gross_loss, 0.01)

        # Max drawdown
        peak = starting_balance
        max_dd = 0
        bal = starting_balance
        for t in trades:
            bal += t.pnl
            peak = max(peak, bal)
            dd = (peak - bal) / peak * 100
            max_dd = max(max_dd, dd)

        results.append({
            "params": dict(zip(keys, combo)),
            "trades": total,
            "wr": round(wr, 1),
            "pnl": round(total_pnl, 2),
            "pf": round(pf, 2),
            "max_dd": round(max_dd, 1),
            "roi": round((total_pnl / starting_balance) * 100, 1),
        })

        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(combos)} combinaties getest...")

    # Sort by PnL (profit)
    results.sort(key=lambda x: x["pnl"], reverse=True)

    print(f"\n{'─' * 80}")
    print(f"  TOP 10 BESTE COMBINATIES (gesorteerd op PnL)")
    print(f"{'─' * 80}")
    print(f"{'#':>3} {'PnL':>10} {'ROI':>7} {'WR%':>6} {'PF':>5} {'DD%':>5} "
          f"{'Trades':>6}  Parameters")
    print(f"{'─' * 80}")

    for i, r in enumerate(results[:10]):
        params_str = " | ".join(f"{k}={v}" for k, v in r["params"].items())
        print(f"{i+1:>3} ${r['pnl']:>+9,.2f} {r['roi']:>+6.1f}% {r['wr']:>5.1f}% "
              f"{r['pf']:>5.2f} {r['max_dd']:>4.1f}% {r['trades']:>6}  {params_str}")

    # Also show worst 3 for contrast
    print(f"\n  SLECHTSTE 3:")
    for r in results[-3:]:
        params_str = " | ".join(f"{k}={v}" for k, v in r["params"].items())
        print(f"    ${r['pnl']:>+9,.2f} {r['roi']:>+6.1f}% WR:{r['wr']:.0f}% "
              f"PF:{r['pf']:.2f}  {params_str}")

    # Save results
    RESULTS_DIR.mkdir(exist_ok=True)
    results_file = RESULTS_DIR / f"optimize_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Resultaten opgeslagen → {results_file}")

    if results:
        best = results[0]
        print(f"\n{'═' * 80}")
        print(f"  AANBEVOLEN SETTINGS:")
        for k, v in best["params"].items():
            print(f"    {k} = {v}")
        print(f"  Verwachte ROI: {best['roi']:+.1f}% | WR: {best['wr']:.1f}% | "
              f"PF: {best['pf']:.2f} | Max DD: {best['max_dd']:.1f}%")
        print(f"{'═' * 80}")

    return results


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Gold Scalper Backtester")
    parser.add_argument("--fetch", action="store_true", help="Fetch historical data via MetaAPI")
    parser.add_argument("--weeks", type=int, default=12, help="Number of weeks to fetch (default: 12)")
    parser.add_argument("--run", action="store_true", help="Run backtest")
    parser.add_argument("--optimize", action="store_true", help="Run parameter optimization")
    parser.add_argument("--balance", type=float, default=5000.0, help="Starting balance (default: 5000)")
    args = parser.parse_args()

    if not any([args.fetch, args.run, args.optimize]):
        parser.print_help()
        print("\nVoorbeelden:")
        print("  python backtest.py --fetch --weeks 12        # Data ophalen")
        print("  python backtest.py --run                      # Backtest draaien")
        print("  python backtest.py --optimize                 # Parameters optimaliseren")
        print("  python backtest.py --fetch --weeks 8 --run --optimize  # Alles")
        return

    if args.fetch:
        asyncio.run(fetch_historical_data(args.weeks))

    if args.run:
        print("Loading data...")
        candles_1m = load_candles("1m")
        candles_5m = load_candles("5m")
        print(f"Loaded: {len(candles_1m)} 1M candles, {len(candles_5m)} 5M candles")

        cfg = ScalpConfig()
        engine = BacktestEngine(cfg, args.balance)

        print("Running backtest...")
        start_time = time.time()
        trades = engine.run(candles_1m, candles_5m)
        elapsed = time.time() - start_time
        print(f"Backtest complete: {len(trades)} trades in {elapsed:.1f}s")

        report = weekly_report(trades, args.balance)
        print_report(report, args.balance, cfg)

        # Save trades
        RESULTS_DIR.mkdir(exist_ok=True)
        trades_file = RESULTS_DIR / f"trades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(trades_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["direction", "entry", "sl", "tp", "tp1", "lots",
                           "open_time", "close_time", "close_reason", "pnl",
                           "confluence", "reason"])
            for t in trades:
                writer.writerow([t.direction.value, t.entry, t.sl, t.tp, t.tp1,
                               t.lots, t.open_time, t.close_time, t.close_reason,
                               round(t.pnl, 2), t.confluence, t.reason])
        print(f"Trades opgeslagen → {trades_file}")

    if args.optimize:
        print("Loading data...")
        candles_1m = load_candles("1m")
        candles_5m = load_candles("5m")
        print(f"Loaded: {len(candles_1m)} 1M candles, {len(candles_5m)} 5M candles")

        optimize(candles_1m, candles_5m, args.balance)


if __name__ == "__main__":
    main()
