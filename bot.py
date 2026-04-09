"""
╔══════════════════════════════════════════════════════════════╗
║     XAUUSD GOLD SCALPER — OPTIMIZER v2.0 (WR FOCUS)         ║
║  Target: 60%+ win rate met PF > 1.2                         ║
║  Extra filters: EMA50, sweep required, body ratio,           ║
║  volume confirm, higher confluence thresholds                ║
╚══════════════════════════════════════════════════════════════╝
"""

import sys
import json
import math
import time
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, List, Tuple
from enum import Enum

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("pip install pandas numpy")
    sys.exit(1)

try:
    import yfinance as yf
except ImportError:
    print("pip install yfinance")
    sys.exit(1)


class Direction(Enum):
    LONG = "buy"
    SHORT = "sell"

class TradePhase(Enum):
    OPEN = "open"
    TP1_HIT = "tp1_hit"
    CLOSED = "closed"


@dataclass
class Config:
    START_BALANCE: float = 5000.0
    COMMISSION_PER_LOT: float = 7.0
    SIMULATED_SPREAD: float = 0.30
    RISK_PERCENT: float = 0.5
    MAX_DAILY_LOSS_PERCENT: float = 3.0
    MAX_TOTAL_DRAWDOWN_PERCENT: float = 10.0
    MAX_CONCURRENT_TRADES: int = 2
    MAX_DAILY_TRADES: int = 20
    MAX_CONSECUTIVE_LOSSES: int = 5

    ATR_PERIOD: int = 10
    ATR_SL_MULTIPLIER: float = 1.0
    MIN_SL_POINTS: float = 2.0
    MAX_SL_POINTS: float = 10.0
    RR_RATIO: float = 2.0

    PARTIAL_PERCENT: float = 0.50
    TP1_RR: float = 0.8
    MOVE_SL_TO_BE: bool = True

    SWING_LOOKBACK: int = 3
    EMA_FAST: int = 9
    EMA_SLOW: int = 21
    USE_EMA_FILTER: bool = True

    ENGULF_BODY_RATIO: float = 0.60
    MOMENTUM_CANDLE_ATR: float = 0.8
    EXHAUSTION_WICK_RATIO: float = 0.65

    USE_MEAN_REVERSION: bool = True
    BB_PERIOD: int = 20
    BB_STD_DEV: float = 2.0
    RSI_PERIOD: int = 7
    RSI_OVERSOLD: float = 25.0
    RSI_OVERBOUGHT: float = 75.0
    MR_CONFLUENCE_SCORE: int = 2

    ROUND_NUMBER_INTERVAL: float = 50.0
    ROUND_NUMBER_ZONE: float = 3.0

    MIN_CONFLUENCE: int = 4
    TRADE_COOLDOWN_BARS: int = 6
    LOSS_COOLDOWN_BARS: int = 15

    USE_EMA50_TREND: bool = False
    REQUIRE_SWEEP: bool = False
    REQUIRE_STRUCTURE: bool = False
    MIN_BODY_RATIO: float = 0.0
    MIN_SCORE: int = 0
    REQUIRE_VOLUME: bool = False
    RSI_FILTER: bool = False
    WICK_REJECTION_BONUS: bool = True


def calculate_indicators(df):
    df = df.copy()
    df["ema9"] = df["Close"].ewm(span=9).mean()
    df["ema21"] = df["Close"].ewm(span=21).mean()
    df["ema50"] = df["Close"].ewm(span=50).mean()
    df["tr"] = np.maximum(df["High"] - df["Low"], np.maximum(abs(df["High"] - df["Close"].shift(1)), abs(df["Low"] - df["Close"].shift(1))))
    df["atr"] = df["tr"].rolling(14).mean()
    delta = df["Close"].diff()
    gain = delta.clip(lower=0).rolling(7).mean()
    loss = (-delta.clip(upper=0)).rolling(7).mean()
    rs = gain / loss.replace(0, np.nan)
    df["rsi"] = 100 - (100 / (1 + rs))
    d14 = df["Close"].diff()
    g14 = d14.clip(lower=0).rolling(14).mean()
    l14 = (-d14.clip(upper=0)).rolling(14).mean()
    rs14 = g14 / l14.replace(0, np.nan)
    df["rsi14"] = 100 - (100 / (1 + rs14))
    df["bb_mid"] = df["Close"].rolling(20).mean()
    df["bb_std"] = df["Close"].rolling(20).std()
    df["bb_upper"] = df["bb_mid"] + 2 * df["bb_std"]
    df["bb_lower"] = df["bb_mid"] - 2 * df["bb_std"]
    df["body"] = abs(df["Close"] - df["Open"])
    df["candle_range"] = df["High"] - df["Low"]
    df["upper_wick"] = df["High"] - df[["Close", "Open"]].max(axis=1)
    df["lower_wick"] = df[["Close", "Open"]].min(axis=1) - df["Low"]
    if "Volume" in df.columns:
        df["avg_volume"] = df["Volume"].rolling(20).mean()
    else:
        df["Volume"] = 0
        df["avg_volume"] = 0
    return df


def detect_swings(df, lookback=3):
    sh, sl = [], []
    for i in range(lookback, len(df) - lookback):
        h, l = df["High"].iloc[i], df["Low"].iloc[i]
        if all(h >= df["High"].iloc[i + j] and h >= df["High"].iloc[i - j] for j in range(1, lookback + 1)):
            sh.append(i)
        if all(l <= df["Low"].iloc[i + j] and l <= df["Low"].iloc[i - j] for j in range(1, lookback + 1)):
            sl.append(i)
    return sh, sl


def detect_signal(df, i, cfg, swing_highs, swing_lows):
    if i < 60:
        return None
    price = df["Close"].iloc[i]
    atr = df["atr"].iloc[i]
    if pd.isna(atr) or atr <= 0:
        return None
    ema9, ema21, ema50 = df["ema9"].iloc[i], df["ema21"].iloc[i], df["ema50"].iloc[i]
    rsi = df["rsi"].iloc[i]
    if pd.isna(ema9) or pd.isna(ema21) or pd.isna(ema50):
        return None
    ts = df.index[i]
    if not hasattr(ts, 'hour'):
        return None
    if not (7 <= ts.hour < 17):
        return None

    confluence = 0
    reasons = []
    votes = {Direction.LONG: 0, Direction.SHORT: 0}
    has_sweep = False
    has_structure = False

    # 1. EMA trend
    if cfg.USE_EMA_FILTER:
        if ema9 > ema21:
            votes[Direction.LONG] += 1
            reasons.append("ema")
        else:
            votes[Direction.SHORT] += 1
            reasons.append("ema")

    # 2. EMA50
    if cfg.USE_EMA50_TREND:
        if price > ema50 and ema9 > ema50:
            votes[Direction.LONG] += 1
            reasons.append("ema50")
        elif price < ema50 and ema9 < ema50:
            votes[Direction.SHORT] += 1
            reasons.append("ema50")

    # 3. Sweep
    ch, cl, cc = df["High"].iloc[i], df["Low"].iloc[i], df["Close"].iloc[i]
    for si in [s for s in swing_lows if s < i and s > i - 25][-3:]:
        if cl < df["Low"].iloc[si] and cc > df["Low"].iloc[si]:
            wick = min(cc, df["Open"].iloc[i]) - cl
            if wick > atr * 0.25:
                votes[Direction.LONG] += 2
                reasons.append("sweep")
                has_sweep = True
                break
    for si in [s for s in swing_highs if s < i and s > i - 25][-3:]:
        if ch > df["High"].iloc[si] and cc < df["High"].iloc[si]:
            wick = ch - max(cc, df["Open"].iloc[i])
            if wick > atr * 0.25:
                votes[Direction.SHORT] += 2
                reasons.append("sweep")
                has_sweep = True
                break

    # 4. OB/engulfing
    if i >= 2:
        prev, curr = df.iloc[i-1], df.iloc[i]
        pb, cb = abs(prev["Close"]-prev["Open"]), abs(curr["Close"]-curr["Open"])
        if curr["Close"]>curr["Open"] and prev["Close"]<prev["Open"] and cb>pb*cfg.ENGULF_BODY_RATIO and curr["Close"]>prev["High"]:
            votes[Direction.LONG] += 1; reasons.append("ob"); has_structure = True
        elif curr["Close"]<curr["Open"] and prev["Close"]>prev["Open"] and cb>pb*cfg.ENGULF_BODY_RATIO and curr["Close"]<prev["Low"]:
            votes[Direction.SHORT] += 1; reasons.append("ob"); has_structure = True

    # 5. FVG
    if i >= 2:
        c1h, c3l = df["High"].iloc[i-2], df["Low"].iloc[i]
        c1l, c3h = df["Low"].iloc[i-2], df["High"].iloc[i]
        mg = atr * 0.25
        if c3l > c1h and (c3l-c1h) >= mg:
            votes[Direction.LONG] += 1; reasons.append("fvg"); has_structure = True
        elif c1l > c3h and (c1l-c3h) >= mg:
            votes[Direction.SHORT] += 1; reasons.append("fvg"); has_structure = True

    # 6. Momentum
    body, total = df["body"].iloc[i], df["candle_range"].iloc[i]
    if total > 0 and body/total >= cfg.ENGULF_BODY_RATIO and body >= atr*cfg.MOMENTUM_CANDLE_ATR:
        if df["Close"].iloc[i] > df["Open"].iloc[i]:
            votes[Direction.LONG] += 1; reasons.append("mom")
        else:
            votes[Direction.SHORT] += 1; reasons.append("mom")

    # 7. Exhaustion
    if total > 0:
        if df["upper_wick"].iloc[i]/total >= cfg.EXHAUSTION_WICK_RATIO:
            votes[Direction.SHORT] += 1; reasons.append("exh")
        elif df["lower_wick"].iloc[i]/total >= cfg.EXHAUSTION_WICK_RATIO:
            votes[Direction.LONG] += 1; reasons.append("exh")

    # 8. Wick rejection
    if cfg.WICK_REJECTION_BONUS and total > 0:
        if df["lower_wick"].iloc[i]/total > 0.5 and df["Close"].iloc[i] > df["Open"].iloc[i]:
            votes[Direction.LONG] += 1; reasons.append("wick")
        elif df["upper_wick"].iloc[i]/total > 0.5 and df["Close"].iloc[i] < df["Open"].iloc[i]:
            votes[Direction.SHORT] += 1; reasons.append("wick")

    # 9. Round number
    nearest = round(price/cfg.ROUND_NUMBER_INTERVAL)*cfg.ROUND_NUMBER_INTERVAL
    if abs(price-nearest) <= cfg.ROUND_NUMBER_ZONE:
        confluence += 1; reasons.append("rn")

    # 10. Mean Reversion
    if cfg.USE_MEAN_REVERSION:
        bb_u, bb_l = df["bb_upper"].iloc[i], df["bb_lower"].iloc[i]
        if not (pd.isna(bb_u) or pd.isna(rsi)):
            bbr = bb_u - bb_l
            if bbr > 0:
                pct_b = (price - bb_l) / bbr
                if pct_b <= 0.05 and rsi <= cfg.RSI_OVERSOLD:
                    votes[Direction.LONG] += cfg.MR_CONFLUENCE_SCORE; reasons.append("MR")
                elif pct_b >= 0.95 and rsi >= cfg.RSI_OVERBOUGHT:
                    votes[Direction.SHORT] += cfg.MR_CONFLUENCE_SCORE; reasons.append("MR")

    # 11. RSI divergence
    rsi14 = df["rsi14"].iloc[i]
    if not pd.isna(rsi14):
        if rsi14 < 30 and df["Close"].iloc[i] > df["Close"].iloc[i-5]:
            votes[Direction.LONG] += 1; reasons.append("rsi_div")
        elif rsi14 > 70 and df["Close"].iloc[i] < df["Close"].iloc[i-5]:
            votes[Direction.SHORT] += 1; reasons.append("rsi_div")

    # Filters
    if cfg.MIN_BODY_RATIO > 0 and total > 0 and body/total < cfg.MIN_BODY_RATIO:
        return None
    if cfg.REQUIRE_VOLUME:
        vol, avg = df["Volume"].iloc[i], df["avg_volume"].iloc[i]
        if not pd.isna(avg) and avg > 0 and vol < avg * 1.2:
            return None

    ls, ss = votes[Direction.LONG], votes[Direction.SHORT]
    if ls > ss and ls >= 1:
        direction = Direction.LONG; confluence += ls
    elif ss > ls and ss >= 1:
        direction = Direction.SHORT; confluence += ss
    else:
        return None

    if cfg.MIN_SCORE > 0 and max(ls, ss) < cfg.MIN_SCORE:
        return None
    if cfg.USE_EMA_FILTER:
        if direction == Direction.LONG and ema9 <= ema21 and "MR" not in reasons:
            return None
        if direction == Direction.SHORT and ema9 >= ema21 and "MR" not in reasons:
            return None
    if cfg.USE_EMA50_TREND:
        if direction == Direction.LONG and price < ema50 and "MR" not in reasons:
            return None
        if direction == Direction.SHORT and price > ema50 and "MR" not in reasons:
            return None
    if cfg.RSI_FILTER and not pd.isna(rsi):
        if direction == Direction.LONG and rsi > 70:
            return None
        if direction == Direction.SHORT and rsi < 30:
            return None
    if cfg.REQUIRE_SWEEP and not has_sweep:
        return None
    if cfg.REQUIRE_STRUCTURE and not has_structure:
        return None
    if confluence < cfg.MIN_CONFLUENCE:
        return None

    return direction, confluence, "|".join(reasons)


@dataclass
class Trade:
    direction: Direction; entry: float; sl: float; tp: float; tp1: float
    lots: float; bar: int; phase: TradePhase = TradePhase.OPEN
    pnl: float = 0.0; remaining: float = 0.0
    def __post_init__(self): self.remaining = self.lots


def run_backtest(df, cfg, sh, sl):
    balance = cfg.START_BALANCE; peak = balance; max_dd = 0.0
    active, closed = [], []; daily_trades = 0; daily_date = ""
    consec_losses = 0; last_trade_bar = -999; last_loss_bar = -999

    for i in range(60, len(df)):
        price, high, low = df["Close"].iloc[i], df["High"].iloc[i], df["Low"].iloc[i]
        atr = df["atr"].iloc[i]
        if pd.isna(atr) or price <= 0: continue
        ts = df.index[i]
        today = str(ts.date()) if hasattr(ts,'date') else str(ts)[:10]
        if today != daily_date: daily_date = today; daily_trades = 0

        for t in list(active):
            sl_hit = (t.direction==Direction.LONG and low<=t.sl) or (t.direction==Direction.SHORT and high>=t.sl)
            if sl_hit:
                t.pnl += ((t.sl-t.entry) if t.direction==Direction.LONG else (t.entry-t.sl))*t.remaining*100
                t.pnl -= cfg.COMMISSION_PER_LOT*t.remaining; balance += t.pnl
                consec_losses += 1; last_loss_bar = i; active.remove(t); closed.append(t); continue
            tp_hit = (t.direction==Direction.LONG and high>=t.tp) or (t.direction==Direction.SHORT and low<=t.tp)
            if tp_hit:
                t.pnl += ((t.tp-t.entry) if t.direction==Direction.LONG else (t.entry-t.tp))*t.remaining*100
                t.pnl -= cfg.COMMISSION_PER_LOT*t.remaining; balance += t.pnl
                consec_losses = 0; active.remove(t); closed.append(t); continue
            if t.phase == TradePhase.OPEN:
                tp1_hit = (t.direction==Direction.LONG and high>=t.tp1) or (t.direction==Direction.SHORT and low<=t.tp1)
                if tp1_hit:
                    cl = round(t.lots*cfg.PARTIAL_PERCENT, 2)
                    if cl >= 0.01:
                        p = ((t.tp1-t.entry) if t.direction==Direction.LONG else (t.entry-t.tp1))*cl*100
                        p -= cfg.COMMISSION_PER_LOT*cl; balance += p; t.pnl += p
                        t.remaining = round(t.remaining-cl, 2); t.phase = TradePhase.TP1_HIT
                        if cfg.MOVE_SL_TO_BE: t.sl = t.entry

        unrealized = sum(((price-t.entry) if t.direction==Direction.LONG else (t.entry-price))*t.remaining*100 for t in active)
        equity = balance + unrealized
        if equity > peak: peak = equity
        dd = (peak-equity)/peak*100 if peak > 0 else 0
        if dd > max_dd: max_dd = dd

        if daily_trades >= cfg.MAX_DAILY_TRADES: continue
        if len(active) >= cfg.MAX_CONCURRENT_TRADES: continue
        if consec_losses >= cfg.MAX_CONSECUTIVE_LOSSES:
            if i - last_loss_bar < cfg.LOSS_COOLDOWN_BARS*2: continue
            consec_losses = 0
        if i - last_trade_bar < cfg.TRADE_COOLDOWN_BARS: continue
        if i - last_loss_bar < cfg.LOSS_COOLDOWN_BARS: continue
        if (cfg.START_BALANCE-balance)/cfg.START_BALANCE*100 >= cfg.MAX_TOTAL_DRAWDOWN_PERCENT: continue

        signal = detect_signal(df, i, cfg, sh, sl)
        if not signal: continue
        direction, score, reason = signal

        sl_dist = max(atr*cfg.ATR_SL_MULTIPLIER, cfg.MIN_SL_POINTS)
        sl_dist = min(sl_dist, cfg.MAX_SL_POINTS)
        entry = price + cfg.SIMULATED_SPREAD if direction==Direction.LONG else price
        if direction == Direction.LONG:
            s,t,t1 = entry-sl_dist, entry+sl_dist*cfg.RR_RATIO, entry+sl_dist*cfg.TP1_RR
        else:
            s,t,t1 = entry+sl_dist, entry-sl_dist*cfg.RR_RATIO, entry-sl_dist*cfg.TP1_RR

        lots = max(0.01, min(round((balance*cfg.RISK_PERCENT/100)/(sl_dist*100), 2), 0.5))
        active.append(Trade(direction=direction, entry=entry, sl=s, tp=t, tp1=t1, lots=lots, bar=i))
        daily_trades += 1; last_trade_bar = i

    if active:
        lp = df["Close"].iloc[-1]
        for t in active:
            t.pnl += ((lp-t.entry) if t.direction==Direction.LONG else (t.entry-lp))*t.remaining*100
            closed.append(t)

    if not closed: return {"trades":0,"pf":0,"wr":0,"pnl":0,"dd":0,"balance":balance}
    wins = [t for t in closed if t.pnl > 0]
    losses = [t for t in closed if t.pnl <= 0]
    tw = sum(t.pnl for t in wins); tl = abs(sum(t.pnl for t in losses))
    return {
        "trades":len(closed),"wins":len(wins),"losses":len(losses),
        "wr":round(len(wins)/len(closed)*100,1),"pf":round(tw/tl,2) if tl>0 else 99,
        "pnl":round(balance-cfg.START_BALANCE,2),"return_pct":round((balance-cfg.START_BALANCE)/cfg.START_BALANCE*100,2),
        "dd":round(max_dd,2),"balance":round(balance,2),
        "avg_win":round(tw/len(wins),2) if wins else 0,"avg_loss":round(tl/len(losses),2) if losses else 0,
    }


def optimize(df):
    print("\n" + "="*60)
    print("  🔧 OPTIMIZER v2.0 — TARGET: 60%+ WIN RATE")
    print("="*60)

    df = calculate_indicators(df)
    sh, sl = detect_swings(df, 3)
    all_results = []
    count = 0

    print("\n  📊 Phase 1: Core × WR filters")
    sl_mults = [0.8, 1.0, 1.2, 1.5, 2.0]
    confluences = [3, 4, 5, 6]
    rr_ratios = [1.5, 2.0, 2.5, 3.0]
    ema50_opts = [True, False]
    sweep_opts = [True, False]
    total = len(sl_mults)*len(confluences)*len(rr_ratios)*len(ema50_opts)*len(sweep_opts)
    print(f"  Testing {total} combinations...")

    for sl_m in sl_mults:
        for conf in confluences:
            for rr in rr_ratios:
                for ema50 in ema50_opts:
                    for sweep in sweep_opts:
                        count += 1
                        cfg = Config(); cfg.ATR_SL_MULTIPLIER=sl_m; cfg.MIN_CONFLUENCE=conf
                        cfg.RR_RATIO=rr; cfg.USE_EMA50_TREND=ema50; cfg.REQUIRE_SWEEP=sweep
                        r = run_backtest(df, cfg, sh, sl)
                        r["params"] = {"sl":sl_m,"conf":conf,"rr":rr,"ema50":ema50,"sweep":sweep}
                        all_results.append(r)
                        if count % 50 == 0: print(f"    {count}/{total}...")

    print(f"  ✅ Phase 1: {count} tested")
    wr_sorted = sorted([r for r in all_results if r["trades"]>=15], key=lambda x: x["wr"], reverse=True)
    print(f"\n  Top 5 by WR:")
    for i,r in enumerate(wr_sorted[:5]):
        p=r["params"]
        print(f"    {i+1}. WR:{r['wr']}% PF:{r['pf']} SL×{p['sl']} Conf≥{p['conf']} RR:{p['rr']} EMA50:{p['ema50']} Sweep:{p['sweep']} {r['trades']}t ${r['pnl']:+.0f}")

    print(f"\n  📊 Phase 2: Fine-tuning top 5")
    phase2 = []; count2 = 0
    for base in wr_sorted[:5]:
        bp = base["params"]
        for tp1 in [0.6, 0.8, 1.0, 1.2]:
            for partial in [0.33, 0.50, 0.67]:
                for cd_t in [3, 6, 12, 24]:
                    for cd_l in [10, 15, 30]:
                        for min_sl in [2.0, 3.0, 4.0, 5.0]:
                            count2 += 1
                            cfg = Config()
                            cfg.ATR_SL_MULTIPLIER=bp["sl"]; cfg.MIN_CONFLUENCE=bp["conf"]
                            cfg.RR_RATIO=bp["rr"]; cfg.USE_EMA50_TREND=bp["ema50"]
                            cfg.REQUIRE_SWEEP=bp["sweep"]; cfg.TP1_RR=tp1
                            cfg.PARTIAL_PERCENT=partial; cfg.TRADE_COOLDOWN_BARS=cd_t
                            cfg.LOSS_COOLDOWN_BARS=cd_l; cfg.MIN_SL_POINTS=min_sl
                            r = run_backtest(df, cfg, sh, sl)
                            r["params"] = {**bp,"tp1":tp1,"partial":partial,"cd_t":cd_t,"cd_l":cd_l,"min_sl":min_sl}
                            phase2.append(r)
                            if count2 % 200 == 0: print(f"    {count2}...")

    all_results += phase2
    print(f"  ✅ Phase 2: {count2} tested")

    print(f"\n  📊 Phase 3: Quality filters")
    viable_top = sorted([r for r in all_results if r["trades"]>=15 and r["pf"]>=1.0], key=lambda x: x["wr"], reverse=True)[:3]
    phase3 = []; count3 = 0
    for base in viable_top:
        bp = base["params"]
        for body_r in [0.0, 0.3, 0.5]:
            for rsi_f in [True, False]:
                for req_s in [True, False]:
                    for wick in [True, False]:
                        for vol in [True, False]:
                            count3 += 1
                            cfg = Config()
                            cfg.ATR_SL_MULTIPLIER=bp["sl"]; cfg.MIN_CONFLUENCE=bp["conf"]
                            cfg.RR_RATIO=bp["rr"]; cfg.USE_EMA50_TREND=bp.get("ema50",False)
                            cfg.REQUIRE_SWEEP=bp.get("sweep",False); cfg.TP1_RR=bp.get("tp1",0.8)
                            cfg.PARTIAL_PERCENT=bp.get("partial",0.50); cfg.TRADE_COOLDOWN_BARS=bp.get("cd_t",6)
                            cfg.LOSS_COOLDOWN_BARS=bp.get("cd_l",15); cfg.MIN_SL_POINTS=bp.get("min_sl",2.0)
                            cfg.MIN_BODY_RATIO=body_r; cfg.RSI_FILTER=rsi_f
                            cfg.REQUIRE_STRUCTURE=req_s; cfg.WICK_REJECTION_BONUS=wick; cfg.REQUIRE_VOLUME=vol
                            r = run_backtest(df, cfg, sh, sl)
                            r["params"] = {**bp,"body_ratio":body_r,"rsi_filter":rsi_f,"req_struct":req_s,"wick":wick,"vol":vol}
                            phase3.append(r)

    all_results += phase3
    print(f"  ✅ Phase 3: {count3} tested")

    viable = [r for r in all_results if r["trades"]>=15 and r["pf"]>=1.0]
    for r in viable:
        wr_bonus = max(0, r["wr"]-50)*0.5
        r["score"] = round((r["wr"]/100)*r["pf"]*math.sqrt(r["trades"])*(1-r["dd"]/100)+wr_bonus, 2)

    high_wr = sorted([r for r in viable if r["wr"]>=60], key=lambda x: (x["wr"],x["pf"]), reverse=True)
    good_wr = sorted([r for r in viable if 55<=r["wr"]<60], key=lambda x: (x["wr"],x["pf"]), reverse=True)

    print(f"\n{'='*60}")
    print(f"  🏆 RESULTS: {len(viable)} viable | {len(high_wr)} with 60%+ WR")
    print(f"{'='*60}")

    show = high_wr[:10] if high_wr else good_wr[:10] if good_wr else sorted(viable, key=lambda x: x["wr"], reverse=True)[:10]
    label = "60%+ WR" if high_wr else "55%+ WR" if good_wr else "BEST AVAILABLE"

    print(f"\n  🎯 {label} SETTINGS:")
    for i,r in enumerate(show):
        p = r["params"]
        print(f"""
  #{i+1} — WR: {r['wr']}% {'⭐' if r['wr']>=60 else ''}
  ├── SL: ATR×{p['sl']} | Min SL: ${p.get('min_sl',2.0)}
  ├── Conf≥{p['conf']} | RR: 1:{p['rr']} | TP1: {p.get('tp1',0.8)}R
  ├── Partial: {p.get('partial',0.50)*100:.0f}% | CD: {p.get('cd_t',6)}/{p.get('cd_l',15)}
  ├── EMA50: {p.get('ema50',False)} | Sweep: {p.get('sweep',False)}
  ├── Body: {p.get('body_ratio',0)} | RSI_f: {p.get('rsi_filter',False)} | Struct: {p.get('req_struct',False)}
  ├── Trades: {r['trades']} | PF: {r['pf']} | PnL: ${r['pnl']:+,.2f}
  └── DD: {r['dd']:.1f}% | Return: {r['return_pct']:+.2f}%""")

    balanced = sorted(viable, key=lambda x: x["score"], reverse=True)
    if balanced:
        best = balanced[0]; bp = best["params"]
        print(f"""
{'='*60}
  ⭐ AANBEVOLEN SETTINGS:
{'='*60}
  ATR_SL_MULTIPLIER = {bp['sl']}
  MIN_SL_POINTS = {bp.get('min_sl',2.0)}
  RR_RATIO = {bp['rr']}
  TP1_RR = {bp.get('tp1',0.8)}
  PARTIAL_PERCENT = {bp.get('partial',0.50)}
  MIN_CONFLUENCE = {bp['conf']}
  USE_EMA50_TREND = {bp.get('ema50',False)}
  REQUIRE_SWEEP = {bp.get('sweep',False)}
  TRADE_COOLDOWN = {bp.get('cd_t',6)}
  LOSS_COOLDOWN = {bp.get('cd_l',15)}

  WR: {best['wr']}% | PF: {best['pf']} | Trades: {best['trades']}
  DD: {best['dd']:.1f}% | Return: {best['return_pct']:+.2f}%
{'='*60}""")

    output = {
        "high_wr": [{"rank":i+1,"params":r["params"],"wr":r["wr"],"pf":r["pf"],"pnl":r["pnl"],"trades":r["trades"],"dd":r["dd"]} for i,r in enumerate(show)],
        "total_tested": len(all_results), "viable": len(viable), "above_60wr": len(high_wr),
    }
    with open("optimization_v2_results.json","w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  📁 Saved: optimization_v2_results.json")
    return show


def main():
    print("""
╔══════════════════════════════════════════════════════════════╗
║     XAUUSD GOLD SCALPER — OPTIMIZER v2.0                    ║
║     🎯 Target: 60%+ Win Rate met PF > 1.2                  ║
╚══════════════════════════════════════════════════════════════╝
    """)
    print("📥 Downloading 60-day gold data...")
    df = yf.download("GC=F", period="60d", interval="5m", progress=True)
    if df.empty: df = yf.download("GC=F", period="60d", interval="1h", progress=True)
    if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
    if df.index.tz is not None: df.index = df.index.tz_localize(None)
    df = df.dropna(subset=["Open","High","Low","Close"])
    df = df[df["Close"]>0]
    print(f"✅ {len(df)} bars: {df.index[0]} → {df.index[-1]}")
    start = time.time()
    optimize(df)
    print(f"\n  ⏱️  Klaar in {time.time()-start:.0f}s")

if __name__ == "__main__":
    main()
