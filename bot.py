"""
╔══════════════════════════════════════════════════════════════╗
║           XAUUSD GOLD SCALPER v1.2 (OPTIMIZED)              ║
║    True Scalping · 1M/5M · SMC + Mean Reversion             ║
║         MetaAPI Cloud SDK · Railway Deploy                   ║
╚══════════════════════════════════════════════════════════════╝

Built for real scalping on XAUUSD Gold:
- 5M structure + 1M precision entries
- 10-second cycle for fast execution
- OPTIMIZED: ATR×1.0 SL, 1:2.5 RR, TP1 at 0.8R
- 50/50 partial close system
- Spread filter (skip when spread is too wide)
- Session scalping: London + NY only
- Asia range sweep entries
- Round number reaction scalps
- Momentum / exhaustion candle detection
- Mean Reversion: Bollinger Bands + RSI + Stochastic RSI
- Backtest: PF 1.31 | WR 55.8% | +54% return | DD 5.2%
"""

import os
import sys
import asyncio
import logging
import sqlite3
import time
import signal
from datetime import datetime, timedelta, timezone
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple
from collections import deque

try:
    from metaapi_cloud_sdk import MetaApi
except ImportError:
    print("pip install metaapi-cloud-sdk")
    sys.exit(1)

try:
    import aiohttp
except ImportError:
    print("pip install aiohttp")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════

@dataclass
class ScalpConfig:
    """Configuration tuned for XAUUSD scalping."""

    # ─── MetaAPI ──────────────────────────────────────────────────
    META_API_TOKEN: str = os.getenv("METAAPI_TOKEN", "")
    ACCOUNT_ID: str = os.getenv("ACCOUNT_ID", "")

    # ─── Telegram ─────────────────────────────────────────────────
    TELEGRAM_TOKEN: str = os.getenv("TG_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TG_CHAT", "")
    TELEGRAM_RATE_LIMIT: float = 1.5

    # ─── Symbol ───────────────────────────────────────────────────
    SYMBOL: str = "XAUUSD"
    POINT: float = 0.01

    # ─── Timeframes ───────────────────────────────────────────────
    TF_STRUCTURE: str = "5m"     # structure & context
    TF_ENTRY: str = "1m"         # precision entry
    CANDLE_LOOKBACK_5M: int = 60
    CANDLE_LOOKBACK_1M: int = 60

    # ─── Sessions / Killzones (UTC) ──────────────────────────────
    ASIA_START: int = 0
    ASIA_END: int = 7
    LONDON_START: int = 7
    LONDON_END: int = 12
    NY_START: int = 12
    NY_END: int = 17
    # London-NY overlap (highest volume for gold)
    OVERLAP_START: int = 12
    OVERLAP_END: int = 15

    # ─── Risk Management ─────────────────────────────────────────
    RISK_PERCENT: float = 0.5          # lower risk per scalp
    MAX_DAILY_LOSS_PERCENT: float = 3.0
    MAX_TOTAL_DRAWDOWN_PERCENT: float = 10.0  # optimized (was 6.0)
    MAX_CONCURRENT_TRADES: int = 2
    MAX_DAILY_TRADES: int = 20         # scalpers need more room
    MAX_CONSECUTIVE_LOSSES: int = 5    # optimized (was 4)

    # ─── Spread Filter ────────────────────────────────────────────
    MAX_SPREAD_POINTS: float = 3.5     # skip if spread > $3.50
    SPREAD_CHECK_ENABLED: bool = True

    # ─── Scalp SL/TP (OPTIMIZED) ────────────────────────────────
    ATR_PERIOD: int = 10               # shorter ATR for scalping
    ATR_SL_MULTIPLIER: float = 1.0     # optimized (was 0.8)
    MIN_SL_POINTS: float = 2.0         # optimized (was 1.5)
    MAX_SL_POINTS: float = 10.0        # optimized (was 6.0)
    DEFAULT_RR_RATIO: float = 2.5      # optimized (was 1.5)
    LONDON_RR_RATIO: float = 2.5       # optimized (was 1.5)
    NY_RR_RATIO: float = 2.5           # optimized (was 2.0)
    OVERLAP_RR_RATIO: float = 2.5      # optimized (was 1.8)

    # ─── Partial Close (OPTIMIZED) ───────────────────────────────
    PARTIAL_PERCENT: float = 0.50      # close 50% at TP1
    TP1_RR_RATIO: float = 0.8          # optimized (was 1.0) — earlier partial
    MOVE_SL_TO_BE: bool = True         # breakeven after TP1

    # ─── SMC Scalp Parameters ────────────────────────────────────
    SWING_LOOKBACK: int = 3            # faster swing detection
    OB_MAX_AGE_CANDLES: int = 20       # OBs expire faster on 1M
    FVG_MIN_SIZE_ATR: float = 0.2      # smaller FVGs valid on 1M
    ZONE_MAX_TESTS: int = 1            # 1 test = mitigated (scalp speed)

    # ─── Momentum / Candle Filters ────────────────────────────────
    ENGULF_BODY_RATIO: float = 0.60    # body must be 60% of candle
    MOMENTUM_CANDLE_ATR: float = 0.8   # momentum candle min size
    EXHAUSTION_WICK_RATIO: float = 0.65  # wick > 65% = exhaustion

    # ─── Round Numbers ────────────────────────────────────────────
    ROUND_NUMBER_INTERVAL: float = 50.0
    ROUND_NUMBER_ZONE: float = 3.0     # tighter zone for scalping

    # ─── EMA Filter ───────────────────────────────────────────────
    EMA_FAST: int = 9
    EMA_SLOW: int = 21
    USE_EMA_FILTER: bool = True        # only scalp with EMA trend

    # ─── Mean Reversion (Bollinger + RSI + StochRSI) ─────────────
    USE_MEAN_REVERSION: bool = True
    BB_PERIOD: int = 20                # Bollinger Band period
    BB_STD_DEV: float = 2.0            # standard deviations
    BB_SQUEEZE_THRESHOLD: float = 0.3  # band width / ATR below this = squeeze
    RSI_PERIOD: int = 7                # shorter RSI for scalping
    RSI_OVERSOLD: float = 25.0         # buy zone
    RSI_OVERBOUGHT: float = 75.0       # sell zone
    RSI_EXTREME_OVERSOLD: float = 15.0 # strong buy
    RSI_EXTREME_OVERBOUGHT: float = 85.0  # strong sell
    STOCH_RSI_PERIOD: int = 7          # StochRSI lookback
    STOCH_RSI_K: int = 3               # %K smoothing
    STOCH_RSI_OVERSOLD: float = 15.0
    STOCH_RSI_OVERBOUGHT: float = 85.0
    MR_REQUIRE_BB_TOUCH: bool = True   # price must touch/pierce BB
    MR_REQUIRE_RSI: bool = True        # RSI must confirm
    MR_CONFLUENCE_SCORE: int = 2       # score when MR triggers
    MR_RR_RATIO: float = 1.5           # tighter RR for mean reversion

    # ─── Confluence ───────────────────────────────────────────────
    MIN_CONFLUENCE: int = 3            # lower threshold = more trades

    # ─── Heartbeat ──────────────────────────────────────────────
    HEARTBEAT_INTERVAL: int = 600      # every 10 minutes (same as v4.0)

    # ─── Cycle Timing ─────────────────────────────────────────────
    MAIN_LOOP_SECONDS: int = 10        # fast 10-second cycle
    WATCHDOG_TIMEOUT: int = 600          # 10 min (was 180, too aggressive)

    # ─── Cooldown (OPTIMIZED) ────────────────────────────────────
    TRADE_COOLDOWN_SECONDS: int = 60   # optimized: 6 bars × 10s = 60s (was 120)
    LOSS_COOLDOWN_SECONDS: int = 150   # optimized: 15 bars × 10s = 150s (was 300)

    # ─── Database ─────────────────────────────────────────────────
    DB_PATH: str = "gold_scalper.db"


# ═══════════════════════════════════════════════════════════════════
#  ENUMS & DATA CLASSES
# ═══════════════════════════════════════════════════════════════════

class Direction(Enum):
    LONG = "buy"
    SHORT = "sell"

class TradePhase(Enum):
    OPEN = "open"
    TP1_HIT = "tp1_hit"
    CLOSED = "closed"

class Session(Enum):
    ASIA = "asia"
    LONDON = "london"
    NY_OVERLAP = "ny_overlap"
    NEW_YORK = "new_york"
    OFF = "off"

@dataclass
class SwingPoint:
    index: int
    price: float
    is_high: bool

@dataclass
class OrderBlock:
    high: float
    low: float
    direction: Direction
    candle_index: int
    tested: int = 0
    mitigated: bool = False

@dataclass
class FairValueGap:
    high: float
    low: float
    direction: Direction
    filled: bool = False

@dataclass
class ScalpTrade:
    id: str
    direction: Direction
    entry: float
    sl: float
    tp: float
    tp1: float
    lots: float
    phase: TradePhase = TradePhase.OPEN
    open_time: float = field(default_factory=time.time)
    pnl: float = 0.0


# ═══════════════════════════════════════════════════════════════════
#  BOT STATE
# ═══════════════════════════════════════════════════════════════════

class BotState:
    def __init__(self, cfg: ScalpConfig):
        self.cfg = cfg
        self.running = True
        self.heartbeat = time.time()

        # Candles
        self.candles_5m: List[dict] = []
        self.candles_1m: List[dict] = []

        # Session
        self.session: Session = Session.OFF
        self.asia_high: float = 0.0
        self.asia_low: float = 999999.0

        # Trades
        self.active_trades: Dict[str, ScalpTrade] = {}
        self.daily_trades: int = 0
        self.daily_pnl: float = 0.0
        self.daily_wins: int = 0
        self.daily_losses: int = 0
        self.consecutive_losses: int = 0
        self.last_trade_time: float = 0.0
        self.last_loss_time: float = 0.0
        self.trade_date: str = ""

        # Balance
        self.start_balance: float = 0.0
        self.balance: float = 0.0
        self.equity: float = 0.0

        # Trend
        self.trend_5m: Optional[Direction] = None
        self.ema_fast: float = 0.0
        self.ema_slow: float = 0.0

        # Telegram
        self.last_tg_time: float = 0.0
        self.last_heartbeat_time: float = 0.0


# ═══════════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════════

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("GoldScalper")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    fh = logging.FileHandler("gold_scalper.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

log = setup_logging()


# ═══════════════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════════════

class Database:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS scalp_trades (
                id TEXT PRIMARY KEY,
                direction TEXT,
                entry REAL, sl REAL, tp REAL,
                lots REAL, phase TEXT,
                open_time TEXT, close_time TEXT,
                pnl REAL DEFAULT 0.0,
                session TEXT
            );
            CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT PRIMARY KEY,
                trades INTEGER, wins INTEGER, losses INTEGER,
                pnl REAL, max_drawdown REAL, avg_rr REAL
            );
        """)
        self.conn.commit()

    def save_trade(self, t: ScalpTrade, session: str = ""):
        self.conn.execute("""
            INSERT OR REPLACE INTO scalp_trades
            (id, direction, entry, sl, tp, lots, phase, open_time, pnl, session)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (t.id, t.direction.value, t.entry, t.sl, t.tp,
              t.lots, t.phase.value,
              datetime.fromtimestamp(t.open_time, tz=timezone.utc).isoformat(),
              t.pnl, session))
        self.conn.commit()

    def save_daily(self, date: str, trades: int, wins: int,
                   losses: int, pnl: float, dd: float):
        self.conn.execute("""
            INSERT OR REPLACE INTO daily_stats
            (date, trades, wins, losses, pnl, max_drawdown)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (date, trades, wins, losses, pnl, dd))
        self.conn.commit()

    def close(self):
        self.conn.close()


# ═══════════════════════════════════════════════════════════════════
#  TELEGRAM
# ═══════════════════════════════════════════════════════════════════

class Telegram:
    def __init__(self, state: BotState):
        self.state = state
        self.cfg = state.cfg

    async def send(self, msg: str, silent: bool = False):
        if not self.cfg.TELEGRAM_TOKEN or not self.cfg.TELEGRAM_CHAT_ID:
            return
        now = time.time()
        wait = self.cfg.TELEGRAM_RATE_LIMIT - (now - self.state.last_tg_time)
        if wait > 0:
            await asyncio.sleep(wait)

        url = f"https://api.telegram.org/bot{self.cfg.TELEGRAM_TOKEN}/sendMessage"
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(url, json={
                    "chat_id": self.cfg.TELEGRAM_CHAT_ID,
                    "text": msg, "parse_mode": "HTML",
                    "disable_notification": silent
                }, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    self.state.last_tg_time = time.time()
        except Exception as e:
            log.warning(f"TG error: {e}")

    async def scalp_opened(self, t: ScalpTrade, session: str, confluence: int):
        e = "🟢" if t.direction == Direction.LONG else "🔴"
        sl_dist = abs(t.entry - t.sl)
        tp_dist = abs(t.tp - t.entry)
        rr = tp_dist / sl_dist if sl_dist > 0 else 0
        await self.send(
            f"{e} <b>SCALP {t.direction.value.upper()}</b>\n"
            f"Entry: ${t.entry:.2f}\n"
            f"SL: ${t.sl:.2f} (${sl_dist:.2f})\n"
            f"TP1: ${t.tp1:.2f} | TP2: ${t.tp:.2f}\n"
            f"Lots: {t.lots} | RR: 1:{rr:.1f}\n"
            f"Session: {session} | Score: {confluence}\n"
            f"Trade #{self.state.daily_trades}/{self.cfg.MAX_DAILY_TRADES}"
        )

    async def scalp_partial(self, t: ScalpTrade):
        await self.send(f"✂️ <b>50% CLOSED</b> @ ${t.tp1:.2f} → SL to BE", silent=True)

    async def scalp_closed(self, t: ScalpTrade):
        e = "✅" if t.pnl > 0 else "❌"
        streak = f"🔥 {self.state.daily_wins}W" if t.pnl > 0 else f"💀 {self.state.consecutive_losses}L streak"
        await self.send(
            f"{e} <b>CLOSED</b> ${t.pnl:+.2f}\n"
            f"Daily: ${self.state.daily_pnl:+.2f} | {streak}\n"
            f"W/L: {self.state.daily_wins}/{self.state.daily_losses}"
        )

    async def daily_report(self):
        wr = self.state.daily_wins / max(self.state.daily_trades, 1) * 100
        await self.send(
            f"📊 <b>DAILY REPORT</b>\n"
            f"Trades: {self.state.daily_trades}\n"
            f"Wins: {self.state.daily_wins} | Losses: {self.state.daily_losses}\n"
            f"PnL: ${self.state.daily_pnl:+.2f}\n"
            f"Win Rate: {wr:.0f}%\n"
            f"Balance: ${self.state.balance:.2f}"
        )

    async def heartbeat(self, price: float, spread: float, positions: int):
        now = time.time()
        if now - self.state.last_heartbeat_time < self.state.cfg.HEARTBEAT_INTERVAL:
            return
        self.state.last_heartbeat_time = now

        s = self.state
        session = s.session.value.upper()
        tradeable = "✅" if s.session in (Session.LONDON, Session.NY_OVERLAP, Session.NEW_YORK) else "❌"
        wr = s.daily_wins / max(s.daily_trades, 1) * 100
        streak = f"{s.consecutive_losses}L" if s.consecutive_losses > 0 else f"0L"
        candles_ok = "✅" if len(s.candles_1m) > 10 and len(s.candles_5m) > 10 else "⚠️"

        # Daily P&L
        pl = s.equity - s.start_balance if s.start_balance > 0 else 0
        pl_pct = (pl / s.start_balance * 100) if s.start_balance > 0 else 0

        # Drawdown
        dd = 0.0
        if s.start_balance > 0 and s.balance < s.start_balance:
            dd = (s.start_balance - s.balance) / s.start_balance * 100

        utc_time = datetime.now(timezone.utc).strftime('%H:%M:%S')

        msg = (
            f"💓 <b>GOLD SCALPER HEARTBEAT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Balance: ${s.balance:,.2f}\n"
            f"📊 Equity: ${s.equity:,.2f}\n"
            f"📈 P&L: ${pl:+,.2f} ({pl_pct:+.2f}%)\n"
            f"📉 Drawdown: {dd:.2f}%\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🥇 XAUUSD: ${price:.2f}\n"
            f"📏 Spread: ${spread:.2f}\n"
            f"🕐 Session: {session} {tradeable}\n"
            f"🎯 Open trades: {positions}/{s.cfg.MAX_CONCURRENT_TRADES}\n"
            f"📅 Trades today: {s.daily_trades}/{s.cfg.MAX_DAILY_TRADES}\n"
            f"💵 Daily PnL: ${s.daily_pnl:+.2f}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📈 W/L: {s.daily_wins}/{s.daily_losses}\n"
            f"🎯 Win Rate: {wr:.0f}%\n"
            f"🔥 Streak: {s.consecutive_losses}L consecutive\n"
            f"📊 EMA 9/21: {s.ema_fast:.2f} / {s.ema_slow:.2f}\n"
            f"📡 Data: {candles_ok} 5M:{len(s.candles_5m)}c | 1M:{len(s.candles_1m)}c\n"
            f"🌏 Asia: ${s.asia_high:.2f} / ${s.asia_low:.2f}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏰ {utc_time} UTC"
        )
        await self.send(msg, silent=True)


# ═══════════════════════════════════════════════════════════════════
#  SESSION MANAGER
# ═══════════════════════════════════════════════════════════════════

class SessionMgr:
    def __init__(self, cfg: ScalpConfig):
        self.cfg = cfg

    def get(self, hour: int) -> Session:
        if self.cfg.OVERLAP_START <= hour < self.cfg.OVERLAP_END:
            return Session.NY_OVERLAP
        if self.cfg.LONDON_START <= hour < self.cfg.LONDON_END:
            return Session.LONDON
        if self.cfg.NY_START <= hour < self.cfg.NY_END:
            return Session.NEW_YORK
        if self.cfg.ASIA_START <= hour < self.cfg.ASIA_END:
            return Session.ASIA
        return Session.OFF

    def is_tradeable(self, hour: int) -> bool:
        return self.get(hour) in (Session.LONDON, Session.NY_OVERLAP, Session.NEW_YORK)

    def get_rr(self, session: Session, cfg: ScalpConfig) -> float:
        if session == Session.NY_OVERLAP:
            return cfg.OVERLAP_RR_RATIO
        if session == Session.NEW_YORK:
            return cfg.NY_RR_RATIO
        if session == Session.LONDON:
            return cfg.LONDON_RR_RATIO
        return cfg.DEFAULT_RR_RATIO

    def calc_asia_range(self, candles_5m: List[dict]) -> Tuple[float, float]:
        asia = []
        for c in candles_5m:
            dt = self._parse_time(c)
            if dt and self.cfg.ASIA_START <= dt.hour < self.cfg.ASIA_END:
                asia.append(c)
        if not asia:
            return 0.0, 999999.0
        return (
            max(c.get("high", 0) for c in asia),
            min(c.get("low", 999999) for c in asia)
        )

    def _parse_time(self, candle: dict) -> Optional[datetime]:
        ts = candle.get("time", "")
        if isinstance(ts, datetime):
            return ts
        try:
            return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None


# ═══════════════════════════════════════════════════════════════════
#  SCALP ANALYSIS ENGINE
# ═══════════════════════════════════════════════════════════════════

class ScalpAnalyzer:
    """Lightweight SMC + price action analysis for scalping."""

    def __init__(self, cfg: ScalpConfig):
        self.cfg = cfg

    # ─── ATR ──────────────────────────────────────────────────────
    def atr(self, candles: List[dict], period: int = 10) -> float:
        if len(candles) < period + 1:
            return 5.0
        trs = []
        for i in range(1, len(candles)):
            h, l = candles[i].get("high", 0), candles[i].get("low", 0)
            pc = candles[i - 1].get("close", 0)
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        return sum(trs[-period:]) / period

    # ─── EMA ──────────────────────────────────────────────────────
    def ema(self, candles: List[dict], period: int) -> float:
        if len(candles) < period:
            return 0.0
        closes = [c.get("close", 0) for c in candles]
        multiplier = 2 / (period + 1)
        ema_val = sum(closes[:period]) / period
        for price in closes[period:]:
            ema_val = (price - ema_val) * multiplier + ema_val
        return ema_val

    # ─── Swing Detection (fast) ───────────────────────────────────
    def swings(self, candles: List[dict]) -> Tuple[List[SwingPoint], List[SwingPoint]]:
        highs, lows = [], []
        lb = self.cfg.SWING_LOOKBACK
        for i in range(lb, len(candles) - lb):
            h = candles[i].get("high", 0)
            l = candles[i].get("low", 0)
            if all(h >= candles[j].get("high", 0) for j in range(i - lb, i + lb + 1) if j != i):
                highs.append(SwingPoint(i, h, True))
            if all(l <= candles[j].get("low", 999999) for j in range(i - lb, i + lb + 1) if j != i):
                lows.append(SwingPoint(i, l, False))
        return highs, lows

    # ─── Order Blocks (1M) ───────────────────────────────────────
    def order_blocks(self, candles: List[dict]) -> List[OrderBlock]:
        obs = []
        for i in range(2, len(candles)):
            c = candles[i]
            p = candles[i - 1]
            co, cc = c.get("open", 0), c.get("close", 0)
            po, pc = p.get("open", 0), p.get("close", 0)
            ph, pl = p.get("high", 0), p.get("low", 0)

            # Bullish OB
            if pc < po and cc > co and cc > ph:
                obs.append(OrderBlock(po, pl, Direction.LONG, i))
            # Bearish OB
            if pc > po and cc < co and cc < pl:
                obs.append(OrderBlock(ph, po, Direction.SHORT, i))

        # Keep recent only
        cutoff = len(candles) - self.cfg.OB_MAX_AGE_CANDLES
        return [ob for ob in obs if ob.candle_index >= cutoff]

    # ─── FVGs (1M) ────────────────────────────────────────────────
    def fvgs(self, candles: List[dict], atr_val: float) -> List[FairValueGap]:
        gaps = []
        min_size = atr_val * self.cfg.FVG_MIN_SIZE_ATR
        for i in range(2, len(candles)):
            c1h = candles[i - 2].get("high", 0)
            c3l = candles[i].get("low", 0)
            c1l = candles[i - 2].get("low", 0)
            c3h = candles[i].get("high", 0)

            if c3l > c1h and (c3l - c1h) >= min_size:
                gaps.append(FairValueGap(c3l, c1h, Direction.LONG))
            if c1l > c3h and (c1l - c3h) >= min_size:
                gaps.append(FairValueGap(c1l, c3h, Direction.SHORT))
        return gaps[-10:]  # keep last 10

    # ─── Liquidity Sweep ──────────────────────────────────────────
    def liquidity_sweep(self, candles: List[dict],
                        swing_highs: List[SwingPoint],
                        swing_lows: List[SwingPoint]) -> Optional[Direction]:
        if len(candles) < 2:
            return None
        last = candles[-1]
        lh, ll, lc = last.get("high", 0), last.get("low", 0), last.get("close", 0)

        for sl in swing_lows[-3:]:
            if ll < sl.price and lc > sl.price:
                return Direction.LONG
        for sh in swing_highs[-3:]:
            if lh > sh.price and lc < sh.price:
                return Direction.SHORT
        return None

    # ─── Momentum Candle ──────────────────────────────────────────
    def is_momentum_candle(self, candle: dict, atr_val: float) -> Optional[Direction]:
        o, c = candle.get("open", 0), candle.get("close", 0)
        h, l = candle.get("high", 0), candle.get("low", 0)
        body = abs(c - o)
        total = h - l
        if total <= 0:
            return None

        if body / total >= self.cfg.ENGULF_BODY_RATIO and body >= atr_val * self.cfg.MOMENTUM_CANDLE_ATR:
            return Direction.LONG if c > o else Direction.SHORT
        return None

    # ─── Exhaustion Candle (reversal signal) ──────────────────────
    def is_exhaustion_candle(self, candle: dict) -> Optional[Direction]:
        o, c = candle.get("open", 0), candle.get("close", 0)
        h, l = candle.get("high", 0), candle.get("low", 0)
        total = h - l
        if total <= 0:
            return None

        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l

        if upper_wick / total >= self.cfg.EXHAUSTION_WICK_RATIO:
            return Direction.SHORT  # bearish exhaustion
        if lower_wick / total >= self.cfg.EXHAUSTION_WICK_RATIO:
            return Direction.LONG   # bullish exhaustion
        return None

    # ─── Round Number ─────────────────────────────────────────────
    def near_round_number(self, price: float) -> bool:
        interval = self.cfg.ROUND_NUMBER_INTERVAL
        nearest = round(price / interval) * interval
        return abs(price - nearest) <= self.cfg.ROUND_NUMBER_ZONE

    # ─── Asia Sweep ───────────────────────────────────────────────
    def asia_sweep(self, price: float, asia_high: float,
                   asia_low: float, candle: dict) -> Optional[Direction]:
        if asia_high <= 0 or asia_low >= 999999:
            return None
        lh = candle.get("high", 0)
        ll = candle.get("low", 0)
        lc = candle.get("close", 0)

        # Swept Asia low → bullish
        if ll < asia_low and lc > asia_low:
            return Direction.LONG
        # Swept Asia high → bearish
        if lh > asia_high and lc < asia_high:
            return Direction.SHORT
        return None

    # ─── Bollinger Bands ──────────────────────────────────────────
    def bollinger_bands(self, candles: List[dict], period: int = 20,
                        std_dev: float = 2.0) -> Tuple[float, float, float]:
        """Returns (upper, middle, lower) band values."""
        if len(candles) < period:
            return 0.0, 0.0, 0.0
        closes = [c.get("close", 0) for c in candles[-period:]]
        middle = sum(closes) / period
        variance = sum((x - middle) ** 2 for x in closes) / period
        std = variance ** 0.5
        return middle + std_dev * std, middle, middle - std_dev * std

    def bb_width(self, upper: float, lower: float, middle: float) -> float:
        """Normalized BB width (0-1 range). Low = squeeze."""
        if middle <= 0:
            return 0.0
        return (upper - lower) / middle

    def bb_percent_b(self, price: float, upper: float, lower: float) -> float:
        """Where price sits in BB (0 = lower, 1 = upper, >1 = above upper)."""
        band_range = upper - lower
        if band_range <= 0:
            return 0.5
        return (price - lower) / band_range

    # ─── RSI ──────────────────────────────────────────────────────
    def rsi(self, candles: List[dict], period: int = 7) -> float:
        if len(candles) < period + 1:
            return 50.0
        closes = [c.get("close", 0) for c in candles]
        gains, losses = [], []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i - 1]
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))

        if len(gains) < period:
            return 50.0

        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period

        # Wilder's smoothing for remaining
        for i in range(len(gains) - period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    # ─── Stochastic RSI ──────────────────────────────────────────
    def stoch_rsi(self, candles: List[dict], rsi_period: int = 7,
                  stoch_period: int = 7, k_smooth: int = 3) -> float:
        """Returns %K value of StochRSI (0-100)."""
        if len(candles) < rsi_period + stoch_period + k_smooth:
            return 50.0

        # Calculate RSI series
        closes = [c.get("close", 0) for c in candles]
        rsi_values = []
        for i in range(rsi_period + 1, len(closes) + 1):
            subset_candles = [{"close": closes[j]} for j in range(i - rsi_period - 1, i)]
            rsi_values.append(self.rsi(subset_candles, rsi_period))

        if len(rsi_values) < stoch_period:
            return 50.0

        # Stochastic of RSI
        recent_rsi = rsi_values[-stoch_period:]
        rsi_high = max(recent_rsi)
        rsi_low = min(recent_rsi)

        if rsi_high == rsi_low:
            return 50.0

        raw_k = ((rsi_values[-1] - rsi_low) / (rsi_high - rsi_low)) * 100

        # Simple smoothing for %K
        if len(rsi_values) >= k_smooth:
            k_values = []
            for j in range(k_smooth):
                idx = len(rsi_values) - k_smooth + j
                window = rsi_values[max(0, idx - stoch_period + 1):idx + 1]
                rh, rl = max(window), min(window)
                if rh == rl:
                    k_values.append(50.0)
                else:
                    k_values.append(((rsi_values[idx] - rl) / (rh - rl)) * 100)
            return sum(k_values) / len(k_values)

        return raw_k

    # ─── Mean Reversion Signal ────────────────────────────────────
    def mean_reversion(self, candles: List[dict], price: float,
                       cfg: 'ScalpConfig') -> Optional[Tuple[Direction, int, str]]:
        """
        Returns (direction, score, reason) or None.
        Combines Bollinger Bands + RSI + StochRSI.
        """
        if not cfg.USE_MEAN_REVERSION:
            return None

        upper, middle, lower = self.bollinger_bands(
            candles, cfg.BB_PERIOD, cfg.BB_STD_DEV
        )
        if middle <= 0:
            return None

        rsi_val = self.rsi(candles, cfg.RSI_PERIOD)
        stoch_val = self.stoch_rsi(candles, cfg.RSI_PERIOD,
                                    cfg.STOCH_RSI_PERIOD, cfg.STOCH_RSI_K)
        pct_b = self.bb_percent_b(price, upper, lower)

        score = 0
        reasons = []
        direction = None

        # ─── Oversold (Bullish mean reversion) ────────────────────
        if pct_b <= 0.05:  # price at or below lower BB
            score += 1
            reasons.append("bb_lower_pierce")
            direction = Direction.LONG
        elif pct_b <= 0.15:  # price near lower BB
            if cfg.MR_REQUIRE_BB_TOUCH:
                pass  # not close enough
            else:
                score += 1
                reasons.append("bb_lower_near")
                direction = Direction.LONG

        if direction == Direction.LONG:
            if rsi_val <= cfg.RSI_EXTREME_OVERSOLD:
                score += 2
                reasons.append(f"rsi_extreme_{rsi_val:.0f}")
            elif rsi_val <= cfg.RSI_OVERSOLD:
                score += 1
                reasons.append(f"rsi_oversold_{rsi_val:.0f}")
            else:
                if cfg.MR_REQUIRE_RSI:
                    return None  # RSI doesn't confirm

            if stoch_val <= cfg.STOCH_RSI_OVERSOLD:
                score += 1
                reasons.append(f"stochrsi_{stoch_val:.0f}")

        # ─── Overbought (Bearish mean reversion) ─────────────────
        if pct_b >= 0.95:  # price at or above upper BB
            score += 1
            reasons.append("bb_upper_pierce")
            direction = Direction.SHORT
        elif pct_b >= 0.85:  # price near upper BB
            if cfg.MR_REQUIRE_BB_TOUCH:
                pass
            else:
                score += 1
                reasons.append("bb_upper_near")
                direction = Direction.SHORT

        if direction == Direction.SHORT:
            if rsi_val >= cfg.RSI_EXTREME_OVERBOUGHT:
                score += 2
                reasons.append(f"rsi_extreme_{rsi_val:.0f}")
            elif rsi_val >= cfg.RSI_OVERBOUGHT:
                score += 1
                reasons.append(f"rsi_overbought_{rsi_val:.0f}")
            else:
                if cfg.MR_REQUIRE_RSI:
                    return None

            if stoch_val >= cfg.STOCH_RSI_OVERBOUGHT:
                score += 1
                reasons.append(f"stochrsi_{stoch_val:.0f}")

        if direction is None or score < 2:
            return None

        reason_str = "MR:" + "+".join(reasons)
        return direction, score, reason_str


# ═══════════════════════════════════════════════════════════════════
#  SIGNAL GENERATOR
# ═══════════════════════════════════════════════════════════════════

class ScalpSignal:
    def __init__(self, state: BotState, analyzer: ScalpAnalyzer, session_mgr: SessionMgr):
        self.state = state
        self.az = analyzer
        self.sm = session_mgr
        self.cfg = state.cfg

    def evaluate(self, price: float, spread: float) -> Optional[Tuple[Direction, float, float, float, int, str]]:
        """
        Returns: (direction, sl, tp1, tp, confluence_score, reason) or None.
        """
        now = datetime.now(timezone.utc)
        hour = now.hour

        # ─── Gate 1: Session ─────────────────────────────────────
        if not self.sm.is_tradeable(hour):
            return None

        # ─── Gate 2: Spread ──────────────────────────────────────
        if self.cfg.SPREAD_CHECK_ENABLED and spread > self.cfg.MAX_SPREAD_POINTS:
            return None

        # ─── Gate 3: Daily limits ────────────────────────────────
        if self.state.daily_trades >= self.cfg.MAX_DAILY_TRADES:
            return None
        if len(self.state.active_trades) >= self.cfg.MAX_CONCURRENT_TRADES:
            return None

        # ─── Gate 4: Daily loss ──────────────────────────────────
        if self.state.start_balance > 0:
            max_loss = self.state.start_balance * (self.cfg.MAX_DAILY_LOSS_PERCENT / 100)
            if self.state.daily_pnl <= -max_loss:
                return None

        # ─── Gate 5: Drawdown ────────────────────────────────────
        if self.state.start_balance > 0 and self.state.balance > 0:
            dd = (self.state.start_balance - self.state.balance) / self.state.start_balance * 100
            if dd >= self.cfg.MAX_TOTAL_DRAWDOWN_PERCENT:
                return None

        # ─── Gate 6: Consecutive losses ──────────────────────────
        if self.state.consecutive_losses >= self.cfg.MAX_CONSECUTIVE_LOSSES:
            cooldown = self.cfg.LOSS_COOLDOWN_SECONDS * 2  # double cooldown
            if time.time() - self.state.last_loss_time < cooldown:
                return None
            self.state.consecutive_losses = 0  # reset after cooldown

        # ─── Gate 7: Trade cooldown ──────────────────────────────
        if time.time() - self.state.last_trade_time < self.cfg.TRADE_COOLDOWN_SECONDS:
            return None

        # ─── Gate 8: Loss cooldown ───────────────────────────────
        if time.time() - self.state.last_loss_time < self.cfg.LOSS_COOLDOWN_SECONDS:
            return None

        # ─── Data check ──────────────────────────────────────────
        if not self.state.candles_1m or not self.state.candles_5m:
            return None

        # ─── Compute indicators ──────────────────────────────────
        atr_1m = self.az.atr(self.state.candles_1m, self.cfg.ATR_PERIOD)
        atr_5m = self.az.atr(self.state.candles_5m, self.cfg.ATR_PERIOD)

        ema_f = self.az.ema(self.state.candles_5m, self.cfg.EMA_FAST)
        ema_s = self.az.ema(self.state.candles_5m, self.cfg.EMA_SLOW)
        self.state.ema_fast = ema_f
        self.state.ema_slow = ema_s

        # 5M trend from EMA
        if self.cfg.USE_EMA_FILTER and ema_f > 0 and ema_s > 0:
            self.state.trend_5m = Direction.LONG if ema_f > ema_s else Direction.SHORT

        # Swings on 1M
        highs_1m, lows_1m = self.az.swings(self.state.candles_1m)

        # OBs and FVGs on 1M
        obs_1m = self.az.order_blocks(self.state.candles_1m)
        fvgs_1m = self.az.fvgs(self.state.candles_1m, atr_1m)

        last_candle = self.state.candles_1m[-1]

        # ─── Build confluence ────────────────────────────────────
        confluence = 0
        reasons = []
        direction_votes: Dict[Direction, int] = {Direction.LONG: 0, Direction.SHORT: 0}

        # 1. EMA trend alignment
        if self.state.trend_5m:
            direction_votes[self.state.trend_5m] += 1
            reasons.append(f"ema_{self.state.trend_5m.value}")

        # 2. Liquidity sweep on 1M
        sweep = self.az.liquidity_sweep(self.state.candles_1m, highs_1m, lows_1m)
        if sweep:
            direction_votes[sweep] += 2
            reasons.append("liq_sweep")

        # 3. Asia range sweep (during London)
        session = self.sm.get(hour)
        if session == Session.LONDON:
            asia_sweep = self.az.asia_sweep(
                price, self.state.asia_high, self.state.asia_low, last_candle
            )
            if asia_sweep:
                direction_votes[asia_sweep] += 2
                reasons.append("asia_sweep")

        # 4. Order block
        for ob in obs_1m:
            if ob.mitigated:
                continue
            if ob.direction == Direction.LONG and ob.low <= price <= ob.high:
                direction_votes[Direction.LONG] += 1
                reasons.append("bull_ob")
                break
            elif ob.direction == Direction.SHORT and ob.low <= price <= ob.high:
                direction_votes[Direction.SHORT] += 1
                reasons.append("bear_ob")
                break

        # 5. FVG
        for fvg in fvgs_1m:
            if fvg.filled:
                continue
            if fvg.direction == Direction.LONG and fvg.low <= price <= fvg.high:
                direction_votes[Direction.LONG] += 1
                reasons.append("fvg")
                break
            elif fvg.direction == Direction.SHORT and fvg.low <= price <= fvg.high:
                direction_votes[Direction.SHORT] += 1
                reasons.append("fvg")
                break

        # 6. Momentum candle
        momentum = self.az.is_momentum_candle(last_candle, atr_1m)
        if momentum:
            direction_votes[momentum] += 1
            reasons.append("momentum")

        # 7. Exhaustion candle (reversal)
        exhaustion = self.az.is_exhaustion_candle(last_candle)
        if exhaustion:
            direction_votes[exhaustion] += 1
            reasons.append("exhaustion")

        # 8. Round number reaction
        if self.az.near_round_number(price):
            # round number adds to confluence but doesn't set direction
            confluence += 1
            reasons.append("round_num")

        # 9. Mean Reversion (Bollinger + RSI + StochRSI)
        mr_signal = self.az.mean_reversion(self.state.candles_1m, price, self.cfg)
        if mr_signal:
            mr_dir, mr_score, mr_reason = mr_signal
            direction_votes[mr_dir] += self.cfg.MR_CONFLUENCE_SCORE
            reasons.append(mr_reason)

        # ─── Determine direction ─────────────────────────────────
        long_score = direction_votes[Direction.LONG]
        short_score = direction_votes[Direction.SHORT]

        if long_score > short_score and long_score >= 1:
            direction = Direction.LONG
            confluence += long_score
        elif short_score > long_score and short_score >= 1:
            direction = Direction.SHORT
            confluence += short_score
        else:
            return None  # no clear direction

        # ─── EMA filter: trade must align with 5M trend ──────────
        # Exception: mean reversion signals can go counter-trend
        if self.cfg.USE_EMA_FILTER and self.state.trend_5m:
            if direction != self.state.trend_5m:
                # Allow if strong mean reversion signal is present
                if not (mr_signal and mr_signal[1] >= 3):
                    return None

        # ─── Minimum confluence ───────────────────────────────────
        if confluence < self.cfg.MIN_CONFLUENCE:
            return None

        # ─── Calculate SL/TP ──────────────────────────────────────
        sl_dist = max(atr_1m * self.cfg.ATR_SL_MULTIPLIER, self.cfg.MIN_SL_POINTS)
        sl_dist = min(sl_dist, self.cfg.MAX_SL_POINTS)

        rr = self.sm.get_rr(session, self.cfg)
        tp_dist = sl_dist * rr
        tp1_dist = sl_dist * self.cfg.TP1_RR_RATIO

        if direction == Direction.LONG:
            sl = price - sl_dist
            tp = price + tp_dist
            tp1 = price + tp1_dist
        else:
            sl = price + sl_dist
            tp = price - tp_dist
            tp1 = price - tp1_dist

        reason_str = " | ".join(reasons)
        log.info(f"⚡ SIGNAL: {direction.value.upper()} | Score: {confluence} | {reason_str}")

        return direction, sl, tp1, tp, confluence, reason_str


# ═══════════════════════════════════════════════════════════════════
#  POSITION MANAGER
# ═══════════════════════════════════════════════════════════════════

class PositionMgr:
    def __init__(self, state: BotState, conn, db: Database, tg: Telegram):
        self.state = state
        self.conn = conn
        self.db = db
        self.tg = tg
        self.cfg = state.cfg

    def calc_lots(self, sl_dist: float) -> float:
        risk = self.state.balance * (self.cfg.RISK_PERCENT / 100)
        per_lot = 100.0  # $1 move = $100 per lot on XAUUSD
        if sl_dist <= 0:
            return 0.01
        lots = risk / (sl_dist * per_lot)
        return max(0.01, min(round(lots, 2), 0.5))  # cap at 0.5 for scalping

    async def open_scalp(self, direction: Direction, price: float,
                         sl: float, tp1: float, tp: float,
                         confluence: int, reason: str):
        sl_dist = abs(price - sl)
        lots = self.calc_lots(sl_dist)
        session = self.state.session.value

        try:
            if direction == Direction.LONG:
                result = await self.conn.create_market_buy_order(
                    self.cfg.SYMBOL, lots, sl, tp,
                    options={"comment": f"Scalp|{reason[:15]}"}
                )
            else:
                result = await self.conn.create_market_sell_order(
                    self.cfg.SYMBOL, lots, sl, tp,
                    options={"comment": f"Scalp|{reason[:15]}"}
                )

            oid = result.get("orderId", result.get("positionId", f"s_{int(time.time())}"))

            trade = ScalpTrade(
                id=oid, direction=direction,
                entry=price, sl=sl, tp=tp, tp1=tp1,
                lots=lots
            )

            self.state.active_trades[oid] = trade
            self.state.daily_trades += 1
            self.state.last_trade_time = time.time()
            self.db.save_trade(trade, session)

            log.info(f"SCALP OPENED: {direction.value} {lots}L @ ${price:.2f} | SL ${sl:.2f} | TP ${tp:.2f}")
            await self.tg.scalp_opened(trade, session, confluence)

        except Exception as e:
            log.error(f"Open error: {e}")

    async def manage_partials(self, price: float):
        for tid, t in list(self.state.active_trades.items()):
            if t.phase != TradePhase.OPEN:
                continue

            hit = (
                (t.direction == Direction.LONG and price >= t.tp1) or
                (t.direction == Direction.SHORT and price <= t.tp1)
            )
            if not hit:
                continue

            close_lots = round(t.lots * self.cfg.PARTIAL_PERCENT, 2)
            if close_lots < 0.01:
                continue

            try:
                # Close 50% of position (same method as v4.0 bot)
                await asyncio.wait_for(
                    self.conn.close_position_partially(tid, close_lots),
                    timeout=10
                )

                t.phase = TradePhase.TP1_HIT
                log.info(f"TP1 HIT: {close_lots}L closed, SL → BE")

                # Move SL to breakeven
                if self.cfg.MOVE_SL_TO_BE:
                    await self.conn.modify_position(tid, stop_loss=t.entry, take_profit=t.tp)
                    t.sl = t.entry

                self.db.save_trade(t)
                await self.tg.scalp_partial(t)

            except Exception as e:
                log.error(f"Partial error: {e}")

    async def sync_positions(self):
        try:
            positions = await self.conn.get_positions()
            open_ids = {p.get("id") for p in positions if p.get("symbol") == self.cfg.SYMBOL}

            for tid, t in list(self.state.active_trades.items()):
                if tid not in open_ids and t.phase != TradePhase.CLOSED:
                    t.phase = TradePhase.CLOSED

                    # Calculate PnL from price difference
                    try:
                        tick = await self.conn.get_symbol_price(self.cfg.SYMBOL)
                        close_price = tick.get("bid", 0) if t.direction == Direction.LONG else tick.get("ask", 0)
                        if close_price > 0:
                            if t.direction == Direction.LONG:
                                t.pnl = (close_price - t.entry) * t.lots * 100  # 100 oz per lot
                            else:
                                t.pnl = (t.entry - close_price) * t.lots * 100
                    except Exception:
                        pass

                    # Fallback: try deals history
                    if t.pnl == 0:
                        try:
                            now = datetime.now(timezone.utc).replace(tzinfo=None)
                            start = now - timedelta(hours=1)
                            history = await asyncio.wait_for(
                                self.conn.get_deals_by_time_range(start, now),
                                timeout=10
                            )
                            if history:
                                for deal in history:
                                    if deal.get("positionId") == tid and deal.get("profit", 0) != 0:
                                        t.pnl = deal.get("profit", 0) + deal.get("swap", 0) + deal.get("commission", 0)
                                        break
                        except Exception:
                            pass

                    # Update state
                    self.state.daily_pnl += t.pnl
                    if t.pnl > 0:
                        self.state.daily_wins += 1
                        self.state.consecutive_losses = 0
                    else:
                        self.state.daily_losses += 1
                        self.state.consecutive_losses += 1
                        self.state.last_loss_time = time.time()

                    self.db.save_trade(t)
                    del self.state.active_trades[tid]

                    log.info(f"CLOSED: {tid} | PnL: ${t.pnl:+.2f} | Daily: ${self.state.daily_pnl:+.2f}")
                    await self.tg.scalp_closed(t)

        except Exception as e:
            log.error(f"Sync error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  MAIN BOT
# ═══════════════════════════════════════════════════════════════════

class GoldScalper:
    def __init__(self):
        self.cfg = ScalpConfig()
        self.state = BotState(self.cfg)
        self.az = ScalpAnalyzer(self.cfg)
        self.sm = SessionMgr(self.cfg)
        self.db = Database(self.cfg.DB_PATH)
        self.tg = Telegram(self.state)
        self.sig = ScalpSignal(self.state, self.az, self.sm)
        self.pos: Optional[PositionMgr] = None
        self.api = None
        self.account = None
        self.conn = None

    async def connect(self):
        log.info("Connecting to MetaAPI...")
        if not self.cfg.META_API_TOKEN or not self.cfg.ACCOUNT_ID:
            log.error("Set METAAPI_TOKEN and ACCOUNT_ID!")
            sys.exit(1)

        self.api = MetaApi(self.cfg.META_API_TOKEN)
        self.account = await self.api.metatrader_account_api.get_account(self.cfg.ACCOUNT_ID)

        if self.account.state != "DEPLOYED":
            await self.account.deploy()

        await self.account.wait_connected()
        self.conn = self.account.get_rpc_connection()
        await self.conn.connect()
        await self.conn.wait_synchronized()

        info = await self.conn.get_account_information()
        self.state.start_balance = info.get("balance", 0)
        self.state.balance = self.state.start_balance
        self.state.trade_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        self.pos = PositionMgr(self.state, self.conn, self.db, self.tg)

        log.info(f"Connected! Balance: ${self.state.start_balance:.2f}")
        await self.tg.send(
            f"🤖 <b>Gold Scalper v1.2 OPTIMIZED</b>\n"
            f"Balance: ${self.state.start_balance:.2f}\n"
            f"Mode: Scalping 1M/5M\n"
            f"SL: ATR×1.0 | RR: 1:2.5 | TP1: 0.8R\n"
            f"Backtest: PF 1.31 | WR 55.8%\n"
            f"Max trades/day: {self.cfg.MAX_DAILY_TRADES}\n"
            f"Risk: {self.cfg.RISK_PERCENT}%"
        )

    async def fetch_data(self):
        try:
            now = datetime.now(timezone.utc)
            start_5m = now - timedelta(minutes=5 * self.cfg.CANDLE_LOOKBACK_5M * 2)
            start_1m = now - timedelta(minutes=1 * self.cfg.CANDLE_LOOKBACK_1M * 2)

            candles_5m = await asyncio.wait_for(
                self.account.get_historical_candles(
                    self.cfg.SYMBOL, self.cfg.TF_STRUCTURE, start_5m
                ), timeout=20
            )
            if candles_5m and len(candles_5m) >= 10:
                self.state.candles_5m = candles_5m
            else:
                log.warning(f"5M candles insufficient: {len(candles_5m) if candles_5m else 0}")

            candles_1m = await asyncio.wait_for(
                self.account.get_historical_candles(
                    self.cfg.SYMBOL, self.cfg.TF_ENTRY, start_1m
                ), timeout=20
            )
            if candles_1m and len(candles_1m) >= 10:
                self.state.candles_1m = candles_1m
            else:
                log.warning(f"1M candles insufficient: {len(candles_1m) if candles_1m else 0}")

            ah, al = self.sm.calc_asia_range(self.state.candles_5m)
            if ah > 0:
                self.state.asia_high = ah
                self.state.asia_low = al

            log.info(f"Data: 5M={len(self.state.candles_5m)}c | 1M={len(self.state.candles_1m)}c | Session={self.state.session.value}")
        except asyncio.TimeoutError:
            log.warning("Candle fetch timeout")
        except Exception as e:
            log.error(f"Data fetch error: {e}")

    async def get_price_and_spread(self) -> Tuple[float, float]:
        try:
            tick = await self.conn.get_symbol_price(self.cfg.SYMBOL)
            bid = tick.get("bid", 0.0)
            ask = tick.get("ask", 0.0)
            spread = ask - bid if ask > 0 and bid > 0 else 0.0
            return bid, spread
        except Exception as e:
            log.error(f"Price error: {e}")
            return 0.0, 999.0

    async def daily_reset(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self.state.trade_date:
            # Send daily report before reset
            if self.state.daily_trades > 0:
                await self.tg.daily_report()

            self.db.save_daily(
                self.state.trade_date,
                self.state.daily_trades, self.state.daily_wins,
                self.state.daily_losses, self.state.daily_pnl, 0.0
            )

            self.state.daily_trades = 0
            self.state.daily_pnl = 0.0
            self.state.daily_wins = 0
            self.state.daily_losses = 0
            self.state.consecutive_losses = 0
            self.state.trade_date = today

            info = await self.conn.get_account_information()
            self.state.start_balance = info.get("balance", 0)
            self.state.balance = self.state.start_balance
            log.info(f"Daily reset. New balance: ${self.state.balance:.2f}")

    async def cycle(self):
        self.state.heartbeat = time.time()

        await self.daily_reset()
        await self.fetch_data()

        price, spread = await self.get_price_and_spread()
        if price <= 0:
            log.warning("Price <= 0, skipping cycle")
            return

        # Update balance
        try:
            info = await self.conn.get_account_information()
            self.state.balance = info.get("balance", self.state.balance)
            self.state.equity = info.get("equity", self.state.equity)
        except Exception:
            pass

        # Update session
        hour = datetime.now(timezone.utc).hour
        self.state.session = self.sm.get(hour)

        # Sync closed trades
        await self.pos.sync_positions()

        # Manage partials
        await self.pos.manage_partials(price)

        # Heartbeat to Telegram
        open_count = len(self.state.active_trades)
        await self.tg.heartbeat(price, spread, open_count)

        # Log status every 60 cycles (~10 min)
        if not hasattr(self, '_cycle_count'):
            self._cycle_count = 0
        self._cycle_count += 1
        if self._cycle_count % 60 == 0:
            tradeable = self.sm.is_tradeable(hour)
            log.info(
                f"STATUS: ${price:.2f} | Spread: ${spread:.2f} | "
                f"Session: {self.state.session.value} | Tradeable: {tradeable} | "
                f"5M: {len(self.state.candles_5m)}c | 1M: {len(self.state.candles_1m)}c | "
                f"Trades: {self.state.daily_trades}/{self.cfg.MAX_DAILY_TRADES} | "
                f"EMA: {self.state.ema_fast:.2f}/{self.state.ema_slow:.2f}"
            )

        # Check for new signal
        signal = self.sig.evaluate(price, spread)
        if signal:
            direction, sl, tp1, tp, confluence, reason = signal
            await self.pos.open_scalp(direction, price, sl, tp1, tp, confluence, reason)

    async def run(self):
        log.info(f"Main loop started (cycle: {self.cfg.MAIN_LOOP_SECONDS}s)")
        while self.state.running:
            try:
                await self.cycle()
            except Exception as e:
                log.error(f"Cycle error: {e}", exc_info=True)
                await self.tg.send(f"⚠️ Error: {str(e)[:100]}")
            await asyncio.sleep(self.cfg.MAIN_LOOP_SECONDS)

    async def start(self):
        try:
            await self.connect()
            await self.run()
        except KeyboardInterrupt:
            log.info("Shutting down...")
        except Exception as e:
            log.error(f"Fatal: {e}", exc_info=True)
            await self.tg.send(f"🛑 FATAL: {str(e)[:200]}")
        finally:
            self.db.close()
            if self.conn:
                try:
                    await self.conn.close()
                except Exception:
                    pass
            log.info("Bot stopped.")


# ═══════════════════════════════════════════════════════════════════
#  WATCHDOG
# ═══════════════════════════════════════════════════════════════════

async def watchdog(state: BotState):
    while state.running:
        await asyncio.sleep(30)
        if time.time() - state.heartbeat > state.cfg.WATCHDOG_TIMEOUT:
            log.critical("Watchdog timeout — restarting!")
            os._exit(1)


# ═══════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════

async def main():
    bot = GoldScalper()
    wd = asyncio.create_task(watchdog(bot.state))

    def shutdown(sig, frame):
        log.info(f"Signal {sig}, stopping...")
        bot.state.running = False

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    await bot.start()
    wd.cancel()


if __name__ == "__main__":
    asyncio.run(main())
