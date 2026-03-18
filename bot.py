import asyncio
import pandas as pd
import numpy as np
import os
import time
import json
import urllib.request
import logging
from datetime import datetime, date, timedelta, timezone
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple
from enum import Enum
from metaapi_cloud_sdk import MetaApi

# ================================================================
#   PROFESSIONAL SMC BOT v3.0
#
#   Gebouwd als een discretionaire trader denkt:
#   1. HTF (1H) bepaalt bias (trend richting)
#   2. MTF (15M) bevestigt structuur (BOS/CHoCH)
#   3. LTF (5M) entry bij POI zone + confirmatie candle
#
#   Kernprincipes:
#   - Nooit chasing → zones opslaan en WACHTEN op retest
#   - Premium/Discount → alleen kopen in discount, verkopen in premium
#   - Liquidity eerst → sweep VOOR entry, niet als entry zelf
#   - Confirmatie verplicht → rejection wick of engulfing bij zone
#   - Kwaliteit > Kwantiteit → minder trades, hoger winpercentage
# ================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("SMC")

# ==================== ENUMS & DATA CLASSES ====================

class Direction(Enum):
    BULL = "bull"
    BEAR = "bear"

class StructureType(Enum):
    BOS = "bos"        # Break of Structure (trend continuation)
    CHOCH = "choch"    # Change of Character (reversal)

class ZoneType(Enum):
    ORDER_BLOCK = "ob"
    FAIR_VALUE_GAP = "fvg"

class ZoneStatus(Enum):
    FRESH = "fresh"          # Nog niet getest
    TESTED = "tested"        # 1x getest, nog geldig
    MITIGATED = "mitigated"  # Doorbroken, niet meer geldig

@dataclass
class SwingPoint:
    index: int
    price: float
    type: str          # "high" of "low"
    timestamp: float
    broken: bool = False

@dataclass
class StructureBreak:
    type: StructureType
    direction: Direction
    level: float
    index: int
    swing_broken: SwingPoint

@dataclass
class Zone:
    type: ZoneType
    direction: Direction
    high: float
    low: float
    midpoint: float
    created_at: float
    status: ZoneStatus = ZoneStatus.FRESH
    test_count: int = 0
    structure_break: Optional[StructureBreak] = None
    symbol: str = ""
    timeframe: str = ""

    @property
    def is_valid(self) -> bool:
        return self.status in (ZoneStatus.FRESH, ZoneStatus.TESTED)

    def contains_price(self, price: float, buffer_pct: float = 0.0) -> bool:
        buf = (self.high - self.low) * buffer_pct
        return (self.low - buf) <= price <= (self.high + buf)

@dataclass
class TradeSetup:
    symbol: str
    direction: Direction
    entry: float
    stop_loss: float
    tp1: float
    tp2: float
    zone: Zone
    grade: str
    score: float
    risk_mult: float
    reasons: List[str]
    regime: str
    rr: float
    htf_bias: Optional[Direction] = None
    mtf_structure: Optional[StructureBreak] = None
    confirmation: str = ""

# ==================== CONFIGURATIE ====================

CHECK_INTERVAL = 5
BASE_RISK = 0.01
MIN_RR = 2.0              # Professioneel: minimaal 2R
DAILY_LOSS_LIMIT = 0.025  # 2.5% strenger dan v2
WEEKLY_LOSS_LIMIT = 0.06  # 6% strenger
MAX_TRADES_PER_ASSET = 2
MAX_TOTAL_TRADES = 6      # Minder trades, hogere kwaliteit
COOLDOWN_AFTER_LOSSES = 2
COOLDOWN_MINUTES = 90
ZONE_MAX_AGE_HOURS = 24
ZONE_MAX_TESTS = 1
MAX_API_CALLS_PER_MIN = 30

SWING_LOOKBACK = 3
MIN_REJECTION_WICK_RATIO = 0.6
MIN_ENGULFING_BODY_RATIO = 1.3

KILLZONES = {
    "asia":       {"start": 0,  "end": 7},
    "london":     {"start": 7,  "end": 10},
    "london_ext": {"start": 10, "end": 13},
    "new_york":   {"start": 13, "end": 16},
    "ny_pm":      {"start": 16, "end": 19},
}

ENTRY_KILLZONES = ["london", "london_ext", "new_york"]
NY_PM_SYMBOLS = ["XAUUSD", "NAS100", "US30", "US500"]

SYMBOL_SPECS = {
    "XAUUSD":  {"pip_size": 0.1,    "pip_value_per_lot": 10,  "max_spread_pips": 30,  "category": "metals"},
    "BTCUSD":  {"pip_size": 1.0,    "pip_value_per_lot": 1,   "max_spread_pips": 50,  "category": "crypto"},
    "EURUSD":  {"pip_size": 0.0001, "pip_value_per_lot": 10,  "max_spread_pips": 15,  "category": "forex"},
    "GBPUSD":  {"pip_size": 0.0001, "pip_value_per_lot": 10,  "max_spread_pips": 18,  "category": "forex"},
    "GBPJPY":  {"pip_size": 0.01,   "pip_value_per_lot": 6.5, "max_spread_pips": 25,  "category": "forex"},
    "USDJPY":  {"pip_size": 0.01,   "pip_value_per_lot": 6.5, "max_spread_pips": 15,  "category": "forex"},
    "AUDUSD":  {"pip_size": 0.0001, "pip_value_per_lot": 10,  "max_spread_pips": 15,  "category": "forex"},
    "EURJPY":  {"pip_size": 0.01,   "pip_value_per_lot": 6.5, "max_spread_pips": 20,  "category": "forex"},
    "NAS100":  {"pip_size": 0.1,    "pip_value_per_lot": 1,   "max_spread_pips": 20,  "category": "indices"},
    "US30":    {"pip_size": 0.1,    "pip_value_per_lot": 1,   "max_spread_pips": 30,  "category": "indices"},
    "US500":   {"pip_size": 0.1,    "pip_value_per_lot": 1,   "max_spread_pips": 15,  "category": "indices"},
}

SYMBOLS = list(SYMBOL_SPECS.keys())

METAAPI_TOKEN = os.getenv("METAAPI_TOKEN")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT = os.getenv("TG_CHAT")

CORRELATION_GROUPS = [
    {"EURUSD", "GBPUSD"},
    {"NAS100", "US500"},
    {"USDJPY", "EURJPY"},
    {"GBPUSD", "GBPJPY"},
]

GRADE_ORDER = ["D", "C", "B", "B+", "A", "A+"]

# ==================== GLOBALE STATE ====================

daily_state = {"date": None, "start_balance": 0, "losses_in_row": 0, "cooldown_until": 0, "trades_today": 0}
weekly_state = {"week": None, "loss": 0, "limit_hit": False, "start_balance": 0}
last_heartbeat = 0
api_call_count = 0
api_call_reset_time = 0
zone_store: Dict[str, List[Zone]] = {}
trade_journal: List[dict] = []
asia_range_cache: Dict[str, dict] = {}
recent_signals: Dict[str, float] = {}
connection_healthy = True
last_connection_check = 0

# ==================== TELEGRAM ====================

def tg(msg: str):
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        payload = json.dumps({"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}).encode()
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        log.warning(f"Telegram error: {e}")

# ==================== RATE LIMITER ====================

async def rate_limited_call(coro):
    global api_call_count, api_call_reset_time
    now = time.time()
    if now - api_call_reset_time > 60:
        api_call_count = 0
        api_call_reset_time = now
    if api_call_count >= MAX_API_CALLS_PER_MIN:
        wait = 60 - (now - api_call_reset_time)
        if wait > 0:
            log.info(f"Rate limit: wacht {wait:.0f}s")
            await asyncio.sleep(wait)
        api_call_count = 0
        api_call_reset_time = time.time()
    api_call_count += 1
    return await coro

# ==================== CONNECTION HEALTH ====================

async def check_connection_health(conn) -> bool:
    global connection_healthy, last_connection_check
    now = time.time()
    if now - last_connection_check < 120:
        return connection_healthy
    last_connection_check = now
    try:
        info = await asyncio.wait_for(conn.get_account_information(), timeout=10)
        if info and "balance" in info:
            connection_healthy = True
            return True
    except Exception as e:
        log.error(f"Connection health check failed: {e}")
        connection_healthy = False
    return connection_healthy

# ==================== HEARTBEAT ====================

async def send_heartbeat(conn):
    global last_heartbeat
    now = time.time()
    if now - last_heartbeat < 600:
        return
    last_heartbeat = now

    try:
        info = await rate_limited_call(conn.get_account_information())
        balance = info["balance"]
        equity = info["equity"]
        positions = await rate_limited_call(conn.get_positions())
    except Exception as e:
        log.error(f"Heartbeat error: {e}")
        return

    pl = equity - balance
    pl_pct = (pl / balance) * 100 if balance > 0 else 0
    kz = get_current_killzone()
    daily_loss = ((daily_state["start_balance"] - balance) / daily_state["start_balance"] * 100) if daily_state["start_balance"] > 0 else 0
    total_zones = sum(len([z for z in zones if z.is_valid]) for zones in zone_store.values())

    msg = f"""<b>💓 SMC v3.0 HEARTBEAT</b>

💰 Balance: ${balance:,.2f}
📊 Equity: ${equity:,.2f}
📈 P&L: ${pl:,.2f} ({pl_pct:+.2f}%)

🎯 Open trades: {len(positions)}
🗺️ Active zones: {total_zones}
🕐 Killzone: {kz.upper() if kz else 'NONE'}

📅 Daily loss: {daily_loss:.2f}%
📆 Weekly loss: {weekly_state['loss']*100:.2f}%
🔥 Losses in row: {daily_state['losses_in_row']}
📊 Trades today: {daily_state['trades_today']}

⏰ {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"""
    tg(msg)

# ==================== KILLZONE & SESSION ====================

def get_current_killzone() -> Optional[str]:
    hour = datetime.now(timezone.utc).hour
    for name, times in KILLZONES.items():
        if times["start"] <= hour < times["end"]:
            return name
    return None


def is_entry_allowed(symbol: str) -> Tuple[bool, Optional[str]]:
    kz = get_current_killzone()
    if not kz:
        return False, None
    if kz == "asia":
        return False, kz
    if kz == "ny_pm":
        return (symbol in NY_PM_SYMBOLS), kz
    if kz in ENTRY_KILLZONES:
        return True, kz
    return False, kz

# ==================== ASIA RANGE ====================

async def update_asia_range(account):
    today = date.today()
    for symbol in SYMBOLS:
        if symbol in asia_range_cache and asia_range_cache[symbol]["date"] == today:
            continue
        try:
            candles = await rate_limited_call(account.get_historical_candles(symbol, "15m", 60))
            if not candles or len(candles) < 10:
                continue
            df = pd.DataFrame(candles)
            if "time" not in df.columns:
                continue

            def parse_ts(t):
                try:
                    if isinstance(t, (int, float)):
                        if t > 1e10:
                            t = t / 1000
                        return datetime.utcfromtimestamp(t)
                    return pd.to_datetime(t, utc=True).replace(tzinfo=None)
                except Exception:
                    return None

            df["dt"] = df["time"].apply(parse_ts)
            df = df.dropna(subset=["dt"])
            today_start = datetime.combine(today, datetime.min.time())
            asia_end = today_start + timedelta(hours=7)
            asia = df[(df["dt"] >= today_start) & (df["dt"] < asia_end)]

            if len(asia) >= 4:
                asia_range_cache[symbol] = {
                    "high": float(asia["high"].max()),
                    "low": float(asia["low"].min()),
                    "mid": float((asia["high"].max() + asia["low"].min()) / 2),
                    "date": today,
                }
                log.info(f"Asia range {symbol}: {asia_range_cache[symbol]['low']:.5f} - {asia_range_cache[symbol]['high']:.5f}")
        except Exception as e:
            log.debug(f"Asia range error {symbol}: {e}")

# ==================== RISK MANAGEMENT ====================

def check_weekly(balance: float) -> bool:
    now = datetime.now(timezone.utc)
    cw = now.isocalendar()[1]
    if weekly_state["week"] != cw:
        weekly_state.update({"week": cw, "loss": 0, "limit_hit": False, "start_balance": balance})
        tg("📊 <b>WEEKLY RESET</b>")
    return not weekly_state["limit_hit"]


def update_weekly_loss(balance: float):
    if weekly_state["start_balance"] > 0:
        loss = (weekly_state["start_balance"] - balance) / weekly_state["start_balance"]
        if loss > weekly_state["loss"]:
            weekly_state["loss"] = loss
            if loss >= WEEKLY_LOSS_LIMIT:
                weekly_state["limit_hit"] = True
                tg(f"🚨 <b>WEEKLY LIMIT</b>: {loss*100:.1f}%")


def check_daily(balance: float) -> bool:
    today = date.today()
    if daily_state["date"] != today:
        daily_state.update({"date": today, "start_balance": balance, "losses_in_row": 0, "cooldown_until": 0, "trades_today": 0})
        return True
    if daily_state["start_balance"] == 0:
        daily_state["start_balance"] = balance
        return True
    loss = (daily_state["start_balance"] - balance) / daily_state["start_balance"]
    if loss >= DAILY_LOSS_LIMIT:
        tg(f"🚨 <b>DAILY LIMIT</b>: {loss*100:.2f}%")
        return False
    return True


def check_cooldown() -> Tuple[bool, int]:
    now = time.time()
    if daily_state["cooldown_until"] > now:
        return False, int((daily_state["cooldown_until"] - now) / 60)
    return True, 0


def register_trade_result(is_win: bool, profit: float = 0):
    if is_win:
        daily_state["losses_in_row"] = 0
    else:
        daily_state["losses_in_row"] += 1
        if daily_state["losses_in_row"] >= COOLDOWN_AFTER_LOSSES:
            daily_state["cooldown_until"] = time.time() + COOLDOWN_MINUTES * 60
            tg(f"🧊 <b>COOLDOWN {COOLDOWN_MINUTES}min</b> na {daily_state['losses_in_row']} losses")


def get_dynamic_risk(balance: float) -> float:
    risk = BASE_RISK
    if daily_state["losses_in_row"] >= 1:
        risk *= 0.5
    if daily_state["start_balance"] > 0:
        cur_loss = (daily_state["start_balance"] - balance) / daily_state["start_balance"]
        if cur_loss >= 0.015:
            risk *= 0.5
    return max(risk, 0.002)


def check_correlation(symbol: str, positions: list) -> bool:
    for group in CORRELATION_GROUPS:
        if symbol in group:
            others = group - {symbol}
            for other in others:
                if any(p["symbol"] == other for p in positions):
                    return False
    return True

# ==================== SPREAD FILTER ====================

async def check_spread(conn, symbol: str) -> Tuple[bool, float]:
    try:
        price = await rate_limited_call(conn.get_symbol_price(symbol))
        spread = price["ask"] - price["bid"]
        spec = SYMBOL_SPECS[symbol]
        spread_pips = spread / spec["pip_size"]
        if spread_pips > spec["max_spread_pips"]:
            return False, spread
        return True, spread
    except Exception:
        return False, 0

# ==================== NEWS FILTER ====================

async def news_filter() -> bool:
    try:
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        res = urllib.request.urlopen(req, timeout=10)
        events = json.loads(res.read().decode())
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        for event in events:
            if event.get("impact") != "High":
                continue
            try:
                t = event["date"].split('+')[0].replace('Z', '')
                et = datetime.strptime(t, "%Y-%m-%dT%H:%M:%S")
                if abs((now - et).total_seconds()) <= 900:
                    tg(f"📰 <b>NEWS BLOCK</b>: {event.get('title','?')}")
                    return False
            except Exception:
                continue
        return True
    except Exception:
        return True

# ==================== INDICATOREN ====================

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema20"] = df["close"].ewm(span=20).mean()
    df["ema50"] = df["close"].ewm(span=50).mean()
    df["ema200"] = df["close"].ewm(span=200).mean()

    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss_s = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss_s.replace(0, np.nan)
    df["rsi"] = 100 - (100 / (1 + rs))

    exp1 = df["close"].ewm(span=12).mean()
    exp2 = df["close"].ewm(span=26).mean()
    df["macd"] = exp1 - exp2
    df["macd_signal"] = df["macd"].ewm(span=9).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]

    df["tr"] = np.maximum(
        df["high"] - df["low"],
        np.maximum(abs(df["high"] - df["close"].shift()), abs(df["low"] - df["close"].shift()))
    )
    df["atr"] = df["tr"].rolling(14).mean()

    df["body"] = abs(df["close"] - df["open"])
    df["upper_wick"] = df["high"] - df[["close", "open"]].max(axis=1)
    df["lower_wick"] = df[["close", "open"]].min(axis=1) - df["low"]
    df["candle_range"] = df["high"] - df["low"]
    return df

# ==================== SWING POINT DETECTIE ====================

def detect_swing_points(df: pd.DataFrame, lookback: int = SWING_LOOKBACK) -> List[SwingPoint]:
    """Fractal swing detection: high/low hoger/lager dan N candles links en rechts"""
    swings = []
    if len(df) < lookback * 2 + 1:
        return swings

    for i in range(lookback, len(df) - lookback):
        is_high = all(
            df["high"].iloc[i] > df["high"].iloc[i - j] and df["high"].iloc[i] > df["high"].iloc[i + j]
            for j in range(1, lookback + 1)
        )
        if is_high:
            ts = df["time"].iloc[i] if "time" in df.columns else i
            swings.append(SwingPoint(
                index=i, price=float(df["high"].iloc[i]),
                type="high", timestamp=float(ts) if isinstance(ts, (int, float)) else i
            ))

        is_low = all(
            df["low"].iloc[i] < df["low"].iloc[i - j] and df["low"].iloc[i] < df["low"].iloc[i + j]
            for j in range(1, lookback + 1)
        )
        if is_low:
            ts = df["time"].iloc[i] if "time" in df.columns else i
            swings.append(SwingPoint(
                index=i, price=float(df["low"].iloc[i]),
                type="low", timestamp=float(ts) if isinstance(ts, (int, float)) else i
            ))

    return sorted(swings, key=lambda s: s.index)

# ==================== MARKET STRUCTURE (BOS / CHoCH) ====================

def analyze_structure(df: pd.DataFrame, swings: List[SwingPoint]) -> Optional[StructureBreak]:
    """
    BOS = continuation break (trend volgt door)
    CHoCH = reversal break (trend wisselt)
    Gebaseerd op swing highs/lows sequence
    """
    if len(swings) < 4:
        return None

    highs = [s for s in swings if s.type == "high"]
    lows = [s for s in swings if s.type == "low"]

    if len(highs) < 2 or len(lows) < 2:
        return None

    current_price = float(df["close"].iloc[-1])
    last_candle_high = float(df["high"].iloc[-1])
    last_candle_low = float(df["low"].iloc[-1])

    recent_highs = highs[-3:]
    recent_lows = lows[-3:]

    was_bullish = len(recent_highs) >= 2 and recent_highs[-1].price > recent_highs[-2].price
    was_bearish = len(recent_highs) >= 2 and recent_highs[-1].price < recent_highs[-2].price

    last_sh = highs[-1]
    if last_candle_high > last_sh.price and current_price > last_sh.price:
        stype = StructureType.CHOCH if was_bearish else StructureType.BOS
        return StructureBreak(
            type=stype, direction=Direction.BULL,
            level=last_sh.price, index=last_sh.index, swing_broken=last_sh
        )

    last_sl = lows[-1]
    if last_candle_low < last_sl.price and current_price < last_sl.price:
        stype = StructureType.CHOCH if was_bullish else StructureType.BOS
        return StructureBreak(
            type=stype, direction=Direction.BEAR,
            level=last_sl.price, index=last_sl.index, swing_broken=last_sl
        )

    return None

# ==================== PREMIUM / DISCOUNT ====================

def get_premium_discount(df: pd.DataFrame, swings: List[SwingPoint]) -> Optional[str]:
    """Boven 61.8% = premium, onder 38.2% = discount, midden = equilibrium"""
    highs = [s for s in swings if s.type == "high"]
    lows = [s for s in swings if s.type == "low"]
    if not highs or not lows:
        return None

    swing_high = max(s.price for s in highs[-5:])
    swing_low = min(s.price for s in lows[-5:])
    if swing_high <= swing_low:
        return None

    current = float(df["close"].iloc[-1])
    position = (current - swing_low) / (swing_high - swing_low)

    if position >= 0.618:
        return "premium"
    elif position <= 0.382:
        return "discount"
    return "equilibrium"

# ==================== ORDER BLOCK DETECTIE ====================

def detect_order_blocks(df: pd.DataFrame, structure: Optional[StructureBreak]) -> List[Zone]:
    """OB = laatste tegenovergestelde candle VOOR displacement die structuur brak"""
    zones = []
    if not structure or len(df) < 20:
        return zones

    avg_body = float(df["body"].tail(20).mean())
    search_start = max(0, structure.index - 2)
    search_end = min(len(df), structure.index + 5)

    for i in range(search_start, search_end):
        if i >= len(df):
            break
        c = df.iloc[i]
        body = abs(c["close"] - c["open"])
        if body < avg_body * 2.0:
            continue

        if structure.direction == Direction.BULL and c["close"] > c["open"]:
            for j in range(i - 1, max(i - 6, 0), -1):
                ob_c = df.iloc[j]
                if ob_c["close"] < ob_c["open"]:
                    zones.append(Zone(
                        type=ZoneType.ORDER_BLOCK, direction=Direction.BULL,
                        high=float(ob_c["high"]), low=float(ob_c["low"]),
                        midpoint=float((ob_c["high"] + ob_c["low"]) / 2),
                        created_at=time.time(), structure_break=structure,
                    ))
                    break

        elif structure.direction == Direction.BEAR and c["close"] < c["open"]:
            for j in range(i - 1, max(i - 6, 0), -1):
                ob_c = df.iloc[j]
                if ob_c["close"] > ob_c["open"]:
                    zones.append(Zone(
                        type=ZoneType.ORDER_BLOCK, direction=Direction.BEAR,
                        high=float(ob_c["high"]), low=float(ob_c["low"]),
                        midpoint=float((ob_c["high"] + ob_c["low"]) / 2),
                        created_at=time.time(), structure_break=structure,
                    ))
                    break

    return zones

# ==================== FVG DETECTIE ====================

def detect_fvgs(df: pd.DataFrame, lookback: int = 10) -> List[Zone]:
    """Bullish FVG: c1.high < c3.low | Bearish FVG: c1.low > c3.high"""
    zones = []
    if len(df) < lookback + 2:
        return zones

    avg_body = float(df["body"].tail(20).mean())

    for i in range(len(df) - 1, max(len(df) - lookback, 2), -1):
        c1, c2, c3 = df.iloc[i - 2], df.iloc[i - 1], df.iloc[i]
        mid_body = abs(c2["close"] - c2["open"])

        if c3["low"] > c1["high"] and mid_body > avg_body * 1.5:
            zones.append(Zone(
                type=ZoneType.FAIR_VALUE_GAP, direction=Direction.BULL,
                high=float(c3["low"]), low=float(c1["high"]),
                midpoint=float((c3["low"] + c1["high"]) / 2),
                created_at=time.time(),
            ))
        elif c3["high"] < c1["low"] and mid_body > avg_body * 1.5:
            zones.append(Zone(
                type=ZoneType.FAIR_VALUE_GAP, direction=Direction.BEAR,
                high=float(c1["low"]), low=float(c3["high"]),
                midpoint=float((c1["low"] + c3["high"]) / 2),
                created_at=time.time(),
            ))

    return zones

# ==================== LIQUIDITY SWEEP ====================

def detect_liquidity_sweep(df: pd.DataFrame, symbol: str, swings: List[SwingPoint]) -> Optional[dict]:
    """Sweep = price breekt door level EN keert hard terug (rejection wick)"""
    if len(df) < 15:
        return None

    last = df.iloc[-1]
    prev = df.iloc[-2]
    atr = float(df["atr"].iloc[-1]) if "atr" in df.columns else float((df["high"] - df["low"]).tail(14).mean())
    highs = [s for s in swings if s.type == "high"]
    lows = [s for s in swings if s.type == "low"]
    tolerance = atr * 0.1

    # Equal Highs sweep
    for i in range(len(highs) - 1):
        for j in range(i + 1, len(highs)):
            if abs(highs[i].price - highs[j].price) < tolerance:
                eq_level = max(highs[i].price, highs[j].price)
                if last["high"] > eq_level and last["close"] < eq_level:
                    wick = last["high"] - max(last["close"], last["open"])
                    if wick > atr * 0.3:
                        return {"type": "bear", "level": eq_level, "reason": "equal_highs_sweep", "strength": "strong"}

    # Equal Lows sweep
    for i in range(len(lows) - 1):
        for j in range(i + 1, len(lows)):
            if abs(lows[i].price - lows[j].price) < tolerance:
                eq_level = min(lows[i].price, lows[j].price)
                if last["low"] < eq_level and last["close"] > eq_level:
                    wick = min(last["close"], last["open"]) - last["low"]
                    if wick > atr * 0.3:
                        return {"type": "bull", "level": eq_level, "reason": "equal_lows_sweep", "strength": "strong"}

    # Asia Range sweep
    if symbol in asia_range_cache:
        ar = asia_range_cache[symbol]
        if last["high"] > ar["high"] and last["close"] < ar["high"]:
            wick = last["high"] - max(last["close"], last["open"])
            if wick > atr * 0.2:
                return {"type": "bear", "level": ar["high"], "reason": "asia_high_sweep", "strength": "medium"}
        if last["low"] < ar["low"] and last["close"] > ar["low"]:
            wick = min(last["close"], last["open"]) - last["low"]
            if wick > atr * 0.2:
                return {"type": "bull", "level": ar["low"], "reason": "asia_low_sweep", "strength": "medium"}

    # Swing point sweep
    if len(highs) >= 2:
        recent_sh = highs[-1]
        if last["high"] > recent_sh.price and last["close"] < prev["close"] and last["close"] < recent_sh.price:
            return {"type": "bear", "level": recent_sh.price, "reason": "swing_high_sweep", "strength": "weak"}
    if len(lows) >= 2:
        recent_sl = lows[-1]
        if last["low"] < recent_sl.price and last["close"] > prev["close"] and last["close"] > recent_sl.price:
            return {"type": "bull", "level": recent_sl.price, "reason": "swing_low_sweep", "strength": "weak"}

    return None

# ==================== CONFIRMATION CANDLE ====================

def check_confirmation(df: pd.DataFrame, direction: Direction, zone: Zone) -> Optional[str]:
    """
    Verplichte confirmatie bij zone:
    1. Rejection wick (60%+ wick ratio)
    2. Engulfing (body > 1.3x vorige)
    3. Pin bar (kleine body, grote wick)
    """
    if len(df) < 3:
        return None

    last = df.iloc[-1]
    prev = df.iloc[-2]

    if direction == Direction.BULL:
        lw = float(last["lower_wick"])
        cr = float(last["candle_range"])
        if cr > 0 and lw / cr >= MIN_REJECTION_WICK_RATIO:
            if last["close"] > last["open"] and zone.contains_price(float(last["low"]), 0.2):
                return "rejection_wick"

        if (last["close"] > last["open"] and prev["close"] < prev["open"]
                and last["body"] > prev["body"] * MIN_ENGULFING_BODY_RATIO
                and last["close"] > prev["open"] and last["open"] < prev["close"]):
            if zone.contains_price(float(last["low"]), 0.3):
                return "engulfing"

        body = float(last["body"])
        if cr > 0 and body / cr < 0.3 and lw / cr > 0.5 and last["close"] > last["open"]:
            return "pin_bar"

    elif direction == Direction.BEAR:
        uw = float(last["upper_wick"])
        cr = float(last["candle_range"])
        if cr > 0 and uw / cr >= MIN_REJECTION_WICK_RATIO:
            if last["close"] < last["open"] and zone.contains_price(float(last["high"]), 0.2):
                return "rejection_wick"

        if (last["close"] < last["open"] and prev["close"] > prev["open"]
                and last["body"] > prev["body"] * MIN_ENGULFING_BODY_RATIO
                and last["close"] < prev["open"] and last["open"] > prev["close"]):
            if zone.contains_price(float(last["high"]), 0.3):
                return "engulfing"

        body = float(last["body"])
        if cr > 0 and body / cr < 0.3 and uw / cr > 0.5 and last["close"] < last["open"]:
            return "pin_bar"

    return None

# ==================== ZONE MANAGEMENT ====================

def store_zones(symbol: str, new_zones: List[Zone]):
    if symbol not in zone_store:
        zone_store[symbol] = []
    now = time.time()
    max_age = ZONE_MAX_AGE_HOURS * 3600
    for z in new_zones:
        z.symbol = symbol
        zone_store[symbol].append(z)
    zone_store[symbol] = [z for z in zone_store[symbol] if z.is_valid and (now - z.created_at) < max_age]
    if len(zone_store[symbol]) > 10:
        zone_store[symbol] = zone_store[symbol][-10:]


def find_active_zone(symbol: str, price: float, direction: Direction) -> Optional[Zone]:
    if symbol not in zone_store:
        return None
    for zone in zone_store[symbol]:
        if not zone.is_valid or zone.direction != direction:
            continue
        if zone.contains_price(price, buffer_pct=0.15):
            return zone
    return None


def mark_zone_tested(zone: Zone):
    zone.test_count += 1
    if zone.test_count > ZONE_MAX_TESTS:
        zone.status = ZoneStatus.MITIGATED
    else:
        zone.status = ZoneStatus.TESTED


def update_zone_status(symbol: str, df: pd.DataFrame):
    if symbol not in zone_store:
        return
    current_price = float(df["close"].iloc[-1])
    for zone in zone_store[symbol]:
        if not zone.is_valid:
            continue
        if zone.direction == Direction.BULL and current_price < zone.low:
            zone.status = ZoneStatus.MITIGATED
        elif zone.direction == Direction.BEAR and current_price > zone.high:
            zone.status = ZoneStatus.MITIGATED

# ==================== MARKET REGIME ====================

def detect_regime(df: pd.DataFrame) -> str:
    if len(df) < 30:
        return "unknown"
    atr = float(df["atr"].iloc[-1])
    atr_avg = float(df["atr"].tail(20).mean())
    ema_dist = abs(float(df["ema50"].iloc[-1]) - float(df["ema200"].iloc[-1]))
    ratio = ema_dist / atr if atr > 0 else 0
    above_ema = (df["close"].tail(10) > df["ema50"].tail(10)).sum()
    vol_expanding = atr > atr_avg * 1.2

    if ratio > 2 and (above_ema >= 8 or above_ema <= 2) and vol_expanding:
        return "trending"
    elif ratio < 0.5 and not vol_expanding:
        return "ranging"
    return "transitioning"

# ==================== HTF BIAS (1H) ====================

async def get_htf_bias(account, symbol: str) -> Optional[Direction]:
    try:
        candles = await rate_limited_call(account.get_historical_candles(symbol, "1h", 120))
        if not candles or len(candles) < 60:
            return None
        df = pd.DataFrame(candles)
        df = calculate_indicators(df)

        price = float(df["close"].iloc[-1])
        ema50 = float(df["ema50"].iloc[-1])
        ema200 = float(df["ema200"].iloc[-1])

        if price > ema50 > ema200:
            return Direction.BULL
        if price < ema50 < ema200:
            return Direction.BEAR
        if ema50 > ema200 * 1.001 and price > ema200:
            return Direction.BULL
        if ema50 < ema200 * 0.999 and price < ema200:
            return Direction.BEAR
        return None
    except Exception:
        return None

# ==================== TRADE GRADING ====================

def grade_setup(htf_bias, structure, zone, confirmation, sweep, premium_discount, regime, direction):
    """
    Verplicht: Zone + Confirmatie (zonder = D)
    A+ >= 10 | A >= 8 | B+ >= 6 | B >= 5 | C < 5 = skip
    """
    score = 0
    reasons = []

    if not zone or not confirmation:
        return "D", 0, 0, ["NO_ZONE" if not zone else "NO_CONFIRM"]

    reasons.append(f"ZONE({zone.type.value})")
    score += 2.0
    reasons.append(f"CONFIRM({confirmation})")
    score += 2.0

    if htf_bias:
        score += 2.5
        reasons.append("HTF_ALIGNED")

    if structure:
        if structure.type == StructureType.CHOCH:
            score += 2.0
            reasons.append("CHoCH")
        else:
            score += 1.5
            reasons.append("BOS")

    if sweep:
        strength = sweep.get("strength", "weak")
        pts = {"strong": 2.5, "medium": 1.5, "weak": 0.5}.get(strength, 0.5)
        score += pts
        reasons.append(f"SWEEP_{strength.upper()}({sweep['reason']})")

    if premium_discount:
        if (direction == Direction.BULL and premium_discount == "discount") or \
           (direction == Direction.BEAR and premium_discount == "premium"):
            score += 1.5
            reasons.append("P/D_ALIGNED")
        elif premium_discount == "equilibrium":
            score -= 0.5
            reasons.append("EQ⚠️")

    if regime == "trending":
        score += 1.0
        reasons.append("TRENDING")
    elif regime == "ranging":
        score -= 1.0
        reasons.append("RANGING⚠️")

    if zone.type == ZoneType.ORDER_BLOCK and zone.structure_break:
        score += 0.5
        reasons.append("OB+STRUCT")

    if score >= 10:
        return "A+", 1.0, score, reasons
    elif score >= 8:
        return "A", 0.75, score, reasons
    elif score >= 6:
        return "B+", 0.6, score, reasons
    elif score >= 5:
        return "B", 0.5, score, reasons
    return "C", 0, score, reasons

# ==================== LOT SIZE ====================

def calculate_lot_size(balance: float, sl_distance: float, symbol: str, risk_pct: float) -> float:
    if sl_distance <= 0:
        return 0
    spec = SYMBOL_SPECS.get(symbol)
    if not spec:
        return 0
    sl_pips = sl_distance / spec["pip_size"]
    risk_amount = balance * risk_pct
    denom = sl_pips * spec["pip_value_per_lot"]
    if denom <= 0:
        return 0
    lot = risk_amount / denom
    return round(max(0.01, min(lot, 3.0)), 2)

# ==================== SL / TP BEREKENING ====================

def calculate_trade_levels(direction: Direction, entry: float, zone: Zone, df: pd.DataFrame):
    """SL achter zone, TP1 op 2R, TP2 op 3R"""
    atr = float(df["atr"].iloc[-1])
    buffer = atr * 0.15

    if direction == Direction.BULL:
        sl = zone.low - buffer
        sl_dist = entry - sl
        if sl_dist < atr * 0.5:
            sl = entry - atr * 0.5
            sl_dist = entry - sl
        tp1 = entry + sl_dist * 2.0
        tp2 = entry + sl_dist * 3.0
    else:
        sl = zone.high + buffer
        sl_dist = sl - entry
        if sl_dist < atr * 0.5:
            sl = entry + atr * 0.5
            sl_dist = sl - entry
        tp1 = entry - sl_dist * 2.0
        tp2 = entry - sl_dist * 3.0

    return sl, tp1, tp2, sl_dist

# ==================== POSITION MANAGEMENT ====================

async def manage_positions(conn, positions: list):
    """Partial bij 1.5R + BE, trailing bij 2.5R"""
    for pos in positions:
        try:
            symbol = pos["symbol"]
            open_p = pos["openPrice"]
            cur_p = pos.get("currentPrice", 0)
            vol = pos["volume"]
            ptype = pos["type"]
            pid = pos["id"]
            sl = pos.get("stopLoss", 0)
            tp = pos.get("takeProfit", 0)

            if cur_p <= 0 or vol <= 0 or symbol not in SYMBOL_SPECS:
                continue

            is_buy = "BUY" in ptype
            profit_dist = (cur_p - open_p) if is_buy else (open_p - cur_p)
            sl_dist = abs(open_p - sl) if sl else 0
            if sl_dist <= 0:
                continue

            if profit_dist >= sl_dist * 1.5 and vol > 0.02:
                partial = round(vol * 0.5, 2)
                if partial >= 0.01:
                    try:
                        await rate_limited_call(conn.close_position_partially(pid, partial))
                        be_buf = sl_dist * 0.15
                        new_sl = (open_p + be_buf) if is_buy else (open_p - be_buf)
                        await rate_limited_call(conn.modify_position(pid, stop_loss=new_sl, take_profit=tp))
                        tg(f"✅ <b>PARTIAL TP</b>: {symbol} 50% @ 1.5R, SL→BE+")
                    except Exception as e:
                        log.warning(f"Partial error {symbol}: {e}")

            elif profit_dist >= sl_dist * 2.5:
                trail_dist = sl_dist * 1.0
                if is_buy:
                    new_sl = cur_p - trail_dist
                    if sl and new_sl > sl:
                        try:
                            await rate_limited_call(conn.modify_position(pid, stop_loss=new_sl, take_profit=tp))
                        except Exception:
                            pass
                else:
                    new_sl = cur_p + trail_dist
                    if sl and new_sl < sl:
                        try:
                            await rate_limited_call(conn.modify_position(pid, stop_loss=new_sl, take_profit=tp))
                        except Exception:
                            pass
        except Exception as e:
            log.warning(f"Position management error: {e}")

# ==================== CLOSED TRADE TRACKING ====================

async def check_closed_trades(conn):
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        history = await rate_limited_call(conn.get_deals_by_time_range(start, now))
        if not history:
            return
        for deal in history[-5:]:
            profit = deal.get("profit", 0)
            did = deal.get("id", "")
            dk = f"deal_{did}"
            if dk in recent_signals:
                continue
            if profit != 0:
                recent_signals[dk] = time.time()
                register_trade_result(profit > 0, profit)
                emoji = "💰" if profit > 0 else "💔"
                tg(f"{emoji} <b>TRADE {'WON' if profit>0 else 'LOST'}</b>: {'+'if profit>0 else ''}{profit:.2f}")
    except Exception as e:
        log.debug(f"Closed trades error: {e}")

# ==================== CANDLE CLOSE CHECK ====================

def is_candle_just_closed(tf_min: int = 5) -> bool:
    now = datetime.now(timezone.utc)
    return now.minute % tf_min == 0 and now.second < 45

# ==================== HOOFDSTRATEGIE ====================

async def analyze_and_find_setup(account, conn, symbol, positions, balance) -> Optional[TradeSetup]:
    """
    Top-down:
    1. HTF (1H) bias
    2. MTF (15M) structuur + zones opslaan
    3. LTF (5M) entry bij zone + confirmatie
    4. Premium/Discount filter
    5. Grade en valideer
    """
    try:
        # Stap 1: HTF Bias
        htf_bias = await get_htf_bias(account, symbol)

        # Stap 2: MTF (15M)
        candles_15m = await rate_limited_call(account.get_historical_candles(symbol, "15m", 100))
        if not candles_15m or len(candles_15m) < 40:
            return None

        df_15m = pd.DataFrame(candles_15m)
        df_15m = calculate_indicators(df_15m)
        swings_15m = detect_swing_points(df_15m)
        structure_15m = analyze_structure(df_15m, swings_15m)
        regime = detect_regime(df_15m)

        if structure_15m:
            new_obs = detect_order_blocks(df_15m, structure_15m)
            for ob in new_obs:
                ob.timeframe = "15m"
            store_zones(symbol, new_obs)

        new_fvgs = detect_fvgs(df_15m)
        for fvg in new_fvgs:
            fvg.timeframe = "15m"
        store_zones(symbol, new_fvgs)

        # Stap 3: LTF (5M)
        candles_5m = await rate_limited_call(account.get_historical_candles(symbol, "5m", 100))
        if not candles_5m or len(candles_5m) < 50:
            return None

        df_5m = pd.DataFrame(candles_5m)
        df_5m = calculate_indicators(df_5m)
        swings_5m = detect_swing_points(df_5m)

        update_zone_status(symbol, df_5m)

        structure_5m = analyze_structure(df_5m, swings_5m)
        if structure_5m:
            new_obs_5m = detect_order_blocks(df_5m, structure_5m)
            for ob in new_obs_5m:
                ob.timeframe = "5m"
            store_zones(symbol, new_obs_5m)

        new_fvgs_5m = detect_fvgs(df_5m)
        for fvg in new_fvgs_5m:
            fvg.timeframe = "5m"
        store_zones(symbol, new_fvgs_5m)

        sweep = detect_liquidity_sweep(df_5m, symbol, swings_5m)
        pd_zone = get_premium_discount(df_15m, swings_15m)
        current_price = float(df_5m["close"].iloc[-1])

        # Bepaal richting
        direction = None
        if htf_bias == Direction.BULL and (not structure_15m or structure_15m.direction == Direction.BULL):
            direction = Direction.BULL
        elif htf_bias == Direction.BEAR and (not structure_15m or structure_15m.direction == Direction.BEAR):
            direction = Direction.BEAR
        elif structure_15m and structure_15m.type == StructureType.CHOCH:
            direction = structure_15m.direction
        elif htf_bias:
            direction = htf_bias

        if not direction:
            return None

        # P/D filter
        if direction == Direction.BULL and pd_zone == "premium":
            return None
        if direction == Direction.BEAR and pd_zone == "discount":
            return None

        # Zoek zone
        active_zone = find_active_zone(symbol, current_price, direction)
        if not active_zone:
            return None

        # Check confirmatie
        confirmation = check_confirmation(df_5m, direction, active_zone)
        if not confirmation:
            return None

        # Spread
        spread_ok, spread = await check_spread(conn, symbol)
        if not spread_ok:
            return None

        # Grade
        grade, risk_mult, score, reasons = grade_setup(
            htf_bias=(htf_bias == direction),
            structure=structure_15m if structure_15m and structure_15m.direction == direction else structure_5m,
            zone=active_zone, confirmation=confirmation,
            sweep=sweep if sweep and sweep["type"] == direction.value else None,
            premium_discount=pd_zone, regime=regime, direction=direction,
        )

        if grade in ("C", "D"):
            return None

        # Levels
        price_data = await rate_limited_call(conn.get_symbol_price(symbol))
        entry = price_data["ask"] if direction == Direction.BULL else price_data["bid"]
        sl, tp1, tp2, sl_dist = calculate_trade_levels(direction, entry, active_zone, df_5m)

        rr = abs((tp2 - entry) / sl_dist) if sl_dist > 0 else 0
        if rr < MIN_RR:
            return None

        mark_zone_tested(active_zone)

        return TradeSetup(
            symbol=symbol, direction=direction, entry=entry,
            stop_loss=sl, tp1=tp1, tp2=tp2, zone=active_zone,
            grade=grade, score=score, risk_mult=risk_mult,
            reasons=reasons, regime=regime, rr=rr,
            htf_bias=htf_bias, mtf_structure=structure_15m,
            confirmation=confirmation,
        )

    except Exception as e:
        log.error(f"Analyse error {symbol}: {e}")
        return None

# ==================== TRADE EXECUTIE ====================

async def execute_trade(conn, setup: TradeSetup, balance: float) -> bool:
    risk_pct = get_dynamic_risk(balance) * setup.risk_mult
    sl_dist = abs(setup.entry - setup.stop_loss)
    lot = calculate_lot_size(balance, sl_dist, setup.symbol, risk_pct)

    if lot < 0.01:
        return False

    try:
        if setup.direction == Direction.BULL:
            await rate_limited_call(conn.create_market_buy_order(setup.symbol, lot, setup.stop_loss, setup.tp2))
        else:
            await rate_limited_call(conn.create_market_sell_order(setup.symbol, lot, setup.stop_loss, setup.tp2))
    except Exception as e:
        tg(f"❌ <b>ORDER FAIL</b>: {setup.symbol} — {e}")
        return False

    daily_state["trades_today"] += 1

    trade_journal.append({
        "time": datetime.now(timezone.utc).isoformat(),
        "symbol": setup.symbol, "direction": setup.direction.value,
        "grade": setup.grade, "score": setup.score,
        "entry": setup.entry, "sl": setup.stop_loss,
        "tp1": setup.tp1, "tp2": setup.tp2,
        "lot": lot, "rr": setup.rr, "risk_pct": risk_pct,
        "reasons": setup.reasons, "regime": setup.regime,
        "confirmation": setup.confirmation, "zone_type": setup.zone.type.value,
    })

    kz = get_current_killzone()
    r = " | ".join(setup.reasons)
    de = "🟢" if setup.direction == Direction.BULL else "🔴"

    tg(f"""<b>{de} TRADE OPENED — Grade {setup.grade}</b>

📌 {setup.symbol} | {setup.direction.value.upper()}
🕐 KZ: {kz.upper() if kz else '?'}
🔍 Regime: {setup.regime.upper()}

💰 Entry: {setup.entry:.5f}
🛑 SL: {setup.stop_loss:.5f}
🎯 TP1: {setup.tp1:.5f} (50% partial)
🎯 TP2: {setup.tp2:.5f} (runner)

📊 RR: 1:{setup.rr:.1f} | Lots: {lot}
💵 Risk: {risk_pct*100:.2f}%
✅ Confirm: {setup.confirmation}
🗺️ Zone: {setup.zone.type.value} ({setup.zone.timeframe})

📋 Score: {setup.score:.1f} | {r}
💰 Balance: ${balance:,.2f}""")

    return True

# ==================== DIAGNOSTIEK ====================

async def run_diagnostics(conn, account):
    log.info("=" * 60)
    log.info("DIAGNOSTICS — SMC Bot v3.0")
    log.info("=" * 60)

    try:
        info = await conn.get_account_information()
        log.info(f"Balance: ${info['balance']} | Equity: ${info['equity']}")
    except Exception as e:
        log.error(f"Account error: {e}")

    now = datetime.now(timezone.utc)
    kz = get_current_killzone()
    log.info(f"Time: {now.strftime('%H:%M:%S')} UTC | Killzone: {kz or 'NONE'}")
    log.info(f"Symbols: {len(SYMBOLS)} | Entry KZs: {', '.join(ENTRY_KILLZONES)}")
    log.info(f"Min RR: {MIN_RR} | Max trades: {MAX_TOTAL_TRADES}")

    for s in SYMBOLS:
        try:
            candles = await rate_limited_call(account.get_historical_candles(s, "5m", 20))
            status = f"OK {len(candles)}c" if candles and len(candles) >= 10 else "FAIL"
            p = await rate_limited_call(conn.get_symbol_price(s))
            spread = p["ask"] - p["bid"]
            spec = SYMBOL_SPECS[s]
            spread_pips = spread / spec["pip_size"]
            allowed, _ = is_entry_allowed(s)
            log.info(f"  {s:10} | {status} | spread: {spread_pips:.1f} pips | entry: {'YES' if allowed else 'NO'}")
        except Exception as e:
            log.info(f"  {s:10} | ERROR {e}")

    log.info("=" * 60)

# ==================== HOOFDLOOP ====================

async def run():
    try:
        log.info("Connecting to MetaAPI...")
        api = MetaApi(METAAPI_TOKEN)
        account = await api.metatrader_account_api.get_account(ACCOUNT_ID)
        await account.wait_connected()

        conn = account.get_rpc_connection()
        await conn.connect()

        log.info("Synchronizing...")
        await conn.wait_synchronized(timeout_in_seconds=120)
        log.info("Synchronized!")

        await asyncio.sleep(2)

        global last_heartbeat
        last_heartbeat = 0

        await run_diagnostics(conn, account)

        tg(f"""🚀 <b>SMC BOT v3.0 GESTART</b>

📊 {len(SYMBOLS)} symbols | Entry KZs: {', '.join(ENTRY_KILLZONES)}
🎯 Min RR: {MIN_RR} | Max trades: {MAX_TOTAL_TRADES}

⚡ <b>Strategie:</b>
• Top-down: 1H bias → 15M structuur → 5M entry
• Zone-based: OB/FVG opslaan → wachten op retest
• Confirmatie verplicht: rejection wick / engulfing / pin bar
• Premium/Discount filter
• BOS/CHoCH structuur analyse""")

        while True:
            try:
                await send_heartbeat(conn)

                if not await check_connection_health(conn):
                    log.warning("Connection unhealthy, reconnecting...")
                    try:
                        await conn.connect()
                        await conn.wait_synchronized(timeout_in_seconds=60)
                        log.info("Reconnected!")
                    except Exception as e:
                        log.error(f"Reconnect failed: {e}")
                        await asyncio.sleep(30)
                        continue

                kz = get_current_killzone()

                if kz == "asia":
                    await update_asia_range(account)
                    await asyncio.sleep(60)
                    continue

                if kz not in ENTRY_KILLZONES and kz != "ny_pm":
                    info = await rate_limited_call(conn.get_account_information())
                    positions = await rate_limited_call(conn.get_positions())
                    if positions:
                        await manage_positions(conn, positions)
                    await asyncio.sleep(30)
                    continue

                info = await rate_limited_call(conn.get_account_information())
                balance = info["balance"]
                equity = info["equity"]
                positions = await rate_limited_call(conn.get_positions())

                update_weekly_loss(balance)
                await check_closed_trades(conn)

                if positions:
                    await manage_positions(conn, positions)

                if not check_weekly(balance):
                    await asyncio.sleep(60)
                    continue
                if not check_daily(balance):
                    await asyncio.sleep(60)
                    continue

                ok, remaining = check_cooldown()
                if not ok:
                    await asyncio.sleep(60)
                    continue

                if not is_candle_just_closed(5):
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue

                if not await news_filter():
                    await asyncio.sleep(60)
                    continue

                if len(positions) >= MAX_TOTAL_TRADES:
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue

                for symbol in SYMBOLS:
                    allowed, _ = is_entry_allowed(symbol)
                    if not allowed:
                        continue
                    if sum(1 for p in positions if p["symbol"] == symbol) >= MAX_TRADES_PER_ASSET:
                        continue
                    if not check_correlation(symbol, positions):
                        continue

                    sig_key = f"{symbol}_{int(time.time()/900)}"
                    if sig_key in recent_signals:
                        continue

                    setup = await analyze_and_find_setup(account, conn, symbol, positions, balance)
                    if not setup:
                        continue

                    success = await execute_trade(conn, setup, balance)
                    if success:
                        recent_signals[sig_key] = time.time()

                    await asyncio.sleep(1)

                now = time.time()
                for k in list(recent_signals.keys()):
                    if now - recent_signals[k] > 7200:
                        del recent_signals[k]

                await asyncio.sleep(CHECK_INTERVAL)

            except Exception as e:
                log.error(f"Loop error: {e}")
                await asyncio.sleep(10 if "timed out" in str(e).lower() else 5)

    except Exception as e:
        log.critical(f"FATAL: {e}")
        tg(f"❌ <b>FATAL</b>: {str(e)[:100]}")
        raise e

# ==================== START ====================

if __name__ == "__main__":
    log.info("=" * 50)
    log.info("PROFESSIONAL SMC BOT v3.0")
    log.info("Zone-based | Confirmation Required | Top-Down")
    log.info("=" * 50)

    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            tg("🛑 <b>BOT GESTOPT</b>")
            log.info("Stopped by user")
            break
        except Exception as e:
            tg(f"💥 <b>CRASH</b>: {str(e)[:80]}")
            log.error(f"Crash: {e} — Restart in 10s...")
            time.sleep(10)
