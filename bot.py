import asyncio
import pandas as pd
import numpy as np
import os
import time
import json
import math
import urllib.request
import logging
import pickle
from pathlib import Path
from datetime import datetime, date, timedelta, timezone
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple
from enum import Enum
from metaapi_cloud_sdk import MetaApi

# ================================================================
# PROFESSIONAL SMC BOT v3.2 — REFINED
# 
# Verbeteringen t.o.v. v3.1:
# - Momentum entries VERWIJDERD (alleen echte zones)
# - Risk stack vereenvoudigd: 3 lagen i.p.v. 9
# - Min grade B+ (C/B trades verwijderd)
# - Spread-weighted RR berekening
# - Multi-TF zone confluence (1H zones)
# - Session high/low tracking + sweeps
# - Cooldown PER SYMBOOL i.p.v. globaal
# - Volume confirmatie
# - 33/33/33 partial close strategie
# - Swing lookback 3 (was 2)
# - Zone max tests 1 (was 2)
# - Priority score bonus VERWIJDERD
# - Adaptive boosts met TTL cache
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
    BOS = "bos"
    CHOCH = "choch"

class ZoneType(Enum):
    ORDER_BLOCK = "ob"
    FAIR_VALUE_GAP = "fvg"

class ZoneStatus(Enum):
    FRESH = "fresh"
    TESTED = "tested"
    MITIGATED = "mitigated"

@dataclass
class SwingPoint:
    index: int
    price: float
    type: str
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
        return self.status in (ZoneStatus.FRESH,)

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
    tp3: float
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
    confluence: bool = False

# ==================== CONFIGURATIE ====================

CHECK_INTERVAL = 5
BASE_RISK = 0.015

# === VEREENVOUDIGDE RISK: slechts 3 lagen ===
# Laag 1: Grade-based risk (ingebakken in grade)
GRADE_RISK = {
    "A+": 0.020,   # 2.0%
    "A":  0.015,    # 1.5%
    "B+": 0.010,    # 1.0%
}

# Laag 2: Drawdown protectie
DAILY_LOSS_LIMIT = 0.03
WEEKLY_LOSS_LIMIT = 0.08

# Laag 3: Streak modifier (max ±30%)
STREAK_MODIFIER = {
    "wins_4plus": 1.3,
    "wins_2_3": 1.15,
    "neutral": 1.0,
    "loss_1": 0.8,
    "loss_2": 0.6,
    "loss_3plus": 0.4,
}

MIN_RR = 2.0
MAX_TRADES_PER_ASSET = 2
MAX_TOTAL_TRADES = 6
MAX_TRADES_PER_DAY = 10

# === GOLD TRADING MODE ===
GOLD_SCALP = {
    "enabled": True,
    "symbol": "XAUUSD",
    "max_trades": 3,
    "min_rr": 1.5,
    "min_grade": "B+",
    "tp1_mult": 2.0,
    "tp2_mult": 3.0,
    "tp3_mult": 4.5,
    "dedup_seconds": 300,
}

# === MINIMUM SL AFSTAND PER CATEGORIE ===
MIN_SL_PIPS = {
    "metals": 50,
    "forex": 15,
    "indices": 200,
}

COOLDOWN_AFTER_LOSSES = 3
COOLDOWN_MINUTES = 45
ZONE_MAX_AGE_HOURS = 48
ZONE_MAX_TESTS = 1          # Terug naar 1 — geteste zone is zwak
MAX_API_CALLS_PER_MIN = 50
SWING_LOOKBACK = 3           # Terug naar 3 — minder noise
MIN_REJECTION_WICK_RATIO = 0.45
MIN_ENGULFING_BODY_RATIO = 1.1
MIN_STRONG_CLOSE_BODY_RATIO = 1.5

KILLZONES = {
    "asia":       {"start": 0,  "end": 7},
    "london":     {"start": 7,  "end": 10},
    "london_ext": {"start": 10, "end": 13},
    "new_york":   {"start": 13, "end": 16},
    "ny_pm":      {"start": 16, "end": 19},
}

ENTRY_KILLZONES = ["asia", "london", "london_ext", "new_york", "ny_pm"]
ASIA_ENTRY_SYMBOLS = ["USDJPY", "GBPJPY", "XAUUSD"]
NY_PM_SYMBOLS = ["XAUUSD", "USTEC", "US30"]

SYMBOL_SPECS = {
    "XAUUSD":  {"pip_size": 0.1,    "pip_value_per_lot": 10,  "max_spread_pips": 35,  "category": "metals",  "leverage": 20, "contract": 100, "min_lot": 0.01, "lot_step": 0.01},
    "GBPJPY":  {"pip_size": 0.01,   "pip_value_per_lot": 6.5, "max_spread_pips": 30,  "category": "forex",   "leverage": 20, "contract": 100000, "min_lot": 0.01, "lot_step": 0.01},
    "USDJPY":  {"pip_size": 0.01,   "pip_value_per_lot": 6.5, "max_spread_pips": 18,  "category": "forex",   "leverage": 30, "contract": 100000, "min_lot": 0.01, "lot_step": 0.01},
    "USTEC":   {"pip_size": 0.1,    "pip_value_per_lot": 1,   "max_spread_pips": 25,  "category": "indices",  "leverage": 20, "contract": 1, "min_lot": 0.1, "lot_step": 0.1},
    "US30":    {"pip_size": 0.1,    "pip_value_per_lot": 1,   "max_spread_pips": 35,  "category": "indices",  "leverage": 20, "contract": 1, "min_lot": 0.1, "lot_step": 0.1},
}

SYMBOLS = ["XAUUSD", "USTEC", "GBPJPY"] + [s for s in SYMBOL_SPECS.keys() if s not in ["XAUUSD", "USTEC", "GBPJPY"]]

METAAPI_TOKEN = os.getenv("METAAPI_TOKEN")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT = os.getenv("TG_CHAT")

CORRELATION_GROUPS = [
    {"USTEC", "US30"},
]

GRADE_ORDER = ["D", "C", "B", "B+", "A", "A+"]

# ==================== GLOBALE STATE ====================

daily_state = {"date": None, "start_balance": 0, "trades_today": 0}
weekly_state = {"week": None, "loss": 0, "limit_hit": False, "start_balance": 0}
last_heartbeat = 0
api_call_count = 0
api_call_reset_time = 0
zone_store: Dict[str, List[Zone]] = {}
htf_zone_store: Dict[str, List[Zone]] = {}  # NIEUW: 1H zones apart
trade_journal: List[dict] = []
asia_range_cache: Dict[str, dict] = {}
recent_signals: Dict[str, float] = {}
connection_healthy = True
last_connection_check = 0
watchdog_last_loop = time.time()
watchdog_max_silence = 600
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 20

# === COOLDOWN PER SYMBOOL (was globaal) ===
symbol_cooldowns: Dict[str, dict] = {}  # {"GBPJPY": {"losses": 2, "until": timestamp}}

# === SESSION HIGH/LOW TRACKING ===
session_levels: Dict[str, Dict[str, dict]] = {}
# Structuur: {"XAUUSD": {"london": {"high": 1.23, "low": 1.20, "date": date}}}

# === ADAPTIVE BOOSTS MET CACHE ===
_adaptive_cache = {"data": {}, "last_calc": 0, "ttl": 3600}  # 1 uur TTL

# ===== PERFORMANCE TRACKER =====
performance = {
    "wins": 0,
    "losses": 0,
    "consecutive_wins": 0,
    "consecutive_losses": 0,
    "recent_results": [],
    "peak_balance": 0,
    "session_start_balance": 0,
    "total_profit": 0,
    "grade_stats": {},
    "kz_stats": {},
}

LOT_LIMITS = {
    "forex":   {"min": 0.01, "max": 5.0},
    "metals":  {"min": 0.01, "max": 3.0},
    "indices": {"min": 0.01, "max": 3.0},
    "crypto":  {"min": 0.01, "max": 2.0},
}

# ==================== TELEGRAM ====================

tg_fail_count = 0
tg_last_success = time.time()

def tg(msg: str):
    global tg_fail_count, tg_last_success
    if not TG_TOKEN or not TG_CHAT:
        return
    for attempt in range(3):
        try:
            url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
            payload = json.dumps({"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}).encode()
            req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=8 + attempt * 4)
            tg_fail_count = 0
            tg_last_success = time.time()
            return
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                tg_fail_count += 1
                log.warning(f"Telegram FAILED 3x (total fails: {tg_fail_count}): {e}")

def tg_health_check() -> bool:
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getMe"
        req = urllib.request.Request(url, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
        return True
    except Exception:
        return False

# ==================== SAFE API CALL ====================

async def safe_call(coro_func, *args, retries=3, timeout=30, default=None, label="API"):
    global api_call_count, api_call_reset_time
    for attempt in range(retries):
        try:
            now = time.time()
            if now - api_call_reset_time > 60:
                api_call_count = 0
                api_call_reset_time = now
            if api_call_count >= MAX_API_CALLS_PER_MIN:
                wait = 60 - (now - api_call_reset_time)
                if wait > 0:
                    await asyncio.sleep(wait)
                api_call_count = 0
                api_call_reset_time = time.time()
            api_call_count += 1
            result = await asyncio.wait_for(coro_func(*args), timeout=timeout)
            mark_api_success()
            return result
        except asyncio.TimeoutError:
            log.debug(f"{label} timeout (poging {attempt+1}/{retries})")
            await asyncio.sleep(2 * (attempt + 1))
        except Exception as e:
            err = str(e).lower()
            if "not connected" in err or "not synchronized" in err or "socket" in err:
                mark_api_failure()
                log.debug(f"{label} connection error: {e}")
                await asyncio.sleep(3)
            elif attempt < retries - 1:
                log.debug(f"{label} error (poging {attempt+1}): {e}")
                await asyncio.sleep(2)
            else:
                log.debug(f"{label} failed after {retries}: {e}")
                mark_api_failure()
    return default

async def rate_limited_call(coro):
    global api_call_count, api_call_reset_time
    now = time.time()
    if now - api_call_reset_time > 60:
        api_call_count = 0
        api_call_reset_time = now
    if api_call_count >= MAX_API_CALLS_PER_MIN:
        wait = 60 - (now - api_call_reset_time)
        if wait > 0:
            await asyncio.sleep(wait)
        api_call_count = 0
        api_call_reset_time = time.time()
    api_call_count += 1
    try:
        result = await asyncio.wait_for(coro, timeout=30)
        mark_api_success()
        return result
    except asyncio.TimeoutError:
        mark_api_failure()
        log.debug("rate_limited_call timeout")
        return None
    except (asyncio.CancelledError, asyncio.exceptions.CancelledError):
        mark_api_failure()
        log.warning("rate_limited_call CancelledError (WebSocket disconnect)")
        return None
    except Exception as e:
        mark_api_failure()
        log.debug(f"rate_limited_call error: {e}")
        return None

# ==================== CANDLE HELPER ====================

TF_MINUTES = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}

async def get_candles(account, symbol: str, timeframe: str, count: int):
    tf_min = TF_MINUTES.get(timeframe, 15)
    start = datetime.now(timezone.utc) - timedelta(minutes=tf_min * count * 1.5)
    try:
        result = await safe_call(
            account.get_historical_candles, symbol, timeframe, start,
            retries=2, timeout=20, default=None, label=f"candles_{symbol}_{timeframe}"
        )
        return result
    except Exception as e:
        log.debug(f"get_candles {symbol} {timeframe}: {e}")
        return None

# ==================== CONNECTION HEALTH ====================

consecutive_api_fails = 0
MAX_API_FAILS_BEFORE_RECONNECT = 7

async def check_connection_health(conn) -> bool:
    return True

def mark_api_success():
    global consecutive_api_fails
    consecutive_api_fails = 0

def mark_api_failure():
    global consecutive_api_fails
    consecutive_api_fails += 1
    if consecutive_api_fails >= MAX_API_FAILS_BEFORE_RECONNECT:
        log.warning(f"API failed {consecutive_api_fails}x in a row — reconnect needed")
        return True
    return False

def needs_reconnect() -> bool:
    return consecutive_api_fails >= MAX_API_FAILS_BEFORE_RECONNECT

def reset_api_fails():
    global consecutive_api_fails
    consecutive_api_fails = 0

# ==================== HEARTBEAT ====================

async def send_heartbeat(conn):
    global last_heartbeat
    now = time.time()
    if now - last_heartbeat < 600:
        return
    last_heartbeat = now
    try:
        info = await rate_limited_call(conn.get_account_information())
        if not info or "balance" not in info:
            tg(f"💓 <b>HEARTBEAT</b> (limited)\n⚠️ Account info niet beschikbaar\n⏰ {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
            return
        balance = info["balance"]
        equity = info["equity"]
        positions = await rate_limited_call(conn.get_positions())
        if positions is None:
            positions = []
    except Exception as e:
        log.error(f"Heartbeat error: {e}")
        tg(f"💓 <b>HEARTBEAT</b> (limited)\n⚠️ Data ophalen mislukt: {str(e)[:60]}\n⏰ {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
        return

    pl = equity - balance
    pl_pct = (pl / balance) * 100 if balance > 0 else 0
    kz = get_current_killzone()
    daily_loss = ((daily_state["start_balance"] - balance) / daily_state["start_balance"] * 100) if daily_state["start_balance"] > 0 else 0
    total_zones = sum(len([z for z in zones if z.is_valid]) for zones in zone_store.values())
    total_htf_zones = sum(len([z for z in zones if z.is_valid]) for zones in htf_zone_store.values())
    tg_status = "✅" if tg_fail_count == 0 else f"⚠️ {tg_fail_count} fails"

    # Cooldown status
    active_cooldowns = [s for s, cd in symbol_cooldowns.items() if cd.get("until", 0) > now]

    msg = f"""<b>💓 SMC v3.2 HEARTBEAT</b>
💰 Balance: ${balance:,.2f}
📊 Equity: ${equity:,.2f}
📈 P&L: ${pl:,.2f} ({pl_pct:+.2f}%)
🎯 Open trades: {len(positions)}
🗺️ Zones: {total_zones} (5M/15M) + {total_htf_zones} (1H)
🕐 Killzone: {kz.upper() if kz else 'NONE'}
📅 Daily loss: {daily_loss:.2f}%
📆 Weekly loss: {weekly_state['loss']*100:.2f}%
📊 Trades today: {daily_state['trades_today']}/{MAX_TRADES_PER_DAY}
📈 Performance:
  W/L: {performance['wins']}/{performance['losses']}
  Streak: {performance['consecutive_wins']}W / {performance['consecutive_losses']}L
  WR: {(performance['wins']/(performance['wins']+performance['losses'])*100) if (performance['wins']+performance['losses']) > 0 else 0:.0f}%
  Peak: ${performance['peak_balance']:,.2f}
🧊 Cooldowns: {', '.join(active_cooldowns) if active_cooldowns else 'geen'}
🔗 Connection: {'✅' if connection_healthy else '❌'}
📡 Telegram: {tg_status}
⚙️ Loop errors: {consecutive_errors}
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
        return (symbol in ASIA_ENTRY_SYMBOLS), kz
    if kz == "ny_pm":
        return (symbol in NY_PM_SYMBOLS), kz
    if kz in ENTRY_KILLZONES:
        return True, kz
    return False, kz

# ==================== SESSION HIGH/LOW TRACKING (NIEUW) ====================

def update_session_levels(symbol: str, high: float, low: float):
    """Track high/low per killzone per symbool"""
    kz = get_current_killzone()
    if not kz:
        return
    today = date.today()
    if symbol not in session_levels:
        session_levels[symbol] = {}
    key = kz
    if key not in session_levels[symbol] or session_levels[symbol][key].get("date") != today:
        session_levels[symbol][key] = {"high": high, "low": low, "date": today}
    else:
        if high > session_levels[symbol][key]["high"]:
            session_levels[symbol][key]["high"] = high
        if low < session_levels[symbol][key]["low"]:
            session_levels[symbol][key]["low"] = low

def get_previous_session_levels(symbol: str) -> Optional[dict]:
    """Haal high/low op van vorige killzone voor sweep detectie"""
    kz = get_current_killzone()
    if not kz or symbol not in session_levels:
        return None

    # Volgorde: asia → london → london_ext → new_york → ny_pm
    kz_order = ["asia", "london", "london_ext", "new_york", "ny_pm"]
    try:
        idx = kz_order.index(kz)
        if idx > 0:
            prev_kz = kz_order[idx - 1]
            if prev_kz in session_levels[symbol]:
                return session_levels[symbol][prev_kz]
    except ValueError:
        pass
    return None

# ==================== ASIA RANGE ====================

async def update_asia_range(account):
    today = date.today()
    for symbol in SYMBOLS:
        if symbol in asia_range_cache and asia_range_cache[symbol]["date"] == today:
            continue
        try:
            candles = await get_candles(account, symbol, "15m", 60)
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
        was_set = weekly_state["week"] is not None
        weekly_state.update({"week": cw, "loss": 0, "limit_hit": False, "start_balance": balance})
        if was_set:
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
        daily_state.update({"date": today, "start_balance": balance, "trades_today": 0})
        return True
    if daily_state["start_balance"] == 0:
        daily_state["start_balance"] = balance
        return True
    loss = (daily_state["start_balance"] - balance) / daily_state["start_balance"]
    if loss >= DAILY_LOSS_LIMIT:
        tg(f"🚨 <b>DAILY LIMIT</b>: {loss*100:.2f}%")
        return False
    return True

def check_cooldown(symbol: str) -> Tuple[bool, int]:
    """COOLDOWN PER SYMBOOL — andere symbolen worden niet geblokkeerd"""
    now = time.time()
    if symbol in symbol_cooldowns:
        cd = symbol_cooldowns[symbol]
        if cd.get("until", 0) > now:
            return False, int((cd["until"] - now) / 60)
    return True, 0

def register_trade_result(symbol: str, is_win: bool, profit: float = 0):
    """Update cooldown PER SYMBOOL"""
    if symbol not in symbol_cooldowns:
        symbol_cooldowns[symbol] = {"losses": 0, "until": 0}

    if is_win:
        symbol_cooldowns[symbol]["losses"] = 0
    else:
        symbol_cooldowns[symbol]["losses"] += 1
        if symbol_cooldowns[symbol]["losses"] >= COOLDOWN_AFTER_LOSSES:
            symbol_cooldowns[symbol]["until"] = time.time() + COOLDOWN_MINUTES * 60
            tg(f"🧊 <b>COOLDOWN {symbol} {COOLDOWN_MINUTES}min</b> na {symbol_cooldowns[symbol]['losses']} losses")

    update_performance(is_win, profit)

def get_dynamic_risk(balance: float, grade: str = "B+") -> float:
    """
    VEREENVOUDIGDE RISK — 3 lagen:
    1. Grade-based risk (vast)
    2. Drawdown protectie (daily loss scaling)
    3. Streak modifier (±30%)
    """
    # === LAAG 1: Grade risk ===
    risk = GRADE_RISK.get(grade, 0.010)

    # === LAAG 2: Drawdown protectie ===
    if daily_state["start_balance"] > 0:
        day_loss = (daily_state["start_balance"] - balance) / daily_state["start_balance"]
        if day_loss >= 0.02:
            risk *= 0.3
        elif day_loss >= 0.015:
            risk *= 0.5

    if performance["peak_balance"] > 0 and balance > 0:
        drawdown = (performance["peak_balance"] - balance) / performance["peak_balance"]
        if drawdown >= 0.04:
            risk *= 0.25
        elif drawdown >= 0.02:
            risk *= 0.5

    # === LAAG 3: Streak modifier ===
    if performance["consecutive_wins"] >= 4:
        risk *= STREAK_MODIFIER["wins_4plus"]
    elif performance["consecutive_wins"] >= 2:
        risk *= STREAK_MODIFIER["wins_2_3"]
    elif performance["consecutive_losses"] >= 3:
        risk *= STREAK_MODIFIER["loss_3plus"]
    elif performance["consecutive_losses"] >= 2:
        risk *= STREAK_MODIFIER["loss_2"]
    elif performance["consecutive_losses"] >= 1:
        risk *= STREAK_MODIFIER["loss_1"]

    return max(risk, 0.002)  # Min 0.2%

def update_performance(is_win: bool, profit: float = 0, grade: str = "", kz: str = ""):
    if is_win:
        performance["wins"] += 1
        performance["consecutive_wins"] += 1
        performance["consecutive_losses"] = 0
    else:
        performance["losses"] += 1
        performance["consecutive_losses"] += 1
        performance["consecutive_wins"] = 0

    performance["total_profit"] += profit
    performance["recent_results"].append(is_win)
    if len(performance["recent_results"]) > 20:
        performance["recent_results"] = performance["recent_results"][-20:]

    if grade:
        if grade not in performance["grade_stats"]:
            performance["grade_stats"][grade] = {"w": 0, "l": 0}
        if is_win:
            performance["grade_stats"][grade]["w"] += 1
        else:
            performance["grade_stats"][grade]["l"] += 1

    if kz:
        if kz not in performance["kz_stats"]:
            performance["kz_stats"][kz] = {"w": 0, "l": 0}
        if is_win:
            performance["kz_stats"][kz]["w"] += 1
        else:
            performance["kz_stats"][kz]["l"] += 1

def update_peak_balance(balance: float):
    if balance > performance["peak_balance"]:
        performance["peak_balance"] = balance
    if performance["session_start_balance"] == 0:
        performance["session_start_balance"] = balance

# ==================== ADAPTIVE TRADE DATA (MET CACHE) ====================

TRADE_DATA_FILE = "/tmp/smc_trade_history.json"

def save_trade_data(trade: dict):
    try:
        history = load_trade_history()
        history.append(trade)
        if len(history) > 500:
            history = history[-500:]
        with open(TRADE_DATA_FILE, "w") as f:
            json.dump(history, f)
    except Exception as e:
        log.debug(f"Trade data save error: {e}")

def load_trade_history() -> list:
    try:
        if os.path.exists(TRADE_DATA_FILE):
            with open(TRADE_DATA_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return []

def get_adaptive_boosts() -> dict:
    """
    Cached adaptive boosts — herbereken max 1x per uur.
    Vereist minimaal 20 trades per categorie voor betrouwbaarheid.
    """
    now = time.time()
    if _adaptive_cache["data"] and (now - _adaptive_cache["last_calc"]) < _adaptive_cache["ttl"]:
        return _adaptive_cache["data"]

    history = load_trade_history()
    if len(history) < 10:
        return {}

    boosts = {"symbol": {}, "confirmation": {}, "killzone": {}, "zone_type": {}}

    for category in boosts:
        stats = {}
        for t in history:
            key = t.get(category, "unknown")
            if not key:
                continue
            if key not in stats:
                stats[key] = {"wins": 0, "losses": 0, "profit": 0}
            if t.get("profit", 0) > 0:
                stats[key]["wins"] += 1
            else:
                stats[key]["losses"] += 1
            stats[key]["profit"] += t.get("profit", 0)

        for key, s in stats.items():
            total = s["wins"] + s["losses"]
            if total < 20:  # Minimum 20 trades voor betrouwbaarheid
                boosts[category][key] = 1.0
                continue
            wr = s["wins"] / total
            boosts[category][key] = round(0.6 + wr * 1.2, 2)

    _adaptive_cache["data"] = boosts
    _adaptive_cache["last_calc"] = now
    return boosts

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
        res = urllib.request.urlopen(req, timeout=5)
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

    # === VOLUME INDICATORS (NIEUW) ===
    if "tickVolume" in df.columns:
        df["volume"] = df["tickVolume"]
    elif "volume" not in df.columns:
        df["volume"] = 0
    df["avg_volume"] = df["volume"].rolling(20).mean()

    return df

# ==================== SWING POINT DETECTIE ====================

def detect_swing_points(df: pd.DataFrame, lookback: int = SWING_LOOKBACK) -> List[SwingPoint]:
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

    # === SESSION HIGH/LOW SWEEP (NIEUW) ===
    prev_session = get_previous_session_levels(symbol)
    if prev_session:
        if last["high"] > prev_session["high"] and last["close"] < prev_session["high"]:
            wick = last["high"] - max(last["close"], last["open"])
            if wick > atr * 0.25:
                return {"type": "bear", "level": prev_session["high"], "reason": "session_high_sweep", "strength": "strong"}
        if last["low"] < prev_session["low"] and last["close"] > prev_session["low"]:
            wick = min(last["close"], last["open"]) - last["low"]
            if wick > atr * 0.25:
                return {"type": "bull", "level": prev_session["low"], "reason": "session_low_sweep", "strength": "strong"}

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
    if len(df) < 3:
        return None
    last = df.iloc[-1]
    prev = df.iloc[-2]

    # === VOLUME CHECK (NIEUW) ===
    has_volume = False
    if "volume" in df.columns and "avg_volume" in df.columns:
        vol = float(last["volume"])
        avg_vol = float(last["avg_volume"])
        if avg_vol > 0 and vol > avg_vol * 1.3:
            has_volume = True

    if direction == Direction.BULL:
        lw = float(last["lower_wick"])
        cr = float(last["candle_range"])

        # Rejection wick
        if cr > 0 and lw / cr >= MIN_REJECTION_WICK_RATIO:
            if last["close"] > last["open"] and zone.contains_price(float(last["low"]), 0.2):
                return "rejection_wick_vol" if has_volume else "rejection_wick"

        # Engulfing
        if (last["close"] > last["open"] and prev["close"] < prev["open"]
            and last["body"] > prev["body"] * MIN_ENGULFING_BODY_RATIO
            and last["close"] > prev["open"] and last["open"] < prev["close"]):
            if zone.contains_price(float(last["low"]), 0.3):
                return "engulfing_vol" if has_volume else "engulfing"

        # Pin bar
        body = float(last["body"])
        if cr > 0 and body / cr < 0.3 and lw / cr > 0.5 and last["close"] > last["open"]:
            return "pin_bar_vol" if has_volume else "pin_bar"

        # Strong bullish close
        avg_body = float(df["body"].tail(20).mean())
        if (last["close"] > last["open"] and last["body"] > avg_body * MIN_STRONG_CLOSE_BODY_RATIO
            and zone.contains_price(float(last["open"]), 0.4)):
            return "strong_close_vol" if has_volume else "strong_close"

    elif direction == Direction.BEAR:
        uw = float(last["upper_wick"])
        cr = float(last["candle_range"])

        if cr > 0 and uw / cr >= MIN_REJECTION_WICK_RATIO:
            if last["close"] < last["open"] and zone.contains_price(float(last["high"]), 0.2):
                return "rejection_wick_vol" if has_volume else "rejection_wick"

        if (last["close"] < last["open"] and prev["close"] > prev["open"]
            and last["body"] > prev["body"] * MIN_ENGULFING_BODY_RATIO
            and last["close"] < prev["open"] and last["open"] > prev["close"]):
            if zone.contains_price(float(last["high"]), 0.3):
                return "engulfing_vol" if has_volume else "engulfing"

        body = float(last["body"])
        if cr > 0 and body / cr < 0.3 and uw / cr > 0.5 and last["close"] < last["open"]:
            return "pin_bar_vol" if has_volume else "pin_bar"

        avg_body = float(df["body"].tail(20).mean())
        if (last["close"] < last["open"] and last["body"] > avg_body * MIN_STRONG_CLOSE_BODY_RATIO
            and zone.contains_price(float(last["open"]), 0.4)):
            return "strong_close_vol" if has_volume else "strong_close"

    return None

# ==================== ZONE MANAGEMENT ====================

ZONE_SAVE_FILE = "/tmp/smc_zones.json"
ZONE_SAVE_INTERVAL = 60
_last_zone_save = 0

def save_zones_to_disk():
    global _last_zone_save
    now = time.time()
    if now - _last_zone_save < ZONE_SAVE_INTERVAL:
        return
    _last_zone_save = now
    try:
        data = {"zones": {}, "htf_zones": {}}
        for symbol, zones in zone_store.items():
            data["zones"][symbol] = []
            for z in zones:
                if z.is_valid:
                    data["zones"][symbol].append({
                        "type": z.type.value, "direction": z.direction.value,
                        "high": z.high, "low": z.low, "midpoint": z.midpoint,
                        "created_at": z.created_at, "status": z.status.value,
                        "test_count": z.test_count, "symbol": z.symbol, "timeframe": z.timeframe,
                    })
        for symbol, zones in htf_zone_store.items():
            data["htf_zones"][symbol] = []
            for z in zones:
                if z.is_valid:
                    data["htf_zones"][symbol].append({
                        "type": z.type.value, "direction": z.direction.value,
                        "high": z.high, "low": z.low, "midpoint": z.midpoint,
                        "created_at": z.created_at, "status": z.status.value,
                        "test_count": z.test_count, "symbol": z.symbol, "timeframe": z.timeframe,
                    })
        with open(ZONE_SAVE_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        log.warning(f"Zone save error: {e}")

def load_zones_from_disk():
    try:
        if not os.path.exists(ZONE_SAVE_FILE):
            return 0
        with open(ZONE_SAVE_FILE, "r") as f:
            data = json.load(f)
        count = 0
        now = time.time()
        max_age = ZONE_MAX_AGE_HOURS * 3600

        for store, key in [(zone_store, "zones"), (htf_zone_store, "htf_zones")]:
            for symbol, zones_data in data.get(key, {}).items():
                if symbol not in store:
                    store[symbol] = []
                for zd in zones_data:
                    if now - zd["created_at"] > max_age:
                        continue
                    zone = Zone(
                        type=ZoneType(zd["type"]), direction=Direction(zd["direction"]),
                        high=zd["high"], low=zd["low"], midpoint=zd["midpoint"],
                        created_at=zd["created_at"], status=ZoneStatus(zd["status"]),
                        test_count=zd["test_count"], symbol=zd.get("symbol", symbol),
                        timeframe=zd.get("timeframe", "loaded"),
                    )
                    store[symbol].append(zone)
                    count += 1
        log.info(f"Zones geladen van disk: {count} zones")
        return count
    except Exception as e:
        log.warning(f"Zone load error: {e}")
        return 0

def store_zones(symbol: str, new_zones: List[Zone], htf: bool = False):
    store = htf_zone_store if htf else zone_store
    if symbol not in store:
        store[symbol] = []
    now = time.time()
    max_age = ZONE_MAX_AGE_HOURS * 3600
    for z in new_zones:
        z.symbol = symbol
        store[symbol].append(z)
    store[symbol] = [z for z in store[symbol] if z.is_valid and (now - z.created_at) < max_age]
    if len(store[symbol]) > 20:
        store[symbol] = store[symbol][-20:]
    save_zones_to_disk()

def find_active_zone(symbol: str, price: float, direction: Direction) -> Optional[Zone]:
    if symbol not in zone_store:
        return None

    tf_priority = {"15m": 0, "5m": 1}
    candidates = []

    for zone in zone_store[symbol]:
        if not zone.is_valid or zone.direction != direction:
            continue
        if zone.contains_price(price, buffer_pct=0.25):
            priority = tf_priority.get(zone.timeframe, 3)
            candidates.append((priority, zone))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]

def mark_zone_tested(zone: Zone):
    zone.test_count += 1
    if zone.test_count > ZONE_MAX_TESTS:
        zone.status = ZoneStatus.MITIGATED
    else:
        zone.status = ZoneStatus.MITIGATED  # 1 test = mitigated (strenger)

def update_zone_status(symbol: str, df: pd.DataFrame):
    current_price = float(df["close"].iloc[-1])
    for store in [zone_store, htf_zone_store]:
        if symbol not in store:
            continue
        for zone in store[symbol]:
            if not zone.is_valid:
                continue
            if zone.direction == Direction.BULL and current_price < zone.low:
                zone.status = ZoneStatus.MITIGATED
            elif zone.direction == Direction.BEAR and current_price > zone.high:
                zone.status = ZoneStatus.MITIGATED

# ==================== MULTI-TF ZONE CONFLUENCE (NIEUW) ====================

def check_zone_confluence(zone: Zone, symbol: str) -> bool:
    """Check of een 5M/15M zone BINNEN een 1H zone valt → confluence"""
    if symbol not in htf_zone_store:
        return False
    for z1h in htf_zone_store[symbol]:
        if not z1h.is_valid or z1h.direction != zone.direction:
            continue
        # Zone valt binnen 1H zone (met kleine tolerantie)
        if zone.low >= z1h.low * 0.998 and zone.high <= z1h.high * 1.002:
            return True
        # Zone overlapt significant met 1H zone
        overlap_low = max(zone.low, z1h.low)
        overlap_high = min(zone.high, z1h.high)
        if overlap_high > overlap_low:
            overlap_size = overlap_high - overlap_low
            zone_size = zone.high - zone.low
            if zone_size > 0 and overlap_size / zone_size > 0.5:
                return True
    return False

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

# ==================== HTF BIAS (1H) + ZONES ====================

async def get_htf_bias(account, symbol: str) -> Optional[Direction]:
    """1H bias + zone detectie op 1H timeframe"""
    try:
        candles = await get_candles(account, symbol, "1h", 120)
        if not candles or len(candles) < 60:
            return None
        df = pd.DataFrame(candles)
        df = calculate_indicators(df)
        price = float(df["close"].iloc[-1])
        ema50 = float(df["ema50"].iloc[-1])
        ema200 = float(df["ema200"].iloc[-1])

        # === NIEUW: Detecteer zones op 1H ===
        swings_1h = detect_swing_points(df, lookback=4)  # Grotere lookback voor 1H
        structure_1h = analyze_structure(df, swings_1h)
        if structure_1h:
            new_obs_1h = detect_order_blocks(df, structure_1h)
            for ob in new_obs_1h:
                ob.timeframe = "1h"
            store_zones(symbol, new_obs_1h, htf=True)

            new_fvgs_1h = detect_fvgs(df, lookback=8)
            for fvg in new_fvgs_1h:
                fvg.timeframe = "1h"
            store_zones(symbol, new_fvgs_1h, htf=True)

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

def grade_setup(htf_bias, structure, zone, confirmation, sweep, premium_discount, regime, direction, symbol="", has_confluence=False):
    """
    Verplicht: Zone + Confirmatie
    Confluence (1H zone overlap) geeft +2.0 bonus
    Volume confirmatie geeft +1.0 bonus
    Min grade: B+ (score >= 6)
    """
    score = 0
    reasons = []

    if not zone or not confirmation:
        return "D", 0, 0, ["NO_ZONE" if not zone else "NO_CONFIRM"]

    reasons.append(f"ZONE({zone.type.value})")
    score += 2.0

    if hasattr(zone, 'timeframe') and zone.timeframe == "15m":
        score += 1.0
        reasons.append("HTF_ZONE")

    reasons.append(f"CONFIRM({confirmation})")
    score += 2.0

    # === VOLUME BONUS (NIEUW) ===
    if "_vol" in confirmation:
        score += 1.0
        reasons.append("VOLUME✓")

    # === CONFLUENCE BONUS (NIEUW) ===
    if has_confluence:
        score += 2.0
        reasons.append("CONFLUENCE_1H")

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

    if regime == "trending":
        score += 1.0
        reasons.append("TRENDING")
    elif regime == "ranging":
        score -= 1.0
        reasons.append("RANGING⚠️")

    if zone.type == ZoneType.ORDER_BLOCK and zone.structure_break:
        score += 0.5
        reasons.append("OB+STRUCT")

    # Grade bepaling — MINIMUM B+ (score >= 6)
    if score >= 10:
        has_sweep = any("SWEEP" in r for r in reasons)
        has_pd = any("P/D_ALIGNED" in r for r in reasons)
        has_choch = any("CHoCH" in r for r in reasons)
        has_conf = any("CONFLUENCE" in r for r in reasons)
        if has_sweep or has_pd or has_choch or has_conf:
            return "A+", 1.0, score, reasons
        else:
            return "A", 1.0, score, reasons
    elif score >= 8:
        return "A", 0.75, score, reasons
    elif score >= 6:
        return "B+", 0.6, score, reasons
    # Alles onder 6 = geen trade
    return "D", 0, score, reasons

# ==================== LOT SIZE ====================

def calculate_lot_size(balance: float, sl_distance: float, symbol: str, risk_pct: float) -> Tuple[float, dict]:
    if sl_distance <= 0:
        return 0, {"error": "sl_distance <= 0"}
    spec = SYMBOL_SPECS.get(symbol)
    if not spec:
        return 0, {"error": "unknown symbol"}

    sl_pips = sl_distance / spec["pip_size"]
    risk_amount = balance * risk_pct
    denom = sl_pips * spec["pip_value_per_lot"]
    if denom <= 0:
        return 0, {"error": "denom <= 0"}

    raw_lot = risk_amount / denom
    limits = LOT_LIMITS.get(spec["category"], {"min": 0.01, "max": 3.0})
    min_lot = spec.get("min_lot", 0.01)
    lot_step = spec.get("lot_step", 0.01)

    lot = math.floor(raw_lot / lot_step) * lot_step
    lot = round(lot, 2)
    lot = max(min_lot, min(lot, limits["max"]))

    if raw_lot < min_lot * 0.5:
        return 0, {"error": f"lot te klein: {raw_lot:.4f} < min {min_lot}"}

    details = {
        "risk_amount": round(risk_amount, 2),
        "sl_pips": round(sl_pips, 1),
        "raw_lot": round(raw_lot, 4),
        "final_lot": lot,
        "capped": raw_lot > limits["max"],
        "floored": raw_lot < min_lot,
        "category": spec["category"],
    }
    return lot, details

# ==================== SL / TP BEREKENING ====================

def calculate_trade_levels(direction: Direction, entry: float, zone: Zone, df: pd.DataFrame, symbol: str = "", spread: float = 0):
    """
    SL achter zone, TP1/TP2/TP3 voor 33/33/33 partial close.
    SPREAD-WEIGHTED RR: entry wordt gecorrigeerd voor spread.
    """
    atr = float(df["atr"].iloc[-1])
    buffer = atr * 0.15
    max_sl_distance = atr * 2.5

    spec = SYMBOL_SPECS.get(symbol, {})
    category = spec.get("category", "forex")
    pip_size = spec.get("pip_size", 0.0001)
    min_sl_pips = MIN_SL_PIPS.get(category, 15)
    min_sl_dist = min_sl_pips * pip_size

    is_gold = GOLD_SCALP["enabled"] and symbol == GOLD_SCALP["symbol"]
    tp1_mult = GOLD_SCALP["tp1_mult"] if is_gold else 2.0
    tp2_mult = GOLD_SCALP["tp2_mult"] if is_gold else 3.0
    tp3_mult = GOLD_SCALP["tp3_mult"] if is_gold else 4.5

    # === SPREAD-WEIGHTED ENTRY (NIEUW) ===
    # Effective entry = entry + spread cost
    effective_entry = entry + spread if direction == Direction.BULL else entry - spread

    if direction == Direction.BULL:
        sl = zone.low - buffer
        sl_dist = effective_entry - sl

        if sl_dist < max(atr * 1.0, min_sl_dist):
            sl_dist = max(atr * 1.0, min_sl_dist)
            sl = effective_entry - sl_dist

        if sl_dist > max_sl_distance:
            sl = effective_entry - max_sl_distance
            sl_dist = max_sl_distance

        tp1 = entry + sl_dist * tp1_mult
        tp2 = entry + sl_dist * tp2_mult
        tp3 = entry + sl_dist * tp3_mult
    else:
        sl = zone.high + buffer
        sl_dist = sl - effective_entry

        if sl_dist < max(atr * 1.0, min_sl_dist):
            sl_dist = max(atr * 1.0, min_sl_dist)
            sl = effective_entry + sl_dist

        if sl_dist > max_sl_distance:
            sl = effective_entry + max_sl_distance
            sl_dist = max_sl_distance

        tp1 = entry - sl_dist * tp1_mult
        tp2 = entry - sl_dist * tp2_mult
        tp3 = entry - sl_dist * tp3_mult

    return sl, tp1, tp2, tp3, sl_dist

# ==================== POSITION MANAGEMENT ====================

async def manage_positions(conn, positions: list):
    """
    33/33/33 partial close strategie:
    1. 33% close bij TP1 (2.0R) → SL naar breakeven
    2. 33% close bij TP2 (3.0R) → SL naar TP1
    3. 33% runner naar TP3 met trailing SL
    """
    now = datetime.now(timezone.utc)

    # === VRIJDAG AUTO-CLOSE ===
    if now.weekday() == 4 and now.hour >= 21 and now.minute >= 30:
        for pos in positions:
            try:
                symbol = pos["symbol"]
                pid = pos["id"]
                profit = pos.get("profit", 0) + pos.get("swap", 0) + pos.get("commission", 0)
                await asyncio.wait_for(conn.close_position(pid), timeout=10)
                emoji = "💰" if profit >= 0 else "💔"
                tg(f"🕐 <b>FRIDAY CLOSE</b>: {symbol} {emoji} {'+'if profit>=0 else ''}{profit:.2f}")
            except Exception as e:
                log.warning(f"Friday close error {pos.get('symbol','?')}: {e}")
        return

    # === TRADE MANAGEMENT PER POSITIE ===
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
            spec = SYMBOL_SPECS.get(symbol)
            if cur_p <= 0 or vol <= 0 or not spec:
                continue
            is_buy = "BUY" in ptype
            profit_dist = (cur_p - open_p) if is_buy else (open_p - cur_p)
            sl_dist = abs(open_p - sl) if sl else 0
            if sl_dist <= 0:
                continue

            min_lot = spec.get("min_lot", 0.01)
            lot_step = spec.get("lot_step", 0.01)

            is_gold = GOLD_SCALP["enabled"] and symbol == GOLD_SCALP["symbol"]

            # === FASE 1: PARTIAL 33% bij TP1 (2.0R) ===
            if profit_dist >= sl_dist * 2.0:
                partial = math.floor((vol * 0.33) / lot_step) * lot_step
                partial = round(partial, 2)
                remaining = round(vol - partial, 2)

                # Check of SL nog ver van entry is (= nog geen partial gedaan)
                be_zone = abs(sl - open_p) < sl_dist * 0.3 if sl else False

                if partial >= min_lot and remaining >= min_lot and not be_zone:
                    try:
                        await asyncio.wait_for(
                            conn.close_position_partially(pid, partial),
                            timeout=10
                        )
                        # SL naar breakeven
                        be_buf = sl_dist * 0.1
                        new_sl = (open_p + be_buf) if is_buy else (open_p - be_buf)
                        await asyncio.wait_for(
                            conn.modify_position(pid, stop_loss=new_sl, take_profit=tp),
                            timeout=10
                        )
                        tg(f"✅ <b>TP1 HIT</b>: {symbol}\n💰 33% gesloten ({partial} lots)\n🛡️ SL → breakeven\n📊 Runner: {remaining} lots")
                    except Exception as e:
                        log.warning(f"Partial TP1 error {symbol}: {e}")

            # === FASE 2: PARTIAL 33% bij TP2 (3.0R) ===
            if profit_dist >= sl_dist * 3.0:
                # Check of SL bij breakeven is (= TP1 partial al gedaan)
                sl_near_be = abs(sl - open_p) < sl_dist * 0.4 if sl else False
                # Check of SL NIET al bij TP1 is (= TP2 partial nog niet gedaan)
                tp1_level = (open_p + sl_dist * 2.0) if is_buy else (open_p - sl_dist * 2.0)
                sl_near_tp1 = abs(sl - tp1_level) < sl_dist * 0.3 if sl else False

                if sl_near_be and not sl_near_tp1:
                    partial2 = math.floor((vol * 0.5) / lot_step) * lot_step  # 50% van remaining ≈ 33% van original
                    partial2 = round(partial2, 2)
                    remaining2 = round(vol - partial2, 2)

                    if partial2 >= min_lot and remaining2 >= min_lot:
                        try:
                            await asyncio.wait_for(
                                conn.close_position_partially(pid, partial2),
                                timeout=10
                            )
                            # SL naar TP1 level
                            new_sl = tp1_level
                            await asyncio.wait_for(
                                conn.modify_position(pid, stop_loss=round(new_sl, 5), take_profit=tp),
                                timeout=10
                            )
                            tg(f"✅ <b>TP2 HIT</b>: {symbol}\n💰 33% gesloten ({partial2} lots)\n🛡️ SL → TP1 level\n📊 Runner: {remaining2} lots naar TP3")
                        except Exception as e:
                            log.warning(f"Partial TP2 error {symbol}: {e}")

            # === FASE 3: TRAILING SL bij 4.0R+ (runner) ===
            if profit_dist >= sl_dist * 4.0:
                trail_dist = sl_dist * 1.5
                if is_buy:
                    new_sl = cur_p - trail_dist
                    if sl and new_sl > sl + spec["pip_size"]:
                        try:
                            await asyncio.wait_for(
                                conn.modify_position(pid, stop_loss=round(new_sl, 5), take_profit=tp),
                                timeout=10
                            )
                        except Exception:
                            pass
                else:
                    new_sl = cur_p + trail_dist
                    if sl and new_sl < sl - spec["pip_size"]:
                        try:
                            await asyncio.wait_for(
                                conn.modify_position(pid, stop_loss=round(new_sl, 5), take_profit=tp),
                                timeout=10
                            )
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
                symbol = deal.get("symbol", "UNKNOWN")

                journal_match = None
                for j in reversed(trade_journal):
                    if j.get("symbol") == symbol:
                        journal_match = j
                        break

                kz = get_current_killzone() or "unknown"
                grade = journal_match.get("grade", "") if journal_match else ""
                confirmation = journal_match.get("confirmation", "") if journal_match else ""
                zone_type = journal_match.get("zone_type", "") if journal_match else ""
                direction = journal_match.get("direction", "") if journal_match else ""

                register_trade_result(symbol, profit > 0, profit)

                trade_data = {
                    "time": now.isoformat(),
                    "symbol": symbol,
                    "direction": direction,
                    "profit": profit,
                    "grade": grade,
                    "confirmation": confirmation,
                    "zone_type": zone_type,
                    "killzone": kz,
                    "won": profit > 0,
                }
                save_trade_data(trade_data)

                emoji = "💰" if profit > 0 else "💔"
                tg(f"{emoji} <b>TRADE {'WON' if profit>0 else 'LOST'}</b>: {symbol} {'+'if profit>0 else ''}{profit:.2f}")
    except Exception as e:
        log.debug(f"Closed trades error: {e}")

# ==================== CANDLE CLOSE CHECK ====================

def is_candle_just_closed(tf_min: int = 5) -> bool:
    now = datetime.now(timezone.utc)
    return now.minute % tf_min == 0 and now.second < 60

# ==================== HOOFDSTRATEGIE ====================

async def analyze_and_find_setup(account, conn, symbol, positions, balance) -> Optional[TradeSetup]:
    """
    Top-down (ZONDER momentum entries):
    1. HTF (1H) bias + zone detectie
    2. MTF (15M) structuur + zones
    3. LTF (5M) entry bij zone + confirmatie
    4. Zone confluence check (1H overlap)
    5. Spread-weighted RR
    6. Grade en valideer (min B+)
    """
    try:
        # Stap 1: HTF Bias + 1H zones
        htf_bias = await get_htf_bias(account, symbol)

        # Stap 2: MTF (15M)
        candles_15m = await get_candles(account, symbol, "15m", 100)
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
        candles_5m = await get_candles(account, symbol, "5m", 100)
        if not candles_5m or len(candles_5m) < 50:
            return None
        df_5m = pd.DataFrame(candles_5m)
        df_5m = calculate_indicators(df_5m)
        swings_5m = detect_swing_points(df_5m)
        update_zone_status(symbol, df_5m)

        # Update session levels
        update_session_levels(symbol, float(df_5m["high"].iloc[-1]), float(df_5m["low"].iloc[-1]))

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
        elif structure_15m and structure_15m.type == StructureType.BOS:
            direction = structure_15m.direction

        if not direction:
            return None

        # P/D filter
        if direction == Direction.BULL and pd_zone == "premium":
            if not (sweep and sweep["type"] == "bull"):
                return None
        if direction == Direction.BEAR and pd_zone == "discount":
            if not (sweep and sweep["type"] == "bear"):
                return None

        # Zoek zone — GEEN momentum entry fallback
        active_zone = find_active_zone(symbol, current_price, direction)
        if not active_zone:
            return None  # Geen zone = geen trade. Punt.

        # Check confirmatie
        confirmation = check_confirmation(df_5m, direction, active_zone)
        if not confirmation:
            return None

        # === ZONE CONFLUENCE CHECK (NIEUW) ===
        has_confluence = check_zone_confluence(active_zone, symbol)

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
            symbol=symbol, has_confluence=has_confluence,
        )

        # Min grade B+ — alles daaronder = geen trade
        if grade not in ("A+", "A", "B+"):
            return None

        # Levels met spread-weighted RR
        price_data = await rate_limited_call(conn.get_symbol_price(symbol))
        if not price_data:
            return None
        entry = price_data["ask"] if direction == Direction.BULL else price_data["bid"]
        actual_spread = price_data["ask"] - price_data["bid"]

        sl, tp1, tp2, tp3, sl_dist = calculate_trade_levels(direction, entry, active_zone, df_5m, symbol, actual_spread)

        # === SPREAD-WEIGHTED RR (NIEUW) ===
        effective_entry = entry + actual_spread if direction == Direction.BULL else entry - actual_spread
        rr = abs((tp3 - effective_entry) / (abs(effective_entry - sl))) if sl_dist > 0 else 0

        # Gold scalp: lagere RR toegestaan
        is_gold_scalp = GOLD_SCALP["enabled"] and symbol == GOLD_SCALP["symbol"]
        min_rr = GOLD_SCALP["min_rr"] if is_gold_scalp else MIN_RR
        if rr < min_rr:
            return None

        mark_zone_tested(active_zone)

        return TradeSetup(
            symbol=symbol, direction=direction, entry=entry,
            stop_loss=sl, tp1=tp1, tp2=tp2, tp3=tp3, zone=active_zone,
            grade=grade, score=score, risk_mult=risk_mult,
            reasons=reasons, regime=regime, rr=rr,
            htf_bias=htf_bias, mtf_structure=structure_15m,
            confirmation=confirmation, confluence=has_confluence,
        )
    except Exception as e:
        log.error(f"Analyse error {symbol}: {e}")
        return None

# ==================== TRADE EXECUTIE ====================

async def execute_trade(conn, setup: TradeSetup, balance: float) -> bool:
    # === VEREENVOUDIGDE RISK: alleen grade + drawdown + streak ===
    risk_pct = get_dynamic_risk(balance, grade=setup.grade) * setup.risk_mult

    sl_dist = abs(setup.entry - setup.stop_loss)
    lot, lot_details = calculate_lot_size(balance, sl_dist, setup.symbol, risk_pct)

    if lot < 0.01:
        return False

    # === MARGIN CHECK ===
    try:
        info = await rate_limited_call(conn.get_account_information())
        free_margin = info.get("freeMargin", balance)
        spec = SYMBOL_SPECS.get(setup.symbol, {})
        leverage = spec.get("leverage", 20)
        margin_needed = (lot * spec.get("contract", 100000) * setup.entry) / leverage
        max_margin_use = free_margin * 0.80

        if margin_needed > max_margin_use and margin_needed > 0:
            reduction = max_margin_use / margin_needed
            old_lot = lot
            min_lot = spec.get("min_lot", 0.01)
            lot_step = spec.get("lot_step", 0.01)
            lot = math.floor((lot * reduction) / lot_step) * lot_step
            lot = round(max(min_lot, lot), 2)
            lot_details["margin_reduced"] = True
            lot_details["original_lot"] = old_lot

            final_margin = (lot * spec.get("contract", 100000) * setup.entry) / leverage
            if final_margin > free_margin * 0.90:
                tg(f"⚠️ <b>MARGIN SKIP</b>: {setup.symbol}\nMargin: €{final_margin:.0f} | Free: €{free_margin:.0f}")
                return False
    except Exception as e:
        log.warning(f"Margin check error: {e}")
        spec = SYMBOL_SPECS.get(setup.symbol, {})
        lot = min(lot, spec.get("min_lot", 0.01))
        if lot < spec.get("min_lot", 0.01):
            return False

    update_peak_balance(balance)

    try:
        if setup.direction == Direction.BULL:
            result = await asyncio.wait_for(
                conn.create_market_buy_order(setup.symbol, lot, setup.stop_loss, setup.tp3),
                timeout=15
            )
        else:
            result = await asyncio.wait_for(
                conn.create_market_sell_order(setup.symbol, lot, setup.stop_loss, setup.tp3),
                timeout=15
            )

        if not result or result.get("stringCode") == "ERR_NO_ERROR" or "orderId" not in str(result):
            await asyncio.sleep(1)
            positions = await rate_limited_call(conn.get_positions())
            if positions:
                found = any(p.get("symbol") == setup.symbol for p in positions)
                if not found:
                    tg(f"❌ <b>ORDER NOT CONFIRMED</b>: {setup.symbol}")
                    return False
            mark_api_success()
        else:
            mark_api_success()
    except Exception as e:
        tg(f"❌ <b>ORDER FAIL</b>: {setup.symbol} — {e}")
        mark_api_failure()
        return False

    daily_state["trades_today"] += 1
    kz = get_current_killzone()

    trade_journal.append({
        "time": datetime.now(timezone.utc).isoformat(),
        "symbol": setup.symbol, "direction": setup.direction.value,
        "grade": setup.grade, "score": setup.score,
        "entry": setup.entry, "sl": setup.stop_loss,
        "tp1": setup.tp1, "tp2": setup.tp2, "tp3": setup.tp3,
        "lot": lot, "rr": setup.rr, "risk_pct": risk_pct,
        "reasons": setup.reasons, "regime": setup.regime,
        "confirmation": setup.confirmation, "zone_type": setup.zone.type.value,
        "lot_details": lot_details, "confluence": setup.confluence,
    })

    r = " | ".join(setup.reasons)
    de = "🟢" if setup.direction == Direction.BULL else "🔴"
    cap_warn = " ⚠️CAP" if lot_details.get("capped") else ""
    margin_warn = " ⚠️MARGIN" if lot_details.get("margin_reduced") else ""
    wr = sum(performance["recent_results"][-10:]) / max(len(performance["recent_results"][-10:]), 1) * 100
    conf_str = " | 🏛️ 1H CONFLUENCE" if setup.confluence else ""

    tg(f"""<b>{de} TRADE OPENED — Grade {setup.grade}</b>
📌 {setup.symbol} | {setup.direction.value.upper()}
🕐 KZ: {kz.upper() if kz else '?'}
🔍 Regime: {setup.regime.upper()}{conf_str}
💰 Entry: {setup.entry:.5f}
🛑 SL: {setup.stop_loss:.5f}
🎯 TP1: {setup.tp1:.5f} (33%)
🎯 TP2: {setup.tp2:.5f} (33%)
🎯 TP3: {setup.tp3:.5f} (runner 33%)
📊 RR: 1:{setup.rr:.1f} | Lots: {lot}{cap_warn}{margin_warn}
💵 Risk: {risk_pct*100:.2f}% (${lot_details['risk_amount']})
📏 SL: {lot_details['sl_pips']:.1f} pips
✅ Confirm: {setup.confirmation}
🗺️ Zone: {setup.zone.type.value} ({setup.zone.timeframe})
📈 Streak: {performance['consecutive_wins']}W / {performance['consecutive_losses']}L
🎯 Recent WR: {wr:.0f}%
📋 Score: {setup.score:.1f} | {r}
💰 Balance: ${balance:,.2f}""")

    return True

# ==================== DIAGNOSTIEK ====================

async def run_diagnostics(conn, account):
    log.info("=" * 60)
    log.info("DIAGNOSTICS — SMC Bot v3.2 REFINED")
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
    log.info(f"Min RR: {MIN_RR} | Max trades: {MAX_TOTAL_TRADES} | Max/day: {MAX_TRADES_PER_DAY}")
    log.info(f"Min grade: B+ | Zone tests: {ZONE_MAX_TESTS} | Swing lookback: {SWING_LOOKBACK}")
    for s in SYMBOLS:
        try:
            candles = await get_candles(account, s, "5m", 20)
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
        log.info("Connecting to MetaAPI…")
        api = MetaApi(METAAPI_TOKEN)
        account = await api.metatrader_account_api.get_account(ACCOUNT_ID)

        conn = None
        for attempt in range(5):
            try:
                log.info(f"Connection poging {attempt + 1}/5…")
                account_info = account.state
                log.info(f"Account state: {account_info}")

                if account_info != "DEPLOYED":
                    log.info("Account niet deployed, deploying…")
                    try:
                        await account.deploy()
                    except Exception as e:
                        log.warning(f"Deploy fout (kan normaal zijn): {e}")
                    await asyncio.sleep(5)

                try:
                    await asyncio.wait_for(account.wait_connected(), timeout=60)
                    log.info("Account connected!")
                except asyncio.TimeoutError:
                    log.warning(f"Account connect timeout poging {attempt + 1}")
                    if attempt >= 2:
                        log.info("Forcing undeploy + redeploy…")
                        try:
                            await account.undeploy()
                            await asyncio.sleep(10)
                            await account.deploy()
                            await asyncio.sleep(10)
                        except Exception as e:
                            log.warning(f"Redeploy fout: {e}")
                    await asyncio.sleep(10)
                    continue

                conn = account.get_rpc_connection()
                await conn.connect()
                log.info("Synchroniseren…")
                try:
                    await asyncio.wait_for(conn.wait_synchronized(), timeout=90)
                    log.info("Gesynchroniseerd!")
                    break
                except asyncio.TimeoutError:
                    log.warning(f"Sync timeout poging {attempt + 1}")
                    try:
                        await conn.close()
                    except Exception:
                        pass
                    conn = None
                    await asyncio.sleep(15)
                    continue
            except Exception as e:
                log.error(f"Connection poging {attempt + 1} mislukt: {e}")
                if conn:
                    try:
                        await conn.close()
                    except Exception:
                        pass
                    conn = None
                await asyncio.sleep(15)

        if not conn:
            tg("❌ <b>FATAL: Kon niet verbinden na 5 pogingen</b>")
            raise Exception("Connection failed after 5 attempts")

        try:
            test_info = await asyncio.wait_for(conn.get_account_information(), timeout=15)
            log.info(f"Connection verified: ${test_info['balance']} balance")
        except Exception as e:
            tg(f"⚠️ Connection verificatie mislukt: {e}")
            raise Exception(f"Connection verification failed: {e}")

        await asyncio.sleep(2)

        global last_heartbeat, watchdog_last_loop, consecutive_errors, consecutive_api_fails
        last_heartbeat = 0
        watchdog_last_loop = time.time()
        consecutive_errors = 0

        try:
            init_info = await conn.get_account_information()
            performance["session_start_balance"] = init_info["balance"]
            performance["peak_balance"] = init_info["balance"]
        except Exception:
            pass

        zones_loaded = load_zones_from_disk()
        mark_api_success()
        await run_diagnostics(conn, account)

        tg(f"""🚀 <b>SMC BOT v3.2 REFINED GESTART</b>
📊 {len(SYMBOLS)} symbols | Entry KZs: {', '.join(ENTRY_KILLZONES)}
🎯 Min RR: {MIN_RR} | Max trades: {MAX_TOTAL_TRADES} | Max/dag: {MAX_TRADES_PER_DAY}
🏛️ Min grade: B+ | Zone tests: {ZONE_MAX_TESTS}
🗺️ Zones geladen: {zones_loaded}
⚡ <b>v3.2 Verbeteringen:</b>
• 1H zone confluence detectie
• Session high/low sweep tracking
• Spread-weighted RR
• 33/33/33 partial close
• Cooldown per symbool
• Volume confirmatie
• Geen momentum entries""")

        while True:
            try:
                watchdog_last_loop = time.time()
                consecutive_errors = 0

                await send_heartbeat(conn)

                if needs_reconnect():
                    log.warning(f"API failed {consecutive_api_fails}x — reconnecting…")
                    tg("⚠️ <b>CONNECTION ISSUE</b> — auto-recovering…")
                    reconnected = False

                    for attempt in range(3):
                        try:
                            log.info(f"Soft reconnect {attempt + 1}/3...")
                            try:
                                await conn.close()
                            except Exception:
                                pass
                            await asyncio.sleep(5 * (attempt + 1))
                            conn = account.get_rpc_connection()
                            await conn.connect()
                            await asyncio.wait_for(conn.wait_synchronized(), timeout=30)
                            test = await asyncio.wait_for(conn.get_account_information(), timeout=10)
                            if test and "balance" in test:
                                mark_api_success()
                                connection_healthy = True
                                reconnected = True
                                tg("✅ <b>RECOVERED</b>")
                                break
                        except Exception as e:
                            log.warning(f"Soft reconnect {attempt + 1}/3: {e}")

                    if not reconnected:
                        log.info("Soft reconnect failed — wacht 2 min...")
                        reset_api_fails()
                        await asyncio.sleep(120)
                        try:
                            conn = account.get_rpc_connection()
                            await conn.connect()
                            await asyncio.wait_for(conn.wait_synchronized(), timeout=60)
                            test = await asyncio.wait_for(conn.get_account_information(), timeout=10)
                            if test and "balance" in test:
                                mark_api_success()
                                reconnected = True
                                tg("✅ <b>RECOVERED</b> na 2 min")
                        except Exception as e:
                            log.warning(f"2-min retry failed: {e}")

                    if not reconnected:
                        log.info("Trying undeploy/redeploy...")
                        try:
                            await account.undeploy()
                            await asyncio.sleep(15)
                            await account.deploy()
                            await asyncio.sleep(30)
                            conn = account.get_rpc_connection()
                            await conn.connect()
                            await asyncio.wait_for(conn.wait_synchronized(), timeout=90)
                            test = await asyncio.wait_for(conn.get_account_information(), timeout=10)
                            if test and "balance" in test:
                                mark_api_success()
                                reconnected = True
                                tg("✅ <b>RECOVERED via REDEPLOY</b>")
                        except Exception as e:
                            log.error(f"Redeploy failed: {e}")

                    if not reconnected:
                        reset_api_fails()
                        tg("⚠️ <b>CONNECTION DOWN</b> — wacht 5 min...")
                        await asyncio.sleep(300)
                        try:
                            conn = account.get_rpc_connection()
                            await conn.connect()
                            await asyncio.wait_for(conn.wait_synchronized(), timeout=60)
                            mark_api_success()
                            tg("✅ <b>RECOVERED</b> na 5 min")
                        except Exception:
                            log.warning("Nog steeds geen connectie")
                        continue

                kz = get_current_killzone()

                if kz == "asia":
                    await update_asia_range(account)

                if kz not in ENTRY_KILLZONES:
                    try:
                        info = await rate_limited_call(conn.get_account_information())
                        positions = await rate_limited_call(conn.get_positions())
                        if info and positions:
                            await manage_positions(conn, positions)
                    except Exception:
                        pass
                    await asyncio.sleep(30)
                    continue

                info = await rate_limited_call(conn.get_account_information())
                if not info or "balance" not in info:
                    log.warning("Kon account info niet ophalen, skip cyclus")
                    await asyncio.sleep(10)
                    continue

                balance = info["balance"]
                equity = info["equity"]
                positions = await rate_limited_call(conn.get_positions())
                if positions is None:
                    positions = []

                update_peak_balance(equity)
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

                if not is_candle_just_closed(5):
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue

                if not await news_filter():
                    await asyncio.sleep(60)
                    continue

                if len(positions) >= MAX_TOTAL_TRADES:
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue

                if daily_state["trades_today"] >= MAX_TRADES_PER_DAY:
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue

                for symbol in SYMBOLS:
                    watchdog_last_loop = time.time()
                    try:
                        allowed, _ = is_entry_allowed(symbol)
                        if not allowed:
                            continue

                        # Gold scalp max trades
                        if GOLD_SCALP["enabled"] and symbol == GOLD_SCALP["symbol"]:
                            max_trades = GOLD_SCALP["max_trades"]
                        else:
                            max_trades = MAX_TRADES_PER_ASSET

                        if sum(1 for p in positions if p["symbol"] == symbol) >= max_trades:
                            continue

                        if not check_correlation(symbol, positions):
                            continue

                        # === COOLDOWN PER SYMBOOL ===
                        cd_ok, cd_remaining = check_cooldown(symbol)
                        if not cd_ok:
                            continue

                        is_gold_scalp = GOLD_SCALP["enabled"] and symbol == GOLD_SCALP["symbol"]
                        dedup_secs = GOLD_SCALP["dedup_seconds"] if is_gold_scalp else 900
                        sig_key = f"{symbol}_{int(time.time()/dedup_secs)}"
                        if sig_key in recent_signals:
                            continue

                        setup = await analyze_and_find_setup(account, conn, symbol, positions, balance)
                        if not setup:
                            continue

                        success = await execute_trade(conn, setup, balance)
                        if success:
                            recent_signals[sig_key] = time.time()
                    except Exception as e:
                        log.debug(f"Symbol {symbol} error (skipping): {e}")
                    await asyncio.sleep(0.5)

                now = time.time()
                for k in list(recent_signals.keys()):
                    if now - recent_signals[k] > 7200:
                        del recent_signals[k]

                await asyncio.sleep(CHECK_INTERVAL)

            except (asyncio.CancelledError, asyncio.exceptions.CancelledError) as e:
                consecutive_errors += 1
                log.warning(f"CancelledError #{consecutive_errors}: {e}")
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    tg(f"🚨 <b>TOO MANY DISCONNECTS</b> — force restart")
                    raise Exception("Too many CancelledErrors")
                await asyncio.sleep(15)
            except Exception as e:
                consecutive_errors += 1
                log.error(f"Loop error #{consecutive_errors}: {e}")
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    tg(f"🚨 <b>{MAX_CONSECUTIVE_ERRORS} CONSECUTIVE ERRORS</b> — force restart\n{str(e)[:60]}")
                    raise Exception(f"Too many consecutive errors: {consecutive_errors}")
                if consecutive_errors % 5 == 0:
                    tg(f"⚠️ <b>{consecutive_errors} LOOP ERRORS</b>\n{str(e)[:80]}")
                await asyncio.sleep(10 if "timed out" in str(e).lower() else 5)

    except (asyncio.CancelledError, asyncio.exceptions.CancelledError) as e:
        log.critical(f"FATAL CancelledError: {e}")
        tg(f"❌ <b>FATAL DISCONNECT</b> — herstarting...")
        raise Exception(f"CancelledError: {e}")
    except Exception as e:
        log.critical(f"FATAL: {e}")
        tg(f"❌ <b>FATAL</b>: {str(e)[:100]}")
        raise e

# ==================== START ====================

def watchdog_thread():
    global watchdog_last_loop
    while True:
        time.sleep(60)
        silence = time.time() - watchdog_last_loop
        if silence > watchdog_max_silence:
            log.critical(f"WATCHDOG: Loop silent for {silence:.0f}s — force restart!")
            tg(f"🐕 <b>WATCHDOG TRIGGERED</b>\nLoop niet actief voor {silence:.0f}s\nForce restart…")
            os._exit(1)

if __name__ == "__main__":
    import threading

    log.info("=" * 50)
    log.info("PROFESSIONAL SMC BOT v3.2 — REFINED")
    log.info("Zone-based | Confluence | Spread-weighted RR")
    log.info("33/33/33 Partial | Per-symbol cooldown")
    log.info("Min grade B+ | No momentum entries")
    log.info("=" * 50)

    wd = threading.Thread(target=watchdog_thread, daemon=True)
    wd.start()
    log.info("🐕 Watchdog thread gestart")

    restart_count = 0
    while True:
        try:
            restart_count += 1
            if restart_count > 1:
                tg(f"🔄 <b>AUTO RESTART #{restart_count}</b>")
            asyncio.run(run())
        except KeyboardInterrupt:
            tg("🛑 <b>BOT GESTOPT</b>")
            log.info("Stopped by user")
            break
        except Exception as e:
            tg(f"💥 <b>CRASH #{restart_count}</b>: {str(e)[:80]}\n🔄 Restart in 15s…")
            log.error(f"Crash #{restart_count}: {e}")
            wait = min(15 * restart_count, 60)
            log.info(f"Restart in {wait}s…")
            time.sleep(wait)
