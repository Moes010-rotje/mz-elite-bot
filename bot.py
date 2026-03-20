import asyncio
import pandas as pd
import numpy as np
import os
import time
import json
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
#   PROFESSIONAL SMC BOT v3.1 — AGGRESSIVE MODE
#
#   Gebouwd als een discretionaire trader denkt:
#   1. HTF (1H) bepaalt bias (trend richting)
#   2. MTF (15M) bevestigt structuur (BOS/CHoCH)
#   3. LTF (5M) entry bij POI zone + confirmatie candle
#
#   v3.1 AGGRESSIVE aanpassingen:
#   - Alle killzones actief (Asia voor JPY/Gold pairs)
#   - Versoepelde confirmatie (strong close + lagere wick ratio)
#   - Zones 2x testbaar + grotere buffer
#   - C-grade trades met halve risico
#   - Momentum entries zonder zone als HTF + structuur aligned
#   - Min RR 1.5 | Max 15 trades | Equilibrium toegestaan
#   - Target: 3-15 trades per dag
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
MIN_RR = 1.5              # Verlaagd voor meer trades (was 2.0)
DAILY_LOSS_LIMIT = 0.03   # 3% (was 2.5%)
WEEKLY_LOSS_LIMIT = 0.08  # 8% (was 6%)
MAX_TRADES_PER_ASSET = 3  # 3 per asset (was 2)
MAX_TOTAL_TRADES = 15     # 15 max (was 6)

# === GOLD PRIORITY: XAUUSD wordt zwaarder gewogen ===
PRIORITY_SYMBOLS = ["XAUUSD", "NAS100", "GBPJPY"]  # Top 3 SMC assets
PRIORITY_MAX_TRADES = 5        # 5 trades tegelijk (normaal 3)
PRIORITY_SCORE_BONUS = 2.0     # +2.0 score bonus → sneller A/A+
PRIORITY_RISK_MULT = 1.3       # 30% meer risico op priority symbolen
PRIORITY_ALL_KILLZONES = True  # Mag in ALLE killzones traden
COOLDOWN_AFTER_LOSSES = 3  # Na 3 losses (was 2)
COOLDOWN_MINUTES = 45      # 45 min (was 90)
ZONE_MAX_AGE_HOURS = 48    # Zones leven 48u (was 24)
ZONE_MAX_TESTS = 2         # 2x testbaar (was 1)
MAX_API_CALLS_PER_MIN = 60  # 7 symbolen × ~5 calls = 35 per cyclus + overhead

SWING_LOOKBACK = 2         # Snellere swing detectie (was 3)
MIN_REJECTION_WICK_RATIO = 0.45   # Versoepeld (was 0.6)
MIN_ENGULFING_BODY_RATIO = 1.1    # Versoepeld (was 1.3)
MIN_STRONG_CLOSE_BODY_RATIO = 1.5 # Nieuw: strong directional close

KILLZONES = {
    "asia":       {"start": 0,  "end": 7},
    "london":     {"start": 7,  "end": 10},
    "london_ext": {"start": 10, "end": 13},
    "new_york":   {"start": 13, "end": 16},
    "ny_pm":      {"start": 16, "end": 19},
}

ENTRY_KILLZONES = ["asia", "london", "london_ext", "new_york", "ny_pm"]
ASIA_ENTRY_SYMBOLS = ["USDJPY", "GBPJPY", "EURJPY", "XAUUSD"]
NY_PM_SYMBOLS = ["XAUUSD", "NAS100", "US30"]

SYMBOL_SPECS = {
    "XAUUSD":  {"pip_size": 0.1,    "pip_value_per_lot": 10,  "max_spread_pips": 35,  "category": "metals",  "leverage": 20, "contract": 100},
    "GBPUSD":  {"pip_size": 0.0001, "pip_value_per_lot": 10,  "max_spread_pips": 22,  "category": "forex",   "leverage": 30, "contract": 100000},
    "GBPJPY":  {"pip_size": 0.01,   "pip_value_per_lot": 6.5, "max_spread_pips": 30,  "category": "forex",   "leverage": 20, "contract": 100000},
    "USDJPY":  {"pip_size": 0.01,   "pip_value_per_lot": 6.5, "max_spread_pips": 18,  "category": "forex",   "leverage": 30, "contract": 100000},
    "EURJPY":  {"pip_size": 0.01,   "pip_value_per_lot": 6.5, "max_spread_pips": 25,  "category": "forex",   "leverage": 20, "contract": 100000},
    "NAS100":  {"pip_size": 0.1,    "pip_value_per_lot": 1,   "max_spread_pips": 25,  "category": "indices",  "leverage": 20, "contract": 1},
    "US30":    {"pip_size": 0.1,    "pip_value_per_lot": 1,   "max_spread_pips": 35,  "category": "indices",  "leverage": 20, "contract": 1},
}

# Priority symbolen eerst → worden als eerste geanalyseerd
SYMBOLS = ["XAUUSD", "NAS100", "GBPJPY"] + [s for s in SYMBOL_SPECS.keys() if s not in ["XAUUSD", "NAS100", "GBPJPY"]]

METAAPI_TOKEN = os.getenv("METAAPI_TOKEN")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT = os.getenv("TG_CHAT")

CORRELATION_GROUPS = [
    {"NAS100", "US30"},
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
watchdog_last_loop = time.time()  # Watchdog: laatste succesvolle loop iteratie
watchdog_max_silence = 600        # 10 min zonder loop = hang detected (was 300)
consecutive_errors = 0            # Tel opeenvolgende loop errors
MAX_CONSECUTIVE_ERRORS = 20       # Na 20 errors: force restart

# ===== PERFORMANCE TRACKER (voor dynamische lotsize) =====
performance = {
    "wins": 0,
    "losses": 0,
    "consecutive_wins": 0,
    "consecutive_losses": 0,
    "recent_results": [],       # Laatste 20 trades: True=win, False=loss
    "peak_balance": 0,          # Hoogste balance (drawdown calc)
    "session_start_balance": 0, # Balance bij bot start
    "total_profit": 0,
    "grade_stats": {},          # {"A+": {"w": 0, "l": 0}, ...}
    "kz_stats": {},             # {"london": {"w": 0, "l": 0}, ...}
}

# Killzone risk multipliers — meer risico in betere sessies
KILLZONE_RISK_MULT = {
    "london":     1.2,   # Beste liquidity, hogere risk
    "new_york":   1.2,
    "london_ext": 1.0,
    "ny_pm":      0.8,   # Minder volume
    "asia":       0.7,   # Laagste volume
}

# Lotsize limieten per categorie
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
    """Telegram met 3x retry en exponential backoff"""
    global tg_fail_count, tg_last_success
    if not TG_TOKEN or not TG_CHAT:
        return
    for attempt in range(3):
        try:
            url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
            payload = json.dumps({"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}).encode()
            req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=8 + attempt * 4)  # 8s, 12s, 16s
            tg_fail_count = 0
            tg_last_success = time.time()
            return
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt)  # 1s, 2s
            else:
                tg_fail_count += 1
                log.warning(f"Telegram FAILED 3x (total fails: {tg_fail_count}): {e}")


def tg_health_check() -> bool:
    """Check of Telegram nog bereikbaar is"""
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getMe"
        req = urllib.request.Request(url, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
        return True
    except Exception:
        return False

# ==================== SAFE API CALL (BULLETPROOF) ====================

async def safe_call(coro_func, *args, retries=3, timeout=30, default=None, label="API"):
    """
    Universele wrapper voor ELKE API call.
    Trackt successes en failures voor reactive reconnect.
    """
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
            mark_api_success()  # Succes → reset fail counter
            return result

        except asyncio.TimeoutError:
            log.debug(f"{label} timeout (poging {attempt+1}/{retries})")
            await asyncio.sleep(2 * (attempt + 1))
        except Exception as e:
            err = str(e).lower()
            if "not connected" in err or "not synchronized" in err or "socket" in err:
                mark_api_failure()  # Connection error → tel mee
                log.debug(f"{label} connection error: {e}")
                await asyncio.sleep(3)
            elif attempt < retries - 1:
                log.debug(f"{label} error (poging {attempt+1}): {e}")
                await asyncio.sleep(2)
            else:
                log.debug(f"{label} failed after {retries}: {e}")

    mark_api_failure()  # Alle retries gefaald
    return default


async def rate_limited_call(coro):
    """Legacy wrapper met success/failure tracking"""
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
    except Exception as e:
        mark_api_failure()
        log.debug(f"rate_limited_call error: {e}")
        return None

# ==================== CANDLE HELPER ====================

TF_MINUTES = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}

async def get_candles(account, symbol: str, timeframe: str, count: int):
    """Haal candles op met retry en timeout protectie"""
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

# ==================== CONNECTION HEALTH (REACTIVE) ====================

# Niet meer proactief checken. Alleen reconnecten als een ECHTE operatie faalt.
# Dit voorkomt onnodige disconnects.
consecutive_api_fails = 0
MAX_API_FAILS_BEFORE_RECONNECT = 5  # Pas na 5 opeenvolgende echte fails reconnecten

async def check_connection_health(conn) -> bool:
    """
    REACTIVE health check: retourneer altijd True.
    Reconnect wordt nu ALLEEN getriggerd via mark_api_failure()
    als echte operaties (candles, trades, account info) herhaaldelijk falen.
    """
    return True


def mark_api_success():
    """Call na elke succesvolle API operatie"""
    global consecutive_api_fails
    consecutive_api_fails = 0


def mark_api_failure():
    """Call na elke gefaalde API operatie. Na 5 fails → reconnect nodig."""
    global consecutive_api_fails
    consecutive_api_fails += 1
    if consecutive_api_fails >= MAX_API_FAILS_BEFORE_RECONNECT:
        log.warning(f"API failed {consecutive_api_fails}x in a row — reconnect needed")
        return True  # Reconnect nodig
    return False


def needs_reconnect() -> bool:
    """Check of we moeten reconnecten"""
    return consecutive_api_fails >= MAX_API_FAILS_BEFORE_RECONNECT

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
        # Probeer toch een minimale heartbeat te sturen
        tg(f"💓 <b>HEARTBEAT</b> (limited)\n⚠️ Data ophalen mislukt: {str(e)[:60]}\n⏰ {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
        return

    pl = equity - balance
    pl_pct = (pl / balance) * 100 if balance > 0 else 0
    kz = get_current_killzone()
    daily_loss = ((daily_state["start_balance"] - balance) / daily_state["start_balance"] * 100) if daily_state["start_balance"] > 0 else 0
    total_zones = sum(len([z for z in zones if z.is_valid]) for zones in zone_store.values())
    uptime_min = int((now - watchdog_last_loop) / 60) if watchdog_last_loop else 0
    tg_status = "✅" if tg_fail_count == 0 else f"⚠️ {tg_fail_count} fails"

    msg = f"""<b>💓 SMC v3.1 HEARTBEAT</b>

💰 Balance: ${balance:,.2f}
📊 Equity: ${equity:,.2f}
📈 P&L: ${pl:,.2f} ({pl_pct:+.2f}%)

🎯 Open trades: {len(positions)}
🗺️ Active zones: {total_zones}
🕐 Killzone: {kz.upper() if kz else 'NONE'}

📅 Daily loss: {daily_loss:.2f}%
📆 Weekly loss: {weekly_state['loss']*100:.2f}%
📊 Trades today: {daily_state['trades_today']}

📈 Performance:
  W/L: {performance['wins']}/{performance['losses']}
  Streak: {performance['consecutive_wins']}W / {performance['consecutive_losses']}L
  WR: {(performance['wins']/(performance['wins']+performance['losses'])*100) if (performance['wins']+performance['losses']) > 0 else 0:.0f}%
  Peak: ${performance['peak_balance']:,.2f}

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

    # Update performance tracker
    update_performance(is_win, profit)


def get_dynamic_risk(balance: float, grade: str = "B", kz: str = None) -> float:
    """
    DYNAMISCHE RISICO BEREKENING
    
    Factoren die risico beïnvloeden:
    1. Base risk (1%)
    2. Win/loss streak → scale up bij winst, down bij verlies
    3. Equity curve → meer risico als je in profit bent, minder bij drawdown
    4. Killzone → hogere risk in London/NY, lager in Asia
    5. Grade → A+ krijgt meer, C krijgt minder
    6. Recent winrate → boven 55% = bonus, onder 40% = penalty
    7. Dagverlies protectie → hard afschalen bij drawdown
    """
    risk = BASE_RISK  # 1%

    # === 1. LOSS STREAK PROTECTIE ===
    if daily_state["losses_in_row"] >= 3:
        risk *= 0.25    # 3+ losses: kwart risico
    elif daily_state["losses_in_row"] >= 2:
        risk *= 0.4     # 2 losses: 40%
    elif daily_state["losses_in_row"] >= 1:
        risk *= 0.6     # 1 loss: 60%

    # === 2. WIN STREAK BONUS ===
    if performance["consecutive_wins"] >= 4:
        risk *= 1.4     # 4+ wins: +40%
    elif performance["consecutive_wins"] >= 3:
        risk *= 1.25    # 3 wins: +25%
    elif performance["consecutive_wins"] >= 2:
        risk *= 1.1     # 2 wins: +10%

    # === 3. EQUITY CURVE SCALING ===
    if performance["peak_balance"] > 0 and balance > 0:
        drawdown = (performance["peak_balance"] - balance) / performance["peak_balance"]
        if drawdown >= 0.04:
            risk *= 0.25     # 4%+ drawdown: noodrem
        elif drawdown >= 0.02:
            risk *= 0.5      # 2%+ drawdown: halveer
        elif drawdown >= 0.01:
            risk *= 0.75     # 1%+ drawdown: 75%
        elif drawdown <= 0:
            # In profit → licht opschalen (compound growth)
            growth = (balance - performance["session_start_balance"]) / performance["session_start_balance"] if performance["session_start_balance"] > 0 else 0
            if growth > 0.05:
                risk *= 1.2   # 5%+ groei: +20%
            elif growth > 0.02:
                risk *= 1.1   # 2%+ groei: +10%

    # === 4. KILLZONE MULTIPLIER ===
    if kz:
        kz_mult = KILLZONE_RISK_MULT.get(kz, 1.0)
        risk *= kz_mult

    # === 5. GRADE MULTIPLIER ===
    grade_risk_mult = {
        "A+": 1.5,
        "A":  1.2,
        "B+": 1.0,
        "B":  0.75,
        "C":  0.4,
    }
    risk *= grade_risk_mult.get(grade, 0.75)

    # === 6. RECENT WINRATE ===
    recent = performance["recent_results"]
    if len(recent) >= 8:
        winrate = sum(recent[-10:]) / len(recent[-10:])
        if winrate >= 0.65:
            risk *= 1.2    # 65%+ winrate: +20%
        elif winrate >= 0.55:
            risk *= 1.1    # 55%+ winrate: +10%
        elif winrate <= 0.35:
            risk *= 0.5    # 35%- winrate: halveer
        elif winrate <= 0.40:
            risk *= 0.7    # 40%- winrate: -30%

    # === 7. DAGVERLIES PROTECTIE ===
    if daily_state["start_balance"] > 0:
        day_loss = (daily_state["start_balance"] - balance) / daily_state["start_balance"]
        if day_loss >= 0.02:
            risk *= 0.3    # 2%+ dagverlies: hard afschalen
        elif day_loss >= 0.015:
            risk *= 0.5    # 1.5%+: halveer

    # === FLOOR & CEILING ===
    return max(risk, 0.002)  # Min 0.2%, geen max hier (max via lot limits)


def update_performance(is_win: bool, profit: float = 0, grade: str = "", kz: str = ""):
    """Update performance tracker na trade resultaat"""
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

    # Grade stats
    if grade:
        if grade not in performance["grade_stats"]:
            performance["grade_stats"][grade] = {"w": 0, "l": 0}
        if is_win:
            performance["grade_stats"][grade]["w"] += 1
        else:
            performance["grade_stats"][grade]["l"] += 1

    # Killzone stats
    if kz:
        if kz not in performance["kz_stats"]:
            performance["kz_stats"][kz] = {"w": 0, "l": 0}
        if is_win:
            performance["kz_stats"][kz]["w"] += 1
        else:
            performance["kz_stats"][kz]["l"] += 1


def update_peak_balance(balance: float):
    """Track peak balance voor drawdown berekening"""
    if balance > performance["peak_balance"]:
        performance["peak_balance"] = balance
    if performance["session_start_balance"] == 0:
        performance["session_start_balance"] = balance


# ==================== ADAPTIVE TRADE DATA ====================

TRADE_DATA_FILE = "/tmp/smc_trade_history.json"

def save_trade_data(trade: dict):
    """Sla individuele trade op naar disk met alle details"""
    try:
        history = load_trade_history()
        history.append(trade)
        # Max 500 trades bewaren
        if len(history) > 500:
            history = history[-500:]
        with open(TRADE_DATA_FILE, "w") as f:
            json.dump(history, f)
    except Exception as e:
        log.debug(f"Trade data save error: {e}")


def load_trade_history() -> list:
    """Laad trade history van disk"""
    try:
        if os.path.exists(TRADE_DATA_FILE):
            with open(TRADE_DATA_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return []


def get_adaptive_boosts() -> dict:
    """
    Analyseer trade history en genereer boosts per symbool, confirmatie, killzone, zone type.
    Winnende combinaties krijgen een hogere score, verliezende een lagere.

    Returns: {
        "symbol": {"XAUUSD": 1.5, "GBPJPY": 0.8, ...},
        "confirmation": {"strong_close": 1.3, "pin_bar": 0.9, ...},
        "killzone": {"new_york": 1.4, "london": 1.0, ...},
        "zone_type": {"ob": 1.2, "fvg": 0.8, ...},
        "best_setups": [...],  # Top 3 winnende combinaties
    }
    """
    history = load_trade_history()
    if len(history) < 3:
        return {}  # Te weinig data

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
            if total < 2:
                boosts[category][key] = 1.0  # Te weinig data
                continue
            wr = s["wins"] / total
            # Boost = 0.6 bij 0% WR → 1.8 bij 100% WR, lineair
            boosts[category][key] = round(0.6 + wr * 1.2, 2)

    # Vind top 3 winnende combinaties
    combos = {}
    for t in history:
        combo_key = f"{t.get('symbol','?')}|{t.get('killzone','?')}|{t.get('confirmation','?')}"
        if combo_key not in combos:
            combos[combo_key] = {"wins": 0, "losses": 0, "profit": 0}
        if t.get("profit", 0) > 0:
            combos[combo_key]["wins"] += 1
        else:
            combos[combo_key]["losses"] += 1
        combos[combo_key]["profit"] += t.get("profit", 0)

    sorted_combos = sorted(combos.items(), key=lambda x: x[1]["profit"], reverse=True)
    boosts["best_setups"] = sorted_combos[:5]

    return boosts


def get_symbol_boost(symbol: str) -> float:
    """Haal adaptive boost op voor een specifiek symbool"""
    boosts = get_adaptive_boosts()
    if not boosts:
        return 1.0
    return boosts.get("symbol", {}).get(symbol, 1.0)


def get_setup_boost(symbol: str, confirmation: str, killzone: str, zone_type: str) -> float:
    """Combineer alle boosts voor een specifieke setup"""
    boosts = get_adaptive_boosts()
    if not boosts:
        return 1.0

    sym_boost = boosts.get("symbol", {}).get(symbol, 1.0)
    conf_boost = boosts.get("confirmation", {}).get(confirmation, 1.0)
    kz_boost = boosts.get("killzone", {}).get(killzone, 1.0)
    zt_boost = boosts.get("zone_type", {}).get(zone_type, 1.0)

    # Gewogen gemiddelde: symbool telt zwaarst (40%), rest elk 20%
    combined = sym_boost * 0.4 + conf_boost * 0.2 + kz_boost * 0.2 + zt_boost * 0.2
    # Normaliseer rond 1.0 en begrens 0.5 - 2.0
    return max(0.5, min(2.0, combined))


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
        res = urllib.request.urlopen(req, timeout=5)  # 5s timeout (was 10)
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
        return True  # Bij error: gewoon door, nooit blokkeren

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

        # Strong bullish close in/near zone (nieuw voor aggressive mode)
        avg_body = float(df["body"].tail(20).mean())
        if (last["close"] > last["open"] and last["body"] > avg_body * MIN_STRONG_CLOSE_BODY_RATIO
                and zone.contains_price(float(last["open"]), 0.4)):
            return "strong_close"

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

        # Strong bearish close in/near zone
        avg_body = float(df["body"].tail(20).mean())
        if (last["close"] < last["open"] and last["body"] > avg_body * MIN_STRONG_CLOSE_BODY_RATIO
                and zone.contains_price(float(last["open"]), 0.4)):
            return "strong_close"

    return None

# ==================== ZONE MANAGEMENT ====================

ZONE_SAVE_FILE = "/tmp/smc_zones.pkl"
ZONE_SAVE_INTERVAL = 60  # Sla zones op elke 60 seconden
_last_zone_save = 0

def save_zones_to_disk():
    """Sla zone store op naar disk zodat het een herstart overleeft"""
    global _last_zone_save
    now = time.time()
    if now - _last_zone_save < ZONE_SAVE_INTERVAL:
        return
    _last_zone_save = now
    try:
        data = {}
        for symbol, zones in zone_store.items():
            data[symbol] = []
            for z in zones:
                if z.is_valid:
                    data[symbol].append({
                        "type": z.type.value,
                        "direction": z.direction.value,
                        "high": z.high, "low": z.low, "midpoint": z.midpoint,
                        "created_at": z.created_at,
                        "status": z.status.value,
                        "test_count": z.test_count,
                        "symbol": z.symbol,
                        "timeframe": z.timeframe,
                    })
        with open(ZONE_SAVE_FILE, "w") as f:
            json.dump(data, f)
        log.debug(f"Zones opgeslagen: {sum(len(v) for v in data.values())} zones")
    except Exception as e:
        log.warning(f"Zone save error: {e}")


def load_zones_from_disk():
    """Laad zones van disk na herstart"""
    try:
        if not os.path.exists(ZONE_SAVE_FILE):
            return 0
        with open(ZONE_SAVE_FILE, "r") as f:
            data = json.load(f)

        count = 0
        now = time.time()
        max_age = ZONE_MAX_AGE_HOURS * 3600

        for symbol, zones_data in data.items():
            if symbol not in zone_store:
                zone_store[symbol] = []
            for zd in zones_data:
                if now - zd["created_at"] > max_age:
                    continue  # Verlopen zone
                zone = Zone(
                    type=ZoneType(zd["type"]),
                    direction=Direction(zd["direction"]),
                    high=zd["high"], low=zd["low"], midpoint=zd["midpoint"],
                    created_at=zd["created_at"],
                    status=ZoneStatus(zd["status"]),
                    test_count=zd["test_count"],
                    symbol=zd.get("symbol", symbol),
                    timeframe=zd.get("timeframe", "loaded"),
                )
                zone_store[symbol].append(zone)
                count += 1

        log.info(f"Zones geladen van disk: {count} zones")
        return count
    except Exception as e:
        log.warning(f"Zone load error: {e}")
        return 0


def store_zones(symbol: str, new_zones: List[Zone]):
    if symbol not in zone_store:
        zone_store[symbol] = []
    now = time.time()
    max_age = ZONE_MAX_AGE_HOURS * 3600
    for z in new_zones:
        z.symbol = symbol
        zone_store[symbol].append(z)
    zone_store[symbol] = [z for z in zone_store[symbol] if z.is_valid and (now - z.created_at) < max_age]
    if len(zone_store[symbol]) > 20:
        zone_store[symbol] = zone_store[symbol][-20:]

    # Auto-save na elke zone update
    save_zones_to_disk()


def find_active_zone(symbol: str, price: float, direction: Direction) -> Optional[Zone]:
    if symbol not in zone_store:
        return None
    for zone in zone_store[symbol]:
        if not zone.is_valid or zone.direction != direction:
            continue
        if zone.contains_price(price, buffer_pct=0.25):
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
        candles = await get_candles(account, symbol, "1h", 120)
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

def grade_setup(htf_bias, structure, zone, confirmation, sweep, premium_discount, regime, direction, symbol=""):
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
        # Equilibrium = neutraal in aggressive mode (geen penalty)

    if regime == "trending":
        score += 1.0
        reasons.append("TRENDING")
    elif regime == "ranging":
        score -= 1.0
        reasons.append("RANGING⚠️")

    if zone.type == ZoneType.ORDER_BLOCK and zone.structure_break:
        score += 0.5
        reasons.append("OB+STRUCT")

    # === ADAPTIVE BOOST: winnende patronen krijgen hogere score ===
    adaptive = get_symbol_boost(symbol)
    if adaptive > 1.1:
        bonus = min((adaptive - 1.0) * 3.0, 2.0)  # Max +2.0 score bonus
        score += bonus
        reasons.append(f"ADAPTIVE({adaptive:.1f}x)")

    # === PRIORITY SYMBOL BOOST ===
    if symbol in PRIORITY_SYMBOLS:
        score += PRIORITY_SCORE_BONUS
        reasons.append(f"PRIORITY(+{PRIORITY_SCORE_BONUS})")

    if score >= 10:
        return "A+", 1.0, score, reasons
    elif score >= 8:
        return "A", 0.75, score, reasons
    elif score >= 6:
        return "B+", 0.6, score, reasons
    elif score >= 4.5:
        return "B", 0.5, score, reasons
    elif score >= 3.5:
        return "C", 0.25, score, reasons    # C trades met halve risico (was skip)
    return "D", 0, score, reasons

# ==================== LOT SIZE ====================

def calculate_lot_size(balance: float, sl_distance: float, symbol: str, risk_pct: float) -> Tuple[float, dict]:
    """
    Dynamische lot-sizing met:
    - Correcte pip value per instrument
    - Category-specifieke min/max lots
    - Risk amount logging voor transparantie
    """
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

    # Category limits
    limits = LOT_LIMITS.get(spec["category"], {"min": 0.01, "max": 3.0})
    lot = round(max(limits["min"], min(raw_lot, limits["max"])), 2)

    details = {
        "risk_amount": round(risk_amount, 2),
        "sl_pips": round(sl_pips, 1),
        "raw_lot": round(raw_lot, 4),
        "final_lot": lot,
        "capped": raw_lot > limits["max"],
        "floored": raw_lot < limits["min"],
        "category": spec["category"],
    }
    return lot, details

# ==================== SL / TP BEREKENING ====================

def calculate_trade_levels(direction: Direction, entry: float, zone: Zone, df: pd.DataFrame):
    """SL achter zone, TP1 op 1.5R, TP2 op 2.5R (aggressive)"""
    atr = float(df["atr"].iloc[-1])
    buffer = atr * 0.15

    if direction == Direction.BULL:
        sl = zone.low - buffer
        sl_dist = entry - sl
        if sl_dist < atr * 0.5:
            sl = entry - atr * 0.5
            sl_dist = entry - sl
        tp1 = entry + sl_dist * 1.5
        tp2 = entry + sl_dist * 2.5
    else:
        sl = zone.high + buffer
        sl_dist = sl - entry
        if sl_dist < atr * 0.5:
            sl = entry + atr * 0.5
            sl_dist = sl - entry
        tp1 = entry - sl_dist * 1.5
        tp2 = entry - sl_dist * 2.5

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
                symbol = deal.get("symbol", "UNKNOWN")
                
                # Match terug naar journal entry voor rijke data
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

                register_trade_result(profit > 0, profit)

                # Sla rijke trade data op naar disk
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
                boost = get_symbol_boost(symbol)
                boost_str = f" | Boost: {boost:.1f}x" if boost != 1.0 else ""
                tg(f"{emoji} <b>TRADE {'WON' if profit>0 else 'LOST'}</b>: {symbol} {'+'if profit>0 else ''}{profit:.2f}{boost_str}")
    except Exception as e:
        log.debug(f"Closed trades error: {e}")

# ==================== CANDLE CLOSE CHECK ====================

def is_candle_just_closed(tf_min: int = 5) -> bool:
    now = datetime.now(timezone.utc)
    return now.minute % tf_min == 0 and now.second < 60  # 60s window (was 45)

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

        # Bepaal richting — AGGRESSIVE: ook zonder HTF als structuur duidelijk is
        direction = None

        # Prioriteit 1: HTF + structuur aligned
        if htf_bias == Direction.BULL and (not structure_15m or structure_15m.direction == Direction.BULL):
            direction = Direction.BULL
        elif htf_bias == Direction.BEAR and (not structure_15m or structure_15m.direction == Direction.BEAR):
            direction = Direction.BEAR

        # Prioriteit 2: CHoCH overrulet (reversal)
        elif structure_15m and structure_15m.type == StructureType.CHOCH:
            direction = structure_15m.direction

        # Prioriteit 3: alleen HTF bias
        elif htf_bias:
            direction = htf_bias

        # Prioriteit 4 (NIEUW): alleen structuur zonder HTF — aggressive
        elif structure_15m and structure_15m.type == StructureType.BOS:
            direction = structure_15m.direction

        # Prioriteit 5 (NIEUW): alleen 5M structuur als er een sweep was
        elif structure_5m and sweep and sweep["type"] == structure_5m.direction.value:
            direction = structure_5m.direction

        if not direction:
            return None

        # P/D filter — AGGRESSIVE: alleen harde premium/discount block, equilibrium OK
        if direction == Direction.BULL and pd_zone == "premium":
            # Toch toestaan als er een sweep is (liquiditeit gepakt boven)
            if not (sweep and sweep["type"] == "bull"):
                return None
        if direction == Direction.BEAR and pd_zone == "discount":
            if not (sweep and sweep["type"] == "bear"):
                return None

        # Zoek zone
        active_zone = find_active_zone(symbol, current_price, direction)

        # AGGRESSIVE: als geen zone gevonden, probeer momentum entry
        # met een "virtuele zone" rond recent swing level
        if not active_zone:
            # Momentum entry: HTF aligned + structuur + sterke candle
            rsi = float(df_5m["rsi"].iloc[-1])
            macd_h = float(df_5m["macd_hist"].iloc[-1])
            macd_h_prev = float(df_5m["macd_hist"].iloc[-2])
            ema20 = float(df_5m["ema20"].iloc[-1])
            atr = float(df_5m["atr"].iloc[-1])

            mom_bull = (htf_bias == Direction.BULL and direction == Direction.BULL
                       and 50 < rsi < 72 and macd_h > macd_h_prev
                       and current_price > ema20)
            mom_bear = (htf_bias == Direction.BEAR and direction == Direction.BEAR
                       and 28 < rsi < 50 and macd_h < macd_h_prev
                       and current_price < ema20)

            if mom_bull or mom_bear:
                # Creeer virtuele zone rond recent swing
                if direction == Direction.BULL:
                    recent_low = float(df_5m["low"].tail(10).min())
                    active_zone = Zone(
                        type=ZoneType.ORDER_BLOCK, direction=Direction.BULL,
                        high=recent_low + atr * 0.5, low=recent_low,
                        midpoint=recent_low + atr * 0.25, created_at=time.time(),
                        symbol=symbol, timeframe="5m_momentum",
                    )
                else:
                    recent_high = float(df_5m["high"].tail(10).max())
                    active_zone = Zone(
                        type=ZoneType.ORDER_BLOCK, direction=Direction.BEAR,
                        high=recent_high, low=recent_high - atr * 0.5,
                        midpoint=recent_high - atr * 0.25, created_at=time.time(),
                        symbol=symbol, timeframe="5m_momentum",
                    )
            else:
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
            symbol=symbol,
        )

        # AGGRESSIVE: alleen D skippen, C trades toegestaan
        if grade == "D":
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
    kz = get_current_killzone()
    risk_pct = get_dynamic_risk(balance, grade=setup.grade, kz=kz) * setup.risk_mult

    # === ADAPTIVE RISK BOOST: winnende symbolen/setups krijgen meer risico ===
    adaptive_boost = get_setup_boost(
        setup.symbol,
        setup.confirmation or "",
        kz or "",
        setup.zone.type.value if setup.zone else "",
    )
    risk_pct *= adaptive_boost

    # === PRIORITY SYMBOL RISK BOOST ===
    if setup.symbol in PRIORITY_SYMBOLS:
        risk_pct *= PRIORITY_RISK_MULT

    sl_dist = abs(setup.entry - setup.stop_loss)
    lot, lot_details = calculate_lot_size(balance, sl_dist, setup.symbol, risk_pct)
    lot_details["adaptive_boost"] = adaptive_boost

    if lot < 0.01:
        return False

    # === MARGIN CHECK: bereken benodigde margin en pas lot aan ===
    try:
        info = await rate_limited_call(conn.get_account_information())
        free_margin = info.get("freeMargin", balance)
        spec = SYMBOL_SPECS.get(setup.symbol, {})
        leverage = spec.get("leverage", 20)

        # Margin berekening: (lots × contract × prijs) / leverage
        margin_needed = (lot * spec.get("contract", 100000) * setup.entry) / leverage

        # Margin moet in account currency (EUR) — voor USD pairs delen door exchange rate
        # Simpele benadering: als margin > 80% free margin, verklein lot
        max_margin_use = free_margin * 0.80  # Max 80% van vrije margin gebruiken

        if margin_needed > max_margin_use and margin_needed > 0:
            reduction = max_margin_use / margin_needed
            old_lot = lot
            lot = round(max(0.01, lot * reduction), 2)
            lot_details["margin_reduced"] = True
            lot_details["original_lot"] = old_lot
            log.info(f"Margin check: {setup.symbol} lot {old_lot} → {lot} (margin {margin_needed:.0f} > free {free_margin:.0f})")

        # Dubbel check: als margin nog steeds te hoog, skip trade
        final_margin = (lot * spec.get("contract", 100000) * setup.entry) / leverage
        if final_margin > free_margin * 0.90:
            log.warning(f"Margin te hoog voor {setup.symbol}: {final_margin:.0f} > {free_margin:.0f}")
            tg(f"⚠️ <b>MARGIN SKIP</b>: {setup.symbol}\nMargin: €{final_margin:.0f} | Free: €{free_margin:.0f}")
            return False

    except Exception as e:
        log.warning(f"Margin check error: {e}")
        # Bij fout: gebruik kleinere lot als safety
        lot = min(lot, 0.05)

    if lot < 0.01:
        return False

    # Update peak balance voor equity curve tracking
    update_peak_balance(balance)

    try:
        if setup.direction == Direction.BULL:
            result = await asyncio.wait_for(
                conn.create_market_buy_order(setup.symbol, lot, setup.stop_loss, setup.tp2),
                timeout=15
            )
        else:
            result = await asyncio.wait_for(
                conn.create_market_sell_order(setup.symbol, lot, setup.stop_loss, setup.tp2),
                timeout=15
            )

        # Verifieer dat order echt geplaatst is
        if not result or result.get("stringCode") == "ERR_NO_ERROR" or "orderId" not in str(result):
            # Sommige brokers retourneren success zonder orderId — check posities
            await asyncio.sleep(1)
            positions = await rate_limited_call(conn.get_positions())
            if positions:
                found = any(p.get("symbol") == setup.symbol for p in positions)
                if not found:
                    tg(f"❌ <b>ORDER NOT CONFIRMED</b>: {setup.symbol} — geen positie gevonden na order")
                    return False
            mark_api_success()
        else:
            mark_api_success()

    except Exception as e:
        tg(f"❌ <b>ORDER FAIL</b>: {setup.symbol} — {e}")
        mark_api_failure()
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
        "lot_details": lot_details,
    })

    r = " | ".join(setup.reasons)
    de = "🟢" if setup.direction == Direction.BULL else "🔴"
    cap_warn = " ⚠️CAP" if lot_details.get("capped") else ""
    margin_warn = " ⚠️MARGIN" if lot_details.get("margin_reduced") else ""
    wr = sum(performance["recent_results"][-10:]) / max(len(performance["recent_results"][-10:]), 1) * 100

    tg(f"""<b>{de} TRADE OPENED — Grade {setup.grade}</b>

📌 {setup.symbol} | {setup.direction.value.upper()}
🕐 KZ: {kz.upper() if kz else '?'}
🔍 Regime: {setup.regime.upper()}

💰 Entry: {setup.entry:.5f}
🛑 SL: {setup.stop_loss:.5f}
🎯 TP1: {setup.tp1:.5f} (50% partial)
🎯 TP2: {setup.tp2:.5f} (runner)

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
        log.info("Connecting to MetaAPI...")
        api = MetaApi(METAAPI_TOKEN)
        account = await api.metatrader_account_api.get_account(ACCOUNT_ID)

        # === ROBUUSTE CONNECTION SETUP ===
        # Probleem: MetaAPI SDK kan vastlopen in connect-disconnect loop
        # Oplossing: meerdere pogingen met account redeploy als fallback

        conn = None
        for attempt in range(5):
            try:
                log.info(f"Connection poging {attempt + 1}/5...")

                # Stap 1: Check account state en deploy indien nodig
                account_info = account.state
                log.info(f"Account state: {account_info}")

                if account_info != "DEPLOYED":
                    log.info("Account niet deployed, deploying...")
                    try:
                        await account.deploy()
                    except Exception as e:
                        log.warning(f"Deploy fout (kan normaal zijn): {e}")
                    await asyncio.sleep(5)

                # Stap 2: Wacht op account connected met timeout
                try:
                    await asyncio.wait_for(account.wait_connected(), timeout=60)
                    log.info("Account connected!")
                except asyncio.TimeoutError:
                    log.warning(f"Account connect timeout poging {attempt + 1}")
                    # Undeploy + redeploy forceren
                    if attempt >= 2:
                        log.info("Forcing undeploy + redeploy...")
                        try:
                            await account.undeploy()
                            await asyncio.sleep(10)
                            await account.deploy()
                            await asyncio.sleep(10)
                        except Exception as e:
                            log.warning(f"Redeploy fout: {e}")
                    await asyncio.sleep(10)
                    continue

                # Stap 3: Maak RPC connection
                conn = account.get_rpc_connection()
                await conn.connect()

                # Stap 4: Synchroniseer met korte timeout, retry bij falen
                log.info("Synchroniseren...")
                try:
                    await asyncio.wait_for(
                        conn.wait_synchronized(),
                        timeout=90
                    )
                    log.info("Gesynchroniseerd!")
                    break  # SUCCESS — uit de loop
                except asyncio.TimeoutError:
                    log.warning(f"Sync timeout poging {attempt + 1}")
                    # Sluit connection en probeer opnieuw
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
            tg("❌ <b>FATAL: Kon niet verbinden na 5 pogingen</b>\nCheck MetaAPI dashboard!")
            raise Exception("Connection failed after 5 attempts")

        # === EXTRA VALIDATIE: test of connection echt werkt ===
        try:
            test_info = await asyncio.wait_for(conn.get_account_information(), timeout=15)
            log.info(f"Connection verified: ${test_info['balance']} balance")
        except Exception as e:
            tg(f"⚠️ Connection gemaakt maar verificatie mislukt: {e}")
            raise Exception(f"Connection verification failed: {e}")

        await asyncio.sleep(2)

        global last_heartbeat, watchdog_last_loop, consecutive_errors, consecutive_api_fails
        last_heartbeat = 0
        watchdog_last_loop = time.time()
        consecutive_errors = 0

        # Initialiseer performance tracker met startbalance
        try:
            init_info = await conn.get_account_information()
            performance["session_start_balance"] = init_info["balance"]
            performance["peak_balance"] = init_info["balance"]
            log.info(f"Performance tracker init: ${init_info['balance']}")
        except Exception:
            pass

        # === LAAD ZONES VAN DISK (overleeft herstart) ===
        zones_loaded = load_zones_from_disk()
        mark_api_success()  # Reset fail counter bij startup

        await run_diagnostics(conn, account)

        tg(f"""🚀 <b>SMC BOT v3.1 AGGRESSIVE GESTART</b>

📊 {len(SYMBOLS)} symbols | Entry KZs: {', '.join(ENTRY_KILLZONES)}
🎯 Min RR: {MIN_RR} | Max trades: {MAX_TOTAL_TRADES}
🔥 Target: 3-15 trades/dag
🗺️ Zones geladen: {zones_loaded}

⚡ <b>Strategie:</b>
• Top-down: 1H bias → 15M structuur → 5M entry
• Zone-based: OB/FVG opslaan → wachten op retest
• Zone persistence: overleeft herstart
• Fast reconnect: 5 pogingen + redeploy fallback
• Dual health check: API + data freshness""")

        while True:
            try:
                # === WATCHDOG: markeer succesvolle loop ===
                watchdog_last_loop = time.time()
                consecutive_errors = 0  # Reset na succesvolle iteratie

                await send_heartbeat(conn)

                # === REACTIVE RECONNECT: alleen als echte operaties herhaaldelijk falen ===
                if needs_reconnect():
                    log.warning(f"API failed {consecutive_api_fails}x — reconnecting...")
                    tg("⚠️ <b>CONNECTION ISSUE</b> — auto-recovering...")
                    reconnected = False

                    for attempt in range(5):
                        try:
                            log.info(f"Reconnect {attempt + 1}/5...")
                            try:
                                await conn.close()
                            except Exception:
                                pass
                            await asyncio.sleep(3)

                            conn = account.get_rpc_connection()
                            await conn.connect()
                            await asyncio.wait_for(conn.wait_synchronized(), timeout=30)

                            test = await asyncio.wait_for(conn.get_account_information(), timeout=8)
                            if test and "balance" in test:
                                mark_api_success()  # Reset fail counter
                                connection_healthy = True
                                reconnected = True
                                log.info(f"Reconnected in {attempt + 1} pogingen")
                                break

                        except Exception as e:
                            log.warning(f"Reconnect {attempt + 1}/5: {e}")
                            await asyncio.sleep(5 * (attempt + 1))

                    if reconnected:
                        tg("✅ <b>RECOVERED</b> — bot draait weer")
                    else:
                        try:
                            await account.undeploy()
                            await asyncio.sleep(5)
                            await account.deploy()
                            await asyncio.sleep(10)
                            conn = account.get_rpc_connection()
                            await conn.connect()
                            await asyncio.wait_for(conn.wait_synchronized(), timeout=60)
                            mark_api_success()
                            connection_healthy = True
                            reconnected = True
                            tg("✅ <b>RECOVERED via REDEPLOY</b>")
                        except Exception as e:
                            log.error(f"Redeploy failed: {e}")

                    if not reconnected:
                        tg("❌ <b>RECONNECT FAILED</b> — force restart...")
                        raise Exception("All reconnect methods failed")


                kz = get_current_killzone()

                # Asia: range mapping + trading voor select pairs
                if kz == "asia":
                    await update_asia_range(account)
                    # Ga door met normal flow — Asia is nu entry killzone

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

                # === HAAL ACCOUNT DATA OP (met None protectie) ===
                info = await rate_limited_call(conn.get_account_information())
                if not info or "balance" not in info:
                    log.warning("Kon account info niet ophalen, skip cyclus")
                    await asyncio.sleep(10)
                    continue

                balance = info["balance"]
                equity = info["equity"]

                positions = await rate_limited_call(conn.get_positions())
                if positions is None:
                    positions = []  # Behandel als geen open posities

                # Track peak balance voor dynamische lotsize
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
                    # Watchdog: reset timer zodat lange analyse cyclus geen trigger geeft
                    watchdog_last_loop = time.time()

                    try:
                        allowed, _ = is_entry_allowed(symbol)
                        if not allowed:
                            # Priority symbolen mogen in ALLE killzones
                            if not (PRIORITY_ALL_KILLZONES and symbol in PRIORITY_SYMBOLS):
                                continue
                        max_trades = PRIORITY_MAX_TRADES if symbol in PRIORITY_SYMBOLS else MAX_TRADES_PER_ASSET
                        if sum(1 for p in positions if p["symbol"] == symbol) >= max_trades:
                            continue
                        if not check_correlation(symbol, positions):
                            continue

                        sig_key = f"{symbol}_{int(time.time()/300)}"  # 5 min dedup
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

                    await asyncio.sleep(0.5)  # Korte pauze, minder dan 1s

                now = time.time()
                for k in list(recent_signals.keys()):
                    if now - recent_signals[k] > 7200:
                        del recent_signals[k]

                await asyncio.sleep(CHECK_INTERVAL)

            except Exception as e:
                consecutive_errors += 1
                log.error(f"Loop error #{consecutive_errors}: {e}")

                # Na te veel errors: force restart
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    tg(f"🚨 <b>{MAX_CONSECUTIVE_ERRORS} CONSECUTIVE ERRORS</b> — force restart\nLaatste: {str(e)[:60]}")
                    raise Exception(f"Too many consecutive errors: {consecutive_errors}")

                # Elke 5 errors: stuur warning
                if consecutive_errors % 5 == 0:
                    tg(f"⚠️ <b>{consecutive_errors} LOOP ERRORS</b>\n{str(e)[:80]}")

                await asyncio.sleep(10 if "timed out" in str(e).lower() else 5)

    except Exception as e:
        log.critical(f"FATAL: {e}")
        tg(f"❌ <b>FATAL</b>: {str(e)[:100]}")
        raise e

# ==================== START ====================

def watchdog_thread():
    """
    Aparte thread die de main loop monitort.
    Als de loop langer dan 5 min niet reageert: force exit zodat
    de outer while-loop een restart triggert.
    """
    global watchdog_last_loop
    while True:
        time.sleep(60)  # Check elke minuut
        silence = time.time() - watchdog_last_loop
        if silence > watchdog_max_silence:
            log.critical(f"WATCHDOG: Loop silent for {silence:.0f}s — force restart!")
            tg(f"🐕 <b>WATCHDOG TRIGGERED</b>\nLoop niet actief voor {silence:.0f}s\nForce restart...")
            # Force exit — de outer while True herstart alles
            os._exit(1)


if __name__ == "__main__":
    import threading

    log.info("=" * 50)
    log.info("PROFESSIONAL SMC BOT v3.1 — AGGRESSIVE")
    log.info("Zone-based | Momentum Entries | 3-15 trades/day")
    log.info("Watchdog enabled | Telegram retry 3x")
    log.info("=" * 50)

    # Start watchdog als daemon thread
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
            tg(f"💥 <b>CRASH #{restart_count}</b>: {str(e)[:80]}\n🔄 Restart in 15s...")
            log.error(f"Crash #{restart_count}: {e}")

            # Exponential backoff bij herhaalde crashes (max 60s)
            wait = min(15 * restart_count, 60)
            log.info(f"Restart in {wait}s...")
            time.sleep(wait)
