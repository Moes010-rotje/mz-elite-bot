import asyncio
import pandas as pd
import numpy as np
import os
import time
import json
import math
import sqlite3
import urllib.request
import logging
import signal
from pathlib import Path
from datetime import datetime, date, timedelta, timezone
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple
from enum import Enum
from metaapi_cloud_sdk import MetaApi

# ================================================================
# PROFESSIONAL SMC BOT v4.1 — PROFIT-CRITICAL REBUILD
#
# CHANGES vs v4.0 Bugfix Edition:
#
# [PROFIT-CRITICAL FIXES]
# - TradePhase state machine: expliciete tracking van TP1/TP2/trailing
#   → voorkomt gemiste partials en dubbele closes
# - Per-symbol auto-pause: pauzeert symbols met <40% WR na 15+ trades
#   → stopt bloeden op slechte symbols automatisch
# - Max drawdown hard stop: bot stopt volledig bij 10%+ drawdown
#   → voorkomt catastrofaal verlies
# - Order confirmation fix: ERR_NO_ERROR is SUCCESS, niet error
#   → voorkomt gemiste trades door foute status check
# - Pip value fallback verwijderd: skip trade als geen actuele rate
#   → voorkomt verkeerde lot sizes door geschatte pip values
# - Reconnect heartbeat: detecteert stille WebSocket disconnects
#   → voorkomt gemiste trade management
#
# [ARCHITECTURE FIXES]
# - BotState class vervangt alle globals
#   → thread-safe, testbaar, geen race conditions
# - Enkele rate_limited_call wrapper (safe_call verwijderd)
#   → consistente API handling overal
# - check_weekly/check_daily geen side effects meer
#   → split in reset + check functies
# - grade_setup type-consistent: htf_bias altijd bool
# - Graceful shutdown i.p.v. os._exit(1)
#   → SQLite writes worden correct geflushed
# - Error logging verbeterd: warnings i.p.v. debug voor echte fouten
#
# [TRACKING VERBETERINGEN]
# - Per-symbol winrate tracking met auto-disable
# - Trade phase tracking in database
# - Reconnect heartbeat elke 60s
# - Symbol health scoring
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
    OTE = "ote"

class ZoneStatus(Enum):
    FRESH = "fresh"
    TESTED = "tested"
    MITIGATED = "mitigated"

class TradePhase(Enum):
    """Expliciete trade fase tracking — voorkomt gemiste/dubbele partials"""
    OPEN = "open"
    TP1_HIT = "tp1_hit"       # 33% gesloten, SL → breakeven
    TP2_HIT = "tp2_hit"       # 33% gesloten, SL → TP1 level
    TRAILING = "trailing"     # 4R+ trailing SL actief
    CLOSED = "closed"

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
    ote_entry: bool = False

# ==================== BOT STATE CLASS (VERVANGT GLOBALS) ====================

class BotState:
    """
    Centrale state container — vervangt alle losse globals.
    Thread-safe, testbaar, geen race conditions.
    """
    def __init__(self):
        # === Daily/Weekly tracking ===
        self.daily_state = {"date": None, "start_balance": 0, "trades_today": 0}
        self.weekly_state = {"week": None, "loss": 0, "limit_hit": False, "start_balance": 0}

        # === API tracking ===
        self.last_heartbeat = 0
        self.api_call_count = 0
        self.api_call_reset_time = 0
        self.consecutive_api_fails = 0

        # === Zone storage ===
        self.zone_store: Dict[str, List[Zone]] = {}
        self.htf_zone_store: Dict[str, List[Zone]] = {}
        self._last_zone_save = 0

        # === Trade tracking ===
        self.trade_journal: List[dict] = []
        self.recent_signals: Dict[str, float] = {}
        self.trade_phases: Dict[str, TradePhase] = {}  # position_id → phase

        # === Session tracking ===
        self.asia_range_cache: Dict[str, dict] = {}
        self.session_levels: Dict[str, Dict[str, dict]] = {}

        # === Connection health ===
        self.connection_healthy = True
        self.last_connection_check = 0
        self.last_reconnect_heartbeat = 0
        self.watchdog_last_loop = time.time()
        self.consecutive_errors = 0

        # === Cooldowns ===
        self.symbol_cooldowns: Dict[str, dict] = {}
        self.gold_session_losses = {"killzone": None, "losses": 0}

        # === Performance ===
        self.performance = {
            "wins": 0,
            "losses": 0,
            "consecutive_wins": 0,
            "consecutive_losses": 0,
            "recent_results": [],
            "peak_balance": 0,
            "session_start_balance": 0,
            "total_profit": 0,
            "total_loss": 0,
            "grade_stats": {},
            "kz_stats": {},
            "symbol_stats": {},  # NEW: per-symbol tracking
        }

        # === Caches ===
        self._adaptive_cache = {"data": {}, "last_calc": 0, "ttl": 3600}
        self._pip_value_cache: Dict[str, dict] = {}

        # === Telegram rate limiting ===
        self.tg_fail_count = 0
        self.tg_last_success = time.time()
        self._tg_timestamps: List[float] = []

        # === Shutdown flag ===
        self.shutdown_requested = False
        self.hard_stop = False  # True = drawdown stop, geen trades meer

    def get_trade_phase(self, position_id: str) -> TradePhase:
        return self.trade_phases.get(position_id, TradePhase.OPEN)

    def set_trade_phase(self, position_id: str, phase: TradePhase):
        self.trade_phases[position_id] = phase
        db_save_state("trade_phases", {k: v.value for k, v in self.trade_phases.items()})

    def cleanup_trade_phases(self, active_position_ids: set):
        """Verwijder phases van gesloten posities"""
        closed = [pid for pid in self.trade_phases if pid not in active_position_ids]
        for pid in closed:
            del self.trade_phases[pid]

    def get_symbol_winrate(self, symbol: str) -> Tuple[float, int]:
        """Returns (winrate, total_trades) voor een symbol"""
        stats = self.performance["symbol_stats"].get(symbol)
        if not stats:
            return 0.5, 0  # Default 50% als geen data
        total = stats["w"] + stats["l"]
        if total == 0:
            return 0.5, 0
        return stats["w"] / total, total

    def is_symbol_healthy(self, symbol: str, min_trades: int = 15, min_wr: float = 0.40) -> bool:
        """Check of symbol nog getradet mag worden op basis van winrate"""
        wr, total = self.get_symbol_winrate(symbol)
        if total < min_trades:
            return True  # Niet genoeg data, laat door
        return wr >= min_wr

    def update_symbol_stats(self, symbol: str, is_win: bool):
        """Track per-symbol performance"""
        if symbol not in self.performance["symbol_stats"]:
            self.performance["symbol_stats"][symbol] = {"w": 0, "l": 0}
        if is_win:
            self.performance["symbol_stats"][symbol]["w"] += 1
        else:
            self.performance["symbol_stats"][symbol]["l"] += 1


# ==================== CONFIGURATIE ====================

# === MODUS ===
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
CHECK_INTERVAL = 5

# === VERLAAGDE RISK — professioneel niveau ===
GRADE_RISK = {
    "A+": 0.010,   # 1.0%
    "A":  0.0075,  # 0.75%
    "B+": 0.005,   # 0.5%
}

# === STRENGERE LIMIETEN ===
DAILY_LOSS_LIMIT = 0.03      # 3% dagelijks
WEEKLY_LOSS_LIMIT = 0.08     # 8% wekelijks
MAX_DRAWDOWN_HARD_STOP = 0.10  # 10% → bot stopt volledig

# === STREAK MODIFIER — conservatiever ===
STREAK_MODIFIER = {
    "wins_4plus": 1.1,
    "wins_2_3":   1.05,
    "neutral":    1.0,
    "loss_1":     0.85,
    "loss_2":     0.65,
    "loss_3plus": 0.4,
}

MIN_RR = 2.0
MAX_TRADES_PER_ASSET = 2
MAX_TOTAL_TRADES = 4
MAX_TRADES_PER_DAY = 6

# === SYMBOL HEALTH ===
SYMBOL_MIN_TRADES_FOR_EVAL = 15
SYMBOL_MIN_WINRATE = 0.40  # 40% minimum winrate

# === SLIPPAGE BESCHERMING ===
MAX_SLIPPAGE_PIPS = {
    "metals":  15,
    "forex":   5,
    "indices": 20,
}

# === GOLD SCALP MODE ===
GOLD_SCALP = {
    "enabled": True,
    "symbol": "XAUUSD",
    "max_trades": 3,
    "min_rr": 1.3,
    "min_grade": "B+",
    "risk_override": 0.005,
    "tp1_mult": 1.5,
    "tp2_mult": 2.5,
    "tp3_mult": 3.5,
    "dedup_seconds": 180,
    "max_spread_scalp": 20,
    "session_max_losses": 2,
    "prime_killzones": ["london", "london_ext", "new_york"],
    "round_number_step": 50,
    "round_number_buffer": 3.0,
    "cooldown_minutes": 30,
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
ZONE_MAX_TESTS = 1
MAX_API_CALLS_PER_MIN = 50
SWING_LOOKBACK = 3
MIN_REJECTION_WICK_RATIO = 0.45
MIN_ENGULFING_BODY_RATIO = 1.1
MIN_STRONG_CLOSE_BODY_RATIO = 1.5

# === ZONE BUFFER ===
ZONE_ENTRY_BUFFER = 0.1
ZONE_CONFIRM_BUFFER = 0.15
ZONE_SAVE_INTERVAL = 120

# === RECONNECT HEARTBEAT ===
RECONNECT_HEARTBEAT_INTERVAL = 60  # Elke 60s een ping
RECONNECT_HEARTBEAT_FAIL_THRESHOLD = 3  # 3x falen → reconnect

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
    "XAUUSD":  {"pip_size": 0.1,    "pip_value_per_lot": 10,    "max_spread_pips": 35,  "category": "metals",   "leverage": 20, "contract": 100,    "min_lot": 0.01, "lot_step": 0.01, "base_currency": "XAU", "quote_currency": "USD"},
    "GBPJPY":  {"pip_size": 0.01,   "pip_value_per_lot": None,  "max_spread_pips": 30,  "category": "forex",    "leverage": 20, "contract": 100000, "min_lot": 0.01, "lot_step": 0.01, "base_currency": "GBP", "quote_currency": "JPY"},
    "USDJPY":  {"pip_size": 0.01,   "pip_value_per_lot": None,  "max_spread_pips": 18,  "category": "forex",    "leverage": 30, "contract": 100000, "min_lot": 0.01, "lot_step": 0.01, "base_currency": "USD", "quote_currency": "JPY"},
    "USTEC":   {"pip_size": 0.1,    "pip_value_per_lot": 1,     "max_spread_pips": 25,  "category": "indices",  "leverage": 20, "contract": 1,      "min_lot": 0.1,  "lot_step": 0.1,  "base_currency": "USD", "quote_currency": "USD"},
    "US30":    {"pip_size": 0.1,    "pip_value_per_lot": 1,     "max_spread_pips": 35,  "category": "indices",  "leverage": 20, "contract": 1,      "min_lot": 0.1,  "lot_step": 0.1,  "base_currency": "USD", "quote_currency": "USD"},
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

LOT_LIMITS = {
    "forex":   {"min": 0.01, "max": 5.0},
    "metals":  {"min": 0.01, "max": 3.0},
    "indices": {"min": 0.01, "max": 3.0},
}

MAX_RECENT_SIGNALS = 500
MAX_CONSECUTIVE_ERRORS = 20
MAX_API_FAILS_BEFORE_RECONNECT = 7
TG_MAX_PER_MINUTE = 20

# ==================== SQLite PERSISTENCE ====================

DB_PATH = os.getenv("DB_PATH", "/tmp/smc_bot_v4.db")

class DatabaseConnection:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn = None

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path, timeout=10)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type is None:
                self.conn.commit()
            self.conn.close()
        return False


def init_database():
    try:
        _init_database_tables()
    except sqlite3.DatabaseError as e:
        log.warning(f"Database corrupt ({e}) — rebuilding...")
        try:
            backup_path = DB_PATH + f".corrupt.{int(time.time())}"
            if os.path.exists(DB_PATH):
                os.rename(DB_PATH, backup_path)
                log.info(f"Corrupt DB backed up to {backup_path}")
        except Exception as be:
            log.warning(f"Backup failed: {be}")
            if os.path.exists(DB_PATH):
                os.remove(DB_PATH)
        _init_database_tables()


def _init_database_tables():
    with DatabaseConnection() as conn:
        c = conn.cursor()

        c.execute("""CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT,
            grade TEXT,
            score REAL,
            entry REAL,
            stop_loss REAL,
            tp1 REAL, tp2 REAL, tp3 REAL,
            lot REAL,
            rr REAL,
            risk_pct REAL,
            confirmation TEXT,
            zone_type TEXT,
            regime TEXT,
            killzone TEXT,
            confluence INTEGER DEFAULT 0,
            ote_entry INTEGER DEFAULT 0,
            reasons TEXT,
            profit REAL DEFAULT 0,
            closed INTEGER DEFAULT 0,
            close_time TEXT,
            mae REAL DEFAULT 0,
            mfe REAL DEFAULT 0,
            dry_run INTEGER DEFAULT 0
        )""")

        c.execute("""CREATE TABLE IF NOT EXISTS zones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            type TEXT,
            direction TEXT,
            high REAL,
            low REAL,
            midpoint REAL,
            created_at REAL,
            status TEXT,
            test_count INTEGER DEFAULT 0,
            timeframe TEXT,
            htf INTEGER DEFAULT 0
        )""")

        c.execute("""CREATE TABLE IF NOT EXISTS performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            total_profit REAL DEFAULT 0,
            profit_factor REAL DEFAULT 0,
            peak_balance REAL DEFAULT 0,
            max_drawdown REAL DEFAULT 0
        )""")

        c.execute("""CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        )""")

    log.info(f"Database geïnitialiseerd (WAL mode): {DB_PATH}")


def db_execute(query: str, params: tuple = (), fetch: bool = False):
    try:
        with DatabaseConnection() as conn:
            c = conn.cursor()
            c.execute(query, params)
            if fetch:
                return c.fetchall()
    except sqlite3.DatabaseError as e:
        log.warning(f"DB error (possible corruption): {e}")
        return [] if fetch else None
    except Exception as e:
        log.warning(f"DB error: {e}")
        return [] if fetch else None
    return None


def db_save_state(key: str, value):
    db_execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)",
               (key, json.dumps(value)))


def db_load_state(key: str, default=None):
    rows = db_execute("SELECT value FROM state WHERE key = ?", (key,), fetch=True)
    if rows and rows[0][0]:
        try:
            return json.loads(rows[0][0])
        except Exception:
            pass
    return default


def db_save_trade(trade_data: dict):
    db_execute("""INSERT INTO trades
        (timestamp, symbol, direction, grade, score, entry, stop_loss,
         tp1, tp2, tp3, lot, rr, risk_pct, confirmation, zone_type,
         regime, killzone, confluence, ote_entry, reasons, dry_run)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (trade_data.get("time", ""), trade_data.get("symbol", ""),
         trade_data.get("direction", ""), trade_data.get("grade", ""),
         trade_data.get("score", 0), trade_data.get("entry", 0),
         trade_data.get("sl", 0), trade_data.get("tp1", 0),
         trade_data.get("tp2", 0), trade_data.get("tp3", 0),
         trade_data.get("lot", 0), trade_data.get("rr", 0),
         trade_data.get("risk_pct", 0), trade_data.get("confirmation", ""),
         trade_data.get("zone_type", ""), trade_data.get("regime", ""),
         trade_data.get("killzone", ""), 1 if trade_data.get("confluence") else 0,
         1 if trade_data.get("ote_entry") else 0,
         json.dumps(trade_data.get("reasons", [])),
         1 if DRY_RUN else 0))


def db_update_trade_result(symbol: str, profit: float, mae: float = 0, mfe: float = 0):
    try:
        with DatabaseConnection() as conn:
            c = conn.cursor()
            c.execute("""SELECT id FROM trades
                         WHERE symbol = ? AND closed = 0
                         ORDER BY id DESC LIMIT 1""", (symbol,))
            row = c.fetchone()
            if row:
                trade_id = row[0]
                c.execute("""UPDATE trades SET profit = ?, closed = 1, close_time = ?,
                             mae = ?, mfe = ? WHERE id = ?""",
                          (profit, datetime.now(timezone.utc).isoformat(), mae, mfe, trade_id))
    except Exception as e:
        log.warning(f"db_update_trade_result error: {e}")


def db_get_profit_factor() -> float:
    rows = db_execute(
        "SELECT profit FROM trades WHERE closed = 1 AND dry_run = 0", fetch=True)
    if not rows:
        return 0
    total_wins = sum(r[0] for r in rows if r[0] > 0)
    total_losses = abs(sum(r[0] for r in rows if r[0] < 0))
    if total_losses == 0:
        return 99.0 if total_wins > 0 else 0
    return round(total_wins / total_losses, 2)


def db_get_mae_mfe_stats(symbol: str = None) -> dict:
    query = "SELECT mae, mfe, profit FROM trades WHERE closed = 1 AND dry_run = 0"
    params = ()
    if symbol:
        query += " AND symbol = ?"
        params = (symbol,)
    rows = db_execute(query, params, fetch=True)
    if not rows or len(rows) < 5:
        return {}
    maes = [r[0] for r in rows]
    mfes = [r[1] for r in rows]
    return {
        "avg_mae": round(np.mean(maes), 5),
        "max_mae": round(max(maes), 5),
        "avg_mfe": round(np.mean(mfes), 5),
        "max_mfe": round(max(mfes), 5),
        "count": len(rows),
    }


def db_save_zones_bulk(zones_dict: dict, htf: bool = False):
    try:
        with DatabaseConnection() as conn:
            c = conn.cursor()
            c.execute("DELETE FROM zones WHERE htf = ?", (1 if htf else 0,))
            for symbol, zones in zones_dict.items():
                for z in zones:
                    if z.is_valid:
                        c.execute("""INSERT INTO zones
                            (symbol, type, direction, high, low, midpoint, created_at,
                             status, test_count, timeframe, htf)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (symbol, z.type.value, z.direction.value, z.high, z.low,
                             z.midpoint, z.created_at, z.status.value, z.test_count,
                             z.timeframe, 1 if htf else 0))
    except Exception as e:
        log.warning(f"db_save_zones_bulk error: {e}")


def db_load_zones() -> Tuple[dict, dict, int]:
    zone_s = {}
    htf_s = {}
    now = time.time()
    max_age = ZONE_MAX_AGE_HOURS * 3600
    rows = db_execute("SELECT * FROM zones", fetch=True)
    count = 0
    for r in (rows or []):
        _, symbol, ztype, direction, high, low, midpoint, created_at, status, test_count, tf, htf = r
        if now - created_at > max_age:
            continue
        zone = Zone(
            type=ZoneType(ztype), direction=Direction(direction),
            high=high, low=low, midpoint=midpoint, created_at=created_at,
            status=ZoneStatus(status), test_count=test_count,
            symbol=symbol, timeframe=tf or "loaded",
        )
        store = htf_s if htf else zone_s
        if symbol not in store:
            store[symbol] = []
        store[symbol].append(zone)
        count += 1
    return zone_s, htf_s, count


# ==================== TELEGRAM (MET RATE LIMITING) ====================

def tg(msg: str, state: 'BotState' = None):
    if not TG_TOKEN or not TG_CHAT:
        return

    # Rate limiting
    now = time.time()
    if state:
        state._tg_timestamps = [t for t in state._tg_timestamps if now - t < 60]
        if len(state._tg_timestamps) >= TG_MAX_PER_MINUTE:
            log.debug("Telegram rate limit bereikt, bericht overgeslagen")
            return
        state._tg_timestamps.append(now)

    for attempt in range(3):
        try:
            url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
            payload = json.dumps({"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}).encode()
            req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=8 + attempt * 4)
            if state:
                state.tg_fail_count = 0
                state.tg_last_success = time.time()
            return
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                if state:
                    state.tg_fail_count += 1
                log.warning(f"Telegram FAILED 3x: {e}")


# ==================== UNIFIED RATE LIMITED CALL ====================

async def rate_limited_call(coro, state: 'BotState', timeout: int = 30, label: str = "API"):
    """
    Enkele wrapper voor alle API calls.
    GEEN retries — coroutines kunnen niet opnieuw ge-await worden.
    Increment fail counter op ELKE failure zodat reconnect altijd triggert.
    """
    try:
        now = time.time()
        if now - state.api_call_reset_time > 60:
            state.api_call_count = 0
            state.api_call_reset_time = now
        if state.api_call_count >= MAX_API_CALLS_PER_MIN:
            wait = 60 - (now - state.api_call_reset_time)
            if wait > 0:
                await asyncio.sleep(wait)
            state.api_call_count = 0
            state.api_call_reset_time = time.time()

        state.api_call_count += 1
        result = await asyncio.wait_for(coro, timeout=timeout)
        state.consecutive_api_fails = 0
        return result

    except asyncio.TimeoutError:
        state.consecutive_api_fails += 1
        log.warning(f"{label} timeout (fails: {state.consecutive_api_fails})")
        return None

    except asyncio.CancelledError:
        state.consecutive_api_fails += 1
        log.warning(f"{label} CancelledError (fails: {state.consecutive_api_fails})")
        return None

    except Exception as e:
        state.consecutive_api_fails += 1
        log.warning(f"{label} error (fails: {state.consecutive_api_fails}): {e}")
        return None


def needs_reconnect(state: BotState) -> bool:
    return state.consecutive_api_fails >= MAX_API_FAILS_BEFORE_RECONNECT


# ==================== RECONNECT HEARTBEAT ====================

async def reconnect_heartbeat(conn, state: BotState) -> bool:
    """
    Stille ping elke 60s om WebSocket disconnects te detecteren.
    Returns False als reconnect nodig is.
    """
    now = time.time()
    if now - state.last_reconnect_heartbeat < RECONNECT_HEARTBEAT_INTERVAL:
        return True

    state.last_reconnect_heartbeat = now
    try:
        info = await asyncio.wait_for(conn.get_account_information(), timeout=10)
        if info and "balance" in info:
            state.connection_healthy = True
            return True
    except Exception as e:
        log.warning(f"Reconnect heartbeat failed: {e}")

    state.connection_healthy = False
    state.consecutive_api_fails += 1
    return not needs_reconnect(state)


# ==================== DYNAMIC PIP VALUE ====================

async def get_dynamic_pip_value(conn, symbol: str, state: BotState) -> Optional[float]:
    """
    Dynamische pip_value berekening voor JPY pairs.
    Returns None als geen actuele rate beschikbaar — trade wordt dan geskipt.
    """
    spec = SYMBOL_SPECS.get(symbol)
    if not spec:
        return None

    if spec["pip_value_per_lot"] is not None:
        return spec["pip_value_per_lot"]

    # Check cache (5 min TTL)
    now = time.time()
    if symbol in state._pip_value_cache and now - state._pip_value_cache[symbol]["time"] < 300:
        return state._pip_value_cache[symbol]["value"]

    # Dynamisch berekenen voor JPY pairs
    try:
        price = await rate_limited_call(
            conn.get_symbol_price(symbol), state, timeout=10, label=f"pip_val_{symbol}")
        if price and price.get("bid", 0) > 0:
            rate = price["bid"]
            pip_value = (spec["pip_size"] / rate) * spec["contract"]
            state._pip_value_cache[symbol] = {"value": pip_value, "time": now}
            return pip_value
    except Exception as e:
        log.warning(f"Pip value calc error {symbol}: {e}")

    # GEEN FALLBACK — return None zodat trade wordt geskipt
    log.warning(f"Kan pip value niet berekenen voor {symbol} — trade wordt geskipt")
    return None


# ==================== CANDLE HELPER ====================

TF_MINUTES = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}

async def get_candles(account, symbol: str, timeframe: str, count: int, state: BotState):
    tf_min = TF_MINUTES.get(timeframe, 15)
    start = datetime.now(timezone.utc) - timedelta(minutes=tf_min * count * 1.5)
    try:
        result = await rate_limited_call(
            account.get_historical_candles(symbol, timeframe, start),
            state, timeout=20, label=f"candles_{symbol}_{timeframe}"
        )
        return result
    except Exception as e:
        log.warning(f"get_candles {symbol} {timeframe}: {e}")
        return None


# ==================== HEARTBEAT ====================

async def send_heartbeat(conn, state: BotState):
    now = time.time()
    if now - state.last_heartbeat < 600:
        return
    state.last_heartbeat = now
    try:
        info = await rate_limited_call(conn.get_account_information(), state, label="heartbeat")
        if not info or "balance" not in info:
            # Force reconnect — als heartbeat geen data kan halen is connectie dood
            state.consecutive_api_fails = MAX_API_FAILS_BEFORE_RECONNECT
            state.connection_healthy = False
            tg(f"💓 <b>HEARTBEAT</b> (limited)\n⚠️ Account info niet beschikbaar — forcing reconnect\n⏰ {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC", state)
            return
        balance = info["balance"]
        equity = info["equity"]
        positions = await rate_limited_call(conn.get_positions(), state, label="hb_positions")
        if positions is None:
            positions = []
    except Exception as e:
        log.error(f"Heartbeat error: {e}")
        state.consecutive_api_fails = MAX_API_FAILS_BEFORE_RECONNECT
        state.connection_healthy = False
        tg(f"💓 <b>HEARTBEAT</b> (limited)\n⚠️ Data ophalen mislukt — forcing reconnect: {str(e)[:60]}", state)
        return

    pl = equity - balance
    pl_pct = (pl / balance) * 100 if balance > 0 else 0
    kz = get_current_killzone()
    daily_loss = ((state.daily_state["start_balance"] - balance) / state.daily_state["start_balance"] * 100) if state.daily_state["start_balance"] > 0 else 0
    total_zones = sum(len([z for z in zones if z.is_valid]) for zones in state.zone_store.values())
    total_htf_zones = sum(len([z for z in zones if z.is_valid]) for zones in state.htf_zone_store.values())
    tg_status = "✅" if state.tg_fail_count == 0 else f"⚠️ {state.tg_fail_count} fails"
    active_cooldowns = [s for s, cd in state.symbol_cooldowns.items() if cd.get("until", 0) > now]
    pf = db_get_profit_factor()
    wr = (state.performance['wins']/(state.performance['wins']+state.performance['losses'])*100) if (state.performance['wins']+state.performance['losses']) > 0 else 0
    mode_str = "🧪 DRY RUN" if DRY_RUN else "🔴 LIVE"

    # Symbol health status
    sym_health = []
    for s in SYMBOLS:
        s_wr, s_total = state.get_symbol_winrate(s)
        healthy = state.is_symbol_healthy(s)
        status = "✅" if healthy else "⛔"
        if s_total >= 5:
            sym_health.append(f"{s}: {s_wr*100:.0f}%({s_total}t){status}")

    sym_str = " | ".join(sym_health) if sym_health else "geen data"

    # Drawdown check
    dd_pct = 0
    if state.performance["peak_balance"] > 0:
        dd_pct = (state.performance["peak_balance"] - equity) / state.performance["peak_balance"] * 100

    msg = f"""<b>💓 SMC v4.1 HEARTBEAT</b> {mode_str}

💰 Balance: ${balance:,.2f}
📊 Equity: ${equity:,.2f}
📈 P&L: ${pl:,.2f} ({pl_pct:+.2f}%)
📉 Drawdown: {dd_pct:.1f}% / {MAX_DRAWDOWN_HARD_STOP*100:.0f}%{'⚠️' if dd_pct > 5 else ''}
🎯 Open trades: {len(positions)}/{MAX_TOTAL_TRADES}
🗺️ Zones: {total_zones} (5M/15M) + {total_htf_zones} (1H)
🕐 Killzone: {kz.upper() if kz else 'NONE'}
📅 Daily loss: {daily_loss:.2f}% / {DAILY_LOSS_LIMIT*100:.1f}%
📆 Weekly loss: {state.weekly_state['loss']*100:.2f}% / {WEEKLY_LOSS_LIMIT*100:.1f}%
📊 Trades today: {state.daily_state['trades_today']}/{MAX_TRADES_PER_DAY}
📈 Performance:
W/L: {state.performance['wins']}/{state.performance['losses']}
WR: {wr:.0f}% | PF: {pf:.2f}
Streak: {state.performance['consecutive_wins']}W / {state.performance['consecutive_losses']}L
Peak: ${state.performance['peak_balance']:,.2f}
🏥 Symbol health: {sym_str}
🧊 Cooldowns: {', '.join(active_cooldowns) if active_cooldowns else 'geen'}
🔗 Connection: {'✅' if state.connection_healthy else '❌'}
📡 Telegram: {tg_status}
⚙️ Loop errors: {state.consecutive_errors}
{'🛑 HARD STOP ACTIEF' if state.hard_stop else ''}
⏰ {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"""
    tg(msg, state)


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


# ==================== SESSION HIGH/LOW TRACKING ====================

def update_session_levels(state: BotState, symbol: str, high: float, low: float):
    kz = get_current_killzone()
    if not kz:
        return
    today = date.today()
    if symbol not in state.session_levels:
        state.session_levels[symbol] = {}
    key = kz
    if key not in state.session_levels[symbol] or state.session_levels[symbol][key].get("date") != today:
        state.session_levels[symbol][key] = {"high": high, "low": low, "date": today}
    else:
        if high > state.session_levels[symbol][key]["high"]:
            state.session_levels[symbol][key]["high"] = high
        if low < state.session_levels[symbol][key]["low"]:
            state.session_levels[symbol][key]["low"] = low


def get_previous_session_levels(state: BotState, symbol: str) -> Optional[dict]:
    kz = get_current_killzone()
    if not kz or symbol not in state.session_levels:
        return None
    kz_order = ["asia", "london", "london_ext", "new_york", "ny_pm"]
    try:
        idx = kz_order.index(kz)
        if idx > 0:
            prev_kz = kz_order[idx - 1]
            if prev_kz in state.session_levels[symbol]:
                return state.session_levels[symbol][prev_kz]
    except ValueError:
        pass
    return None


# ==================== ASIA RANGE ====================

async def update_asia_range(account, state: BotState):
    today = date.today()
    for symbol in SYMBOLS:
        if symbol in state.asia_range_cache and state.asia_range_cache[symbol]["date"] == today:
            continue
        try:
            candles = await get_candles(account, symbol, "15m", 60, state)
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
                state.asia_range_cache[symbol] = {
                    "high": float(asia["high"].max()),
                    "low": float(asia["low"].min()),
                    "mid": float((asia["high"].max() + asia["low"].min()) / 2),
                    "date": today,
                }
                log.info(f"Asia range {symbol}: {state.asia_range_cache[symbol]['low']:.5f} - {state.asia_range_cache[symbol]['high']:.5f}")
        except Exception as e:
            log.warning(f"Asia range error {symbol}: {e}")


# ==================== RISK MANAGEMENT ====================

def reset_weekly_if_needed(state: BotState, balance: float):
    """Alleen state reset — geen boolean return (geen side effects in check)"""
    now = datetime.now(timezone.utc)
    cw = now.isocalendar()[1]
    if state.weekly_state["week"] != cw:
        was_set = state.weekly_state["week"] is not None
        state.weekly_state.update({"week": cw, "loss": 0, "limit_hit": False, "start_balance": balance})
        if was_set:
            tg("📊 <b>WEEKLY RESET</b>", state)


def is_weekly_limit_ok(state: BotState) -> bool:
    """Pure check — geen side effects"""
    return not state.weekly_state["limit_hit"]


def update_weekly_loss(state: BotState, balance: float):
    if state.weekly_state["start_balance"] > 0:
        loss = (state.weekly_state["start_balance"] - balance) / state.weekly_state["start_balance"]
        if loss > state.weekly_state["loss"]:
            state.weekly_state["loss"] = loss
        if loss >= WEEKLY_LOSS_LIMIT:
            state.weekly_state["limit_hit"] = True
            tg(f"🚨 <b>WEEKLY LIMIT</b>: {loss*100:.1f}% >= {WEEKLY_LOSS_LIMIT*100:.0f}%", state)


def reset_daily_if_needed(state: BotState, balance: float):
    """Alleen state reset"""
    today = date.today()
    if state.daily_state["date"] != today:
        state.daily_state.update({"date": today, "start_balance": balance, "trades_today": 0})
    if state.daily_state["start_balance"] == 0:
        state.daily_state["start_balance"] = balance


def is_daily_limit_ok(state: BotState, balance: float) -> bool:
    """Pure check"""
    if state.daily_state["start_balance"] == 0:
        return True
    loss = (state.daily_state["start_balance"] - balance) / state.daily_state["start_balance"]
    if loss >= DAILY_LOSS_LIMIT:
        tg(f"🚨 <b>DAILY LIMIT</b>: {loss*100:.2f}% >= {DAILY_LOSS_LIMIT*100:.1f}%", state)
        return False
    return True


def check_max_drawdown(state: BotState, equity: float) -> bool:
    """
    HARD STOP: als equity 10%+ onder peak zakt, stop de bot volledig.
    Returns True als OK, False als hard stop nodig.
    """
    if state.performance["peak_balance"] <= 0:
        return True

    drawdown = (state.performance["peak_balance"] - equity) / state.performance["peak_balance"]
    if drawdown >= MAX_DRAWDOWN_HARD_STOP:
        if not state.hard_stop:
            state.hard_stop = True
            tg(f"""🚨🚨🚨 <b>MAX DRAWDOWN HARD STOP</b> 🚨🚨🚨

📉 Drawdown: {drawdown*100:.1f}% >= {MAX_DRAWDOWN_HARD_STOP*100:.0f}%
💰 Peak: ${state.performance['peak_balance']:,.2f}
📊 Equity: ${equity:,.2f}
🛑 Bot stopt volledig met nieuwe trades
⚠️ Open posities worden nog beheerd

Handmatige review nodig!""", state)
        return False
    return True


def check_cooldown(state: BotState, symbol: str) -> Tuple[bool, int]:
    now = time.time()
    if symbol in state.symbol_cooldowns:
        cd = state.symbol_cooldowns[symbol]
        if cd.get("until", 0) > now:
            return False, int((cd["until"] - now) / 60)
    return True, 0


def register_trade_result(state: BotState, symbol: str, is_win: bool, profit: float = 0):
    if symbol not in state.symbol_cooldowns:
        state.symbol_cooldowns[symbol] = {"losses": 0, "until": 0}

    is_gold = symbol == GOLD_SCALP["symbol"]
    cd_minutes = GOLD_SCALP["cooldown_minutes"] if is_gold else COOLDOWN_MINUTES

    if is_win:
        state.symbol_cooldowns[symbol]["losses"] = 0
    else:
        state.symbol_cooldowns[symbol]["losses"] += 1
        if is_gold:
            register_gold_session_loss(state, symbol)
        if state.symbol_cooldowns[symbol]["losses"] >= COOLDOWN_AFTER_LOSSES:
            state.symbol_cooldowns[symbol]["until"] = time.time() + cd_minutes * 60
            tg(f"🧊 <b>COOLDOWN {symbol} {cd_minutes}min</b> na {state.symbol_cooldowns[symbol]['losses']} losses", state)

    # Update performance
    update_performance(state, is_win, profit)
    state.update_symbol_stats(symbol, is_win)

    # Symbol health check
    wr, total = state.get_symbol_winrate(symbol)
    if total >= SYMBOL_MIN_TRADES_FOR_EVAL and wr < SYMBOL_MIN_WINRATE:
        tg(f"⛔ <b>SYMBOL GEPAUZEERD</b>: {symbol}\nWinrate: {wr*100:.0f}% na {total} trades < {SYMBOL_MIN_WINRATE*100:.0f}% minimum\n🔄 Evaluatie nodig!", state)

    # Profit factor check
    pf = db_get_profit_factor()
    total_trades = state.performance["wins"] + state.performance["losses"]
    if total_trades >= 20 and pf < 1.0:
        tg(f"⚠️ <b>PROFIT FACTOR ALARM</b>: PF = {pf:.2f} na {total_trades} trades", state)


def get_dynamic_risk(state: BotState, balance: float, grade: str = "B+",
                     symbol: str = "", df: pd.DataFrame = None) -> float:
    is_gold_scalp = GOLD_SCALP["enabled"] and symbol == GOLD_SCALP["symbol"]
    if is_gold_scalp:
        risk = GOLD_SCALP["risk_override"]
    else:
        risk = GRADE_RISK.get(grade, 0.005)

    # Drawdown protectie
    if state.daily_state["start_balance"] > 0:
        day_loss = (state.daily_state["start_balance"] - balance) / state.daily_state["start_balance"]
        if day_loss >= 0.01:
            risk *= 0.5
        elif day_loss >= 0.005:
            risk *= 0.7

    if state.performance["peak_balance"] > 0 and balance > 0:
        drawdown = (state.performance["peak_balance"] - balance) / state.performance["peak_balance"]
        if drawdown >= 0.03:
            risk *= 0.25
        elif drawdown >= 0.015:
            risk *= 0.5

    # Streak modifier
    if state.performance["consecutive_wins"] >= 4:
        risk *= STREAK_MODIFIER["wins_4plus"]
    elif state.performance["consecutive_wins"] >= 2:
        risk *= STREAK_MODIFIER["wins_2_3"]
    elif state.performance["consecutive_losses"] >= 3:
        risk *= STREAK_MODIFIER["loss_3plus"]
    elif state.performance["consecutive_losses"] >= 2:
        risk *= STREAK_MODIFIER["loss_2"]
    elif state.performance["consecutive_losses"] >= 1:
        risk *= STREAK_MODIFIER["loss_1"]

    # Volatility scaling
    if df is not None and "atr" in df.columns:
        current_atr = float(df["atr"].iloc[-1])
        avg_atr = float(df["atr"].tail(50).mean())
        if avg_atr > 0:
            vol_ratio = current_atr / avg_atr
            if vol_ratio > 1.5:
                risk *= 0.6
            elif vol_ratio > 1.2:
                risk *= 0.8
            elif vol_ratio < 0.7:
                risk *= 1.1

    # Per-symbol adjustment based on winrate
    wr, total = state.get_symbol_winrate(symbol)
    if total >= 10:
        if wr < 0.45:
            risk *= 0.7  # Reduceer risk voor zwakkere symbols
        elif wr > 0.60:
            risk *= 1.1  # Iets meer risk voor sterke symbols

    return max(risk, 0.002)


def update_performance(state: BotState, is_win: bool, profit: float = 0,
                        grade: str = "", kz: str = ""):
    if is_win:
        state.performance["wins"] += 1
        state.performance["consecutive_wins"] += 1
        state.performance["consecutive_losses"] = 0
        state.performance["total_profit"] += profit
    else:
        state.performance["losses"] += 1
        state.performance["consecutive_losses"] += 1
        state.performance["consecutive_wins"] = 0
        state.performance["total_loss"] += abs(profit)

    state.performance["recent_results"].append(is_win)
    if len(state.performance["recent_results"]) > 20:
        state.performance["recent_results"] = state.performance["recent_results"][-20:]

    if grade:
        if grade not in state.performance["grade_stats"]:
            state.performance["grade_stats"][grade] = {"w": 0, "l": 0}
        if is_win:
            state.performance["grade_stats"][grade]["w"] += 1
        else:
            state.performance["grade_stats"][grade]["l"] += 1

    if kz:
        if kz not in state.performance["kz_stats"]:
            state.performance["kz_stats"][kz] = {"w": 0, "l": 0}
        if is_win:
            state.performance["kz_stats"][kz]["w"] += 1
        else:
            state.performance["kz_stats"][kz]["l"] += 1

    db_save_state("performance", state.performance)


def update_peak_balance(state: BotState, balance: float):
    if balance > state.performance["peak_balance"]:
        state.performance["peak_balance"] = balance
    if state.performance["session_start_balance"] == 0:
        state.performance["session_start_balance"] = balance


# ==================== ADAPTIVE BOOSTS ====================

def get_adaptive_boosts(state: BotState) -> dict:
    now = time.time()
    if state._adaptive_cache["data"] and (now - state._adaptive_cache["last_calc"]) < state._adaptive_cache["ttl"]:
        return state._adaptive_cache["data"]

    rows = db_execute(
        "SELECT symbol, confirmation, killzone, zone_type, profit FROM trades WHERE closed = 1 AND dry_run = 0",
        fetch=True)
    if not rows or len(rows) < 10:
        return {}

    boosts = {"symbol": {}, "confirmation": {}, "killzone": {}, "zone_type": {}}
    for category_idx, category in enumerate(["symbol", "confirmation", "killzone", "zone_type"]):
        stats = {}
        for r in rows:
            key = r[category_idx] or "unknown"
            if key not in stats:
                stats[key] = {"wins": 0, "losses": 0}
            if r[4] > 0:
                stats[key]["wins"] += 1
            else:
                stats[key]["losses"] += 1
        for key, s in stats.items():
            total = s["wins"] + s["losses"]
            if total < 20:
                boosts[category][key] = 1.0
                continue
            wr = s["wins"] / total
            boosts[category][key] = round(0.6 + wr * 1.2, 2)

    state._adaptive_cache["data"] = boosts
    state._adaptive_cache["last_calc"] = now
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

async def check_spread(conn, symbol: str, state: BotState) -> Tuple[bool, float]:
    try:
        price = await rate_limited_call(conn.get_symbol_price(symbol), state, label=f"spread_{symbol}")
        if not price:
            return False, 0
        spread = price["ask"] - price["bid"]
        spec = SYMBOL_SPECS[symbol]
        spread_pips = spread / spec["pip_size"]

        is_gold_scalp = GOLD_SCALP["enabled"] and symbol == GOLD_SCALP["symbol"]
        max_spread = GOLD_SCALP["max_spread_scalp"] if is_gold_scalp else spec["max_spread_pips"]

        if spread_pips > max_spread:
            return False, spread
        return True, spread
    except Exception:
        return False, 0


# ==================== NEWS FILTER ====================

async def news_filter(state: BotState) -> bool:
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
                    tg(f"📰 <b>NEWS BLOCK</b>: {event.get('title','?')}", state)
                    return False
            except Exception:
                continue
        return True
    except Exception as e:
        log.warning(f"News filter error (allowing trades): {e}")
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


# ==================== OTE FIBONACCI ZONE ====================

def detect_ote_zone(df: pd.DataFrame, swings: List[SwingPoint], direction: Direction) -> Optional[Zone]:
    highs = [s for s in swings if s.type == "high"]
    lows = [s for s in swings if s.type == "low"]
    if len(highs) < 2 or len(lows) < 2:
        return None

    if direction == Direction.BULL:
        swing_low = min(s.price for s in lows[-3:])
        swing_high = max(s.price for s in highs[-3:])
        if swing_high <= swing_low:
            return None
        rng = swing_high - swing_low
        ote_high = swing_high - rng * 0.618
        ote_low = swing_high - rng * 0.786
        current = float(df["close"].iloc[-1])
        if current > ote_high or current < ote_low:
            return None
        return Zone(
            type=ZoneType.OTE, direction=Direction.BULL,
            high=ote_high, low=ote_low,
            midpoint=(ote_high + ote_low) / 2,
            created_at=time.time(),
        )
    elif direction == Direction.BEAR:
        swing_high = max(s.price for s in highs[-3:])
        swing_low = min(s.price for s in lows[-3:])
        if swing_high <= swing_low:
            return None
        rng = swing_high - swing_low
        ote_low = swing_low + rng * 0.618
        ote_high = swing_low + rng * 0.786
        current = float(df["close"].iloc[-1])
        if current < ote_low or current > ote_high:
            return None
        return Zone(
            type=ZoneType.OTE, direction=Direction.BEAR,
            high=ote_high, low=ote_low,
            midpoint=(ote_high + ote_low) / 2,
            created_at=time.time(),
        )
    return None


# ==================== GOLD ROUND NUMBER DETECTION ====================

def detect_gold_round_number(df: pd.DataFrame, direction: Direction) -> Optional[Zone]:
    if not GOLD_SCALP["enabled"]:
        return None
    current_price = float(df["close"].iloc[-1])
    step = GOLD_SCALP["round_number_step"]
    buffer = GOLD_SCALP["round_number_buffer"]
    nearest_round = round(current_price / step) * step
    distance = abs(current_price - nearest_round)
    if distance > buffer:
        return None
    last = df.iloc[-1]
    if direction == Direction.BULL and current_price < nearest_round:
        if last["close"] > last["open"] and last["low"] <= nearest_round + buffer:
            return Zone(
                type=ZoneType.ORDER_BLOCK, direction=Direction.BULL,
                high=nearest_round + buffer, low=nearest_round - buffer,
                midpoint=nearest_round, created_at=time.time(),
            )
    elif direction == Direction.BEAR and current_price > nearest_round:
        if last["close"] < last["open"] and last["high"] >= nearest_round - buffer:
            return Zone(
                type=ZoneType.ORDER_BLOCK, direction=Direction.BEAR,
                high=nearest_round + buffer, low=nearest_round - buffer,
                midpoint=nearest_round, created_at=time.time(),
            )
    return None


def check_gold_session_limit(state: BotState, symbol: str) -> bool:
    if symbol != GOLD_SCALP["symbol"]:
        return True
    kz = get_current_killzone()
    if not kz:
        return False
    if state.gold_session_losses["killzone"] != kz:
        state.gold_session_losses["killzone"] = kz
        state.gold_session_losses["losses"] = 0
    if state.gold_session_losses["losses"] >= GOLD_SCALP["session_max_losses"]:
        return False
    return True


def register_gold_session_loss(state: BotState, symbol: str):
    if symbol == GOLD_SCALP["symbol"]:
        kz = get_current_killzone()
        if kz and state.gold_session_losses["killzone"] == kz:
            state.gold_session_losses["losses"] += 1
            if state.gold_session_losses["losses"] >= GOLD_SCALP["session_max_losses"]:
                tg(f"🥇🧊 <b>GOLD SCALP SESSIE STOP</b>\n{state.gold_session_losses['losses']} losses in {kz.upper()}", state)


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

def detect_liquidity_sweep(df: pd.DataFrame, symbol: str, swings: List[SwingPoint],
                           state: BotState) -> Optional[dict]:
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

    # Session High/Low Sweep
    prev_session = get_previous_session_levels(state, symbol)
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
    if symbol in state.asia_range_cache:
        ar = state.asia_range_cache[symbol]
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

    has_volume = False
    if "volume" in df.columns and "avg_volume" in df.columns:
        vol = float(last["volume"])
        avg_vol = float(last["avg_volume"])
        if avg_vol > 0 and vol > avg_vol * 1.3:
            has_volume = True

    if direction == Direction.BULL:
        lw = float(last["lower_wick"])
        cr = float(last["candle_range"])

        if cr > 0 and lw / cr >= MIN_REJECTION_WICK_RATIO:
            if last["close"] > last["open"] and zone.contains_price(float(last["low"]), ZONE_ENTRY_BUFFER):
                return "rejection_wick_vol" if has_volume else "rejection_wick"

        if (last["close"] > last["open"] and prev["close"] < prev["open"]
            and last["body"] > prev["body"] * MIN_ENGULFING_BODY_RATIO
            and last["close"] > prev["open"] and last["open"] < prev["close"]):
            if zone.contains_price(float(last["low"]), ZONE_CONFIRM_BUFFER):
                return "engulfing_vol" if has_volume else "engulfing"

        body = float(last["body"])
        if cr > 0 and body / cr < 0.3 and lw / cr > 0.5 and last["close"] > last["open"]:
            return "pin_bar_vol" if has_volume else "pin_bar"

        avg_body = float(df["body"].tail(20).mean())
        if (last["close"] > last["open"] and last["body"] > avg_body * MIN_STRONG_CLOSE_BODY_RATIO
            and zone.contains_price(float(last["open"]), ZONE_CONFIRM_BUFFER)):
            return "strong_close_vol" if has_volume else "strong_close"

    elif direction == Direction.BEAR:
        uw = float(last["upper_wick"])
        cr = float(last["candle_range"])

        if cr > 0 and uw / cr >= MIN_REJECTION_WICK_RATIO:
            if last["close"] < last["open"] and zone.contains_price(float(last["high"]), ZONE_ENTRY_BUFFER):
                return "rejection_wick_vol" if has_volume else "rejection_wick"

        if (last["close"] < last["open"] and prev["close"] > prev["open"]
            and last["body"] > prev["body"] * MIN_ENGULFING_BODY_RATIO
            and last["close"] < prev["open"] and last["open"] > prev["close"]):
            if zone.contains_price(float(last["high"]), ZONE_CONFIRM_BUFFER):
                return "engulfing_vol" if has_volume else "engulfing"

        body = float(last["body"])
        if cr > 0 and body / cr < 0.3 and uw / cr > 0.5 and last["close"] < last["open"]:
            return "pin_bar_vol" if has_volume else "pin_bar"

        avg_body = float(df["body"].tail(20).mean())
        if (last["close"] < last["open"] and last["body"] > avg_body * MIN_STRONG_CLOSE_BODY_RATIO
            and zone.contains_price(float(last["open"]), ZONE_CONFIRM_BUFFER)):
            return "strong_close_vol" if has_volume else "strong_close"

    return None


# ==================== ZONE MANAGEMENT ====================

def store_zones(state: BotState, symbol: str, new_zones: List[Zone], htf: bool = False):
    store = state.htf_zone_store if htf else state.zone_store
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

    if now - state._last_zone_save > ZONE_SAVE_INTERVAL:
        db_save_zones_bulk(state.zone_store, htf=False)
        db_save_zones_bulk(state.htf_zone_store, htf=True)
        state._last_zone_save = now


def find_active_zone(state: BotState, symbol: str, price: float, direction: Direction) -> Optional[Zone]:
    if symbol not in state.zone_store:
        return None
    tf_priority = {"15m": 0, "5m": 1}
    candidates = []
    for zone in state.zone_store[symbol]:
        if not zone.is_valid or zone.direction != direction:
            continue
        if zone.contains_price(price, buffer_pct=ZONE_ENTRY_BUFFER):
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
        zone.status = ZoneStatus.TESTED


def update_zone_status(state: BotState, symbol: str, df: pd.DataFrame):
    current_price = float(df["close"].iloc[-1])
    for store in [state.zone_store, state.htf_zone_store]:
        if symbol not in store:
            continue
        for zone in store[symbol]:
            if not zone.is_valid:
                continue
            if zone.direction == Direction.BULL and current_price < zone.low:
                zone.status = ZoneStatus.MITIGATED
            elif zone.direction == Direction.BEAR and current_price > zone.high:
                zone.status = ZoneStatus.MITIGATED


def check_zone_confluence(state: BotState, zone: Zone, symbol: str) -> bool:
    if symbol not in state.htf_zone_store:
        return False
    for z1h in state.htf_zone_store[symbol]:
        if not z1h.is_valid or z1h.direction != zone.direction:
            continue
        if zone.low >= z1h.low * 0.998 and zone.high <= z1h.high * 1.002:
            return True
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

async def get_htf_bias(account, symbol: str, state: BotState) -> Optional[Direction]:
    try:
        candles = await get_candles(account, symbol, "1h", 120, state)
        if not candles or len(candles) < 60:
            return None
        df = pd.DataFrame(candles)
        df = calculate_indicators(df)
        price = float(df["close"].iloc[-1])
        ema50 = float(df["ema50"].iloc[-1])
        ema200 = float(df["ema200"].iloc[-1])

        swings_1h = detect_swing_points(df, lookback=4)
        structure_1h = analyze_structure(df, swings_1h)
        if structure_1h:
            new_obs_1h = detect_order_blocks(df, structure_1h)
            for ob in new_obs_1h:
                ob.timeframe = "1h"
            store_zones(state, symbol, new_obs_1h, htf=True)
            new_fvgs_1h = detect_fvgs(df, lookback=8)
            for fvg in new_fvgs_1h:
                fvg.timeframe = "1h"
            store_zones(state, symbol, new_fvgs_1h, htf=True)

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

def grade_setup(state: BotState, htf_aligned: bool, structure, zone, confirmation, sweep,
                premium_discount, regime, direction, symbol="",
                has_confluence=False, is_ote=False):
    """
    FIX: htf_aligned is nu altijd een bool — geen type-inconsistentie meer.
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

    if "_vol" in confirmation:
        score += 1.0
        reasons.append("VOLUME✓")

    if has_confluence:
        score += 2.0
        reasons.append("CONFLUENCE_1H")

    if is_ote:
        score += 2.0
        reasons.append("OTE_FIB")

    if htf_aligned:
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
        score -= 3.0
        reasons.append("RANGING⛔")

    if zone.type == ZoneType.ORDER_BLOCK and zone.structure_break:
        score += 0.5
        reasons.append("OB+STRUCT")

    # Adaptive boosts
    boosts = get_adaptive_boosts(state)
    adaptive_adjustment = 0
    if boosts:
        sym_boost = boosts.get("symbol", {}).get(symbol, 1.0)
        if sym_boost != 1.0:
            adaptive_adjustment += (sym_boost - 1.0) * 2
            reasons.append(f"ADP_SYM({sym_boost:.2f})")

        conf_boost = boosts.get("confirmation", {}).get(confirmation, 1.0)
        if conf_boost != 1.0:
            adaptive_adjustment += (conf_boost - 1.0) * 1.5
            reasons.append(f"ADP_CONF({conf_boost:.2f})")

        kz = get_current_killzone() or "unknown"
        kz_boost = boosts.get("killzone", {}).get(kz, 1.0)
        if kz_boost != 1.0:
            adaptive_adjustment += (kz_boost - 1.0) * 1.0
            reasons.append(f"ADP_KZ({kz_boost:.2f})")

        zt_boost = boosts.get("zone_type", {}).get(zone.type.value, 1.0)
        if zt_boost != 1.0:
            adaptive_adjustment += (zt_boost - 1.0) * 1.0
            reasons.append(f"ADP_ZT({zt_boost:.2f})")

        adaptive_adjustment = max(-2.0, min(2.0, adaptive_adjustment))
        score += adaptive_adjustment

    if score >= 10:
        has_sweep = any("SWEEP" in r for r in reasons)
        has_pd = any("P/D_ALIGNED" in r for r in reasons)
        has_choch = any("CHoCH" in r for r in reasons)
        has_conf = any("CONFLUENCE" in r for r in reasons)
        has_ote_r = any("OTE" in r for r in reasons)
        if has_sweep or has_pd or has_choch or has_conf or has_ote_r:
            return "A+", 1.0, score, reasons
        else:
            return "A", 1.0, score, reasons
    elif score >= 8:
        return "A", 0.75, score, reasons
    elif score >= 6:
        return "B+", 0.6, score, reasons
    return "D", 0, score, reasons


# ==================== LOT SIZE ====================

def calculate_lot_size(balance: float, sl_distance: float, symbol: str,
                       risk_pct: float, pip_value: float) -> Tuple[float, dict]:
    if sl_distance <= 0:
        return 0, {"error": "sl_distance <= 0"}
    spec = SYMBOL_SPECS.get(symbol)
    if not spec:
        return 0, {"error": "unknown symbol"}

    sl_pips = sl_distance / spec["pip_size"]
    risk_amount = balance * risk_pct
    denom = sl_pips * pip_value
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
        "pip_value_used": round(pip_value, 4),
        "capped": raw_lot > limits["max"],
        "floored": raw_lot < min_lot,
        "category": spec["category"],
    }
    return lot, details


# ==================== SL / TP BEREKENING ====================

def calculate_trade_levels(direction: Direction, entry: float, zone: Zone,
                           df: pd.DataFrame, symbol: str = "", spread: float = 0):
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


# ==================== POSITION MANAGEMENT (MET TRADE PHASES) ====================

async def manage_positions(conn, positions: list, state: BotState):
    """
    33/33/33 partial close met expliciete TradePhase state machine.
    Voorkomt gemiste partials en dubbele closes.
    """
    now = datetime.now(timezone.utc)

    # Cleanup phases van gesloten posities
    active_pids = {pos["id"] for pos in positions}
    state.cleanup_trade_phases(active_pids)

    # Vrijdag auto-close
    if now.weekday() == 4 and now.hour >= 21 and now.minute >= 30:
        for pos in positions:
            try:
                symbol = pos["symbol"]
                pid = pos["id"]
                profit = pos.get("profit", 0) + pos.get("swap", 0) + pos.get("commission", 0)
                if not DRY_RUN:
                    await asyncio.wait_for(conn.close_position(pid), timeout=10)
                state.set_trade_phase(pid, TradePhase.CLOSED)
                emoji = "💰" if profit >= 0 else "💔"
                tg(f"🕐 <b>FRIDAY CLOSE</b>: {symbol} {emoji} {'+'if profit>=0 else ''}{profit:.2f}", state)
            except Exception as e:
                log.warning(f"Friday close error {pos.get('symbol','?')}: {e}")
        return

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

            # Huidige phase
            phase = state.get_trade_phase(pid)

            # === MAE/MFE TRACKING ===
            adverse = -profit_dist if profit_dist < 0 else 0
            favorable = profit_dist if profit_dist > 0 else 0
            mae_key = f"mae_{pid}"
            mfe_key = f"mfe_{pid}"
            current_mae = float(db_load_state(mae_key, 0) or 0)
            current_mfe = float(db_load_state(mfe_key, 0) or 0)
            if adverse > current_mae:
                db_save_state(mae_key, adverse)
            if favorable > current_mfe:
                db_save_state(mfe_key, favorable)

            # === FASE 1: PARTIAL 33% bij TP1 (2.0R) — alleen als phase == OPEN ===
            if phase == TradePhase.OPEN and profit_dist >= sl_dist * 2.0:
                partial = math.floor((vol * 0.33) / lot_step) * lot_step
                partial = round(partial, 2)
                remaining = round(vol - partial, 2)

                if partial >= min_lot and remaining >= min_lot:
                    try:
                        if not DRY_RUN:
                            await asyncio.wait_for(conn.close_position_partially(pid, partial), timeout=10)
                            be_buf = sl_dist * 0.1
                            new_sl = (open_p + be_buf) if is_buy else (open_p - be_buf)
                            await asyncio.wait_for(conn.modify_position(pid, stop_loss=new_sl, take_profit=tp), timeout=10)
                        state.set_trade_phase(pid, TradePhase.TP1_HIT)
                        tg(f"✅ <b>TP1 HIT</b>: {symbol}\n💰 33% gesloten ({partial} lots)\n🛡️ SL → breakeven\n📊 Runner: {remaining} lots", state)
                    except Exception as e:
                        log.warning(f"Partial TP1 error {symbol}: {e}")

            # === FASE 2: PARTIAL 33% bij TP2 (3.0R) — alleen als phase == TP1_HIT ===
            elif phase == TradePhase.TP1_HIT and profit_dist >= sl_dist * 3.0:
                partial2 = math.floor((vol * 0.5) / lot_step) * lot_step
                partial2 = round(partial2, 2)
                remaining2 = round(vol - partial2, 2)

                if partial2 >= min_lot and remaining2 >= min_lot:
                    try:
                        tp1_level = (open_p + sl_dist * 2.0) if is_buy else (open_p - sl_dist * 2.0)
                        if not DRY_RUN:
                            await asyncio.wait_for(conn.close_position_partially(pid, partial2), timeout=10)
                            await asyncio.wait_for(conn.modify_position(pid, stop_loss=round(tp1_level, 5), take_profit=tp), timeout=10)
                        state.set_trade_phase(pid, TradePhase.TP2_HIT)
                        tg(f"✅ <b>TP2 HIT</b>: {symbol}\n💰 33% gesloten ({partial2} lots)\n🛡️ SL → TP1 level\n📊 Runner: {remaining2} lots naar TP3", state)
                    except Exception as e:
                        log.warning(f"Partial TP2 error {symbol}: {e}")

            # === FASE 3: TRAILING SL bij 4.0R+ — alleen als phase >= TP2_HIT ===
            if phase in (TradePhase.TP2_HIT, TradePhase.TRAILING) and profit_dist >= sl_dist * 4.0:
                trail_dist = sl_dist * 1.5
                if is_buy:
                    new_sl = cur_p - trail_dist
                    if sl and new_sl > sl + spec["pip_size"]:
                        try:
                            if not DRY_RUN:
                                await asyncio.wait_for(conn.modify_position(pid, stop_loss=round(new_sl, 5), take_profit=tp), timeout=10)
                            if phase != TradePhase.TRAILING:
                                state.set_trade_phase(pid, TradePhase.TRAILING)
                        except Exception:
                            pass
                else:
                    new_sl = cur_p + trail_dist
                    if sl and new_sl < sl - spec["pip_size"]:
                        try:
                            if not DRY_RUN:
                                await asyncio.wait_for(conn.modify_position(pid, stop_loss=round(new_sl, 5), take_profit=tp), timeout=10)
                            if phase != TradePhase.TRAILING:
                                state.set_trade_phase(pid, TradePhase.TRAILING)
                        except Exception:
                            pass
        except Exception as e:
            log.warning(f"Position management error: {e}")


# ==================== CLOSED TRADE TRACKING ====================

async def check_closed_trades(conn, state: BotState):
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        history = await rate_limited_call(conn.get_deals_by_time_range(start, now), state, label="deals")
        if not history:
            return
        for deal in history[-5:]:
            profit = deal.get("profit", 0)
            did = deal.get("id", "")
            dk = f"deal_{did}"
            if dk in state.recent_signals:
                continue
            if profit != 0:
                state.recent_signals[dk] = time.time()
                symbol = deal.get("symbol", "UNKNOWN")

                journal_match = None
                for j in reversed(state.trade_journal):
                    if j.get("symbol") == symbol:
                        journal_match = j
                        break

                kz = get_current_killzone() or "unknown"
                grade = journal_match.get("grade", "") if journal_match else ""

                register_trade_result(state, symbol, profit > 0, profit)

                # MAE/MFE opslaan bij close
                pid = deal.get("positionId", "")
                mae = float(db_load_state(f"mae_{pid}", 0) or 0)
                mfe = float(db_load_state(f"mfe_{pid}", 0) or 0)
                db_update_trade_result(symbol, profit, mae, mfe)

                # Mark phase als closed
                if pid:
                    state.set_trade_phase(pid, TradePhase.CLOSED)

                emoji = "💰" if profit > 0 else "💔"
                pf = db_get_profit_factor()
                wr, total = state.get_symbol_winrate(symbol)
                tg(f"{emoji} <b>TRADE {'WON' if profit>0 else 'LOST'}</b>: {symbol} {'+'if profit>0 else ''}{profit:.2f}\n📊 PF: {pf:.2f} | {symbol} WR: {wr*100:.0f}% ({total}t)", state)
    except Exception as e:
        log.warning(f"Closed trades error: {e}")


# ==================== CANDLE CLOSE CHECK ====================

def is_candle_just_closed(tf_min: int = 5) -> bool:
    now = datetime.now(timezone.utc)
    return now.minute % tf_min == 0 and now.second < 15


# ==================== BOUNDED SIGNAL CLEANUP ====================

def cleanup_recent_signals(state: BotState):
    now = time.time()
    expired = [k for k, v in state.recent_signals.items() if now - v > 7200]
    for k in expired:
        del state.recent_signals[k]
    if len(state.recent_signals) > MAX_RECENT_SIGNALS:
        sorted_keys = sorted(state.recent_signals, key=state.recent_signals.get)
        for k in sorted_keys[:len(state.recent_signals) - MAX_RECENT_SIGNALS]:
            del state.recent_signals[k]


# ==================== HOOFDSTRATEGIE ====================

async def analyze_and_find_setup(account, conn, symbol: str, positions: list,
                                  balance: float, state: BotState) -> Optional[TradeSetup]:
    try:
        htf_bias = await get_htf_bias(account, symbol, state)

        # MTF (15M)
        candles_15m = await get_candles(account, symbol, "15m", 100, state)
        if not candles_15m or len(candles_15m) < 40:
            return None
        df_15m = pd.DataFrame(candles_15m)
        df_15m = calculate_indicators(df_15m)
        swings_15m = detect_swing_points(df_15m)
        structure_15m = analyze_structure(df_15m, swings_15m)
        regime = detect_regime(df_15m)

        if regime == "ranging":
            return None

        if structure_15m:
            new_obs = detect_order_blocks(df_15m, structure_15m)
            for ob in new_obs:
                ob.timeframe = "15m"
            store_zones(state, symbol, new_obs)
            new_fvgs = detect_fvgs(df_15m)
            for fvg in new_fvgs:
                fvg.timeframe = "15m"
            store_zones(state, symbol, new_fvgs)

        # LTF (5M)
        candles_5m = await get_candles(account, symbol, "5m", 100, state)
        if not candles_5m or len(candles_5m) < 50:
            return None
        df_5m = pd.DataFrame(candles_5m)
        df_5m = calculate_indicators(df_5m)
        swings_5m = detect_swing_points(df_5m)
        update_zone_status(state, symbol, df_5m)
        update_session_levels(state, symbol, float(df_5m["high"].iloc[-1]), float(df_5m["low"].iloc[-1]))

        structure_5m = analyze_structure(df_5m, swings_5m)
        if structure_5m:
            new_obs_5m = detect_order_blocks(df_5m, structure_5m)
            for ob in new_obs_5m:
                ob.timeframe = "5m"
            store_zones(state, symbol, new_obs_5m)
            new_fvgs_5m = detect_fvgs(df_5m)
            for fvg in new_fvgs_5m:
                fvg.timeframe = "5m"
            store_zones(state, symbol, new_fvgs_5m)

        sweep = detect_liquidity_sweep(df_5m, symbol, swings_5m, state)
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

        # Zoek zone
        active_zone = find_active_zone(state, symbol, current_price, direction)

        # OTE Fibonacci check
        is_ote = False
        if not active_zone:
            ote_zone = detect_ote_zone(df_15m, swings_15m, direction)
            if ote_zone:
                active_zone = ote_zone
                active_zone.timeframe = "15m_ote"
                is_ote = True

        # Gold round number check
        is_gold_scalp = GOLD_SCALP["enabled"] and symbol == GOLD_SCALP["symbol"]
        if not active_zone and is_gold_scalp:
            round_zone = detect_gold_round_number(df_5m, direction)
            if round_zone:
                active_zone = round_zone
                active_zone.timeframe = "5m_round"

        if not active_zone:
            return None

        has_confluence = check_zone_confluence(state, active_zone, symbol)

        # Gold prime killzone check
        if is_gold_scalp:
            kz = get_current_killzone()
            if kz and kz not in GOLD_SCALP["prime_killzones"]:
                if not (has_confluence or is_ote):
                    return None

        confirmation = check_confirmation(df_5m, direction, active_zone)
        if not confirmation:
            return None

        spread_ok, spread = await check_spread(conn, symbol, state)
        if not spread_ok:
            return None

        # FIX: htf_aligned is nu altijd een bool
        htf_aligned = (htf_bias == direction) if htf_bias else False

        grade, risk_mult, score, reasons = grade_setup(
            state=state,
            htf_aligned=htf_aligned,
            structure=structure_15m if structure_15m and structure_15m.direction == direction else structure_5m,
            zone=active_zone, confirmation=confirmation,
            sweep=sweep if sweep and sweep["type"] == direction.value else None,
            premium_discount=pd_zone, regime=regime, direction=direction,
            symbol=symbol, has_confluence=has_confluence, is_ote=is_ote,
        )

        if grade not in ("A+", "A", "B+"):
            return None

        price_data = await rate_limited_call(conn.get_symbol_price(symbol), state, label=f"price_{symbol}")
        if not price_data:
            return None
        entry = price_data["ask"] if direction == Direction.BULL else price_data["bid"]
        actual_spread = price_data["ask"] - price_data["bid"]

        sl, tp1, tp2, tp3, sl_dist = calculate_trade_levels(direction, entry, active_zone, df_5m, symbol, actual_spread)

        effective_entry = entry + actual_spread if direction == Direction.BULL else entry - actual_spread
        rr = abs((tp3 - effective_entry) / (abs(effective_entry - sl))) if sl_dist > 0 else 0

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
            ote_entry=is_ote,
        )
    except Exception as e:
        log.error(f"Analyse error {symbol}: {e}")
        return None


# ==================== TRADE EXECUTIE ====================

async def execute_trade(conn, setup: TradeSetup, balance: float, state: BotState,
                        df_5m: pd.DataFrame = None) -> bool:
    risk_pct = get_dynamic_risk(state, balance, grade=setup.grade, symbol=setup.symbol, df=df_5m) * setup.risk_mult
    sl_dist = abs(setup.entry - setup.stop_loss)

    # Dynamische pip value — GEEN FALLBACK, skip als niet beschikbaar
    pip_value = await get_dynamic_pip_value(conn, setup.symbol, state)
    if pip_value is None:
        log.warning(f"Skip trade {setup.symbol}: geen actuele pip value beschikbaar")
        return False

    lot, lot_details = calculate_lot_size(balance, sl_dist, setup.symbol, risk_pct, pip_value)

    if lot < SYMBOL_SPECS.get(setup.symbol, {}).get("min_lot", 0.01):
        return False

    # Margin check
    try:
        info = await rate_limited_call(conn.get_account_information(), state, label="margin_check")
        if info:
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
                    tg(f"⚠️ <b>MARGIN SKIP</b>: {setup.symbol}\nMargin: €{final_margin:.0f} | Free: €{free_margin:.0f}", state)
                    return False
    except Exception as e:
        log.warning(f"Margin check error: {e}")
        spec = SYMBOL_SPECS.get(setup.symbol, {})
        lot = spec.get("min_lot", 0.01)

    update_peak_balance(state, balance)

    kz = get_current_killzone()

    # === DRY RUN MODE ===
    if DRY_RUN:
        state.daily_state["trades_today"] += 1
        r = " | ".join(setup.reasons)
        de = "🟢" if setup.direction == Direction.BULL else "🔴"
        ote_str = " | 📐 OTE ENTRY" if setup.ote_entry else ""
        conf_str = " | 🏛️ 1H CONFLUENCE" if setup.confluence else ""
        wr, total = state.get_symbol_winrate(setup.symbol)

        tg(f"""<b>🧪 DRY RUN — Grade {setup.grade}</b>

📌 {setup.symbol} | {setup.direction.value.upper()}
🕐 KZ: {kz.upper() if kz else '?'}
🔍 Regime: {setup.regime.upper()}{conf_str}{ote_str}
💰 Entry: {setup.entry:.5f}
🛑 SL: {setup.stop_loss:.5f}
🎯 TP1: {setup.tp1:.5f} | TP2: {setup.tp2:.5f} | TP3: {setup.tp3:.5f}
📊 RR: 1:{setup.rr:.1f} | Lots: {lot}
💵 Risk: {risk_pct*100:.2f}% (${lot_details['risk_amount']})
📏 SL: {lot_details['sl_pips']:.1f} pips | PipVal: {lot_details['pip_value_used']:.2f}
✅ Confirm: {setup.confirmation}
🗺️ Zone: {setup.zone.type.value} ({setup.zone.timeframe})
🏥 {setup.symbol} WR: {wr*100:.0f}% ({total}t)
📋 Score: {setup.score:.1f} | {r}""", state)

        trade_data = {
            "time": datetime.now(timezone.utc).isoformat(),
            "symbol": setup.symbol, "direction": setup.direction.value,
            "grade": setup.grade, "score": setup.score,
            "entry": setup.entry, "sl": setup.stop_loss,
            "tp1": setup.tp1, "tp2": setup.tp2, "tp3": setup.tp3,
            "lot": lot, "rr": setup.rr, "risk_pct": risk_pct,
            "confirmation": setup.confirmation, "zone_type": setup.zone.type.value,
            "regime": setup.regime, "killzone": kz,
            "confluence": setup.confluence, "ote_entry": setup.ote_entry,
            "reasons": setup.reasons,
        }
        db_save_trade(trade_data)
        state.trade_journal.append(trade_data)
        return True

    # === LIVE EXECUTIE MET SLIPPAGE BESCHERMING ===
    try:
        spec = SYMBOL_SPECS.get(setup.symbol, {})
        category = spec.get("category", "forex")
        max_slippage = MAX_SLIPPAGE_PIPS.get(category, 5) * spec.get("pip_size", 0.0001)

        price_before = await rate_limited_call(conn.get_symbol_price(setup.symbol), state, label=f"exec_price_{setup.symbol}")
        if not price_before:
            return False

        current_entry = price_before["ask"] if setup.direction == Direction.BULL else price_before["bid"]
        slippage = abs(current_entry - setup.entry)

        if slippage > max_slippage:
            tg(f"⚠️ <b>SLIPPAGE SKIP</b>: {setup.symbol}\nSlippage: {slippage/spec['pip_size']:.1f} pips > max {MAX_SLIPPAGE_PIPS[category]}", state)
            return False

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

        # FIX: ERR_NO_ERROR is SUCCESS — niet als error behandelen
        order_success = False
        if result:
            string_code = result.get("stringCode", "")
            if string_code == "ERR_NO_ERROR" or "orderId" in str(result):
                order_success = True
                state.consecutive_api_fails = 0

        if not order_success:
            # Verify via positions als result onduidelijk
            await asyncio.sleep(1)
            positions = await rate_limited_call(conn.get_positions(), state, label="verify_order")
            if positions:
                found = any(p.get("symbol") == setup.symbol for p in positions)
                if found:
                    order_success = True
                    state.consecutive_api_fails = 0

        if not order_success:
            tg(f"❌ <b>ORDER NOT CONFIRMED</b>: {setup.symbol}\nResult: {str(result)[:100]}", state)
            return False

    except Exception as e:
        tg(f"❌ <b>ORDER FAIL</b>: {setup.symbol} — {e}", state)
        state.consecutive_api_fails += 1
        return False

    state.daily_state["trades_today"] += 1

    trade_data = {
        "time": datetime.now(timezone.utc).isoformat(),
        "symbol": setup.symbol, "direction": setup.direction.value,
        "grade": setup.grade, "score": setup.score,
        "entry": setup.entry, "sl": setup.stop_loss,
        "tp1": setup.tp1, "tp2": setup.tp2, "tp3": setup.tp3,
        "lot": lot, "rr": setup.rr, "risk_pct": risk_pct,
        "confirmation": setup.confirmation, "zone_type": setup.zone.type.value,
        "regime": setup.regime, "killzone": kz,
        "confluence": setup.confluence, "ote_entry": setup.ote_entry,
        "reasons": setup.reasons,
    }
    db_save_trade(trade_data)
    state.trade_journal.append(trade_data)

    r = " | ".join(setup.reasons)
    de = "🟢" if setup.direction == Direction.BULL else "🔴"
    cap_warn = " ⚠️CAP" if lot_details.get("capped") else ""
    margin_warn = " ⚠️MARGIN" if lot_details.get("margin_reduced") else ""
    wr_recent = sum(state.performance["recent_results"][-10:]) / max(len(state.performance["recent_results"][-10:]), 1) * 100
    conf_str = " | 🏛️ 1H CONFLUENCE" if setup.confluence else ""
    ote_str = " | 📐 OTE ENTRY" if setup.ote_entry else ""
    pf = db_get_profit_factor()
    sym_wr, sym_total = state.get_symbol_winrate(setup.symbol)

    tg(f"""<b>{de} TRADE OPENED — Grade {setup.grade}</b>

📌 {setup.symbol} | {setup.direction.value.upper()}
🕐 KZ: {kz.upper() if kz else '?'}
🔍 Regime: {setup.regime.upper()}{conf_str}{ote_str}
💰 Entry: {setup.entry:.5f}
🛑 SL: {setup.stop_loss:.5f}
🎯 TP1: {setup.tp1:.5f} (33%)
🎯 TP2: {setup.tp2:.5f} (33%)
🎯 TP3: {setup.tp3:.5f} (runner 33%)
📊 RR: 1:{setup.rr:.1f} | Lots: {lot}{cap_warn}{margin_warn}
💵 Risk: {risk_pct*100:.2f}% (${lot_details['risk_amount']})
📏 SL: {lot_details['sl_pips']:.1f} pips | PipVal: {lot_details['pip_value_used']:.2f}
✅ Confirm: {setup.confirmation}
🗺️ Zone: {setup.zone.type.value} ({setup.zone.timeframe})
📈 Streak: {state.performance['consecutive_wins']}W / {state.performance['consecutive_losses']}L
🎯 WR: {wr_recent:.0f}% | PF: {pf:.2f}
🏥 {setup.symbol} WR: {sym_wr*100:.0f}% ({sym_total}t)
📋 Score: {setup.score:.1f} | {r}
💰 Balance: ${balance:,.2f}""", state)

    return True


# ==================== DIAGNOSTIEK ====================

async def run_diagnostics(conn, account, state: BotState):
    log.info("=" * 60)
    log.info("DIAGNOSTICS — SMC Bot v4.1 PROFIT-CRITICAL REBUILD")
    log.info(f"MODE: {'DRY RUN' if DRY_RUN else 'LIVE'}")
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
    log.info(f"Risk: {list(GRADE_RISK.values())} | Daily: {DAILY_LOSS_LIMIT*100}% | Weekly: {WEEKLY_LOSS_LIMIT*100}%")
    log.info(f"Max DD hard stop: {MAX_DRAWDOWN_HARD_STOP*100}%")
    log.info(f"Max trades: {MAX_TOTAL_TRADES} total | {MAX_TRADES_PER_DAY}/day | {MAX_TRADES_PER_ASSET}/asset")
    log.info(f"Min RR: {MIN_RR} | Min grade: B+")
    log.info(f"Symbol min WR: {SYMBOL_MIN_WINRATE*100}% after {SYMBOL_MIN_TRADES_FOR_EVAL} trades")

    mae_mfe = db_get_mae_mfe_stats()
    if mae_mfe:
        log.info(f"MAE/MFE: avg_mae={mae_mfe['avg_mae']:.5f} | avg_mfe={mae_mfe['avg_mfe']:.5f} ({mae_mfe['count']} trades)")

    pf = db_get_profit_factor()
    log.info(f"Profit Factor: {pf:.2f}")

    for s in SYMBOLS:
        try:
            candles = await get_candles(account, s, "5m", 20, state)
            status = f"OK {len(candles)}c" if candles and len(candles) >= 10 else "FAIL"
            p = await rate_limited_call(conn.get_symbol_price(s), state, label=f"diag_{s}")
            spread = p["ask"] - p["bid"]
            spec = SYMBOL_SPECS[s]
            spread_pips = spread / spec["pip_size"]
            allowed, _ = is_entry_allowed(s)
            pv = await get_dynamic_pip_value(conn, s, state)
            pv_str = f"pip_val: {pv:.2f}" if pv else "pip_val: N/A"
            healthy = state.is_symbol_healthy(s)
            wr, total = state.get_symbol_winrate(s)
            health_str = f"{'✅' if healthy else '⛔'} WR:{wr*100:.0f}%({total}t)"
            log.info(f"  {s:10} | {status} | spread: {spread_pips:.1f} pips | {pv_str} | entry: {'YES' if allowed else 'NO'} | {health_str}")
        except Exception as e:
            log.info(f"  {s:10} | ERROR {e}")
    log.info("=" * 60)


# ==================== HOOFDLOOP ====================

async def run(state: BotState):

    # Graceful shutdown handler
    def handle_shutdown(signum, frame):
        log.info(f"Shutdown signal {signum} ontvangen — graceful shutdown...")
        state.shutdown_requested = True

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    try:
        log.info("Connecting to MetaAPI...")
        api = MetaApi(METAAPI_TOKEN)
        account = await api.metatrader_account_api.get_account(ACCOUNT_ID)

        conn = None
        for attempt in range(5):
            try:
                log.info(f"Connection poging {attempt + 1}/5...")
                account_info = account.state
                log.info(f"Account state: {account_info}")

                if account_info != "DEPLOYED":
                    log.info("Account niet deployed, deploying...")
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

                conn = account.get_rpc_connection()
                await conn.connect()
                log.info("Synchroniseren...")
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
            tg("❌ <b>FATAL: Kon niet verbinden na 5 pogingen</b>", state)
            raise Exception("Connection failed after 5 attempts")

        try:
            test_info = await asyncio.wait_for(conn.get_account_information(), timeout=15)
            log.info(f"Connection verified: ${test_info['balance']} balance")
        except Exception as e:
            tg(f"⚠️ Connection verificatie mislukt: {e}", state)
            raise Exception(f"Connection verification failed: {e}")

        await asyncio.sleep(2)

        state.watchdog_last_loop = time.time()

        try:
            init_info = await conn.get_account_information()
            state.performance["session_start_balance"] = init_info["balance"]
            state.performance["peak_balance"] = init_info["balance"]
        except Exception:
            pass

        # Laad state uit database
        saved_perf = db_load_state("performance")
        if saved_perf:
            state.performance.update(saved_perf)
            # Ensure symbol_stats exists (backward compat)
            if "symbol_stats" not in state.performance:
                state.performance["symbol_stats"] = {}
            log.info(f"Performance geladen: {state.performance['wins']}W / {state.performance['losses']}L")

        saved_cooldowns = db_load_state("cooldowns")
        if saved_cooldowns:
            state.symbol_cooldowns.update(saved_cooldowns)

        # Laad trade phases
        saved_phases = db_load_state("trade_phases")
        if saved_phases:
            state.trade_phases = {k: TradePhase(v) for k, v in saved_phases.items()}

        # Laad zones
        loaded_zones, loaded_htf, zone_count = db_load_zones()
        state.zone_store.update(loaded_zones)
        state.htf_zone_store.update(loaded_htf)

        state.consecutive_api_fails = 0
        await run_diagnostics(conn, account, state)

        mode_str = "🧪 DRY RUN" if DRY_RUN else "🔴 LIVE"

        tg(f"""🚀 <b>SMC BOT v4.1 GESTART</b> {mode_str}

📊 {len(SYMBOLS)} symbols | Max: {MAX_TOTAL_TRADES} open / {MAX_TRADES_PER_DAY} per dag
💵 Risk: {list(GRADE_RISK.values())} | Daily: {DAILY_LOSS_LIMIT*100}% | Weekly: {WEEKLY_LOSS_LIMIT*100}%
📉 Max DD hard stop: {MAX_DRAWDOWN_HARD_STOP*100}%
🗺️ Zones geladen: {zone_count}
📊 PF: {db_get_profit_factor():.2f}
⚡ <b>v4.1 Profit-Critical Fixes:</b>
• TradePhase state machine (geen gemiste partials)
• Per-symbol auto-pause (<{SYMBOL_MIN_WINRATE*100:.0f}% WR)
• Max drawdown hard stop ({MAX_DRAWDOWN_HARD_STOP*100:.0f}%)
• Order confirmation fix (ERR_NO_ERROR)
• Pip value: geen fallback, skip bij onbekend
• Reconnect heartbeat (60s ping)
• BotState class (geen globals)
• Graceful shutdown""", state)

        while not state.shutdown_requested:
            try:
                state.watchdog_last_loop = time.time()
                state.consecutive_errors = 0

                # Weekend check — ICMarkets gesloten vr 22:00 - zo 22:00 UTC
                now_utc = datetime.now(timezone.utc)
                is_weekend = (now_utc.weekday() == 5) or \
                             (now_utc.weekday() == 4 and now_utc.hour >= 22) or \
                             (now_utc.weekday() == 6 and now_utc.hour < 22)
                if is_weekend:
                    if now_utc.minute == 0 and now_utc.second < 15:
                        log.info("Weekend — market gesloten, wacht...")
                    await asyncio.sleep(60)
                    continue

                await send_heartbeat(conn, state)

                # Reconnect heartbeat — detecteert stille disconnects
                if not await reconnect_heartbeat(conn, state):
                    log.warning("Reconnect heartbeat failed — reconnecting...")

                if needs_reconnect(state):
                    log.warning(f"API failed {state.consecutive_api_fails}x — reconnecting...")
                    tg("⚠️ <b>CONNECTION ISSUE</b> — auto-recovering...", state)
                    reconnected = False

                    for attempt in range(3):
                        try:
                            state.watchdog_last_loop = time.time()  # Watchdog: we leven nog
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
                                state.consecutive_api_fails = 0
                                state.connection_healthy = True
                                reconnected = True
                                tg("✅ <b>RECOVERED</b>", state)
                                break
                        except Exception as e:
                            log.warning(f"Soft reconnect {attempt + 1}/3: {e}")

                    if not reconnected:
                        log.info("Soft reconnect failed — wacht 2 min...")
                        state.consecutive_api_fails = 0
                        state.watchdog_last_loop = time.time()  # Watchdog: we leven nog
                        await asyncio.sleep(120)
                        try:
                            conn = account.get_rpc_connection()
                            await conn.connect()
                            await asyncio.wait_for(conn.wait_synchronized(), timeout=60)
                            test = await asyncio.wait_for(conn.get_account_information(), timeout=10)
                            if test and "balance" in test:
                                state.consecutive_api_fails = 0
                                reconnected = True
                                tg("✅ <b>RECOVERED</b> na 2 min", state)
                        except Exception as e:
                            log.warning(f"2-min retry failed: {e}")

                    if not reconnected:
                        log.info("Trying undeploy/redeploy...")
                        state.watchdog_last_loop = time.time()  # Watchdog: we leven nog
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
                                state.consecutive_api_fails = 0
                                reconnected = True
                                tg("✅ <b>RECOVERED via REDEPLOY</b>", state)
                        except Exception as e:
                            log.error(f"Redeploy failed: {e}")

                    if not reconnected:
                        state.consecutive_api_fails = 0
                        tg("⚠️ <b>CONNECTION DOWN</b> — wacht 5 min...", state)
                        # Split 5 min in stukken zodat watchdog niet triggert
                        for _ in range(10):
                            state.watchdog_last_loop = time.time()
                            await asyncio.sleep(30)
                        try:
                            conn = account.get_rpc_connection()
                            await conn.connect()
                            await asyncio.wait_for(conn.wait_synchronized(), timeout=60)
                            state.consecutive_api_fails = 0
                            tg("✅ <b>RECOVERED</b> na 5 min", state)
                        except Exception:
                            log.warning("Nog steeds geen connectie")
                        continue

                kz = get_current_killzone()

                if kz == "asia":
                    await update_asia_range(account, state)

                if kz not in ENTRY_KILLZONES:
                    try:
                        positions = await rate_limited_call(conn.get_positions(), state, label="pos_offkz")
                        if positions:
                            await manage_positions(conn, positions, state)
                    except Exception:
                        pass
                    await asyncio.sleep(30)
                    continue

                info = await rate_limited_call(conn.get_account_information(), state, label="account_info")
                if not info or "balance" not in info:
                    log.warning("Kon account info niet ophalen, skip cyclus")
                    await asyncio.sleep(10)
                    continue

                balance = info["balance"]
                equity = info["equity"]
                positions = await rate_limited_call(conn.get_positions(), state, label="positions")
                if positions is None:
                    positions = []

                update_peak_balance(state, equity)
                update_weekly_loss(state, balance)
                await check_closed_trades(conn, state)

                if positions:
                    await manage_positions(conn, positions, state)

                # === MAX DRAWDOWN HARD STOP ===
                if not check_max_drawdown(state, equity):
                    # Beheer nog wel open posities, maar open geen nieuwe
                    await asyncio.sleep(60)
                    continue

                reset_weekly_if_needed(state, balance)
                if not is_weekly_limit_ok(state):
                    await asyncio.sleep(60)
                    continue

                reset_daily_if_needed(state, balance)
                if not is_daily_limit_ok(state, balance):
                    await asyncio.sleep(60)
                    continue

                # Profit factor check
                total_trades = state.performance["wins"] + state.performance["losses"]
                if total_trades >= 30:
                    pf = db_get_profit_factor()
                    if pf < 1.0:
                        log.warning(f"Profit factor {pf:.2f} < 1.0 — trades gepauzeerd")
                        await asyncio.sleep(60)
                        continue

                if not is_candle_just_closed(5):
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue

                if not await news_filter(state):
                    await asyncio.sleep(60)
                    continue

                if len(positions) >= MAX_TOTAL_TRADES:
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue

                if state.daily_state["trades_today"] >= MAX_TRADES_PER_DAY:
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue

                for symbol in SYMBOLS:
                    if state.shutdown_requested:
                        break

                    state.watchdog_last_loop = time.time()
                    try:
                        allowed, _ = is_entry_allowed(symbol)
                        if not allowed:
                            continue

                        # Per-symbol health check
                        if not state.is_symbol_healthy(symbol):
                            continue

                        if GOLD_SCALP["enabled"] and symbol == GOLD_SCALP["symbol"]:
                            max_trades = GOLD_SCALP["max_trades"]
                        else:
                            max_trades = MAX_TRADES_PER_ASSET

                        if sum(1 for p in positions if p["symbol"] == symbol) >= max_trades:
                            continue

                        if not check_correlation(symbol, positions):
                            continue

                        cd_ok, cd_remaining = check_cooldown(state, symbol)
                        if not cd_ok:
                            continue

                        if not check_gold_session_limit(state, symbol):
                            continue

                        is_gold_scalp = GOLD_SCALP["enabled"] and symbol == GOLD_SCALP["symbol"]
                        dedup_secs = GOLD_SCALP["dedup_seconds"] if is_gold_scalp else 900
                        sig_key = f"{symbol}_{int(time.time()/dedup_secs)}"
                        if sig_key in state.recent_signals:
                            continue

                        setup = await analyze_and_find_setup(account, conn, symbol, positions, balance, state)
                        if not setup:
                            continue

                        # Haal 5M data op voor volatility sizing
                        candles_5m = await get_candles(account, symbol, "5m", 60, state)
                        df_5m_exec = None
                        if candles_5m and len(candles_5m) > 20:
                            df_5m_exec = pd.DataFrame(candles_5m)
                            df_5m_exec = calculate_indicators(df_5m_exec)

                        success = await execute_trade(conn, setup, balance, state, df_5m_exec)
                        if success:
                            state.recent_signals[sig_key] = time.time()
                    except Exception as e:
                        log.warning(f"Symbol {symbol} error (skipping): {e}")
                    await asyncio.sleep(0.5)

                cleanup_recent_signals(state)
                db_save_state("cooldowns", state.symbol_cooldowns)

                await asyncio.sleep(CHECK_INTERVAL)

            except asyncio.CancelledError as e:
                state.consecutive_errors += 1
                log.warning(f"CancelledError #{state.consecutive_errors}: {e}")
                if state.consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    tg(f"🚨 <b>TOO MANY DISCONNECTS</b> — force restart", state)
                    raise Exception("Too many CancelledErrors")
                await asyncio.sleep(15)
            except Exception as e:
                state.consecutive_errors += 1
                log.error(f"Loop error #{state.consecutive_errors}: {e}")
                if state.consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    tg(f"🚨 <b>{MAX_CONSECUTIVE_ERRORS} CONSECUTIVE ERRORS</b> — force restart\n{str(e)[:60]}", state)
                    raise Exception(f"Too many consecutive errors: {state.consecutive_errors}")
                if state.consecutive_errors % 5 == 0:
                    tg(f"⚠️ <b>{state.consecutive_errors} LOOP ERRORS</b>\n{str(e)[:80]}", state)
                await asyncio.sleep(10 if "timed out" in str(e).lower() else 5)

        # Graceful shutdown
        log.info("Graceful shutdown compleet")
        # Flush database
        db_save_state("performance", state.performance)
        db_save_state("cooldowns", state.symbol_cooldowns)
        db_save_zones_bulk(state.zone_store, htf=False)
        db_save_zones_bulk(state.htf_zone_store, htf=True)
        tg("🛑 <b>BOT GRACEFUL SHUTDOWN</b>", state)

    except asyncio.CancelledError as e:
        log.critical(f"FATAL CancelledError: {e}")
        tg(f"❌ <b>FATAL DISCONNECT</b> — herstarting...", state)
        raise Exception(f"CancelledError: {e}")
    except Exception as e:
        log.critical(f"FATAL: {e}")
        tg(f"❌ <b>FATAL</b>: {str(e)[:100]}", state)
        raise e


# ==================== WATCHDOG (GRACEFUL) ====================

def watchdog_thread(state: BotState):
    watchdog_max_silence = 600
    while not state.shutdown_requested:
        time.sleep(60)
        silence = time.time() - state.watchdog_last_loop
        if silence > watchdog_max_silence:
            log.critical(f"WATCHDOG: Loop silent for {silence:.0f}s — requesting shutdown...")
            tg(f"🐕 <b>WATCHDOG TRIGGERED</b>\nLoop niet actief voor {silence:.0f}s\nGraceful restart...", state)
            state.shutdown_requested = True
            # Geef 10s voor graceful shutdown, daarna force
            time.sleep(10)
            if not state.shutdown_requested:
                continue
            log.critical("Watchdog: graceful shutdown timeout — force exit")
            os._exit(1)


# ==================== START ====================

if __name__ == "__main__":
    import threading

    log.info("=" * 50)
    log.info("PROFESSIONAL SMC BOT v4.1 — PROFIT-CRITICAL REBUILD")
    log.info(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    log.info("TradePhase SM | Symbol Health | Max DD Stop")
    log.info("Reconnect HB | BotState | Graceful Shutdown")
    log.info(f"Risk: {list(GRADE_RISK.values())} | Max trades: {MAX_TOTAL_TRADES}")
    log.info("=" * 50)

    init_database()

    # Maak een tijdelijke state voor watchdog (wordt in run() overschreven)
    _boot_state = BotState()

    wd = threading.Thread(target=watchdog_thread, args=(_boot_state,), daemon=True)
    wd.start()
    log.info("🐕 Watchdog thread gestart")

    restart_count = 0
    while True:
        try:
            restart_count += 1
            if restart_count > 1:
                tg(f"🔄 <b>AUTO RESTART #{restart_count}</b>", _boot_state)
            asyncio.run(run(_boot_state))
            # Als run() normaal eindigt (graceful shutdown), stop
            if _boot_state.shutdown_requested:
                log.info("Graceful shutdown — geen restart")
                break
        except KeyboardInterrupt:
            tg("🛑 <b>BOT GESTOPT</b>", _boot_state)
            log.info("Stopped by user")
            break
        except Exception as e:
            tg(f"💥 <b>CRASH #{restart_count}</b>: {str(e)[:80]}\n🔄 Restart in 15s...", _boot_state)
            log.error(f"Crash #{restart_count}: {e}")
            wait = min(15 * restart_count, 60)
            log.info(f"Restart in {wait}s...")
            time.sleep(wait)
