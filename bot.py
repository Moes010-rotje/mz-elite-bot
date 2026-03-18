import asyncio
import pandas as pd
import numpy as np
import os
import time
import json
import urllib.request
import logging
from datetime import datetime, date, timedelta, timezone
from metaapi_cloud_sdk import MetaApi

# ================================================================
#   PROFESSIONAL SMC BOT v2.1 — AGGRESSIVE MODE
#   Features: Killzones | Displacement OB/FVG | Liquidity Sweep |
#   Momentum Confirmatie | Dynamic Risk | Trade Grading |
#   Partial TP | Trailing Stop | Cooldown | Regime Filter |
#   Asia Range Mapping | Correct Lot Sizing per Instrument
# ================================================================

# ==================== CONFIGURATIE ====================

CHECK_INTERVAL = 5
BASE_RISK = 0.01          # 1% basis risico per trade
MIN_RR = 1.5              # minimale risk/reward (verlaagd voor meer trades)
DAILY_LOSS_LIMIT = 0.03   # 3% daily loss limit
WEEKLY_LOSS_LIMIT = 0.10  # 10% weekly loss limit
MAX_TRADES_PER_ASSET = 3  # max 3 trades per asset
MAX_TOTAL_TRADES = 10     # max 10 trades totaal open
WEEKLY_RESET_DAY = 2      # Wednesday
WEEKLY_RESET_HOUR = 22    # 22:00 UTC
COOLDOWN_AFTER_LOSSES = 3 # na 3 verliezers op rij: cooldown
COOLDOWN_MINUTES = 60     # 1 uur cooldown

# ===== KILLZONES (UTC) =====
KILLZONES = {
    "asia":       {"start": 0,  "end": 7},
    "london_kz":  {"start": 7,  "end": 10},
    "london_ext": {"start": 10, "end": 13},
    "ny_kz":      {"start": 13, "end": 16},
    "ny_pm":      {"start": 16, "end": 19},
}

# Alle killzones actief voor trading
TRADING_KILLZONES = ["asia", "london_kz", "london_ext", "ny_kz", "ny_pm"]

# Welke symbolen mogen traden per killzone
KILLZONE_SYMBOLS = {
    "asia":       ["USDJPY", "GBPJPY", "EURJPY", "BTCUSD", "XAUUSD"],
    "london_kz":  "all",
    "london_ext": "all",
    "ny_kz":      "all",
    "ny_pm":      ["XAUUSD", "NAS100", "US30", "US500", "BTCUSD"],
}

# Minimum grade per killzone
KILLZONE_MIN_GRADE = {
    "asia":       "A",
    "london_kz":  "B",
    "london_ext": "B+",
    "ny_kz":      "B",
    "ny_pm":      "B+",
}

# ===== SYMBOLEN MET CONTRACTSPECIFICATIES =====
SYMBOL_SPECS = {
    "XAUUSD":  {"pip_size": 0.1,    "pip_value_per_lot": 10,  "min_spread_atr_ratio": 0.20, "category": "metals"},
    "XAGUSD":  {"pip_size": 0.01,   "pip_value_per_lot": 50,  "min_spread_atr_ratio": 0.20, "category": "metals"},
    "BTCUSD":  {"pip_size": 1.0,    "pip_value_per_lot": 1,   "min_spread_atr_ratio": 0.25, "category": "crypto"},
    "EURUSD":  {"pip_size": 0.0001, "pip_value_per_lot": 10,  "min_spread_atr_ratio": 0.15, "category": "forex"},
    "GBPUSD":  {"pip_size": 0.0001, "pip_value_per_lot": 10,  "min_spread_atr_ratio": 0.15, "category": "forex"},
    "GBPJPY":  {"pip_size": 0.01,   "pip_value_per_lot": 6.5, "min_spread_atr_ratio": 0.15, "category": "forex"},
    "USDJPY":  {"pip_size": 0.01,   "pip_value_per_lot": 6.5, "min_spread_atr_ratio": 0.15, "category": "forex"},
    "USDCHF":  {"pip_size": 0.0001, "pip_value_per_lot": 10,  "min_spread_atr_ratio": 0.15, "category": "forex"},
    "AUDUSD":  {"pip_size": 0.0001, "pip_value_per_lot": 10,  "min_spread_atr_ratio": 0.15, "category": "forex"},
    "EURJPY":  {"pip_size": 0.01,   "pip_value_per_lot": 6.5, "min_spread_atr_ratio": 0.15, "category": "forex"},
    "NAS100":  {"pip_size": 0.1,    "pip_value_per_lot": 1,   "min_spread_atr_ratio": 0.20, "category": "indices"},
    "US30":    {"pip_size": 0.1,    "pip_value_per_lot": 1,   "min_spread_atr_ratio": 0.20, "category": "indices"},
    "US500":   {"pip_size": 0.1,    "pip_value_per_lot": 1,   "min_spread_atr_ratio": 0.20, "category": "indices"},
}

SYMBOLS = list(SYMBOL_SPECS.keys())

METAAPI_TOKEN = os.getenv("METAAPI_TOKEN")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT = os.getenv("TG_CHAT")

# ==================== GLOBALE STATE ====================

daily = {"date": None, "start": 0, "losses_in_row": 0, "cooldown_until": 0, "trades_today": 0}
weekly = {"week": None, "loss": 0, "limit_hit": False, "start_balance": 0}
last_status = 0
open_signals = {}
trade_journal = []
asia_range_cache = {}

correlation_pairs = [
    ["EURUSD", "GBPUSD"],
    ["NAS100", "US500"],
    ["XAUUSD", "XAGUSD"],
    ["USDJPY", "EURJPY"],
    ["GBPUSD", "GBPJPY"],
]

GRADE_ORDER = ["D", "C", "B", "B+", "A", "A+"]

logging.basicConfig(level=logging.INFO)

# ==================== TELEGRAM ====================

def tg(msg):
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        data = json.dumps({"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f"❌ TG error: {e}")


def test_telegram():
    print(f"\n{'='*50}\n🔍 TELEGRAM TEST\n{'='*50}")
    print(f"TG_TOKEN: {TG_TOKEN[:5]}...{TG_TOKEN[-5:] if TG_TOKEN else 'MISSING'}")
    print(f"TG_CHAT: {TG_CHAT}")
    try:
        tg("🧪 BOT GESTART — Professional SMC Bot v2.1 AGGRESSIVE")
        print("✅ Telegram werkt")
        return True
    except Exception as e:
        print(f"❌ Telegram mislukt: {e}")
        return False

# ==================== HEARTBEAT ====================

async def send_heartbeat(conn):
    global last_status
    now = time.time()
    if now - last_status < 600:
        return
    last_status = now

    try:
        info = await conn.get_account_information()
        balance = info["balance"]
        equity = info["equity"]
        positions = await conn.get_positions()
    except Exception as e:
        print(f"Heartbeat error: {e}")
        return

    pl = equity - balance
    pl_pct = (pl / balance) * 100 if balance > 0 else 0
    kz = get_current_killzone()
    daily_loss = ((daily["start"] - balance) / daily["start"] * 100) if daily["start"] > 0 else 0

    msg = f"""<b>💓 HEARTBEAT</b>

💰 Balance: ${balance:.2f}
📊 Equity: ${equity:.2f}
📈 P&L: {pl:.2f} ({pl_pct:.2f}%)

🎯 Open: {len(positions)}
🕐 Killzone: {kz.upper() if kz else 'GEEN'}
📅 Daily loss: {daily_loss:.2f}%
📆 Weekly loss: {weekly['loss']*100:.2f}%
🔥 Losses in row: {daily['losses_in_row']}
📊 Trades vandaag: {daily['trades_today']}

⏰ {datetime.now(timezone.utc).replace(tzinfo=None).strftime('%H:%M:%S')} UTC"""
    tg(msg)

# ==================== KILLZONE FILTER ====================

def get_current_killzone():
    hour = datetime.now(timezone.utc).replace(tzinfo=None).hour
    for name, data in KILLZONES.items():
        if data["start"] <= hour < data["end"]:
            return name
    return None


def is_symbol_allowed_in_killzone(symbol):
    """Check of symbool mag traden in huidige killzone"""
    kz = get_current_killzone()
    if not kz or kz not in TRADING_KILLZONES:
        return None
    allowed = KILLZONE_SYMBOLS.get(kz, "all")
    if allowed != "all" and symbol not in allowed:
        return None
    return kz

# ==================== ASIA RANGE ====================

async def update_asia_range(account):
    today = date.today()
    for symbol in SYMBOLS:
        if symbol in asia_range_cache and asia_range_cache[symbol]["date"] == today:
            continue
        try:
            candles = await account.get_historical_candles(symbol, "15m", 50)
            if not candles or len(candles) < 10:
                continue
            df = pd.DataFrame(candles)
            if "time" in df.columns:
                # Handle both int (unix ms), int (unix s), and string timestamps
                def parse_hour(t):
                    try:
                        if isinstance(t, (int, float)):
                            # Als het een unix timestamp is (ms of s)
                            if t > 1e10:
                                t = t / 1000  # milliseconds naar seconds
                            return datetime.utcfromtimestamp(t).hour
                        else:
                            return pd.to_datetime(t, utc=True).hour
                    except Exception:
                        return -1

                df["hour"] = df["time"].apply(parse_hour)
                asia = df[(df["hour"] >= 0) & (df["hour"] < 7)]
                if len(asia) >= 4:
                    asia_range_cache[symbol] = {
                        "high": asia["high"].max(),
                        "low": asia["low"].min(),
                        "date": today,
                    }
        except Exception:
            continue

# ==================== WEEKLY / DAILY LIMIETEN ====================

def check_weekly(balance):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    cw = now.isocalendar()[1]
    if weekly["week"] != cw:
        if now.weekday() == WEEKLY_RESET_DAY and now.hour >= WEEKLY_RESET_HOUR:
            weekly.update({"week": cw, "loss": 0, "limit_hit": False, "start_balance": balance})
            tg("📊 WEEKLY RESET")
    return not weekly["limit_hit"]


def update_weekly_loss(balance):
    if weekly["start_balance"] > 0:
        loss = (weekly["start_balance"] - balance) / weekly["start_balance"]
        if loss > weekly["loss"]:
            weekly["loss"] = loss
            if loss >= WEEKLY_LOSS_LIMIT:
                weekly["limit_hit"] = True
                tg(f"⚠️ WEEKLY LIMIET ({loss*100:.1f}%)")


def check_daily(balance):
    today = date.today()
    if daily["date"] != today:
        daily.update({"date": today, "start": balance, "losses_in_row": 0, "cooldown_until": 0, "trades_today": 0})
        return True
    if daily["start"] == 0:
        daily["start"] = balance
        return True
    loss = (daily["start"] - balance) / daily["start"]
    if loss >= DAILY_LOSS_LIMIT:
        tg(f"⚠️ DAILY LIMIET: {loss*100:.2f}%")
        return False
    return True

# ==================== COOLDOWN ====================

def check_cooldown():
    now = time.time()
    if daily["cooldown_until"] > now:
        return False, int((daily["cooldown_until"] - now) / 60)
    return True, 0


def register_trade_result(is_win):
    if is_win:
        daily["losses_in_row"] = 0
    else:
        daily["losses_in_row"] += 1
        if daily["losses_in_row"] >= COOLDOWN_AFTER_LOSSES:
            daily["cooldown_until"] = time.time() + COOLDOWN_MINUTES * 60
            tg(f"🧊 COOLDOWN: {COOLDOWN_MINUTES}min na {daily['losses_in_row']} losses")

# ==================== DYNAMIC RISK ====================

def get_dynamic_risk(balance):
    base = BASE_RISK
    if daily["losses_in_row"] >= 1:
        base *= 0.5
    if daily["start"] > 0:
        cur_loss = (daily["start"] - balance) / daily["start"]
        if cur_loss >= 0.015:
            base *= 0.5
    return max(base, 0.0025)

# ==================== SPREAD FILTER ====================

async def check_spread(conn, symbol, atr):
    try:
        price = await conn.get_symbol_price(symbol)
        spread = price["ask"] - price["bid"]
        spec = SYMBOL_SPECS[symbol]
        if spread > atr * spec["min_spread_atr_ratio"]:
            return False, spread
        return True, spread
    except Exception:
        return False, 0

# ==================== NIEUWS FILTER ====================

async def news_filter():
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
                if abs((now - et).total_seconds()) <= 1800:
                    tg(f"📰 NEWS: {event.get('title','?')}")
                    return False
            except Exception:
                continue
        return True
    except Exception:
        return True

# ==================== CORRELATIE FILTER ====================

def check_correlation(symbol, positions):
    for pair in correlation_pairs:
        if symbol in pair:
            other = pair[0] if pair[1] == symbol else pair[1]
            if any(p["symbol"] == other for p in positions):
                return False
            if any(other in k for k in open_signals):
                return False
    return True

# ==================== INDICATOREN ====================

def calculate_indicators(df):
    df = df.copy()
    df["ema50"] = df["close"].ewm(span=50).mean()
    df["ema200"] = df["close"].ewm(span=200).mean()

    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss_s = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss_s
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
    return df

# ==================== SMC DETECTIE ====================

def detect_displacement(df, lookback=5):
    """Impulsive candle > 2x gemiddelde body die structuur breekt"""
    if len(df) < lookback + 5:
        return None
    avg_body = abs(df["close"] - df["open"]).tail(20).mean()

    for i in range(-lookback, 0):
        c = df.iloc[i]
        body = abs(c["close"] - c["open"])
        if body < avg_body * 1.8:  # verlaagd van 2.0 naar 1.8
            continue
        if c["close"] > c["open"] and c["close"] > df.iloc[i-1]["high"]:
            return {"type": "bull", "index": i, "candle": c}
        if c["close"] < c["open"] and c["close"] < df.iloc[i-1]["low"]:
            return {"type": "bear", "index": i, "candle": c}
    return None


def find_order_block(df, displacement):
    """Laatste tegenovergestelde candle VOOR displacement"""
    if not displacement:
        return None
    idx = displacement["index"]
    for i in range(idx - 1, max(idx - 8, -len(df)), -1):  # zoek 8 candles terug (was 6)
        c = df.iloc[i]
        if displacement["type"] == "bull" and c["close"] < c["open"]:
            return {"type": "bull", "high": c["high"], "low": c["low"], "index": i}
        if displacement["type"] == "bear" and c["close"] > c["open"]:
            return {"type": "bear", "high": c["high"], "low": c["low"], "index": i}
    return None


def find_fvg(df, lookback=8):
    """
    Correct FVG:
    Bullish: candle1.high < candle3.low
    Bearish: candle1.low > candle3.high
    """
    if len(df) < lookback + 2:
        return None
    avg_body = abs(df["close"] - df["open"]).tail(20).mean()

    for i in range(-1, -lookback, -1):
        if abs(i) + 2 > len(df):
            break
        c1, c2, c3 = df.iloc[i-2], df.iloc[i-1], df.iloc[i]

        # Bullish FVG
        if c3["low"] > c1["high"]:
            if abs(c2["close"] - c2["open"]) > avg_body * 1.3:  # verlaagd van 1.5
                return {"type": "bull", "top": c3["low"], "bottom": c1["high"],
                        "mid": (c3["low"] + c1["high"]) / 2}
        # Bearish FVG
        if c3["high"] < c1["low"]:
            if abs(c2["close"] - c2["open"]) > avg_body * 1.3:
                return {"type": "bear", "top": c1["low"], "bottom": c3["high"],
                        "mid": (c1["low"] + c3["high"]) / 2}
    return None


def find_liquidity_sweep(df, symbol):
    """Sweep van equal highs/lows, Asia range, swing points"""
    if len(df) < 10:
        return None

    last = df.iloc[-1]
    prev = df.iloc[-2]
    atr = df["atr"].iloc[-1] if "atr" in df.columns else (df["high"] - df["low"]).tail(14).mean()
    tolerance = atr * 0.15  # verhoogd van 0.1 voor meer detecties

    highs = df["high"].tail(20)
    lows = df["low"].tail(20)

    # Equal Highs sweep
    for i in range(len(highs) - 6, len(highs) - 2):
        if i < 0:
            continue
        for j in range(i+1, min(i+6, len(highs) - 1)):
            if abs(highs.iloc[i] - highs.iloc[j]) < tolerance:
                eq = max(highs.iloc[i], highs.iloc[j])
                if last["high"] > eq and last["close"] < eq:
                    return {"type": "bear", "level": eq, "reason": "equal_highs"}

    # Equal Lows sweep
    for i in range(len(lows) - 6, len(lows) - 2):
        if i < 0:
            continue
        for j in range(i+1, min(i+6, len(lows) - 1)):
            if abs(lows.iloc[i] - lows.iloc[j]) < tolerance:
                eq = min(lows.iloc[i], lows.iloc[j])
                if last["low"] < eq and last["close"] > eq:
                    return {"type": "bull", "level": eq, "reason": "equal_lows"}

    # Asia Range sweep
    if symbol in asia_range_cache:
        ar = asia_range_cache[symbol]
        if last["high"] > ar["high"] and last["close"] < ar["high"]:
            return {"type": "bear", "level": ar["high"], "reason": "asia_high"}
        if last["low"] < ar["low"] and last["close"] > ar["low"]:
            return {"type": "bull", "level": ar["low"], "reason": "asia_low"}

    # Swing sweep
    sh = df["high"].tail(15).iloc[:-1].max()  # uitgebreid van 10 naar 15
    sl_val = df["low"].tail(15).iloc[:-1].min()
    if last["high"] > sh and last["close"] < prev["close"]:
        return {"type": "bear", "level": sh, "reason": "swing_high"}
    if last["low"] < sl_val and last["close"] > prev["close"]:
        return {"type": "bull", "level": sl_val, "reason": "swing_low"}

    return None


def market_structure_shift(df):
    """MSS op hogere timeframe"""
    if len(df) < 8:
        return None
    h = df["high"].tolist()
    l = df["low"].tolist()
    c = df["close"].tolist()

    # Bullish MSS
    if l[-3] < l[-4] and l[-2] < l[-3] and c[-1] > h[-2]:
        return "bull"
    # Bearish MSS
    if h[-3] > h[-4] and h[-2] > h[-3] and c[-1] < l[-2]:
        return "bear"

    # Extra: single MSS (minder streng voor meer signals)
    if l[-2] < l[-3] and c[-1] > h[-2]:
        return "bull"
    if h[-2] > h[-3] and c[-1] < l[-2]:
        return "bear"

    return None

# ==================== MARKET REGIME ====================

def detect_market_regime(df):
    if len(df) < 30:
        return "unknown"
    atr = df["atr"].iloc[-1]
    ema_dist = abs(df["ema50"].iloc[-1] - df["ema200"].iloc[-1])
    ratio = ema_dist / atr if atr > 0 else 0
    above = (df["close"].tail(10) > df["ema50"].tail(10)).sum()

    if ratio > 2 and (above >= 8 or above <= 2):
        return "trending"
    elif ratio < 0.5:
        return "ranging"
    return "transitioning"

# ==================== HTF BIAS ====================

async def get_htf_bias(account, symbol):
    """1H trend bias — versoepeld voor meer signalen"""
    try:
        candles = await account.get_historical_candles(symbol, "1h", 100)
        if not candles or len(candles) < 50:
            return None
        df = pd.DataFrame(candles)
        df = calculate_indicators(df)

        ema50 = df["ema50"].iloc[-1]
        ema200 = df["ema200"].iloc[-1]
        price = df["close"].iloc[-1]

        # Sterk
        if price > ema50 > ema200:
            return "bull"
        if price < ema50 < ema200:
            return "bear"
        # Matig
        if ema50 > ema200 and price > ema200:
            return "bull"
        if ema50 < ema200 and price < ema200:
            return "bear"
        # Zwak: alleen EMA50 richting
        if price > ema50:
            return "bull"
        if price < ema50:
            return "bear"

        return None
    except Exception:
        return None

# ==================== CANDLE CLOSE TIMING ====================

def is_candle_just_closed(tf_min=5):
    """Check eerste 60 sec na candle close"""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    if now.minute % tf_min == 0 and now.second < 60:
        return True
    return False

# ==================== TRADE GRADING ====================

def grade_setup(htf_bias, mss, displacement, ob, fvg, liquidity_sweep, regime, momentum):
    """
    A+ = alles aligned (9+)    → 1.0x risk
    A  = sterk (7+)            → 0.75x risk
    B+ = goed (5.5+)           → 0.6x risk
    B  = degelijk (4+)         → 0.5x risk
    C  = minimum (3+)          → 0.25x risk
    D  = skip (<3)             → 0x
    """
    score = 0
    reasons = []

    if htf_bias:
        score += 2; reasons.append("HTF")
    if mss:
        score += 2; reasons.append("MSS")
    if displacement:
        score += 2; reasons.append("DISP")
    if ob:
        score += 1.5; reasons.append("OB")
    if fvg:
        score += 1.5; reasons.append("FVG")
    if liquidity_sweep:
        score += 2.5; reasons.append(f"SWEEP({liquidity_sweep.get('reason','?')})")
    if momentum:
        score += 1; reasons.append("MOM")
    if regime == "trending":
        score += 1; reasons.append("TREND")
    if regime == "ranging":
        score -= 1; reasons.append("RANGE⚠️")

    if score >= 9:
        return "A+", 1.0, score, reasons
    elif score >= 7:
        return "A", 0.75, score, reasons
    elif score >= 5.5:
        return "B+", 0.6, score, reasons
    elif score >= 4:
        return "B", 0.5, score, reasons
    elif score >= 3:
        return "C", 0.25, score, reasons
    else:
        return "D", 0, score, reasons

# ==================== LOT SIZE ====================

def calculate_lot_size(balance, sl_distance, symbol, risk_pct):
    if sl_distance <= 0:
        return 0.01
    spec = SYMBOL_SPECS.get(symbol)
    if not spec:
        return 0.01
    sl_pips = sl_distance / spec["pip_size"]
    risk_amount = balance * risk_pct
    denom = sl_pips * spec["pip_value_per_lot"]
    if denom <= 0:
        return 0.01
    lot = risk_amount / denom
    return round(max(0.01, min(lot, 5)), 2)

# ==================== SL / TP LEVELS ====================

def calculate_levels(signal_type, entry, df, symbol):
    atr = df["atr"].iloc[-1]
    min_sl = atr * 1.0

    if signal_type == "buy":
        swing_low = df["low"].tail(10).min()
        sl = min(swing_low, entry - min_sl) - atr * 0.2
        dist = entry - sl
        tp1 = entry + dist * 1.5
        tp2 = entry + dist * 2.5
    else:
        swing_high = df["high"].tail(10).max()
        sl = max(swing_high, entry + min_sl) + atr * 0.2
        dist = sl - entry
        tp1 = entry - dist * 1.5
        tp2 = entry - dist * 2.5

    return sl, tp1, tp2, abs(dist)

# ==================== POSITION MANAGEMENT ====================

async def manage_positions(conn, positions):
    """Partial TP op 1.5x risk, break-even, trailing na 2x risk"""
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

            if cur_p <= 0 or vol <= 0 or not SYMBOL_SPECS.get(symbol):
                continue

            if "BUY" in ptype:
                profit_dist = cur_p - open_p
            else:
                profit_dist = open_p - cur_p

            sl_dist = abs(open_p - sl) if sl else 0
            if sl_dist <= 0:
                continue

            # TP1: partial close 50% + break-even
            tp1_dist = sl_dist * 1.5
            if profit_dist >= tp1_dist and vol > 0.01:
                partial = round(vol * 0.5, 2)
                if partial >= 0.01:
                    try:
                        await conn.close_position_partially(pid, partial)
                        buf = sl_dist * 0.1
                        new_sl = open_p + buf if "BUY" in ptype else open_p - buf
                        await conn.modify_position(pid, stop_loss=new_sl, take_profit=tp)
                        tg(f"✅ PARTIAL: {symbol} 50% gesloten, SL→BE")
                    except Exception as e:
                        print(f"Partial error {symbol}: {e}")

            # Trailing na 2x risk
            elif profit_dist >= sl_dist * 2.0:
                trail = sl_dist * 0.5
                if "BUY" in ptype:
                    new_sl = cur_p - trail
                    if sl and new_sl > sl:
                        try:
                            await conn.modify_position(pid, stop_loss=new_sl, take_profit=tp)
                        except Exception:
                            pass
                else:
                    new_sl = cur_p + trail
                    if sl and new_sl < sl:
                        try:
                            await conn.modify_position(pid, stop_loss=new_sl, take_profit=tp)
                        except Exception:
                            pass

        except Exception as e:
            print(f"Pos mgmt error: {e}")

# ==================== CLOSED TRADE TRACKING ====================

async def check_closed_trades(conn):
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        history = await conn.get_deals_by_time_range(start, now)
        if not history:
            return
        for deal in history[-5:]:
            profit = deal.get("profit", 0)
            did = deal.get("id", "")
            dk = f"deal_{did}"
            if dk in open_signals:
                continue
            if profit != 0:
                open_signals[dk] = time.time()
                register_trade_result(profit > 0)
                emoji = "💰" if profit > 0 else "💔"
                tg(f"{emoji} TRADE {'WON' if profit>0 else 'LOST'}: {'+'if profit>0 else ''}{profit:.2f}")
    except Exception:
        pass

# ==================== HOOFDSTRATEGIE ====================

async def analyze_symbol(account, conn, symbol, positions, balance):
    """
    Complete SMC analyse — AGGRESSIVE
    Minimum 2 confirmaties, momentum als extra signaal
    """
    try:
        candles_5m = await account.get_historical_candles(symbol, "5m", 100)
        candles_15m = await account.get_historical_candles(symbol, "15m", 100)

        if not candles_5m or not candles_15m:
            return None, None, None

        df_5m = pd.DataFrame(candles_5m)
        df_15m = pd.DataFrame(candles_15m)

        if len(df_5m) < 50 or len(df_15m) < 30:
            return None, None, None

        df_5m = calculate_indicators(df_5m)
        df_15m = calculate_indicators(df_15m)

        # 1. HTF Bias
        htf = await get_htf_bias(account, symbol)

        # 2. Market Regime
        regime = detect_market_regime(df_15m)

        # 3. MSS op 15M
        mss = market_structure_shift(df_15m)

        # 4. SMC op 5M
        disp = detect_displacement(df_5m)
        ob = find_order_block(df_5m, disp) if disp else None
        fvg = find_fvg(df_5m)
        sweep = find_liquidity_sweep(df_5m, symbol)

        # 5. Momentum (RSI + MACD + EMA)
        rsi = df_5m["rsi"].iloc[-1]
        mhist = df_5m["macd_hist"].iloc[-1]
        mhist_prev = df_5m["macd_hist"].iloc[-2]
        price = df_5m["close"].iloc[-1]
        ema50 = df_5m["ema50"].iloc[-1]

        mom_bull = (50 < rsi < 75) and (mhist > mhist_prev) and (price > ema50)
        mom_bear = (25 < rsi < 50) and (mhist < mhist_prev) and (price < ema50)

        # 6. Tel confirmaties
        bull = sum([
            htf == "bull",
            mss == "bull",
            bool(disp and disp["type"] == "bull"),
            bool(fvg and fvg["type"] == "bull"),
            bool(sweep and sweep["type"] == "bull"),
            mom_bull,
        ])
        bear = sum([
            htf == "bear",
            mss == "bear",
            bool(disp and disp["type"] == "bear"),
            bool(fvg and fvg["type"] == "bear"),
            bool(sweep and sweep["type"] == "bear"),
            mom_bear,
        ])

        # Minimum 2 confirmaties
        if bull >= 2 and bull > bear:
            direction = "buy"
        elif bear >= 2 and bear > bull:
            direction = "sell"
        else:
            return None, None, None

        # 7. Grade
        d = "bull" if direction == "buy" else "bear"
        grade, risk_mult, score, reasons = grade_setup(
            htf == d,
            mss == d,
            disp and disp["type"] == d,
            ob,
            fvg and fvg["type"] == d,
            sweep if sweep and sweep["type"] == d else None,
            regime,
            mom_bull if d == "bull" else mom_bear,
        )

        # Skip D
        if grade == "D":
            return None, None, None

        # Killzone grade filter
        kz = get_current_killzone()
        min_g = KILLZONE_MIN_GRADE.get(kz, "B")
        if GRADE_ORDER.index(grade) < GRADE_ORDER.index(min_g):
            return None, None, None

        # 8. Spread check
        atr = df_5m["atr"].iloc[-1]
        ok, spread = await check_spread(conn, symbol, atr)
        if not ok:
            return None, None, None

        return direction, grade, {
            "direction": direction, "grade": grade, "risk_mult": risk_mult,
            "score": score, "reasons": reasons, "regime": regime,
            "df_5m": df_5m, "spread": spread, "atr": atr, "htf_bias": htf,
        }

    except Exception as e:
        print(f"Analyse error {symbol}: {e}")
        return None, None, None

# ==================== TRADE EXECUTIE ====================

async def execute_trade(conn, symbol, direction, grade, details, balance):
    df = details["df_5m"]
    risk_pct = get_dynamic_risk(balance) * details["risk_mult"]

    price = await conn.get_symbol_price(symbol)
    entry = price["ask"] if direction == "buy" else price["bid"]

    sl, tp1, tp2, sl_dist = calculate_levels(direction, entry, df, symbol)

    rr = abs((tp2 - entry) / sl_dist) if sl_dist > 0 else 0
    if rr < MIN_RR:
        return False

    lot = calculate_lot_size(balance, sl_dist, symbol, risk_pct)
    if lot < 0.01:
        return False

    try:
        if direction == "buy":
            await conn.create_market_buy_order(symbol, lot, sl, tp2)
        else:
            await conn.create_market_sell_order(symbol, lot, sl, tp2)
    except Exception as e:
        tg(f"❌ ORDER FAIL: {symbol} — {e}")
        return False

    daily["trades_today"] += 1

    trade_journal.append({
        "time": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(), "symbol": symbol,
        "direction": direction, "grade": grade, "entry": entry,
        "sl": sl, "tp1": tp1, "tp2": tp2, "lot": lot, "rr": rr,
        "risk_pct": risk_pct, "reasons": details["reasons"],
        "regime": details["regime"], "spread": details["spread"],
    })

    kz = get_current_killzone()
    r = " | ".join(details["reasons"])

    tg(f"""<b>✅ TRADE GEOPEND</b>

📌 {symbol} — Grade: <b>{grade}</b>
📈 {direction.upper()} | KZ: {kz.upper() if kz else '?'}

💰 Entry: {entry:.5f}
🛑 SL: {sl:.5f}
🎯 TP1: {tp1:.5f} (50%)
🎯 TP2: {tp2:.5f} (runner)

📊 RR: 1:{rr:.1f} | Lots: {lot}
💵 Risk: {risk_pct*100:.2f}% | Score: {details['score']}
🔍 {details['regime'].upper()} | {r}
💰 Bal: ${balance:.2f}

⏰ {datetime.now(timezone.utc).replace(tzinfo=None).strftime('%H:%M:%S')} UTC""")

    return True

# ==================== DIAGNOSTIEK ====================

async def run_diagnostics(conn, account):
    print(f"\n{'='*60}")
    print("🔍 DIAGNOSTIEK — SMC Bot v2.1 AGGRESSIVE")
    print(f"{'='*60}")

    try:
        info = await conn.get_account_information()
        print(f"\n📊 Balance: ${info['balance']} | Equity: ${info['equity']}")
    except Exception as e:
        print(f"❌ Account error: {e}")

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    kz = get_current_killzone()
    print(f"\n🕐 {now.strftime('%H:%M:%S')} UTC | KZ: {kz or 'GEEN'}")
    print(f"📈 Symbols: {len(SYMBOLS)} | Max trades: {MAX_TOTAL_TRADES}")
    print(f"🎯 Min RR: {MIN_RR} | Killzones: {', '.join(TRADING_KILLZONES)}")
    print(f"\n📈 SYMBOOL CHECK:")
    print("-" * 60)

    for s in SYMBOLS:
        try:
            candles = await account.get_historical_candles(s, "5m", 50)
            status = f"✅ {len(candles)}c" if candles and len(candles) >= 20 else "❌ data"
            p = await conn.get_symbol_price(s)
            spread = p["ask"] - p["bid"]
            allowed = is_symbol_allowed_in_killzone(s)
            kz_status = f"✅ {allowed}" if allowed else "⏭️ blocked"
            print(f"  {s:10} | {status} | spread: {spread:.5f} | {kz_status}")
        except Exception as e:
            print(f"  {s:10} | ❌ {e}")

    print(f"\n{'='*60}\n")

# ==================== HOOFDLOOP ====================

async def run():
    try:
        print("🔄 Verbinden met MetaAPI...")
        api = MetaApi(METAAPI_TOKEN)
        account = await api.metatrader_account_api.get_account(ACCOUNT_ID)
        await account.wait_connected()

        conn = account.get_rpc_connection()
        await conn.connect()

        print("🔄 Synchroniseren...")
        await conn.wait_synchronized(timeout_in_seconds=120)
        print("✅ Gesynchroniseerd!")

        await asyncio.sleep(2)
        test_telegram()

        global last_status
        last_status = time.time()

        await run_diagnostics(conn, account)

        tg(f"""🚀 <b>SMC BOT v2.1 AGGRESSIVE GESTART</b>

📊 {len(SYMBOLS)} symbols | {len(TRADING_KILLZONES)} killzones
🎯 Min RR: {MIN_RR} | Max trades: {MAX_TOTAL_TRADES}
⚡ Displacement OB/FVG | Liquidity Sweep | Momentum | Dynamic Risk | Grading | Partial TP | Trailing | Cooldown | Regime Filter""")

        # === MAIN LOOP ===
        while True:
            try:
                info = await conn.get_account_information()
                balance = info["balance"]
                equity = info["equity"]
                positions = await conn.get_positions()

                update_weekly_loss(balance)
                await send_heartbeat(conn)
                await check_closed_trades(conn)

                # === FILTERS ===
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

                # Globale killzone check
                kz = get_current_killzone()
                if kz not in TRADING_KILLZONES:
                    await asyncio.sleep(30)
                    continue

                # Candle close timing
                if not is_candle_just_closed(5):
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue

                # News
                if not await news_filter():
                    await asyncio.sleep(60)
                    continue

                # Max trades
                if len(positions) >= MAX_TOTAL_TRADES:
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue

                # Asia range update
                await update_asia_range(account)

                # Position management
                await manage_positions(conn, positions)

                # === ANALYZE ALL SYMBOLS ===
                for symbol in SYMBOLS:
                    # Killzone symbol filter
                    if not is_symbol_allowed_in_killzone(symbol):
                        continue

                    # Max per asset
                    if sum(1 for p in positions if p["symbol"] == symbol) >= MAX_TRADES_PER_ASSET:
                        continue

                    # Correlatie
                    if not check_correlation(symbol, positions):
                        continue

                    # Dubbel signaal
                    sig_key = f"{symbol}_{int(time.time()/300)}"  # max 1 trade per 5 min per symbol
                    if sig_key in open_signals:
                        continue

                    # ANALYSE
                    direction, grade, details = await analyze_symbol(
                        account, conn, symbol, positions, balance
                    )
                    if not direction:
                        continue

                    # EXECUTE
                    success = await execute_trade(conn, symbol, direction, grade, details, balance)
                    if success:
                        open_signals[sig_key] = time.time()

                    await asyncio.sleep(1)

                # Cleanup
                now = time.time()
                for k in list(open_signals.keys()):
                    if now - open_signals[k] > 3600:
                        del open_signals[k]

                await asyncio.sleep(CHECK_INTERVAL)

            except Exception as e:
                print(f"Loop error: {e}")
                await asyncio.sleep(10 if "timed out" in str(e).lower() else 5)

    except Exception as e:
        print(f"❌ FATAL: {e}")
        tg(f"❌ FATAL: {str(e)[:100]}")
        raise e

# ==================== START ====================

if __name__ == "__main__":
    print(f"\n{'='*50}")
    print("🚀 PROFESSIONAL SMC BOT v2.1 — AGGRESSIVE MODE")
    print(f"{'='*50}\n")

    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            tg("🛑 BOT GESTOPT")
            print("\n🛑 Gestopt")
            break
        except Exception as e:
            tg(f"💥 CRASH: {str(e)[:80]}")
            print(f"Crash: {e}\nRestart in 5s...")
            time.sleep(5)
