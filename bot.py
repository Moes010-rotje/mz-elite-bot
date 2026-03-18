import asyncio
import pandas as pd
import numpy as np
import os
import time
import json
import urllib.request
import logging
import random
from datetime import datetime, date, timedelta, timezone
from metaapi_cloud_sdk import MetaApi

# ==================== CONFIGURATIE ====================

# --- TEST MODE ---
TEST_MODE = True  # Zet op False voor echte trading
TEST_DURATION = 3600  # 1 uur in seconden
TEST_START_TIME = time.time()
TEST_TRADES_GOAL = 1  # We willen minimaal 1 trade zien

CHECK_INTERVAL = 2  # seconden
RISK = 0.01  # 1% risico per trade
MIN_RR = 2.0  # minimale risk/reward ratio (1:2)
DAILY_LOSS_LIMIT = 0.03  # 3% daily loss limit
MAX_TRADES_PER_ASSET = 4  # max 4 trades per asset
WEEKLY_RESET_DAY = 2  # Wednesday (0=Monday, 2=Wednesday)
WEEKLY_RESET_HOUR = 22  # 22:00 UTC

# Sessie tijden (UTC)
SESSIONS = {
    "asia": {"start": 0, "end": 7, "symbols": ["USDJPY", "GBPJPY", "BTCUSD"]},
    "london": {"start": 7, "end": 16, "symbols": "all"},
    "ny": {"start": 13, "end": 21, "symbols": "all"}
}

SYMBOLS = [
    "XAUUSD", "XAGUSD", "BTCUSD",
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF",
    "NAS100", "US30", "US500"
]

METAAPI_TOKEN = os.getenv("METAAPI_TOKEN")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT = os.getenv("TG_CHAT")

# ==================== GLOBALE VARIABELEN ====================

daily = {"date": None, "start": 0}
weekly = {"week": None, "loss": 0, "limit_hit": False, "start_balance": 0}
last_heartbeat = 0  # Voor heartbeat tracking
open_signals = {}  # Bijhouden welke signalen al zijn gebruikt
correlation_pairs = [
    ["EURUSD", "GBPUSD"],
    ["NAS100", "US500"],
    ["XAUUSD", "XAGUSD"]
]
test_trades_count = 0  # Voor testmodus

logging.basicConfig(level=logging.INFO)

# ==================== TELEGRAM ====================

def tg(msg):
    """Stuur notificatie via Telegram"""
    try:
        # In testmodus, print ook naar console
        if TEST_MODE:
            print(f"\n📱 TELEGRAM: {msg}\n")
            
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        data = json.dumps({"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f"Telegram error: {e}")

# ==================== HEARTBEAT (elke 10 minuten) ====================

async def send_heartbeat(conn, balance, equity, positions):
    """Stuur elke 10 minuten een statusupdate"""
    global last_heartbeat
    
    current_time = time.time()
    
    # Check of het 10 minuten (600 seconden) geleden is
    if current_time - last_heartbeat >= 600:
        last_heartbeat = current_time
        
        # Account informatie opnieuw ophalen voor zekerheid
        try:
            info = await conn.get_account_information()
            balance = info["balance"]
            equity = info["equity"]
            positions = await conn.get_positions()
        except:
            pass
        
        # Bereken P&L
        pl = equity - balance
        pl_pct = (pl / balance) * 100 if balance > 0 else 0
        
        # Bepaal huidige sessie
        current_hour = datetime.utcnow().hour
        session = "outside"
        for s_name, s_data in SESSIONS.items():
            if s_data["start"] <= current_hour < s_data["end"]:
                session = s_name
                break
        
        # Bereken daily loss
        daily_loss = 0
        if daily["start"] > 0:
            daily_loss = (daily["start"] - balance) / daily["start"] * 100
        
        # Test mode info
        test_info = ""
        if TEST_MODE:
            elapsed = time.time() - TEST_START_TIME
            remaining = TEST_DURATION - elapsed
            test_info = f"\n🔬 TEST: {test_trades_count}/1 trades - {int(remaining/60)}m rest"
        
        msg = f"""
<b>🤖 BOT HEARTBEAT</b>

💰 Balance: ${round(balance, 2)}
📊 Equity: ${round(equity, 2)}
📈 P&L: {round(pl, 2)} ({round(pl_pct, 2)}%)

🎯 Open trades: {len(positions)}
🕐 Sessie: {session.upper()}
📅 Daily loss: {round(daily_loss, 2)}%
📆 Weekly loss: {round(weekly['loss']*100, 2)}%
{test_info}
⏰ {datetime.utcnow().strftime('%H:%M:%S')} UTC
"""
        tg(msg)
        
        # Ook in console voor testmodus
        if TEST_MODE:
            print(f"\n💓 HEARTBEAT - {datetime.utcnow().strftime('%H:%M:%S')} UTC")
            print(f"Balance: ${balance}, Trades: {len(positions)}, Daily loss: {round(daily_loss, 2)}%\n")

# ==================== TEST MODULE ====================

def check_test_mode():
    """Check of testmodus actief is en of we nog binnen testtijd zitten"""
    global test_trades_count
    
    if not TEST_MODE:
        return True  # Gewoon doorgaan
    
    elapsed = time.time() - TEST_START_TIME
    remaining = TEST_DURATION - elapsed
    
    # Alleen console output, geen telegram spam
    if int(elapsed) % 60 == 0 and int(elapsed) > 0:
        print(f"\n🔬 TEST MODUS: {int(elapsed/60)} minuten verstreken")
        print(f"📊 Trades gevonden: {test_trades_count}/{TEST_TRADES_GOAL}")
        print(f"⏱️ Resterende tijd: {int(remaining/60)} minuten\n")
    
    if elapsed > TEST_DURATION:
        if test_trades_count >= TEST_TRADES_GOAL:
            tg(f"✅ TEST SUCCESVOL: {test_trades_count} trade(s) in 1 uur!")
            print(f"\n✅ TEST SUCCESVOL: {test_trades_count} trade(s) in 1 uur!\n")
        else:
            tg(f"⚠️ TEST GESTOPT: Slechts {test_trades_count} trade(s) gevonden in 1 uur")
            print(f"\n⚠️ TEST GESTOPT: Slechts {test_trades_count} trade(s) gevonden in 1 uur\n")
        return False
    
    return True

def force_signal_for_test(symbol, current_session):
    """FORCEER een signaal voor testdoeleinden (alleen in testmodus)"""
    if not TEST_MODE:
        return None, None
    
    global test_trades_count
    
    # Als we nog geen trade hebben, forceer er een (10% kans)
    if test_trades_count < TEST_TRADES_GOAL and random.random() < 0.05:  # 5% kans per scan
        test_trades_count += 1
        # Willekeurig buy of sell
        signal = "buy" if random.random() > 0.5 else "sell"
        print(f"\n🎯 TEST: Geforceerd signaal voor {symbol} - {signal.upper()}\n")
        return signal, "TEST SIGNAL"
    
    return None, None

# ==================== WEEKLY LIMIET ====================

def check_weekly(balance):
    """Check of weekly limit is bereikt (reset Wednesday 22:00 UTC)"""
    now = datetime.utcnow()
    current_week = now.isocalendar()[1]
    
    # Reset op Wednesday 22:00 UTC
    if weekly["week"] != current_week:
        if now.weekday() == WEEKLY_RESET_DAY and now.hour >= WEEKLY_RESET_HOUR:
            weekly["week"] = current_week
            weekly["loss"] = 0
            weekly["limit_hit"] = False
            weekly["start_balance"] = balance
            tg("📊 WEEKLY LIMIT RESET")
    
    return not weekly["limit_hit"]

def update_weekly_loss(current_balance):
    """Update weekly loss op basis van huidige drawdown"""
    if weekly["start_balance"] > 0:
        loss = (weekly["start_balance"] - current_balance) / weekly["start_balance"]
        if loss > weekly["loss"]:
            weekly["loss"] = loss
            if loss >= 0.10:  # 10% weekly limit
                weekly["limit_hit"] = True
                tg("⚠️ WEEKLY LIMIET BEREIKT (10%)")

# ==================== SESSIE FILTER ====================

def get_current_session():
    """Bepaal huidige handelssessie"""
    now = datetime.utcnow()
    hour = now.hour
    
    for session_name, session_data in SESSIONS.items():
        if session_data["start"] <= hour < session_data["end"]:
            return session_name
    return None

def session_filter(symbol):
    """Check of asset mag worden verhandeld in huidige sessie"""
    session = get_current_session()
    if not session:
        return False
    
    session_data = SESSIONS[session]
    if session_data["symbols"] == "all" or symbol in session_data["symbols"]:
        return session
    
    return False

# ==================== NIEUWS FILTER ====================

async def news_filter():
    """Check voor high-impact nieuws (30 min voor/na)"""
    try:
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        res = urllib.request.urlopen(req, timeout=10)
        
        events = json.loads(res.read().decode())
        now = datetime.utcnow()
        
        for event in events:
            if event.get("impact") != "High":
                continue
            
            try:
                # Verwijder tijdzone info
                event_time_str = event["date"]
                if '+' in event_time_str:
                    event_time_str = event_time_str.split('+')[0]
                if 'Z' in event_time_str:
                    event_time_str = event_time_str.replace('Z', '')
                
                # Parse als naive datetime
                event_time = datetime.strptime(event_time_str, "%Y-%m-%dT%H:%M:%S")
                
                # Check 30 min voor en na
                time_diff = (now - event_time).total_seconds()
                if -1800 <= time_diff <= 1800:
                    if TEST_MODE:
                        print(f"📰 NIEUWSFILTER: {event.get('title', 'Unknown')}")
                    tg(f"📰 NIEUWSFILTER: {event.get('title', 'Unknown')} - {event_time.strftime('%H:%M')} UTC")
                    return False
                    
            except Exception as e:
                continue
        
        return True
        
    except Exception as e:
        return True

# ==================== CORRELATIE FILTER ====================

def get_best_correlated_setup(symbol, positions):
    """Kies beste setup van gecorreleerde paren"""
    for pair in correlation_pairs:
        if symbol in pair:
            # Tel open posities in dit paar
            pair_positions = [p for p in positions if p["symbol"] in pair]
            
            # Als er al een positie open is in dit paar, geen nieuwe
            if len(pair_positions) >= 1:
                return False
            
            # Check of we al een signaal hebben voor het andere paar
            other_symbol = pair[0] if pair[1] == symbol else pair[1]
            if other_symbol in open_signals:
                return False
    
    return True

# ==================== INDICATOREN ====================

def calculate_indicators(df):
    """Bereken alle technische indicatoren"""
    
    # EMA's
    df["ema50"] = df.close.ewm(span=50).mean()
    df["ema200"] = df.close.ewm(span=200).mean()
    
    # RSI
    delta = df.close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))
    
    # MACD
    exp1 = df.close.ewm(span=12).mean()
    exp2 = df.close.ewm(span=26).mean()
    df["macd"] = exp1 - exp2
    df["macd_signal"] = df["macd"].ewm(span=9).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    
    # ATR
    df["tr"] = np.maximum(
        df.high - df.low,
        np.maximum(
            abs(df.high - df.close.shift()),
            abs(df.low - df.close.shift())
        )
    )
    df["atr"] = df["tr"].rolling(14).mean()
    
    return df

# ==================== SMC INDICATOREN ====================

def find_order_blocks(df):
    """Identificeer Order Blocks"""
    if len(df) < 5:
        return None
    
    last_5 = df.tail(5)
    
    # Bullish Order Block
    if (last_5.iloc[-2].close > last_5.iloc[-3].high and 
        last_5.iloc[-3].close < last_5.iloc[-4].close):
        return "bull"
    
    # Bearish Order Block
    if (last_5.iloc[-2].close < last_5.iloc[-3].low and 
        last_5.iloc[-3].close > last_5.iloc[-4].close):
        return "bear"
    
    return None

def find_fair_value_gap(df):
    """Identificeer Fair Value Gaps"""
    if len(df) < 4:
        return None
    
    c1 = df.iloc[-4]
    c2 = df.iloc[-3]
    c3 = df.iloc[-2]
    
    # Bullish FVG
    if c2.low > c1.high and c3.low > c2.high:
        if c3.low > c1.high:
            return "bull"
    
    # Bearish FVG
    if c2.high < c1.low and c3.high < c2.low:
        if c3.high < c1.low:
            return "bear"
    
    return None

def find_liquidity_sweep(df):
    """Identificeer Liquidity Sweeps"""
    if len(df) < 3:
        return None
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    prev2 = df.iloc[-3]
    
    # Bullish liquidity sweep
    if last.low < prev2.low and last.close > prev.high:
        return "bull"
    
    # Bearish liquidity sweep
    if last.high > prev2.high and last.close < prev.low:
        return "bear"
    
    return None

def market_structure(df):
    """Bepaal marktstructuur (HH/HL of LH/LL)"""
    if len(df) < 5:
        return None
    
    highs = df.high.tail(5).tolist()
    lows = df.low.tail(5).tolist()
    
    # Hogere highs en hogere lows = uptrend
    if highs[-1] > highs[-2] and lows[-1] > lows[-2]:
        if highs[-2] > highs[-3] and lows[-2] > lows[-3]:
            return "bull"
    
    # Lagere highs en lagere lows = downtrend
    if highs[-1] < highs[-2] and lows[-1] < lows[-2]:
        if highs[-2] < highs[-3] and lows[-2] < lows[-3]:
            return "bear"
    
    return None

def break_of_structure(df):
    """Check voor Break of Structure"""
    if len(df) < 3:
        return None
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # Bullish BOS
    if last.high > prev.high and last.close > prev.high:
        return "bull"
    
    # Bearish BOS
    if last.low < prev.low and last.close < prev.low:
        return "bear"
    
    return None

# ==================== MULTI-TIMEFRAME ====================

async def get_htf_trend(account, symbol, timeframe="1h"):
    """Bepaal trend op hoger timeframe"""
    try:
        candles = await account.get_historical_candles(symbol, timeframe, 100)
        df = pd.DataFrame(candles)
        df["ema50"] = df.close.ewm(span=50).mean()
        df["ema200"] = df.close.ewm(span=200).mean()
        
        if df.ema50.iloc[-1] > df.ema200.iloc[-1]:
            return "bull"
        elif df.ema50.iloc[-1] < df.ema200.iloc[-1]:
            return "bear"
        
        return None
    except:
        return None

# ==================== STRATEGIEËN ====================

async def smc_strong_setup(account, symbol, df_5m, df_15m):
    """SMC Strong: 1H + 15M + 5M aligned"""
    
    htf_trend = await get_htf_trend(account, symbol, "1h")
    if not htf_trend:
        return None, None
    
    df_15m = calculate_indicators(df_15m)
    ms_15m = market_structure(df_15m)
    
    df_5m = calculate_indicators(df_5m)
    ls_5m = find_liquidity_sweep(df_5m)
    bos_5m = break_of_structure(df_5m)
    fvg_5m = find_fair_value_gap(df_5m)
    
    if (htf_trend == "bull" and ms_15m == "bull" and 
        (ls_5m == "bull" or bos_5m == "bull" or fvg_5m == "bull")):
        return "buy", "SMC STRONG"
    
    if (htf_trend == "bear" and ms_15m == "bear" and 
        (ls_5m == "bear" or bos_5m == "bear" or fvg_5m == "bear")):
        return "sell", "SMC STRONG"
    
    return None, None

async def smc_normal_setup(account, symbol, df_5m, df_15m):
    """SMC Normal: 15M + 5M aligned"""
    
    df_15m = calculate_indicators(df_15m)
    ms_15m = market_structure(df_15m)
    
    df_5m = calculate_indicators(df_5m)
    ls_5m = find_liquidity_sweep(df_5m)
    bos_5m = break_of_structure(df_5m)
    fvg_5m = find_fair_value_gap(df_5m)
    
    if (ms_15m == "bull" and 
        (ls_5m == "bull" or bos_5m == "bull" or fvg_5m == "bull")):
        return "buy", "SMC NORMAL"
    
    if (ms_15m == "bear" and 
        (ls_5m == "bear" or bos_5m == "bear" or fvg_5m == "bear")):
        return "sell", "SMC NORMAL"
    
    return None, None

async def london_breakout(account, symbol, df_5m):
    """London Breakout (07:00-10:00 UTC)"""
    
    now = datetime.utcnow()
    if not (7 <= now.hour < 10):
        return None, None
    
    if len(df_5m) < 12:
        return None, None
    
    first_6 = df_5m.head(6)
    range_high = first_6.high.max()
    range_low = first_6.low.min()
    
    last = df_5m.iloc[-1]
    
    if last.close > range_high and last.volume > first_6.volume.mean() * 1.2:
        return "buy", "LONDON BREAKOUT"
    
    if last.close < range_low and last.volume > first_6.volume.mean() * 1.2:
        return "sell", "LONDON BREAKOUT"
    
    return None, None

async def ny_breakout(account, symbol, df_5m):
    """NY Breakout (13:00-16:00 UTC)"""
    
    now = datetime.utcnow()
    if not (13 <= now.hour < 16):
        return None, None
    
    if len(df_5m) < 12:
        return None, None
    
    first_6 = df_5m.head(6)
    range_high = first_6.high.max()
    range_low = first_6.low.min()
    
    last = df_5m.iloc[-1]
    
    if last.close > range_high and last.volume > first_6.volume.mean() * 1.2:
        return "buy", "NY BREAKOUT"
    
    if last.close < range_low and last.volume > first_6.volume.mean() * 1.2:
        return "sell", "NY BREAKOUT"
    
    return None, None

# ==================== TRADE MANAGEMENT ====================

def calculate_lot_size(balance, sl_distance):
    """Dynamische lotsize (1% risico)"""
    if sl_distance <= 0:
        return 0.01
    
    lot = (balance * RISK) / (sl_distance * 10)
    return round(max(0.01, min(lot, 5)), 2)

def calculate_levels(signal_type, entry, df):
    """Bereken SL, TP1 en TP2"""
    
    if signal_type == "buy":
        sl = df.low.tail(10).min()
        distance = entry - sl
        if distance <= 0:
            distance = df.atr.iloc[-1] * 0.5
        tp1 = entry + distance
        tp2 = entry + distance * 2
        
    else:
        sl = df.high.tail(10).max()
        distance = sl - entry
        if distance <= 0:
            distance = df.atr.iloc[-1] * 0.5
        tp1 = entry - distance
        tp2 = entry - distance * 2
    
    return sl, tp1, tp2, distance

# ==================== DAILY LIMIET ====================

def check_daily(balance):
    """Check of daily loss limit is bereikt"""
    today = date.today()
    
    if daily["date"] != today:
        daily["date"] = today
        daily["start"] = balance
        return True
    
    if daily["start"] == 0:
        daily["start"] = balance
        return True
    
    loss = (daily["start"] - balance) / daily["start"]
    
    if loss >= DAILY_LOSS_LIMIT:
        tg(f"⚠️ DAILY LIMIET BEREIKT: {round(loss*100, 2)}%")
        return False
    
    return True

# ==================== HOOFDLOOP ====================

async def run():
    """Hoofdloop van de bot"""
    global test_trades_count, last_heartbeat
    
    try:
        # Verbind met MetaAPI
        print("🔄 Verbinden met MetaAPI...")
        api = MetaApi(METAAPI_TOKEN)
        account = await api.metatrader_account_api.get_account(ACCOUNT_ID)
        
        await account.wait_connected()
        
        conn = account.get_rpc_connection()
        await conn.connect()
        await conn.wait_synchronized()
        
        # Reset heartbeat timer
        last_heartbeat = time.time()
        
        # Startbericht
        if TEST_MODE:
            tg("🚀 SONNET 4.6 TESTMODUS GESTART - 1 uur test")
            print("\n" + "="*50)
            print("🚀 TESTMODUS ACTIEF - Zoeken naar 1 trade in 1 uur")
            print("="*50 + "\n")
        else:
            tg("🚀 SONNET 4.6 GESTART")
        
        while True:
            try:
                # Check testmodus
                if not check_test_mode():
                    print("\n🏁 Testmodus beëindigd\n")
                    break
                
                # Account informatie
                info = await conn.get_account_information()
                balance = info["balance"]
                equity = info["equity"]
                
                positions = await conn.get_positions()
                
                # Update weekly loss
                update_weekly_loss(balance)
                
                # HEARTBEAT - elke 10 minuten
                await send_heartbeat(conn, balance, equity, positions)
                
                # Filters
                if not check_weekly(balance):
                    await asyncio.sleep(60)
                    continue
                    
                if not check_daily(balance):
                    await asyncio.sleep(60)
                    continue
                    
                if not await news_filter():
                    await asyncio.sleep(60)
                    continue
                
                # Scan symbolen voor nieuwe signalen
                for symbol in SYMBOLS:
                    
                    # Check sessie
                    current_session = session_filter(symbol)
                    if not current_session:
                        continue
                    
                    # Check correlatie
                    if not get_best_correlated_setup(symbol, positions):
                        continue
                    
                    # Check max trades
                    symbol_positions = [p for p in positions if p["symbol"] == symbol]
                    if len(symbol_positions) >= MAX_TRADES_PER_ASSET:
                        continue
                    
                    # Haal candles op
                    try:
                        candles_5m = await account.get_historical_candles(symbol, "5m", 100)
                        candles_15m = await account.get_historical_candles(symbol, "15m", 100)
                    except:
                        continue
                    
                    if not candles_5m or not candles_15m:
                        continue
                    
                    df_5m = pd.DataFrame(candles_5m)
                    df_15m = pd.DataFrame(candles_15m)
                    
                    if len(df_5m) < 50 or len(df_15m) < 30:
                        continue
                    
                    # Probeer verschillende strategieën
                    signal = None
                    setup_name = None
                    
                    # Echte strategieën
                    signal, setup_name = await smc_strong_setup(account, symbol, df_5m, df_15m)
                    
                    if not signal:
                        signal, setup_name = await smc_normal_setup(account, symbol, df_5m, df_15m)
                    
                    if not signal and current_session == "london":
                        signal, setup_name = await london_breakout(account, symbol, df_5m)
                    
                    if not signal and current_session == "ny":
                        signal, setup_name = await ny_breakout(account, symbol, df_5m)
                    
                    # TEST: Forceer een signaal als we nog geen trade hebben
                    if TEST_MODE and not signal and test_trades_count < TEST_TRADES_GOAL:
                        signal, setup_name = force_signal_for_test(symbol, current_session)
                    
                    if not signal:
                        continue
                    
                    # Controleer of signaal al is gebruikt
                    signal_key = f"{symbol}_{signal}_{int(time.time()/1800)}"
                    if signal_key in open_signals:
                        continue
                    
                    # Prijsinformatie
                    price = await conn.get_symbol_price(symbol)
                    
                    # Bereken levels
                    if signal == "buy":
                        entry = price["ask"]
                        sl, tp1, tp2, distance = calculate_levels("buy", entry, df_5m)
                    else:
                        entry = price["bid"]
                        sl, tp1, tp2, distance = calculate_levels("sell", entry, df_5m)
                    
                    # Check minimale RR
                    rr = abs((tp2 - entry) / distance) if distance != 0 else 0
                    if rr < MIN_RR:
                        continue
                    
                    # Bereken lotsize (heel klein in testmodus)
                    if TEST_MODE:
                        lot = 0.01  # Minimale lot in testmodus
                    else:
                        lot = calculate_lot_size(balance, abs(distance))
                    
                    # Open trade
                    if signal == "buy":
                        order = await conn.create_market_buy_order(symbol, lot, sl, tp2)
                    else:
                        order = await conn.create_market_sell_order(symbol, lot, sl, tp2)
                    
                    # Update test counter
                    if TEST_MODE:
                        test_trades_count += 1
                    
                    # Sla signaal op
                    open_signals[signal_key] = time.time()
                    
                    # Opruimen oude signalen
                    current_time = time.time()
                    for key in list(open_signals.keys()):
                        if current_time - open_signals[key] > 1800:
                            del open_signals[key]
                    
                    # Notificatie
                    msg = f"""
<b>{"🧪 TEST " if TEST_MODE else "✅"} TRADE GEOPEND</b>

📌 {symbol} - {setup_name}
📈 Type: {signal.upper()}
💵 Sessie: {current_session.upper()}

💰 Entry: {round(entry, 5)}
🛑 SL: {round(sl, 5)}
🎯 TP1: {round(tp1, 5)} (50%)
🎯 TP2: {round(tp2, 5)} (50%)

📊 RR: 1:{round(rr, 2)}
📦 Lots: {lot}
💵 Balance: ${round(balance, 2)}

⏰ {datetime.utcnow().strftime('%H:%M:%S')} UTC
"""
                    tg(msg)
                    
                    # Extra console output voor test
                    if TEST_MODE:
                        print(f"\n🎯 TRADE #{test_trades_count} GEPLAATST!")
                        print(f"Symbol: {symbol}, Type: {signal}, Setup: {setup_name}\n")
                    
                    # Wacht even tussen trades
                    await asyncio.sleep(2)
                
                await asyncio.sleep(CHECK_INTERVAL)
                
            except Exception as e:
                print(f"Error in main loop: {e}")
                await asyncio.sleep(5)
                
    except Exception as e:
        tg(f"❌ CONNECTIE FOUT: {str(e)}")
        print(f"Connectie fout: {e}")
        raise e

# ==================== START ====================

if __name__ == "__main__":
    print("\n" + "="*50)
    print("🤖 SONNET 4.6 BOT")
    print("="*50)
    print(f"Testmodus: {'AAN' if TEST_MODE else 'UIT'}")
    if TEST_MODE:
        print(f"Doel: {TEST_TRADES_GOAL} trade in {TEST_DURATION/60} minuten")
    print("="*50 + "\n")
    
    while True:
        try:
            asyncio.run(run())
            break  # Als run() eindigt, stop de loop
        except KeyboardInterrupt:
            tg("🛑 BOT GESTOPT")
            print("\n🛑 Bot gestopt door gebruiker")
            break
        except Exception as e:
            tg(f"💥 CRASH: {str(e)}")
            print(f"Crash: {e}")
            print("Opnieuw starten over 5 seconden...")
            time.sleep(5)
