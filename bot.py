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

# ==================== CONFIGURATIE ====================

CHECK_INTERVAL = 2  # seconden
RISK = 0.01  # 1% risico per trade
MIN_RR = 2.0  # minimale risk/reward ratio (1:2)
DAILY_LOSS_LIMIT = 0.03  # 3% daily loss limit
MAX_TRADES_PER_ASSET = 4  # max 4 trades per asset
WEEKLY_RESET_DAY = 2  # Wednesday (0=Monday, 2=Wednesday)
WEEKLY_RESET_HOUR = 22  # 22:00 UTC

# ===== TEST MODE =====
TEST_MODE = False  # 🔥 UIT - ALLEEN ECHTE SIGNALEN

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
last_status = 0
open_signals = {}
correlation_pairs = [
    ["EURUSD", "GBPUSD"],
    ["NAS100", "US500"],
    ["XAUUSD", "XAGUSD"]
]

logging.basicConfig(level=logging.INFO)

# ==================== TELEGRAM ====================

def tg(msg):
    """Stuur notificatie via Telegram"""
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        data = json.dumps({"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
        print(f"✅ Telegram verstuurd: {msg[:50]}...")
    except Exception as e:
        print(f"❌ Telegram error: {e}")

# ==================== TELEGRAM TEST ====================

def test_telegram():
    """Test of Telegram werkt bij opstarten"""
    print("\n" + "="*50)
    print("🔍 TELEGRAM TEST")
    print("="*50)
    print(f"TG_TOKEN: {TG_TOKEN[:5]}...{TG_TOKEN[-5:] if TG_TOKEN else 'NIET GEVONDEN'}")
    print(f"TG_CHAT: {TG_CHAT}")
    
    try:
        test_msg = "🧪 TEST BERICHT - Als je dit ziet, werkt Telegram! Bot is gestart."
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        data = json.dumps({"chat_id": TG_CHAT, "text": test_msg}).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
        print("✅ Telegram TEST GELUKT! Check je Telegram.")
        return True
    except Exception as e:
        print(f"❌ Telegram TEST MISLUKT: {e}")
        return False

# ==================== HEARTBEAT ====================

async def send_heartbeat(conn, balance, equity, positions):
    """Stuur elke 10 minuten statusupdate"""
    global last_status
    
    current_time = time.time()
    
    if current_time - last_status >= 600:
        last_status = current_time
        
        try:
            info = await conn.get_account_information()
            balance = info["balance"]
            equity = info["equity"]
            positions = await conn.get_positions()
        except Exception as e:
            print(f"Heartbeat data error: {e}")
        
        pl = equity - balance
        pl_pct = (pl / balance) * 100 if balance > 0 else 0
        
        current_hour = datetime.utcnow().hour
        session = "outside"
        for s_name, s_data in SESSIONS.items():
            if s_data["start"] <= current_hour < s_data["end"]:
                session = s_name
                break
        
        daily_loss = 0
        if daily["start"] > 0:
            daily_loss = (daily["start"] - balance) / daily["start"] * 100
        
        msg = f"""
<b>🤖 HEARTBEAT</b>

💰 Balance: ${round(balance, 2)}
📊 Equity: ${round(equity, 2)}
📈 P&L: {round(pl, 2)} ({round(pl_pct, 2)}%)

🎯 Open trades: {len(positions)}
🕐 Sessie: {session.upper()}
📅 Daily loss: {round(daily_loss, 2)}%
📆 Weekly loss: {round(weekly['loss']*100, 2)}%

⏰ {datetime.utcnow().strftime('%H:%M:%S')} UTC
"""
        tg(msg)
        print(f"💓 Heartbeat verstuurd om {datetime.utcnow().strftime('%H:%M:%S')} UTC")

# ==================== WEEKLY LIMIET ====================

def check_weekly(balance):
    now = datetime.utcnow()
    current_week = now.isocalendar()[1]
    
    if weekly["week"] != current_week:
        if now.weekday() == WEEKLY_RESET_DAY and now.hour >= WEEKLY_RESET_HOUR:
            weekly["week"] = current_week
            weekly["loss"] = 0
            weekly["limit_hit"] = False
            weekly["start_balance"] = balance
            tg("📊 WEEKLY LIMIT RESET")
    
    return not weekly["limit_hit"]

def update_weekly_loss(current_balance):
    if weekly["start_balance"] > 0:
        loss = (weekly["start_balance"] - current_balance) / weekly["start_balance"]
        if loss > weekly["loss"]:
            weekly["loss"] = loss
            if loss >= 0.10:
                weekly["limit_hit"] = True
                tg("⚠️ WEEKLY LIMIET BEREIKT (10%)")

# ==================== SESSIE FILTER ====================

def get_current_session():
    now = datetime.utcnow()
    hour = now.hour
    
    for session_name, session_data in SESSIONS.items():
        if session_data["start"] <= hour < session_data["end"]:
            return session_name
    return None

def session_filter(symbol):
    session = get_current_session()
    if not session:
        return False
    
    session_data = SESSIONS[session]
    if session_data["symbols"] == "all" or symbol in session_data["symbols"]:
        return session
    
    return False

# ==================== NIEUWS FILTER ====================

async def news_filter():
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
                event_time_str = event["date"]
                if '+' in event_time_str:
                    event_time_str = event_time_str.split('+')[0]
                if 'Z' in event_time_str:
                    event_time_str = event_time_str.replace('Z', '')
                
                event_time = datetime.strptime(event_time_str, "%Y-%m-%dT%H:%M:%S")
                
                time_diff = (now - event_time).total_seconds()
                if -1800 <= time_diff <= 1800:
                    tg(f"📰 NIEUWSFILTER: {event.get('title', 'Unknown')}")
                    return False
                    
            except Exception as e:
                continue
        
        return True
        
    except Exception as e:
        return True

# ==================== CORRELATIE FILTER ====================

def get_best_correlated_setup(symbol, positions):
    for pair in correlation_pairs:
        if symbol in pair:
            pair_positions = [p for p in positions if p["symbol"] in pair]
            
            if len(pair_positions) >= 1:
                return False
            
            other_symbol = pair[0] if pair[1] == symbol else pair[1]
            if other_symbol in open_signals:
                return False
    
    return True

# ==================== INDICATOREN ====================

def calculate_indicators(df):
    df["ema50"] = df.close.ewm(span=50).mean()
    df["ema200"] = df.close.ewm(span=200).mean()
    
    delta = df.close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))
    
    exp1 = df.close.ewm(span=12).mean()
    exp2 = df.close.ewm(span=26).mean()
    df["macd"] = exp1 - exp2
    df["macd_signal"] = df["macd"].ewm(span=9).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    
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
    if len(df) < 5:
        return None
    
    last_5 = df.tail(5)
    
    if (last_5.iloc[-2].close > last_5.iloc[-3].high and 
        last_5.iloc[-3].close < last_5.iloc[-4].close):
        return "bull"
    
    if (last_5.iloc[-2].close < last_5.iloc[-3].low and 
        last_5.iloc[-3].close > last_5.iloc[-4].close):
        return "bear"
    
    return None

def find_fair_value_gap(df):
    if len(df) < 4:
        return None
    
    c1 = df.iloc[-4]
    c2 = df.iloc[-3]
    c3 = df.iloc[-2]
    
    if c2.low > c1.high and c3.low > c2.high:
        if c3.low > c1.high:
            return "bull"
    
    if c2.high < c1.low and c3.high < c2.low:
        if c3.high < c1.low:
            return "bear"
    
    return None

def find_liquidity_sweep(df):
    if len(df) < 3:
        return None
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    prev2 = df.iloc[-3]
    
    if last.low < prev2.low and last.close > prev.high:
        return "bull"
    
    if last.high > prev2.high and last.close < prev.low:
        return "bear"
    
    return None

def market_structure(df):
    if len(df) < 5:
        return None
    
    highs = df.high.tail(5).tolist()
    lows = df.low.tail(5).tolist()
    
    if highs[-1] > highs[-2] and lows[-1] > lows[-2]:
        if highs[-2] > highs[-3] and lows[-2] > lows[-3]:
            return "bull"
    
    if highs[-1] < highs[-2] and lows[-1] < lows[-2]:
        if highs[-2] < highs[-3] and lows[-2] < lows[-3]:
            return "bear"
    
    return None

def break_of_structure(df):
    if len(df) < 3:
        return None
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    if last.high > prev.high and last.close > prev.high:
        return "bull"
    
    if last.low < prev.low and last.close < prev.low:
        return "bear"
    
    return None

# ==================== MULTI-TIMEFRAME ====================

async def get_htf_trend(account, symbol, timeframe="1h"):
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
    if sl_distance <= 0:
        return 0.01
    
    lot = (balance * RISK) / (sl_distance * 10)
    return round(max(0.01, min(lot, 5)), 2)

def calculate_levels(signal_type, entry, df):
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

async def manage_positions(conn, positions):
    for position in positions:
        try:
            symbol = position["symbol"]
            price = await conn.get_symbol_price(symbol)
        except Exception as e:
            print(f"Position management error: {e}")

# ==================== DAILY LIMIET ====================

def check_daily(balance):
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

# ==================== DIAGNOSTISCHE FUNCTIE ====================

async def run_diagnostics(conn, account):
    print("\n" + "="*60)
    print("🔍 DIAGNOSE: Waarom komen er geen trades?")
    print("="*60)
    
    try:
        info = await conn.get_account_information()
        print(f"\n📊 ACCOUNT INFO:")
        print(f"   Balance: ${info['balance']}")
        print(f"   Equity: ${info['equity']}")
    except Exception as e:
        print(f"❌ Kan account info niet ophalen: {e}")
    
    now = datetime.utcnow()
    print(f"\n🕐 HUIDIGE TIJD: {now.strftime('%H:%M:%S')} UTC")
    
    session = get_current_session()
    if session:
        print(f"✅ ACTIEVE SESSIE: {session.upper()}")
    else:
        print(f"⚠️ GEEN ACTIEVE SESSIE")
    
    print(f"\n📈 TEST ALLE SYMBOLEN:")
    print("-" * 60)
    
    for symbol in SYMBOLS:
        print(f"\n🔍 {symbol}:")
        
        session_check = session_filter(symbol)
        if not session_check:
            print(f"   ⏭️ Sessie filter: GEBLOKKEERD")
        
        try:
            candles_5m = await account.get_historical_candles(symbol, "5m", 50)
            if candles_5m and len(candles_5m) >= 20:
                print(f"   ✅ Data: {len(candles_5m)} candles")
            else:
                print(f"   ❌ Data: Onvoldoende candles")
        except Exception as e:
            print(f"   ❌ Fout bij ophalen data: {e}")
    
    print("\n" + "="*60)
    print("DIAGNOSE VOLTOOID")
    print("="*60 + "\n")

# ==================== HOOFDLOOP ====================

async def run():
    try:
        print("🔄 Verbinden met MetaAPI...")
        api = MetaApi(METAAPI_TOKEN)
        account = await api.metatrader_account_api.get_account(ACCOUNT_ID)
        
        print("🔄 Wachten op account connectie...")
        await account.wait_connected()
        
        conn = account.get_rpc_connection()
        print("🔄 Verbinden met RPC...")
        await conn.connect()
        
        print("🔄 Wachten op synchronisatie met MetaTrader...")
        await conn.wait_synchronized(timeout_in_seconds=120)
        print("✅ Account is gesynchroniseerd met MetaTrader!")
        
        await asyncio.sleep(2)
        
        # Telegram test bij opstarten
        test_telegram()
        
        print("✅ BOT VERBONDEN MET METATRADER - Klaar voor actie")
        
        global last_status
        last_status = time.time()
        
        await run_diagnostics(conn, account)
        
        print("✅ Test modus UIT - alleen echte signalen")
        
        # Stuur een test heartbeat om te checken
        await send_heartbeat(conn, 0, 0, [])
        
        # Hoofdloop
        while True:
            try:
                if not conn.connected:
                    print("⚠️ Verbinding verbroken, opnieuw verbinden...")
                    break
                
                info = await conn.get_account_information()
                balance = info["balance"]
                equity = info["equity"]
                
                positions = await conn.get_positions()
                
                update_weekly_loss(balance)
                
                await send_heartbeat(conn, balance, equity, positions)
                
                if not check_weekly(balance):
                    await asyncio.sleep(60)
                    continue
                    
                if not check_daily(balance):
                    await asyncio.sleep(60)
                    continue
                    
                if not await news_filter():
                    await asyncio.sleep(60)
                    continue
                
                await manage_positions(conn, positions)
                
                for symbol in SYMBOLS:
                    
                    current_session = session_filter(symbol)
                    if not current_session:
                        continue
                    
                    if not get_best_correlated_setup(symbol, positions):
                        continue
                    
                    symbol_positions = [p for p in positions if p["symbol"] == symbol]
                    if len(symbol_positions) >= MAX_TRADES_PER_ASSET:
                        continue
                    
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
                    
                    df_5m = calculate_indicators(df_5m)
                    df_15m = calculate_indicators(df_15m)
                    
                    signal = None
                    setup_name = None
                    
                    signal, setup_name = await smc_strong_setup(account, symbol, df_5m, df_15m)
                    
                    if not signal:
                        signal, setup_name = await smc_normal_setup(account, symbol, df_5m, df_15m)
                    
                    if not signal and current_session == "london":
                        signal, setup_name = await london_breakout(account, symbol, df_5m)
                    
                    if not signal and current_session == "ny":
                        signal, setup_name = await ny_breakout(account, symbol, df_5m)
                    
                    if not signal:
                        continue
                    
                    signal_key = f"{symbol}_{signal}_{int(time.time()/1800)}"
                    if signal_key in open_signals:
                        continue
                    
                    price = await conn.get_symbol_price(symbol)
                    
                    if signal == "buy":
                        entry = price["ask"]
                        sl, tp1, tp2, distance = calculate_levels("buy", entry, df_5m)
                    else:
                        entry = price["bid"]
                        sl, tp1, tp2, distance = calculate_levels("sell", entry, df_5m)
                    
                    rr = abs((tp2 - entry) / distance) if distance != 0 else 0
                    if rr < MIN_RR:
                        continue
                    
                    lot = calculate_lot_size(balance, abs(distance))
                    
                    if signal == "buy":
                        order = await conn.create_market_buy_order(symbol, lot, sl, tp2)
                    else:
                        order = await conn.create_market_sell_order(symbol, lot, sl, tp2)
                    
                    open_signals[signal_key] = time.time()
                    
                    current_time = time.time()
                    for key in list(open_signals.keys()):
                        if current_time - open_signals[key] > 1800:
                            del open_signals[key]
                    
                    msg = f"""
<b>✅ TRADE GEOPEND</b>

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
                    
                    await asyncio.sleep(2)
                
                await asyncio.sleep(CHECK_INTERVAL)
                
            except Exception as e:
                print(f"Error in main loop: {e}")
                if "timed out" in str(e).lower():
                    print("⚠️ Timeout, even wachten...")
                    await asyncio.sleep(10)
                else:
                    print("⚠️ Onverwachte error, maar we blijven draaien")
                    await asyncio.sleep(5)
        
    except Exception as e:
        print(f"❌ Fatale fout: {e}")
        tg(f"❌ FATALE FOUT: {str(e)[:50]}...")
        raise e

# ==================== START ====================

if __name__ == "__main__":
    print("\n" + "="*50)
    print("🚀 SONNET 4.6 BOT STARTEN")
    print("="*50 + "\n")
    
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            tg("🛑 BOT GESTOPT")
            print("\n🛑 Bot gestopt door gebruiker")
            break
        except Exception as e:
            tg(f"💥 CRASH: {str(e)}")
            print(f"Crash: {e}")
            print("Opnieuw starten over 5 seconden...")
            time.sleep(5)
