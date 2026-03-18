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
open_signals = {}  # Bijhouden welke signalen al zijn gebruikt
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
    except Exception as e:
        print(f"Telegram error: {e}")

# ==================== HEARTBEAT ====================

def heartbeat(balance, equity, positions):
    """Stuur elke 10 minuten statusupdate"""
    global last_status
    
    if time.time() - last_status >= 600:  # 10 minuten
        last_status = time.time()
        
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
        
        msg = f"""
<b>🤖 BOT STATUS</b>

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

# ==================== NIEUWS FILTER (GEFIXT) ====================

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
                # FIX: Verwijder tijdzone informatie en parse direct
                event_time_str = event["date"]
                # Verwijder tijdzone info zoals +00:00 of Z
                if '+' in event_time_str:
                    event_time_str = event_time_str.split('+')[0]
                if 'Z' in event_time_str:
                    event_time_str = event_time_str.replace('Z', '')
                
                # Parse als naive datetime
                event_time = datetime.strptime(event_time_str, "%Y-%m-%dT%H:%M:%S")
                
                # Check 30 min voor en na
                time_diff = (now - event_time).total_seconds()
                if -1800 <= time_diff <= 1800:  # 30 min in seconden
                    tg(f"📰 NIEUWSFILTER: {event.get('title', 'Unknown')} - {event_time.strftime('%H:%M')} UTC")
                    return False
                    
            except Exception as e:
                print(f"Event parse error: {e}")
                continue
        
        return True
        
    except Exception as e:
        print(f"News filter error: {e}")
        return True  # Bij fout, gewoon doorgaan

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
            
            # Anders, check of we al een signaal hebben voor het andere paar
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
    
    # ATR voor trailing stops
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
    
    # Bullish Order Block: sterke up candle na consolidatie
    if (last_5.iloc[-2].close > last_5.iloc[-3].high and  # Breakout
        last_5.iloc[-3].close < last_5.iloc[-4].close):   # Vorige candle was bearish
        return "bull"
    
    # Bearish Order Block: sterke down candle na consolidatie
    if (last_5.iloc[-2].close < last_5.iloc[-3].low and   # Breakdown
        last_5.iloc[-3].close > last_5.iloc[-4].close):   # Vorige candle was bullish
        return "bear"
    
    return None

def find_fair_value_gap(df):
    """Identificeer Fair Value Gaps"""
    if len(df) < 4:
        return None
    
    c1 = df.iloc[-4]
    c2 = df.iloc[-3]
    c3 = df.iloc[-2]
    
    # Bullish FVG: gap tussen c1 high en c3 low
    if c2.low > c1.high and c3.low > c2.high:
        if c3.low > c1.high:
            return "bull"
    
    # Bearish FVG: gap tussen c1 low en c3 high  
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
    
    # Bullish liquidity sweep: lagere low dan vorige, dan sluiten boven vorige high
    if last.low < prev2.low and last.close > prev.high:
        return "bull"
    
    # Bearish liquidity sweep: hogere high dan vorige, dan sluiten onder vorige low
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
    
    # Bullish BOS: doorbreken vorige high
    if last.high > prev.high and last.close > prev.high:
        return "bull"
    
    # Bearish BOS: doorbreken vorige low
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
        
        # Trend op basis van EMA
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
    
    # Haal 1h trend op
    htf_trend = await get_htf_trend(account, symbol, "1h")
    if not htf_trend:
        return None, None
    
    # Analyseer 15m
    df_15m = calculate_indicators(df_15m)
    ms_15m = market_structure(df_15m)
    ob_15m = find_order_blocks(df_15m)
    fvg_15m = find_fair_value_gap(df_15m)
    
    # Analyseer 5m
    ms_5m = market_structure(df_5m)
    ls_5m = find_liquidity_sweep(df_5m)
    bos_5m = break_of_structure(df_5m)
    fvg_5m = find_fair_value_gap(df_5m)
    
    # Bullish setup
    if (htf_trend == "bull" and 
        ms_15m == "bull" and 
        (ls_5m == "bull" or bos_5m == "bull" or fvg_5m == "bull")):
        return "buy", "SMC STRONG"
    
    # Bearish setup
    if (htf_trend == "bear" and 
        ms_15m == "bear" and 
        (ls_5m == "bear" or bos_5m == "bear" or fvg_5m == "bear")):
        return "sell", "SMC STRONG"
    
    return None, None

async def smc_normal_setup(account, symbol, df_5m, df_15m):
    """SMC Normal: 15M + 5M aligned"""
    
    # Analyseer 15m
    df_15m = calculate_indicators(df_15m)
    ms_15m = market_structure(df_15m)
    
    # Analyseer 5m  
    ms_5m = market_structure(df_5m)
    ls_5m = find_liquidity_sweep(df_5m)
    bos_5m = break_of_structure(df_5m)
    fvg_5m = find_fair_value_gap(df_5m)
    
    # Bullish setup
    if (ms_15m == "bull" and 
        (ls_5m == "bull" or bos_5m == "bull" or fvg_5m == "bull")):
        return "buy", "SMC NORMAL"
    
    # Bearish setup
    if (ms_15m == "bear" and 
        (ls_5m == "bear" or bos_5m == "bear" or fvg_5m == "bear")):
        return "sell", "SMC NORMAL"
    
    return None, None

async def london_breakout(account, symbol, df_5m):
    """London Breakout (07:00-10:00 UTC)"""
    
    now = datetime.utcnow()
    if not (7 <= now.hour < 10):
        return None, None
    
    # Zoek naar breakout van eerste 30 min range
    if len(df_5m) < 12:  # 30 min = 6 candles van 5m
        return None, None
    
    first_6 = df_5m.head(6)
    range_high = first_6.high.max()
    range_low = first_6.low.min()
    
    last = df_5m.iloc[-1]
    
    # Bullish breakout
    if last.close > range_high and last.volume > first_6.volume.mean() * 1.2:
        return "buy", "LONDON BREAKOUT"
    
    # Bearish breakout
    if last.close < range_low and last.volume > first_6.volume.mean() * 1.2:
        return "sell", "LONDON BREAKOUT"
    
    return None, None

async def ny_breakout(account, symbol, df_5m):
    """NY Breakout (13:00-16:00 UTC)"""
    
    now = datetime.utcnow()
    if not (13 <= now.hour < 16):
        return None, None
    
    # Zoek naar breakout van eerste 30 min range
    if len(df_5m) < 12:
        return None, None
    
    first_6 = df_5m.head(6)
    range_high = first_6.high.max()
    range_low = first_6.low.min()
    
    last = df_5m.iloc[-1]
    
    # Bullish breakout
    if last.close > range_high and last.volume > first_6.volume.mean() * 1.2:
        return "buy", "NY BREAKOUT"
    
    # Bearish breakout
    if last.close < range_low and last.volume > first_6.volume.mean() * 1.2:
        return "sell", "NY BREAKOUT"
    
    return None, None

# ==================== TRADE MANAGEMENT ====================

def calculate_lot_size(balance, sl_distance):
    """Dynamische lotsize (1% risico)"""
    if sl_distance <= 0:
        return 0.01
    
    # Voor XAUUSD andere berekening (factor 10 anders)
    lot = (balance * RISK) / (sl_distance * 10)
    return round(max(0.01, min(lot, 5)), 2)

def calculate_levels(signal_type, entry, df):
    """Bereken SL, TP1 en TP2"""
    
    if signal_type == "buy":
        # SL op recente swing low
        sl = df.low.tail(10).min()
        
        # Dynamische afstand
        distance = entry - sl
        if distance <= 0:
            distance = df.atr.iloc[-1] * 0.5  # fallback naar ATR
        
        # TP levels
        tp1 = entry + distance      # 1:1
        tp2 = entry + distance * 2   # 1:2
        
    else:  # sell
        # SL op recente swing high
        sl = df.high.tail(10).max()
        
        # Dynamische afstand
        distance = sl - entry
        if distance <= 0:
            distance = df.atr.iloc[-1] * 0.5  # fallback naar ATR
        
        # TP levels
        tp1 = entry - distance      # 1:1
        tp2 = entry - distance * 2   # 1:2
    
    return sl, tp1, tp2, distance

async def manage_positions(conn, positions):
    """Beheer open posities"""
    for position in positions:
        try:
            symbol = position["symbol"]
            
            # Haal actuele prijs op
            price = await conn.get_symbol_price(symbol)
            
            # Check of TP1 is bereikt (voor nu eenvoudig)
            if position["type"] == "POSITION_TYPE_BUY":
                if price["bid"] >= position["takeProfit"] * 0.5:  # 50% van TP
                    # Hier zou je partial close kunnen implementeren
                    pass
            else:
                if price["ask"] <= position["takeProfit"] * 1.5:  # 50% van TP
                    pass
                    
        except Exception as e:
            print(f"Position management error: {e}")

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

# ==================== DIAGNOSTISCHE FUNCTIE ====================

async def run_diagnostics(conn, account):
    """Voer een complete diagnose uit"""
    
    print("\n" + "="*60)
    print("🔍 DIAGNOSE: Waarom komen er geen trades?")
    print("="*60)
    
    # 1. Account info
    try:
        info = await conn.get_account_information()
        print(f"\n📊 ACCOUNT INFO:")
        print(f"   Balance: ${info['balance']}")
        print(f"   Equity: ${info['equity']}")
        print(f"   Margin: ${info.get('margin', 'N/A')}")
        print(f"   Free margin: ${info.get('freeMargin', 'N/A')}")
    except Exception as e:
        print(f"❌ Kan account info niet ophalen: {e}")
    
    # 2. MetaAPI status
    try:
        state = account.state
        print(f"\n🔌 METAAPI STATUS: {state}")
    except:
        pass
    
    # 3. Huidige tijd en sessie
    now = datetime.utcnow()
    current_hour = now.hour
    print(f"\n🕐 HUIDIGE TIJD: {now.strftime('%H:%M:%S')} UTC")
    print(f"   Uur: {current_hour}:00")
    
    session = get_current_session()
    if session:
        print(f"✅ ACTIEVE SESSIE: {session.upper()}")
        session_data = SESSIONS[session]
        print(f"   Handelbare uren: {session_data['start']}:00 - {session_data['end']}:00 UTC")
    else:
        print(f"⚠️ GEEN ACTIEVE SESSIE - Buiten handelsuren!")
        print(f"   Handelsuren zijn: Asia (0-7), London (7-16), NY (13-21)")
    
    # 4. Check of nieuwsfilter actief is
    try:
        news_active = await news_filter()  # False als er nieuws is
        if not news_active:
            print(f"⚠️ NIEUWSFILTER ACTIEF - High-impact nieuws binnen 30 min")
        else:
            print(f"✅ NIEUWSFILTER: Geen high-impact nieuws")
    except Exception as e:
        print(f"❌ Nieuwsfilter error: {e}")
    
    # 5. Check daily limit
    if daily["start"] > 0:
        loss = (daily["start"] - info['balance']) / daily["start"]
        print(f"\n📉 DAILY LOSS: {round(loss*100, 2)}% (limit {DAILY_LOSS_LIMIT*100}%)")
        if loss >= DAILY_LOSS_LIMIT:
            print(f"⚠️ DAILY LIMIET BEREIKT - Geen trades mogelijk!")
    else:
        print(f"\n📉 DAILY LOSS: Nog niet berekend")
    
    # 6. Check weekly limit
    if weekly["start_balance"] > 0:
        weekly_loss = (weekly["start_balance"] - info['balance']) / weekly["start_balance"]
        print(f"📆 WEEKLY LOSS: {round(weekly_loss*100, 2)}%")
        if weekly["limit_hit"]:
            print(f"⚠️ WEEKLY LIMIET BEREIKT - Geen trades mogelijk!")
    
    # 7. Test elk symbool
    print(f"\n📈 TEST ALLE SYMBOLEN:")
    print("-" * 60)
    
    for symbol in SYMBOLS:
        print(f"\n🔍 {symbol}:")
        
        # Check sessie filter
        session_check = session_filter(symbol)
        if not session_check:
            print(f"   ⏭️ Sessie filter: GEBLOKKEERD (niet in huidige sessie)")
        else:
            print(f"   ✅ Sessie filter: TOEGESTAAN ({session_check})")
        
        # Check of we candles kunnen krijgen
        try:
            candles_5m = await account.get_historical_candles(symbol, "5m", 100)
            candles_15m = await account.get_historical_candles(symbol, "15m", 100)
            
            if candles_5m and len(candles_5m) >= 50:
                print(f"   ✅ Data: {len(candles_5m)} 5m candles")
            else:
                print(f"   ❌ Data: Onvoldoende 5m candles ({len(candles_5m) if candles_5m else 0}/50)")
            
            if candles_15m and len(candles_15m) >= 30:
                print(f"   ✅ Data: {len(candles_15m)} 15m candles")
            else:
                print(f"   ❌ Data: Onvoldoende 15m candles ({len(candles_15m) if candles_15m else 0}/30)")
            
            # Test strategieën
            if len(candles_5m) >= 50 and len(candles_15m) >= 30:
                df_5m = pd.DataFrame(candles_5m)
                df_15m = pd.DataFrame(candles_15m)
                
                df_5m = calculate_indicators(df_5m)
                df_15m = calculate_indicators(df_15m)
                
                # Test SMC Strong
                signal, setup = await smc_strong_setup(account, symbol, df_5m, df_15m)
                if signal:
                    print(f"   🔥 SMC Strong: {signal.upper()} signaal!")
                else:
                    print(f"   ⏭️ SMC Strong: Geen signaal")
                
                # Test SMC Normal
                signal, setup = await smc_normal_setup(account, symbol, df_5m, df_15m)
                if signal:
                    print(f"   🔥 SMC Normal: {signal.upper()} signaal!")
                else:
                    print(f"   ⏭️ SMC Normal: Geen signaal")
                
                # Test of we een trade zouden kunnen plaatsen
                try:
                    price = await conn.get_symbol_price(symbol)
                    
                    # Test buy setup
                    entry = price['ask']
                    sl, tp1, tp2, distance = calculate_levels("buy", entry, df_5m)
                    rr = abs((tp2 - entry) / distance) if distance != 0 else 0
                    
                    if rr >= MIN_RR:
                        print(f"   ✅ RR berekening: {round(rr,2)} (voldoet aan {MIN_RR})")
                    else:
                        print(f"   ⏭️ RR te laag: {round(rr,2)} (min {MIN_RR})")
                    
                    lot = calculate_lot_size(info['balance'], distance)
                    print(f"   📦 Lot size: {lot}")
                    
                except Exception as e:
                    print(f"   ❌ Prijs ophalen mislukt: {e}")
                    
        except Exception as e:
            print(f"   ❌ Fout bij ophalen data: {e}")
    
    print("\n" + "="*60)
    print("DIAGNOSE VOLTOOID")
    print("="*60 + "\n")

# ==================== HOOFDLOOP ====================

async def run():
    """Hoofdloop van de bot"""
    
    try:
        # Verbind met MetaAPI
        api = MetaApi(METAAPI_TOKEN)
        account = await api.metatrader_account_api.get_account(ACCOUNT_ID)
        
        await account.wait_connected()
        
        conn = account.get_rpc_connection()
        await conn.connect()
        await conn.wait_synchronized()
        
        tg("🚀 SONNET 4.6 GESTART")
        
        # ===== DIAGNOSE UITVOEREN =====
        print("\n" + "="*60)
        print("🧪 DIAGNOSE MODUS ACTIEF")
        print("="*60)
        await run_diagnostics(conn, account)
        
        # Forceer een test trade
        print("\n🎯 FORCEREN TEST TRADE...")
        try:
            symbol = "EURUSD"
            price = await conn.get_symbol_price(symbol)
            lot = 0.01  # Minimale lot
            
            # Simpele SL en TP voor test
            sl = price['bid'] - 0.0010  # 10 pips SL
            tp = price['bid'] + 0.0020  # 20 pips TP
            
            order = await conn.create_market_buy_order(symbol, lot, sl, tp)
            print(f"✅ Test trade geplaatst op {symbol}!")
            tg(f"🧪 TEST TRADE GEPLAATST: {symbol} - Check of trading werkt!")
        except Exception as e:
            print(f"❌ Test trade mislukt: {e}")
            tg(f"❌ TEST TRADE MISLUKT: {e}")
        # ==============================
        
        while True:
            try:
                # Account informatie
                info = await conn.get_account_information()
                balance = info["balance"]
                equity = info["equity"]
                
                positions = await conn.get_positions()
                
                # Update weekly loss
                update_weekly_loss(balance)
                
                # Heartbeat
                heartbeat(balance, equity, positions)
                
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
                
                # Beheer open posities
                await manage_positions(conn, positions)
                
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
                    
                    # Bereken indicatoren
                    df_5m = calculate_indicators(df_5m)
                    df_15m = calculate_indicators(df_15m)
                    
                    # Probeer verschillende strategieën
                    signal = None
                    setup_name = None
                    
                    # SMC Strong (1e prioriteit)
                    signal, setup_name = await smc_strong_setup(account, symbol, df_5m, df_15m)
                    
                    # SMC Normal (2e prioriteit)
                    if not signal:
                        signal, setup_name = await smc_normal_setup(account, symbol, df_5m, df_15m)
                    
                    # London Breakout (3e prioriteit)
                    if not signal and current_session == "london":
                        signal, setup_name = await london_breakout(account, symbol, df_5m)
                    
                    # NY Breakout (3e prioriteit)
                    if not signal and current_session == "ny":
                        signal, setup_name = await ny_breakout(account, symbol, df_5m)
                    
                    if not signal:
                        continue
                    
                    # Controleer of signaal al is gebruikt (laatste 30 min)
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
                    rr = abs
