import asyncio
import pandas as pd
import os
import time
import json
import urllib.request
import logging
from datetime import datetime, date
from metaapi_cloud_sdk import MetaApi

CHECK_INTERVAL = 2
RISK = 0.01
MIN_RR = 1.2
DAILY_LOSS_LIMIT = 0.03
MAX_TRADES_PER_ASSET = 6

SYMBOLS = [
"XAUUSD","XAGUSD","BTCUSD",
"EURUSD","GBPUSD","USDJPY","USDCHF",
"NAS100","US30","US500"
]

METAAPI_TOKEN = os.getenv("METAAPI_TOKEN")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT = os.getenv("TG_CHAT")

daily = {"date":None,"start":0}
last_status = 0

CORRELATED = [
["EURUSD","GBPUSD"],
["NAS100","US500"],
["XAUUSD","XAGUSD"]
]

logging.basicConfig(level=logging.INFO)

# ---------------- TELEGRAM ----------------

def tg(msg):
    try:
        url=f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        data=json.dumps({"chat_id":TG_CHAT,"text":msg}).encode()
        req=urllib.request.Request(url,data=data,headers={"Content-Type":"application/json"})
        urllib.request.urlopen(req)
    except:
        print("TG ERROR")

# ---------------- HEARTBEAT ----------------

def heartbeat(balance,equity,positions):

    global last_status

    if time.time() - last_status >= 600:

        last_status = time.time()

        tg(f"""
BOT STATUS

Balance: {round(balance,2)}
Equity: {round(equity,2)}

Open trades: {len(positions)}

Scanning: {len(SYMBOLS)} assets

Time: {datetime.utcnow().strftime('%H:%M:%S')} UTC
""")

# ---------------- SESSION ----------------

def session_filter():
    return True

# ---------------- NEWS ----------------

def news_filter():
    try:
        url="https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        req=urllib.request.Request(url,headers={"User-Agent":"Mozilla"})
        res=urllib.request.urlopen(req)

        events=json.loads(res.read().decode())
        now=datetime.utcnow()

        for e in events:
            if e.get("impact")!="High":
                continue

            try:
                t=datetime.strptime(e["date"],"%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
            except:
                continue

            if abs((now-t).total_seconds()) < 1800:
                tg("NEWS FILTER ACTIVE")
                return False

        return True

    except:
        return True

# ---------------- CORRELATION ----------------

def correlation_block(symbol,positions):
    for pair in CORRELATED:
        if symbol in pair:
            for p in positions:
                if p["symbol"] in pair:
                    return True
    return False

# ---------------- MAX TRADES ----------------

def max_trades_filter(symbol,positions):
    return sum(1 for p in positions if p["symbol"] == symbol) >= MAX_TRADES_PER_ASSET

# ---------------- INDICATORS ----------------

def indicators(df):

    df["ema50"]=df.close.ewm(span=50).mean()
    df["ema200"]=df.close.ewm(span=200).mean()

    delta=df.close.diff()
    gain=delta.clip(lower=0).rolling(14).mean()
    loss=(-delta.clip(upper=0)).rolling(14).mean()

    rs=gain/loss
    df["rsi"]=100-(100/(1+rs))

    return df

# ---------------- EXTRA SMC ----------------

def market_structure(df):
    if df.high.iloc[-1] > df.high.iloc[-2] and df.low.iloc[-1] > df.low.iloc[-2]:
        return "bull"
    if df.high.iloc[-1] < df.high.iloc[-2] and df.low.iloc[-1] < df.low.iloc[-2]:
        return "bear"
    return None

def liquidity_sweep(df):
    last=df.iloc[-1]
    prev=df.iloc[-2]

    if last.high > prev.high and last.close < prev.high:
        return "sell"
    if last.low < prev.low and last.close > prev.low:
        return "buy"
    return None

def fvg(df):
    c1=df.iloc[-3]
    c3=df.iloc[-1]

    if c1.high < c3.low:
        return "bull"
    if c1.low > c3.high:
        return "bear"
    return None

# ---------------- MULTI TF ----------------

async def multi_tf(account,symbol):
    candles=await account.get_historical_candles(symbol,"1h",100)
    df=pd.DataFrame(candles)
    df["ema50"]=df.close.ewm(span=50).mean()
    df["ema200"]=df.close.ewm(span=200).mean()

    if df.ema50.iloc[-1] > df.ema200.iloc[-1]:
        return "bull"
    if df.ema50.iloc[-1] < df.ema200.iloc[-1]:
        return "bear"
    return None

# ---------------- SIGNAL ----------------

def signal(df, htf):

    last=df.iloc[-1]
    prev=df.iloc[-2]

    trend=market_structure(df)
    gap=fvg(df)

    # 🔥 STRONG (blijft)
    if htf=="bull" and trend=="bull" and gap=="bull":
        return "buy","SMC STRONG"

    if htf=="bear" and trend=="bear" and gap=="bear":
        return "sell","SMC STRONG"

    # 🔥 FAST BREAKOUT
    if last.close > prev.high:
        return "buy","FAST BREAKOUT"

    if last.close < prev.low:
        return "sell","FAST BREAKOUT"

    # 🔥 LOSSE RSI
    if last.rsi > 52:
        return "buy","RSI SCALP"

    if last.rsi < 48:
        return "sell","RSI SCALP"

    return None,None

# ---------------- LOT ----------------

def lot_size(balance,sl_distance):
    lot=(balance*RISK)/(sl_distance*10)
    return round(max(0.01,min(lot,5)),2)

# ---------------- DAILY ----------------

def check_daily(balance):

    today=date.today()

    if daily["date"]!=today:
        daily["date"]=today
        daily["start"]=balance

    loss=(daily["start"]-balance)/daily["start"]

    if loss >= DAILY_LOSS_LIMIT:
        tg("DAILY LOSS LIMIT HIT")
        return False

    return True

# ---------------- TRAILING ----------------

async def trailing(conn):
    try:
        pos=await conn.get_positions()
        for p in pos:
            entry=p["openPrice"]
            sl=p.get("stopLoss",0)

            if p["type"]=="POSITION_TYPE_BUY" and sl < entry:
                await conn.modify_position(p["id"],stop_loss=entry)
                tg(f"TRAILING ACTIVATED: {p['symbol']}")

            if p["type"]=="POSITION_TYPE_SELL" and (sl > entry or sl == 0):
                await conn.modify_position(p["id"],stop_loss=entry)
                tg(f"TRAILING ACTIVATED: {p['symbol']}")
    except:
        pass

# ---------------- MAIN ----------------

async def run():

    api=MetaApi(METAAPI_TOKEN)
    account=await api.metatrader_account_api.get_account(ACCOUNT_ID)

    await account.wait_connected()

    conn=account.get_rpc_connection()
    await conn.connect()
    await conn.wait_synchronized()

    tg("PRO BOT STARTED")

    while True:

        try:

            info=await conn.get_account_information()
            balance=info["balance"]
            equity=info["equity"]

            positions=await conn.get_positions()

            heartbeat(balance,equity,positions)

            if not session_filter():
                await asyncio.sleep(30)
                continue

            if not news_filter():
                await asyncio.sleep(60)
                continue

            if not check_daily(balance):
                await asyncio.sleep(60)
                continue

            await trailing(conn)

            for symbol in SYMBOLS:

                if correlation_block(symbol,positions):
                    continue

                if max_trades_filter(symbol,positions):
                    continue

                candles=await account.get_historical_candles(symbol,"5m",200)
                df=pd.DataFrame(candles)

                df=indicators(df)

                htf=await multi_tf(account,symbol)

                s,setup=signal(df,htf)

                if not s:
                    continue

                price=await conn.get_symbol_price(symbol)

                if s=="buy":
                    entry=price["ask"]
                    sl=df.low.tail(10).min()
                    risk=entry-sl
                    tp1=entry+risk
                    tp2=entry+risk*2

                else:
                    entry=price["bid"]
                    sl=df.high.tail(10).max()
                    risk=sl-entry
                    tp1=entry-risk
                    tp2=entry-risk*2

                rr=abs((tp2-entry)/risk)

                if rr < MIN_RR:
                    continue

                lot=lot_size(balance,abs(risk))

                if s=="buy":
                    await conn.create_market_buy_order(symbol,lot,sl,tp2)
                else:
                    await conn.create_market_sell_order(symbol,lot,sl,tp2)

                tg(f"""
TRADE OPEN

Symbol: {symbol}
Type: {s}
Setup: {setup}

Entry: {entry}
SL: {sl}
TP: {tp2}

RR: {round(rr,2)}
Balance: {round(balance,2)}
""")

            await asyncio.sleep(CHECK_INTERVAL)

        except Exception as e:
            tg(f"BOT ERROR: {str(e)}")
            await asyncio.sleep(5)

while True:
    try:
        asyncio.run(run())
    except Exception as e:
        tg(f"CRASH: {str(e)}")
        time.sleep(5)
