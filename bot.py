import asyncio
import pandas as pd
import os
import time
import json
import urllib.request
import logging
from datetime import datetime, date
from metaapi_cloud_sdk import MetaApi

CHECK_INTERVAL = 15
RISK = 0.01
MIN_RR = 2
DAILY_LOSS_LIMIT = 0.03
MAX_TRADES_PER_ASSET = 4

SYMBOLS = [
"XAUUSD","XAGUSD","BTCUSD",
"EURUSD","GBPUSD","USDJPY","USDCHF",
"NAS100","US30","US500"
]

METAAPI_TOKEN = os.getenv("METAAPI_TOKEN")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT = os.getenv("TG_CHAT")

trade_cooldown = {}
daily = {"date":None,"start":0}

logging.basicConfig(level=logging.INFO)

# ---------------- TELEGRAM ----------------

def tg(msg):
    try:
        url=f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        data=json.dumps({"chat_id":TG_CHAT,"text":msg}).encode()
        req=urllib.request.Request(url,data=data,headers={"Content-Type":"application/json"})
        urllib.request.urlopen(req)
    except:
        pass


# ---------------- HEARTBEAT ----------------

last_status=0

def heartbeat(balance,equity,positions):

    global last_status

    now=time.time()

    if now-last_status < 600:
        return

    last_status=now

    tg(f"""
BOT STATUS

Balance: {round(balance,2)}
Equity: {round(equity,2)}

Open trades: {len(positions)}

Scanning assets: {len(SYMBOLS)}

Time: {datetime.utcnow().strftime('%H:%M:%S')} UTC
""")


# ---------------- SESSION FILTER ----------------

def session_filter():

    h=datetime.utcnow().hour

    london = 7 <= h <= 11
    newyork = 13 <= h <= 17

    return london or newyork


# ---------------- NEWS FILTER ----------------

def news_filter():
    return True

    try:

        now=datetime.utcnow()

        url="https://nfs.faireconomy.media/ff_calendar_thisweek.json"

        req=urllib.request.Request(url,headers={"User-Agent":"Mozilla"})
        res=urllib.request.urlopen(req)

        events=json.loads(res.read().decode())

        for e in events:

            if e["impact"]!="High":
                continue

            if isinstance(e["date"], int):
                t=datetime.fromtimestamp(e["date"])
            else:
                t=datetime.strptime(e["date"],"%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)

            diff=abs((now-t).total_seconds())

            if diff < 1800:
                tg("NEWS FILTER ACTIVE")
                return False

        return True

    except:
        return True


# ---------------- SPREAD FILTER ----------------

def spread_ok(symbol,spread):

    if symbol in ["EURUSD","GBPUSD","USDJPY","USDCHF"]:
        return spread < 0.0003

    if symbol in ["XAUUSD","XAGUSD"]:
        return spread < 0.5

    if symbol == "BTCUSD":
        return spread < 50

    return True


# ---------------- INDICATORS ----------------

def indicators(df):

    df["ema12"]=df.close.ewm(span=12).mean()
    df["ema26"]=df.close.ewm(span=26).mean()

    df["macd"]=df.ema12-df.ema26
    df["signal"]=df.macd.ewm(span=9).mean()

    delta=df.close.diff()

    gain=delta.clip(lower=0).rolling(14).mean()
    loss=(-delta.clip(upper=0)).rolling(14).mean()

    rs=gain/loss
    df["rsi"]=100-(100/(1+rs))

    tr=df.high-df.low
    df["atr"]=tr.rolling(14).mean()

    return df


# ---------------- MARKET STRUCTURE ----------------

def market_structure(df):

    h=df.high
    l=df.low

    if h.iloc[-1] > h.iloc[-2] and l.iloc[-1] > l.iloc[-2]:
        return "bull"

    if h.iloc[-1] < h.iloc[-2] and l.iloc[-1] < l.iloc[-2]:
        return "bear"

    return None


# ---------------- FVG ----------------

def fair_value_gap(df):

    c1=df.iloc[-3]
    c3=df.iloc[-1]

    if c1.high < c3.low:
        return "bull"

    if c1.low > c3.high:
        return "bear"

    return None


# ---------------- LIQUIDITY SWEEP ----------------

def liquidity_sweep(df):

    last=df.iloc[-1]
    prev=df.iloc[-2]

    if last.high > prev.high and last.close < prev.high:
        return "sell"

    if last.low < prev.low and last.close > prev.low:
        return "buy"

    return None


# ---------------- LIQUIDITY POOL ----------------

def liquidity_pool(df):

    highs=df.high.tail(20)
    lows=df.low.tail(20)

    if abs(highs.max()-highs.iloc[-1]) < 0.0001:
        return "sell_liquidity"

    if abs(lows.min()-lows.iloc[-1]) < 0.0001:
        return "buy_liquidity"

    return None


# ---------------- ORDER BLOCK ----------------

def order_block(df):

    last=df.iloc[-2]

    body=abs(last.close-last.open)

    range_candle=last.high-last.low

    if body/range_candle > 0.6:

        if last.close > last.open:
            return "bull"

        else:
            return "bear"

    return None


# ---------------- CORRELATION FILTER ----------------

def correlation_filter(symbol,positions):

    usd_pairs=["EURUSD","GBPUSD","USDJPY","USDCHF"]

    if symbol not in usd_pairs:
        return True

    count=0

    for p in positions:
        if p["symbol"] in usd_pairs:
            count+=1

    if count >=2:
        return False

    return True


# ---------------- HTF TREND ----------------

def htf_trend(df):

    ema50=df.close.ewm(span=50).mean()
    ema200=df.close.ewm(span=200).mean()

    if ema50.iloc[-1] > ema200.iloc[-1]:
        return "bull"

    if ema50.iloc[-1] < ema200.iloc[-1]:
        return "bear"

    return None


# ---------------- SIGNAL ----------------

def signal(df):

    trend=market_structure(df)
    fvg=fair_value_gap(df)
    sweep=liquidity_sweep(df)
    htf=htf_trend(df)

    r=df.iloc[-1]

    if trend=="bull" and fvg=="bull" and sweep=="buy" and r.rsi>50 and htf=="bull":
        return "buy"

    if trend=="bear" and fvg=="bear" and sweep=="sell" and r.rsi<50 and htf=="bear":
        return "sell"

    return None


# ---------------- LOT SIZE ----------------

def lot_size(balance,sl_distance):

    risk_money=balance * RISK

    lot=risk_money/(sl_distance*10)

    lot=max(0.01,min(lot,5))

    return round(lot,2)


# ---------------- DAILY LOSS ----------------

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

            if p["type"]=="POSITION_TYPE_BUY":

                if sl < entry:
                    await conn.modify_position(p["id"],stop_loss=entry)

            else:

                if sl > entry or sl == 0:
                    await conn.modify_position(p["id"],stop_loss=entry)

    except:
        pass


# ---------------- MAIN BOT ----------------

async def run():

    api=MetaApi(METAAPI_TOKEN)

    account=await api.metatrader_account_api.get_account(ACCOUNT_ID)

    await account.wait_connected()

    conn=account.get_rpc_connection()

    await conn.connect()
    await conn.wait_synchronized()

    tg("ELITE BOT STARTED")

    while True:

        try:

            if not session_filter():
                await asyncio.sleep(60)
                continue

            if not news_filter():
                await asyncio.sleep(60)
                continue

            info=await conn.get_account_information()

            balance=info["balance"]
            equity=info["equity"]

            if not check_daily(balance):
                await asyncio.sleep(60)
                continue

            positions=await conn.get_positions()

            heartbeat(balance,equity,positions)

            await trailing(conn)

            for symbol in SYMBOLS:

                if not correlation_filter(symbol,positions):
                    continue

                candles=await account.get_historical_candles(symbol,"5m",200)

                df=pd.DataFrame(candles)

                df=indicators(df)

                s=signal(df)

                if not s:
                    continue

                liq=liquidity_pool(df)
                ob=order_block(df)

                if s=="buy" and liq!="buy_liquidity":
                    continue

                if s=="sell" and liq!="sell_liquidity":
                    continue

                if s=="buy" and ob!="bull":
                    continue

                if s=="sell" and ob!="bear":
                    continue

                price=await conn.get_symbol_price(symbol)

                spread=price["ask"]-price["bid"]

                if not spread_ok(symbol,spread):
                    continue

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

                if rr<MIN_RR:
                    continue

                lot=lot_size(balance,abs(risk))

                lot1=round(lot*0.5,2)
                lot2=round(lot*0.5,2)

                if s=="buy":

                    await conn.create_market_buy_order(symbol,lot1,sl,tp1)
                    await conn.create_market_buy_order(symbol,lot2,sl,tp2)

                else:

                    await conn.create_market_sell_order(symbol,lot1,sl,tp1)
                    await conn.create_market_sell_order(symbol,lot2,sl,tp2)

                tg(f"""
TRADE OPEN

Symbol: {symbol}
Type: {s}

Entry: {entry}
SL: {sl}
TP1: {tp1}
TP2: {tp2}

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

        print("BOT CRASHED",e)

        time.sleep(5)
