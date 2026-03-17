import asyncio
import os
import pandas as pd
import logging
import urllib.request
import json
from datetime import datetime, date
from metaapi_cloud_sdk import MetaApi

# ─── ENV VARIABLES ─────────────────────────

METAAPI_TOKEN = os.environ.get("METAAPI_TOKEN")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID")

TG_TOKEN = os.environ.get("TG_TOKEN")
TG_CHAT = os.environ.get("TG_CHAT")

# ─── SETTINGS ──────────────────────────────

CHECK_EVERY = 60
RISK_PCT = 0.01
MIN_RR = 2
MAX_TRADES = 4
MAX_DAILY_LOSS = 0.03

# ─── LOGGING ───────────────────────────────

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("MZ_ELITE")

# ─── TELEGRAM ──────────────────────────────

def tg(msg):
    try:
        url=f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        data=json.dumps({"chat_id":TG_CHAT,"text":msg}).encode()
        req=urllib.request.Request(url,data=data,headers={"Content-Type":"application/json"})
        urllib.request.urlopen(req)
    except:
        pass

# ─── ASSETS ────────────────────────────────

ASSETS={
"XAUUSD":{"pip":0.10,"decimals":2},
"EURUSD":{"pip":0.0001,"decimals":5},
"GBPUSD":{"pip":0.0001,"decimals":5},
"USDJPY":{"pip":0.01,"decimals":3},
"GBPJPY":{"pip":0.01,"decimals":3},
"BTCUSD":{"pip":1.0,"decimals":2}
}

# ─── SESSION FILTER ────────────────────────

def session():
    h=datetime.utcnow().hour
    if 0<=h<7:return "asia"
    if 7<=h<13:return "london"
    if 13<=h<16:return "london_ny"
    if 16<=h<21:return "ny"
    return "closed"

# ─── NEWS FILTER ───────────────────────────

def news_filter():

    try:
        now=datetime.utcnow()

        url="https://nfs.faireconomy.media/ff_calendar_thisweek.json"

        req=urllib.request.Request(url,headers={"User-Agent":"Mozilla"})
        res=urllib.request.urlopen(req,timeout=5)

        events=json.loads(res.read().decode())

        for e in events:

            if e.get("impact")!="High":
                continue

            try:
                et=datetime.strptime(e["date"],"%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
            except:
                continue

            diff=abs((now-et).total_seconds())

            if diff<1800:
                tg(f"News filter actief\n{e.get('title')}")
                return False

        return True

    except:
        return True

# ─── INDICATORS ────────────────────────────

def indicators(c):

    df=pd.DataFrame(c)

    df["ema20"]=df["close"].ewm(span=20).mean()

    delta=df["close"].diff()

    gain=delta.clip(lower=0).rolling(14).mean()
    loss=(-delta.clip(upper=0)).rolling(14).mean()

    rs=gain/loss

    df["rsi"]=100-(100/(1+rs))

    e1=df["close"].ewm(span=12).mean()
    e2=df["close"].ewm(span=26).mean()

    df["macd"]=e1-e2
    df["signal"]=df["macd"].ewm(span=9).mean()

    df["hist"]=df["macd"]-df["signal"]

    df["swing_high"]=df["high"].rolling(5).max()
    df["swing_low"]=df["low"].rolling(5).min()

    return df

# ─── TREND ─────────────────────────────────

def trend(df):

    if df["close"].iloc[-1] > df["ema20"].iloc[-1]:
        return "bull"

    if df["close"].iloc[-1] < df["ema20"].iloc[-1]:
        return "bear"

    return None

# ─── LOT SIZE ──────────────────────────────

def calc_lot(balance, stop_distance, pip):

    risk_money = balance * RISK_PCT

    pips = stop_distance / pip

    if pips <= 0:
        return 0.01

    lot = risk_money / (pips * 10)

    return round(max(0.01,min(lot,5)),2)

# ─── SIGNAL ────────────────────────────────

def signal(df5, trend):

    r=df5.iloc[-2]
    r1=df5.iloc[-3]

    macd_up=r["hist"]>0 and r1["hist"]<=0
    macd_down=r["hist"]<0 and r1["hist"]>=0

    if trend=="bull":

        if macd_up and r["rsi"]>40:
            return "buy"

    if trend=="bear":

        if macd_down and r["rsi"]<60:
            return "sell"

    return None

# ─── MAIN BOT ──────────────────────────────

async def run():

    api=MetaApi(METAAPI_TOKEN)

    account=await api.metatrader_account_api.get_account(ACCOUNT_ID)

    await account.wait_connected()

    conn=account.get_rpc_connection()

    await conn.connect()
    await conn.wait_synchronized()

    terminal=account.get_terminal_connection()

    await terminal.connect()
    await terminal.wait_synchronized()

    tg("MZ ELITE BOT STARTED")

    daily_start=None

    while True:

        try:

            if session()=="closed":
                await asyncio.sleep(CHECK_EVERY)
                continue

            if not news_filter():
                await asyncio.sleep(CHECK_EVERY)
                continue

            info=await conn.get_account_information()

            balance=info["balance"]

            if daily_start is None:
                daily_start=balance

            loss=(daily_start-balance)/daily_start

            if loss>=MAX_DAILY_LOSS:
                tg("Daily loss limit bereikt")
                await asyncio.sleep(600)
                continue

            positions=await conn.get_positions()

            if len(positions)>=MAX_TRADES:
                await asyncio.sleep(CHECK_EVERY)
                continue

            for symbol in ASSETS:

                price=await conn.get_symbol_price(symbol)

                c5=await terminal.get_historical_candles(symbol,"5m",None,120)

                df5=indicators(c5)

                t=trend(df5)

                s=signal(df5,t)

                if not s:
                    continue

                pip=ASSETS[symbol]["pip"]
                dec=ASSETS[symbol]["decimals"]

                r=df5.iloc[-2]

                if s=="buy":

                    entry=price["ask"]

                    sl=round(r["swing_low"]-pip,dec)

                    risk=entry-sl

                    tp1=round(entry+risk,dec)
                    tp2=round(entry+risk*2,dec)

                else:

                    entry=price["bid"]

                    sl=round(r["swing_high"]+pip,dec)

                    risk=sl-entry

                    tp1=round(entry-risk,dec)
                    tp2=round(entry-risk*2,dec)

                rr=abs((tp2-entry)/risk)

                if rr<MIN_RR:
                    continue

                lot=calc_lot(balance,abs(risk),pip)

                lot1=round(lot*0.5,2)
                lot2=round(lot*0.5,2)

                if s=="buy":

                    await conn.create_market_buy_order(symbol,lot1,sl,tp1)
                    await conn.create_market_buy_order(symbol,lot2,sl,tp2)

                else:

                    await conn.create_market_sell_order(symbol,lot1,sl,tp1)
                    await conn.create_market_sell_order(symbol,lot2,sl,tp2)

                tg(f"""
{s.upper()} {symbol}

RR 1:{round(rr,1)}

Entry {entry}
SL {sl}

TP1 {tp1}
TP2 {tp2}

Balance {round(balance,2)}
""")

        except Exception as e:

            log.error(e)

        await asyncio.sleep(CHECK_EVERY)

import asyncio
import time

while True:
    try:
        asyncio.run(run())
    except Exception as e:
        print("BOT ERROR:", e)
        time.sleep(5)
