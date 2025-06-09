#!/usr/bin/env python3
"""
Ultra-fast, real-fill BTC/USDT futures bot with:
 • single-stream 5m WebSocket
 • piercing pattern entry
 • safe fill-price extraction
 • health endpoint for 24/7 hosting
 • detailed entry/skip logging at INFO
"""

import os, math, hmac, hashlib, logging, asyncio, threading, time
from datetime import datetime, timedelta
from urllib.parse import urlencode

import pandas as pd
import pytz
import aiohttp
import uvicorn
from binance import AsyncClient, BinanceSocketManager
from binance.enums import *
from binance.exceptions import BinanceAPIException

# ──────────────────────────────────────────────────────────────────────────────
#   CONFIG
# ──────────────────────────────────────────────────────────────────────────────

SYMBOL       = "BTCUSDT"
LEVERAGE     = 100
TRADE_MARGIN = 5.0        # USDT per trade
ATR_MULT     = 0.5
MAX_BARS     = 10
LOOKBACK_5   = 500

QTY_PREC     = 3          # quantity stepSize = 0.001
PRICE_PREC   = 1          # price tickSize    = 0.1

BAL_HOURS    = 3          # cache balance every 3h
TZ_UTC       = pytz.UTC

API_KEY      = os.getenv("BINANCE_API_KEY")
API_SECRET   = os.getenv("BINANCE_API_SECRET")
BASE_FAPI    = "https://fapi.binance.com"

if not API_KEY or not API_SECRET:
    raise RuntimeError("Set BINANCE_API_KEY and BINANCE_API_SECRET env vars")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ──────────────────────────────────────────────────────────────────────────────
#   STATE
# ──────────────────────────────────────────────────────────────────────────────

bot_state = {
    "in_position": False,
    "entry_price_theo": None,
    "entry_price_act":  None,
    "take_profit":      None,
    "atr_at_entry":     None,
    "bars_held":        0,
    "quantity":         None,
    "equity":           None,
    "entry_time":       None
}

df5 = pd.DataFrame(columns=["open","high","low","close","volume"],
                   index=pd.DatetimeIndex([], tz=TZ_UTC))

balance_cache = {"last": None, "value": None}
pivot_cache   = {"day":  None, "value": None}

_client = None
_bsm    = None

# ──────────────────────────────────────────────────────────────────────────────
#   UTILITIES
# ──────────────────────────────────────────────────────────────────────────────

def floor_qty(q):
    factor = 10 ** QTY_PREC
    return math.floor(q * factor) / factor

def floor_price(p):
    factor = 10 ** PRICE_PREC
    return math.floor(p * factor) / factor

# ──────────────────────────────────────────────────────────────────────────────
#   BALANCE CACHE
# ──────────────────────────────────────────────────────────────────────────────

async def get_cached_balance():
    now = datetime.now(TZ_UTC)
    if (balance_cache["last"] is None
        or (now - balance_cache["last"]) > timedelta(hours=BAL_HOURS)):
        try:
            ts = int(time.time() * 1000)
            qs = urlencode({"timestamp": ts})
            sig= hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
            url = f"{BASE_FAPI}/fapi/v2/balance?{qs}&signature={sig}"
            headers = {"X-MBX-APIKEY": API_KEY}
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url, headers=headers) as resp:
                    data = await resp.json()
            val = next((float(x["balance"]) for x in data if x["asset"]=="USDT"), 0.0)
            if val <= 0:
                val = 1000.0
            balance_cache["value"] = val
        except Exception as e:
            logging.error(f"get_cached_balance error: {e}")
            if balance_cache["value"] is None:
                balance_cache["value"] = 1000.0
        balance_cache["last"] = now
    return balance_cache["value"]

# ──────────────────────────────────────────────────────────────────────────────
#   PIVOT CACHE
# ──────────────────────────────────────────────────────────────────────────────

async def get_cached_pivot():
    now = datetime.now(TZ_UTC).replace(hour=0,minute=0,second=0,microsecond=0)
    yesterday = now - timedelta(days=1)
    if pivot_cache["day"] == yesterday:
        return pivot_cache["value"]
    try:
        raw = await _client.futures_klines(symbol=SYMBOL, interval="1d", limit=3)
        df1d= pd.DataFrame(raw, columns=[
            "open_time","open","high","low","close","volume",
            "close_time","qav","nTrades","tbav","tbqv","ignore"
        ])
        for c in ["open","high","low","close"]:
            df1d[c] = df1d[c].astype(float)
        df1d["open_time"]=pd.to_datetime(df1d["open_time"],unit="ms",utc=True)
        df1d.set_index("open_time",inplace=True)
        df1d.index=df1d.index.tz_convert(TZ_UTC).floor("D")
        row = df1d.loc[yesterday]
        pv  = (row["high"]+row["low"]+row["close"]) / 3
        pivot_cache.update(day=yesterday, value=pv)
        return pv
    except Exception as e:
        logging.error(f"get_cached_pivot error: {e}")
        return pivot_cache.get("value", 0.0)

# ──────────────────────────────────────────────────────────────────────────────
#   MARGIN CHECK
# ──────────────────────────────────────────────────────────────────────────────

async def can_open_position(qty, price):
    bal = await get_cached_balance()
    required = (qty * price) / LEVERAGE
    if required <= bal:
        return True
    logging.warning(f"Insufficient margin: need {required:.3f}, have {bal:.3f}")
    return False

# ──────────────────────────────────────────────────────────────────────────────
#   ORDER HELPERS WITH SAFE FILL EXTRACTION
# ──────────────────────────────────────────────────────────────────────────────

async def place_limit_ioc_buy(qty, price):
    q = floor_qty(qty)
    p = floor_price(price)
    if q <= 0: raise ValueError("Qty <= 0")
    try:
        return await _client.futures_create_order(
            symbol=SYMBOL, side=SIDE_BUY, type=ORDER_TYPE_LIMIT,
            timeInForce=TIME_IN_FORCE_IOC, quantity=q,
            price=f"{p:.{PRICE_PREC}f}"
        )
    except BinanceAPIException:
        return await _client.futures_create_order(
            symbol=SYMBOL, side=SIDE_BUY,
            type=ORDER_TYPE_MARKET, quantity=q
        )

async def place_limit_ioc_sell(qty, price):
    q = floor_qty(qty)
    p = floor_price(price)
    if q <= 0: raise ValueError("Qty <= 0")
    try:
        return await _client.futures_create_order(
            symbol=SYMBOL, side=SIDE_SELL, type=ORDER_TYPE_LIMIT,
            timeInForce=TIME_IN_FORCE_IOC, quantity=q,
            price=f"{p:.{PRICE_PREC}f}"
        )
    except BinanceAPIException:
        return await _client.futures_create_order(
            symbol=SYMBOL, side=SIDE_SELL,
            type=ORDER_TYPE_MARKET, quantity=q
        )

async def get_orderbook_edge(side):
    try:
        ob = await _client.futures_order_book(symbol=SYMBOL, limit=5)
        return float(ob["bids"][0][0]) if side=="bid" else float(ob["asks"][0][0])
    except:
        return float(df5["close"].iloc[-1])

# ──────────────────────────────────────────────────────────────────────────────
#   INDICATOR COMPUTATION (including Piercing Pattern)
# ──────────────────────────────────────────────────────────────────────────────

def compute_all_indicators(df5_loc, df15_loc, pivot_val):
    df = df5_loc.sort_index().copy()

    # EMAs
    df["ema9"]  = df["close"].ewm(span=9, adjust=False).mean()
    df["ema21"] = df["close"].ewm(span=21,adjust=False).mean()
    df["ema50"] = df["close"].ewm(span=50,adjust=False).mean()

    # RSI14
    delta = df["close"].diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)
    ag = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    al = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    rs = ag / al
    df["rsi14"] = 100 - (100/(1+rs))

    # ATR14
    prev = df["close"].shift(1)
    tr   = pd.concat([
        df["high"]-df["low"],
        (df["high"]-prev).abs(),
        (df["low"]-prev).abs()
    ],axis=1).max(axis=1)
    df["atr14"] = tr.ewm(alpha=1/14, min_periods=14, adjust=False).mean()

    # ADX14
    up   = df["high"]-df["high"].shift(1)
    dn   = df["low"].shift(1)-df["low"]
    pdm  = up.where((up>dn)&(up>0),0).ewm(alpha=1/14,adjust=False).mean()
    ndm  = dn.where((dn>up)&(dn>0),0).ewm(alpha=1/14,adjust=False).mean()
    tr14 = tr.ewm(alpha=1/14,adjust=False).mean()
    df["plus_di"]  = 100*(pdm/tr14)
    df["minus_di"] = 100*(ndm/tr14)
    dx             = (df["plus_di"]-df["minus_di"]).abs()/(df["plus_di"]+df["minus_di"])*100
    df["adx14"]    = dx.ewm(alpha=1/14,adjust=False).mean()

    # 15m EMA50 mapping
    df15_loc["ema50_15"] = df15_loc["close"].ewm(span=50,adjust=False).mean()
    df["ema50_15"]       = df15_loc["ema50_15"].reindex(df.index,method="ffill")

    # VWAP
    df["typ_price"] = (df["high"]+df["low"]+df["close"])/3
    df["date"]      = df.index.floor("D")
    df["cum_vp"]    = (df["typ_price"]*df["volume"]).groupby(df["date"]).cumsum()
    df["cum_vol"]   = df["volume"].groupby(df["date"]).cumsum()
    df["vwap"]      = df["cum_vp"]/df["cum_vol"]

    # Piercing Pattern
    df["o_prev"] = df["open"].shift(1)
    df["c_prev"] = df["close"].shift(1)
    bearish_prev = df["c_prev"] < df["o_prev"]
    midpoint     = df["c_prev"] + 0.5*(df["o_prev"] - df["c_prev"])
    df["pierce"] = bearish_prev & (df["open"] < df["c_prev"]) & (df["close"] >= midpoint)

    return df.dropna(subset=[
        "ema9","ema21","ema50","rsi14","adx14","atr14","ema50_15","vwap","pierce"
    ])

# ──────────────────────────────────────────────────────────────────────────────
#   RUN STRATEGY
# ──────────────────────────────────────────────────────────────────────────────

async def run_strategy():
    global df5, bot_state

    if len(df5) < LOOKBACK_5:
        return

    pivot = await get_cached_pivot()
    df15  = pd.DataFrame({
        "open":   df5["open"].resample("15min").first(),
        "high":   df5["high"].resample("15min").max(),
        "low":    df5["low"].resample("15min").min(),
        "close":  df5["close"].resample("15min").last(),
        "volume": df5["volume"].resample("15min").sum()
    }).dropna()

    df_ind = compute_all_indicators(df5, df15, pivot)
    if df_ind.empty:
        return

    row = df_ind.iloc[-1]
    price      = row["close"]
    high_price = row["high"]
    atr        = row["atr14"]

    # ── EXIT LOGIC ─────────────────────────────────────────────────────────────
    if bot_state["in_position"]:
        bot_state["bars_held"] += 1
        ep_th = bot_state["entry_price_theo"]
        ep_ac = bot_state["entry_price_act"]
        tp    = bot_state["take_profit"]
        qty   = bot_state["quantity"]

        # take-profit
        if high_price >= tp:
            best_ask = await get_orderbook_edge("ask")
            sell_pr  = min(tp, best_ask)
            resp      = await place_limit_ioc_sell(qty, sell_pr)
            if "fills" in resp and resp["fills"]:
                fill = float(resp["fills"][0]["price"])
            elif "avgPrice" in resp:
                fill = float(resp["avgPrice"])
            else:
                fill = ep_th

            logging.info(f"Exit SELL {qty:.3f} @ {fill:.1f} (TP={tp:.1f})")
            R   = (fill - ep_ac)/ep_ac
            pnl = bot_state["equity"]*(LEVERAGE*R)
            bot_state["equity"] += pnl
            logging.info(f"P/L +{pnl:.2f} → Equity={bot_state['equity']:.2f}")
            for k in bot_state: bot_state[k] = None
            return

        # breakeven after MAX_BARS
        if bot_state["bars_held"] >= MAX_BARS:
            resp = await place_limit_ioc_sell(qty, ep_th)
            if "fills" in resp and resp["fills"]:
                fill = float(resp["fills"][0]["price"])
            elif "avgPrice" in resp:
                fill = float(resp["avgPrice"])
            else:
                fill = ep_th
            logging.info(f"Breakeven SELL {qty:.3f} @ {fill:.1f}")
            logging.info(f"P/L +0.00 → Equity={bot_state['equity']:.2f}")
            for k in bot_state: bot_state[k] = None
            return

        return

    # ── ENTRY LOGIC ────────────────────────────────────────────────────────────
    flags = {
        "EMA_stack"   : row["ema9"]>row["ema21"]>row["ema50"],
        "ADX_RSI"     : (row["adx14"]>20) and (row["rsi14"]>55),
        "15min_trend" : price>row["ema50_15"],
        "VWAP"        : price>row["vwap"],
        "Pivot"       : price>pivot,
        "Pierce"      : bool(row["pierce"])
    }

    if all(flags.values()) and atr>0:
        ep_th = price
        tp    = ep_th + ATR_MULT*atr
        raw_q = (TRADE_MARGIN*LEVERAGE)/ep_th
        qty   = floor_qty(raw_q)
        if qty>0 and await can_open_position(qty, ep_th):
            best_bid = await get_orderbook_edge("bid")
            resp     = await place_limit_ioc_buy(qty, best_bid)
            if "fills" in resp and resp["fills"]:
                ep_ac = float(resp["fills"][0]["price"])
            elif "avgPrice" in resp:
                ep_ac = float(resp["avgPrice"])
            else:
                ep_ac = ep_th

            logging.info(f"ENTRY BUY {qty:.3f} @ {ep_ac:.1f} (theo={ep_th:.1f})")
            bot_state.update({
                "in_position":       True,
                "entry_price_theo":  ep_th,
                "entry_price_act":   ep_ac,
                "take_profit":       tp,
                "atr_at_entry":      atr,
                "bars_held":         0,
                "quantity":          qty,
                "equity":            bot_state["equity"],
                "entry_time":        datetime.now(TZ_UTC)
            })
            return

    failed = [k for k,v in flags.items() if not v]
    logging.info(
        "Entry conditions not met: "
        + ", ".join(f"{k}={v}" for k,v in flags.items())
        + f"; failed → {failed}"
    )

# ──────────────────────────────────────────────────────────────────────────────
#   5m WebSocket STREAM
# ──────────────────────────────────────────────────────────────────────────────

async def handle_5m_stream():
    global df5
    logging.info("→ Entering 5m-stream handler")

    raw = await _client.futures_klines(symbol=SYMBOL, interval="5m", limit=LOOKBACK_5)
    tmp = pd.DataFrame(raw, columns=[
        "open_time","open","high","low","close","volume",
        "close_time","qav","nTrades","tbav","tbqv","ignore"
    ])
    for c in ["open","high","low","close","volume"]:
        tmp[c] = tmp[c].astype(float)
    tmp["open_time"] = pd.to_datetime(tmp["open_time"],unit="ms",utc=True)
    tmp.set_index("open_time",inplace=True)
    tmp.index = tmp.index.tz_convert(TZ_UTC)
    df5 = tmp[["open","high","low","close","volume"]].copy()
    logging.info("→ Seeded df5 with REST data, now opening WS")

    ws = _bsm.kline_socket(SYMBOL, "5m")
    try:
        async with ws as stream:
            logging.info("→ WebSocket context entered, now listening")
            while True:
                msg = await stream.recv()
                evt = msg.get("data") if isinstance(msg.get("data"), dict) else msg
                k   = evt.get("k", {})
                if not k.get("x", False):
                    continue

                t0 = pd.to_datetime(k["t"],unit="ms",utc=True).tz_convert(TZ_UTC)
                df5.loc[t0] = [
                    float(k["o"]), float(k["h"]),
                    float(k["l"]), float(k["c"]),
                    float(k["v"])
                ]
                if len(df5) > LOOKBACK_5:
                    df5 = df5.iloc[-LOOKBACK_5:]
                await run_strategy()

    except AttributeError as e:
        if "fail_connection" in str(e):
            logging.debug("Ignored missing fail_connection on socket close")
        else:
            raise

# ──────────────────────────────────────────────────────────────────────────────
#   HEALTH ENDPOINT
# ──────────────────────────────────────────────────────────────────────────────

async def health_app(scope, receive, send):
    if (scope["type"]=="http"
        and scope["method"] in ("GET","HEAD")
        and scope["path"] in ("/","/health")):
        await send({"type":"http.response.start","status":200,
                    "headers":[(b"content-type",b"application/json")]})
        await send({"type":"http.response.body","body":b'{"status":"ok"}'})
    else:
        await send({"type":"http.response.start","status":404,
                    "headers":[(b"content-type",b"text/plain")]})
        await send({"type":"http.response.body","body":b"Not Found"})

# ──────────────────────────────────────────────────────────────────────────────
#   MAIN
# ──────────────────────────────────────────────────────────────────────────────

async def main():
    global _client, _bsm

    _client = await AsyncClient.create(API_KEY, API_SECRET, {"timeout":20})
    _bsm    = BinanceSocketManager(_client)

    bal0 = await get_cached_balance()
    bot_state["equity"] = bal0
    logging.info(f"Starting equity: {bal0:.2f} USDT")

    try:
        await _client.futures_change_leverage(symbol=SYMBOL, leverage=LEVERAGE)
        logging.info(f"Leverage set to {LEVERAGE}×")
    except Exception as e:
        logging.error(f"Leverage setup error: {e}")

    port = int(os.getenv("PORT","8000"))
    threading.Thread(
        target=lambda: uvicorn.run(health_app, host="0.0.0.0", port=port, log_level="error"),
        daemon=True
    ).start()
    logging.info(f"Health endpoint running on port {port}")

    await handle_5m_stream()
    await _client.close_connection()

if __name__ == "__main__":
    asyncio.run(main())
