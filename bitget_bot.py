"""
bitget_bot.py

A live trading bot for BTCUSDT Perpetual Futures on Bitget.
- Fetches initial 5m, 15m, and 1d candle history via REST to seed data.
- Receives live 5-minute candles via WebSocket.
- Resamples 5-minute candles to 15-minute and computes EMA50.
- Caches 1-day candles (refreshed hourly) for daily pivot.
- Implements the “zero-loss, 0.5×ATR” strategy with multi-timeframe confirmation.
- Prints debug notifications when entry conditions are checked and skipped.
- Places orders via CCXT (Bitget futures).
- Shows available USDT balance at startup.
- Saves state to disk for persistence.
- Exposes a dummy HTTP port (8000) for health‐checks.
"""

import os
import time
import json
import math
import asyncio
import threading
from datetime import datetime, timedelta, timezone

import pandas as pd
import numpy as np
import ccxt
import websocket  # pip install websocket-client

from http.server import HTTPServer, BaseHTTPRequestHandler

# ─── 1) CONFIGURATION ──────────────────────────────────────────────────────────

API_KEY     = os.getenv("BITGET_API_KEY")
API_SECRET  = os.getenv("BITGET_API_SECRET")
PASSPHRASE  = os.getenv("BITGET_PASSPHRASE")  # Bitget requires passphrase

if not all([API_KEY, API_SECRET, PASSPHRASE]):
    raise ValueError("Please set BITGET_API_KEY, BITGET_API_SECRET, and BITGET_PASSPHRASE")

WS_SYMBOL    = "BTCUSDT_UMCBL"  # for WebSocket subscription (Bitget format)
CCXT_SYMBOL  = "BTCUSDT"        # for CCXT REST calls (futures)

TIMEFRAME_5M    = "5m"
TIMEFRAME_15M   = "15m"
TIMEFRAME_1D    = "1d"

STATE_FILE      = "bot_state.json"
DATA_DIR        = "data_cache"

TARGET_LEVERAGE = 100
START_EQUITY    = 1000.0
MAX_BARS_HELD   = 10
ATR_MULTIPLIER  = 0.5
REFRESH_1D_HRS  = 1

WS_URL = "wss://ws.bitgetapi.com/mix/v1/stream"
HEALTH_PORT = 8000


# ─── 2) GLOBAL STATE ────────────────────────────────────────────────────────────

if os.path.exists(STATE_FILE):
    with open(STATE_FILE, "r") as f:
        bot_state = json.load(f)
else:
    bot_state = {
        "equity": START_EQUITY,
        "in_position": False,
        "entry_price": None,
        "take_profit": None,
        "atr_at_entry": None,
        "quantity": None,
        "bars_held": 0,
        "entry_time": None
    }

df5m  = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
df15m = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "ema50_15"])
df1d  = pd.DataFrame()

data_lock = threading.Lock()


# ─── 3) HTTP HEALTH‐CHECK SERVER ────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

def start_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    print(f"Health-check server listening on port {HEALTH_PORT}")
    server.serve_forever()


# ─── 4) UTILITY FUNCTIONS ───────────────────────────────────────────────────────

def save_state():
    with open(STATE_FILE, "w") as f:
        json.dump(bot_state, f, indent=2)

def get_ccxt_exchange():
    exchange = ccxt.bitget({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "password": PASSPHRASE,
        "enableRateLimit": True,
        "options": {
            "defaultType": "future",
            "defaultSubType": "UMCBL"
        }
    })
    return exchange

def fetch_initial_history():
    global df5m, df15m
    exchange = get_ccxt_exchange()

    ohlcv5 = exchange.fetch_ohlcv(CCXT_SYMBOL, timeframe=TIMEFRAME_5M, limit=100)
    df5 = pd.DataFrame(ohlcv5, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df5["timestamp"] = pd.to_datetime(df5["timestamp"], unit="ms", utc=True)
    df5m = df5[["timestamp", "open", "high", "low", "close", "volume"]]

    ohlcv15 = exchange.fetch_ohlcv(CCXT_SYMBOL, timeframe=TIMEFRAME_15M, limit=50)
    df15 = pd.DataFrame(ohlcv15, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df15["timestamp"] = pd.to_datetime(df15["timestamp"], unit="ms", utc=True)
    df15["ema50_15"] = df15["close"].ewm(span=50, adjust=False).mean()
    df15m = df15[["timestamp", "open", "high", "low", "close", "volume", "ema50_15"]]

    print("Fetched initial 5m and 15m history")

def fetch_daily_cache():
    global df1d
    exchange = get_ccxt_exchange()
    since = exchange.parse8601((datetime.utcnow() - timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%SZ"))
    ohlcv = exchange.fetch_ohlcv(CCXT_SYMBOL, timeframe=TIMEFRAME_1D, since=since, limit=365)
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df.set_index("timestamp", inplace=True)
    df["pivot"] = (df["high"].shift(1) + df["low"].shift(1) + df["close"].shift(1)) / 3
    df1d = df[["open", "high", "low", "close", "volume", "pivot"]]
    os.makedirs(DATA_DIR, exist_ok=True)
    df1d.to_csv(f"{DATA_DIR}/BTCUSDT_1d_cache.csv")
    print("Refreshed 1d cache")

def resample_15m_from_5m():
    global df5m, df15m
    with data_lock:
        if df5m.empty:
            return
        df5 = df5m.set_index("timestamp").copy()
        df15 = df5.resample("15T").agg({
            "open":  "first",
            "high":  "max",
            "low":   "min",
            "close": "last",
            "volume":"sum"
        }).dropna().reset_index()
        df15["ema50_15"] = df15["close"].ewm(span=50, adjust=False).mean()
        df15m = df15[["timestamp", "open", "high", "low", "close", "volume", "ema50_15"]]

def calculate_indicators():
    global df5m, df15m, df1d
    with data_lock:
        df5 = df5m.copy()
        df15 = df15m.copy()
        df1 = df1d.copy()

    if len(df5) < 50 or len(df15) < 50 or len(df1) < 2:
        return None

    df5["ema9"]  = df5["close"].ewm(span=9, adjust=False).mean()
    df5["ema21"] = df5["close"].ewm(span=21, adjust=False).mean()
    df5["ema50"] = df5["close"].ewm(span=50, adjust=False).mean()

    delta = df5["close"].diff()
    gain  = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    loss  = (-delta).clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    rs    = gain / loss
    df5["rsi"]  = 100 - (100 / (1 + rs))

    high = df5["high"]
    low  = df5["low"]
    prev_close = df5["close"].shift(1)
    tr   = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    tr14 = tr.ewm(alpha=1/14, adjust=False).mean()
    up_move   = high.diff()
    down_move = low.shift(1) - low
    plus_dm   = ((up_move > down_move) & (up_move > 0)) * up_move
    minus_dm  = ((down_move > up_move) & (down_move > 0)) * down_move
    plus_dm14 = plus_dm.ewm(alpha=1/14, adjust=False).mean()
    minus_dm14= minus_dm.ewm(alpha=1/14, adjust=False).mean()
    plus_di   = 100 * (plus_dm14 / tr14)
    minus_di  = 100 * (minus_dm14 / tr14)
    dx        = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di))
    df5["adx"] = dx.ewm(alpha=1/14, adjust=False).mean()

    df5["atr"] = tr.ewm(alpha=1/14, adjust=False).mean()

    df5["date"] = df5["timestamp"].dt.floor("D")
    vwap_vals = []
    for d, grp in df5.groupby("date"):
        tp = (grp["high"] + grp["low"] + grp["close"]) / 3
        cum_vp  = (grp["volume"] * tp).cumsum()
        cum_vol = grp["volume"].cumsum()
        vwap_vals += list((cum_vp / cum_vol).fillna(method="ffill"))
    df5["vwap"] = vwap_vals

    op_prev   = df5["open"].shift(1)
    cl_prev   = df5["close"].shift(1)
    df5["bull_engulf"] = (op_prev > cl_prev) & (df5["open"] < cl_prev) & (df5["close"] > op_prev)

    latest = df5.iloc[-1]
    ts_latest = latest["timestamp"]

    df15_idx = df15[df15["timestamp"] <= ts_latest]
    if df15_idx.empty:
        return None
    ema50_15_val = df15_idx.iloc[-1]["ema50_15"]

    pivot_val = df1.loc[df1.index.date == ts_latest.date(), "pivot"].values
    pivot_val = float(pivot_val[0]) if len(pivot_val) > 0 else np.nan

    return {
        "timestamp":    ts_latest,
        "close":        float(latest["close"]),
        "high":         float(latest["high"]),
        "ema9":         float(latest["ema9"]),
        "ema21":        float(latest["ema21"]),
        "ema50":        float(latest["ema50"]),
        "adx":          float(latest["adx"]),
        "rsi":          float(latest["rsi"]),
        "atr":          float(latest["atr"]),
        "vwap":         float(latest["vwap"]),
        "bull_engulf":  bool(latest["bull_engulf"]),
        "ema50_15":     float(ema50_15_val),
        "pivot":        float(pivot_val) if not np.isnan(pivot_val) else np.nan
    }

def calculate_entry_exit(ind):
    global bot_state
    ts       = ind["timestamp"]
    price    = ind["close"]
    high     = ind["high"]
    ema9     = ind["ema9"]
    ema21    = ind["ema21"]
    ema50    = ind["ema50"]
    adx      = ind["adx"]
    rsi      = ind["rsi"]
    atr      = ind["atr"]
    vwap     = ind["vwap"]
    bull     = ind["bull_engulf"]
    ema50_15 = ind["ema50_15"]
    pivot    = ind["pivot"]

    exchange = get_ccxt_exchange()

    if bot_state["in_position"]:
        bot_state["bars_held"] += 1
        ep     = bot_state["entry_price"]
        tp     = bot_state["take_profit"]
        qty    = bot_state["quantity"]
        equity = bot_state["equity"]

        if high >= tp:
            exchange.create_order(CCXT_SYMBOL, "market", "sell", qty)
            R       = (tp - ep) / ep
            pnl     = equity * (TARGET_LEVERAGE * R)
            bot_state["equity"] = equity + pnl
            print(f"[{ts}] ▶ TP hit. Sold {qty:.6f} BTC @ {tp:.2f}. P/L = {pnl:.2f} USDT.")
            bot_state.update({
                "in_position":   False,
                "entry_price":   None,
                "take_profit":   None,
                "atr_at_entry":  None,
                "quantity":      None,
                "bars_held":     0,
                "entry_time":    None
            })
            save_state()
            return

        if bot_state["bars_held"] >= MAX_BARS_HELD:
            order = exchange.create_order(
                CCXT_SYMBOL, "limit", "sell", qty, ep, {"timeInForce": "GTC"}
            )
            time.sleep(2)
            status = exchange.fetch_order(order["id"], CCXT_SYMBOL)
            if status["status"] != "closed":
                exchange.cancel_order(order["id"], CCXT_SYMBOL)
                exchange.create_order(CCXT_SYMBOL, "market", "sell", qty)
            print(f"[{ts}] ▶ Breakeven exit. Sold {qty:.6f} BTC @ {ep:.2f}. P/L = 0.")
            bot_state.update({
                "in_position":   False,
                "entry_price":   None,
                "take_profit":   None,
                "atr_at_entry":  None,
                "quantity":      None,
                "bars_held":     0,
                "entry_time":    None
            })
            save_state()
            return

    if not bot_state["in_position"]:
        cond1 = (ema9 > ema21 > ema50)
        cond2 = (adx > 20) and (rsi > 55)
        cond3 = (price > ema50_15)
        cond4 = (price > vwap)
        cond5 = (price > pivot)
        cond6 = bull
        all_ok = all([cond1, cond2, cond3, cond4, cond5, cond6, atr > 0])

        if all_ok:
            entry_price = price
            take_profit = entry_price + ATR_MULTIPLIER * atr
            equity      = bot_state["equity"]
            raw_qty     = (equity * TARGET_LEVERAGE) / entry_price
            step_size   = exchange.markets[CCXT_SYMBOL]["limits"]["cost"]["min"]
            qty         = math.floor(raw_qty / step_size) * step_size
            if qty <= 0:
                print(f"[{ts}] ⚠ Qty ≤ 0 (equity={equity:.2f}), skipping entry.")
                return
            exchange.create_order(CCXT_SYMBOL, "market", "buy", qty)
            print(f"[{ts}] ▶ Enter LONG. Bought {qty:.6f} BTC @ {entry_price:.2f}. TP={take_profit:.2f}")
            bot_state.update({
                "in_position":   True,
                "entry_price":   entry_price,
                "take_profit":   take_profit,
                "atr_at_entry":  atr,
                "quantity":      qty,
                "bars_held":     0,
                "entry_time":    ts.isoformat()
            })
            save_state()
            return
        else:
            tstr = ts.isoformat()
            print(
                f"[{tstr}] ❌ Entry conditions not met: "
                f"cond1={cond1}, cond2={cond2}, cond3={cond3}, "
                f"cond4={cond4}, cond5={cond5}, cond6={cond6} → skipping entry."
            )
            return

async def periodic_daily_refresh():
    while True:
        fetch_daily_cache()
        await asyncio.sleep(3600 * REFRESH_1D_HRS)

def on_message(ws, message):
    global df5m
    data = json.loads(message)

    if "arg" in data and data["arg"]["channel"] == "candle5m" and data.get("event") == "update":
        for candle in data["data"]:
            ts_ms, o, h, l, c, v, *_ = candle
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).replace(second=0, microsecond=0)
            entry = {
                "timestamp": ts,
                "open":      float(o),
                "high":      float(h),
                "low":       float(l),
                "close":     float(c),
                "volume":    float(v)
            }
            with data_lock:
                if not df5m.empty and df5m.iloc[-1]["timestamp"] == ts:
                    df5m.iloc[-1] = entry
                else:
                    df5m.loc[len(df5m)] = entry

        resample_15m_from_5m()
        ind = calculate_indicators()
        if ind is not None:
            calculate_entry_exit(ind)

def on_open(ws):
    sub_msg = {
        "op": "subscribe",
        "args": [
            {
                "channel": "candle5m",
                "instId": WS_SYMBOL
            }
        ]
    }
    ws.send(json.dumps(sub_msg))
    print("WebSocket connected and subscribed to 5m candles.")

def on_error(ws, error):
    print("WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed:", close_status_code, close_msg)

def start_websocket():
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()

def print_initial_balance():
    exchange = get_ccxt_exchange()
    balance = exchange.fetch_balance(params={"type": "future"})
    usdt_free = balance.get("USDT", {}).get("free", None)
    if usdt_free is None:
        usdt_free = balance["total"].get("USDT", 0.0)
    print(f"Available USDT balance (futures): {usdt_free:.2f} USDT")

def main():
    print_initial_balance()
    fetch_initial_history()
    threading.Thread(target=start_health_server, daemon=True).start()
    fetch_daily_cache()
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_daily_refresh())
    ws_thread = threading.Thread(target=start_websocket, daemon=True)
    ws_thread.start()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print("Shutting down bot...")

if __name__ == "__main__":
    main()
