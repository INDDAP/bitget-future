"""
bitget_bot.py

A live trading bot for BTCUSDT Perpetual Futures on Bitget.
- Receives 5-minute candles via WebSocket
- Resamples 5-minute candles to 15-minute
- Caches 1-day candles (fetched via REST and refreshed daily)
- Implements the “zero-loss, 0.5×ATR” strategy with multi-timeframe confirmation
- Places orders via CCXT (Bitget futures)
- Saves state to disk for persistence
- Exposes a dummy HTTP port (8000) for health‐checks
- Prints a notification each time entry conditions are checked and skipped
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
PASSPHRASE  = os.getenv("BITGET_API_PASSPHRASE")  # Bitget requires passphrase

if not all([API_KEY, API_SECRET, PASSPHRASE]):
    raise ValueError("Please set BITGET_API_KEY, BITGET_API_SECRET, and BITGET_API_PASSPHRASE")

# Use these two distinct symbols:
WS_SYMBOL    = "BTCUSDT_UMCBL"  # for WebSocket subscription (Bitget format)
CCXT_SYMBOL  = "BTCUSDT"        # for CCXT REST calls (Bitget futures)

TIMEFRAME_5M    = "5m"
TIMEFRAME_15M   = "15m"
TIMEFRAME_1D    = "1d"
STATE_FILE      = "bot_state.json"
DATA_DIR        = "data_cache"   # directory to store cached CSVs

TARGET_LEVERAGE = 100            # leverage to use
START_EQUITY    = 1000.0         # initial equity (purely theoretical for compounding)
MAX_BARS_HELD   = 10             # exit at breakeven after this many 5m bars
ATR_MULTIPLIER  = 0.5            # TP = entry_price + (ATR * ATR_MULTIPLIER)
REFRESH_1D_HRS  = 1              # how often (in hours) to refresh 1d cache

# WebSocket endpoint for Bitget mix (USDT-m futures)
WS_URL = "wss://ws.bitgetapi.com/mix/v1/stream"

# Health-check HTTP port
HEALTH_PORT = 8000


# ─── 2) GLOBAL STATE ────────────────────────────────────────────────────────────

# Load or initialize bot_state (equity, in_position, etc.)
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

# DataFrames holding candle data:
df5m  = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
df15m = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "ema50_15"])
df1d  = pd.DataFrame()  # will hold daily OHLCV + pivot

# Lock to safely update DataFrames from WebSocket thread
data_lock = threading.Lock()


# ─── 3) HTTP HEALTH‐CHECK SERVER ────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_HEAD(self):
        # Respond 200 to any HEAD request
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        # Respond 200 + “OK” to any GET request
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

def start_health_server():
    """
    Starts a minimal HTTP server on HEALTH_PORT that responds "OK" to every HEAD or GET.
    """
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    print(f"Health-check server listening on port {HEALTH_PORT}")
    server.serve_forever()


# ─── 4) UTILITY FUNCTIONS ───────────────────────────────────────────────────────

def save_state():
    """
    Persist bot_state to disk.
    """
    with open(STATE_FILE, "w") as f:
        json.dump(bot_state, f, indent=2)

def get_ccxt_exchange():
    """
    Returns a CCXT instance configured for Bitget futures.
    """
    exchange = ccxt.bitget({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "password": PASSPHRASE,
        "enableRateLimit": True,
        "options": {
            "defaultType": "future",   # we want futures
            "defaultSubType": "UMCBL"  # USDT-m perpetual
        }
    })
    return exchange

def fetch_daily_cache():
    """
    Fetches 1-day candles via CCXT and caches into df1d.
    Refresh once every REFRESH_1D_HRS hours.
    """
    global df1d
    exchange = get_ccxt_exchange()
    since = exchange.parse8601(
        (datetime.utcnow() - timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    ohlcv = exchange.fetch_ohlcv(CCXT_SYMBOL, timeframe=TIMEFRAME_1D, since=since, limit=365)
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df.set_index("timestamp", inplace=True)
    # Compute prior-day pivot = (H_prev + L_prev + C_prev) / 3
    df["pivot"] = (df["high"].shift(1) + df["low"].shift(1) + df["close"].shift(1)) / 3
    df1d = df[["open", "high", "low", "close", "volume", "pivot"]]
    os.makedirs(DATA_DIR, exist_ok=True)
    df1d.to_csv(f"{DATA_DIR}/BTCUSDT_1d_cache.csv")
    print("Refreshed 1d cache")

def resample_15m_from_5m():
    """
    Resample df5m (5-minute candles) into 15-minute candles, compute EMA50 on 15m,
    and store resulting 15m DataFrame in df15m.
    """
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
        # Compute EMA50 on 15m close price
        df15["ema50_15"] = df15["close"].ewm(span=50, adjust=False).mean()
        df15m = df15.copy()

def calculate_indicators():
    """
    Computes all indicators for the latest 5m bar:
      - EMA9, EMA21, EMA50 (5m)
      - RSI(14)
      - ADX(14)
      - ATR(14)
      - VWAP (intraday)
      - Bullish Engulfing (5m)
      - EMA50_15 (from df15m, forward-filled)
      - daily pivot (from df1d)
    Returns a dict of indicator values for the latest bar, or None if insufficient data.
    """
    global df5m, df15m, df1d
    with data_lock:
        df5 = df5m.copy()
        df15 = df15m.copy()
        df1 = df1d.copy()

    # Need at least 50 bars of 5m, 50 bars of 15m, and 2 bars of 1d
    if len(df5) < 50 or len(df15) < 50 or len(df1) < 2:
        return None

    # 5m EMAs
    df5["ema9"]  = df5["close"].ewm(span=9, adjust=False).mean()
    df5["ema21"] = df5["close"].ewm(span=21, adjust=False).mean()
    df5["ema50"] = df5["close"].ewm(span=50, adjust=False).mean()

    # RSI(14)
    delta = df5["close"].diff()
    gain  = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    loss  = (-delta).clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    rs    = gain / loss
    df5["rsi"]  = 100 - (100 / (1 + rs))

    # ADX(14)
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
    dx        = 100 * ( (plus_di - minus_di).abs() / (plus_di + minus_di) )
    df5["adx"] = dx.ewm(alpha=1/14, adjust=False).mean()

    # ATR(14)
    df5["atr"] = tr.ewm(alpha=1/14, adjust=False).mean()

    # VWAP (intraday)
    df5["date"] = df5["timestamp"].dt.floor("D")
    vwap_vals = []
    for d, grp in df5.groupby("date"):
        tp = (grp["high"] + grp["low"] + grp["close"]) / 3
        cum_vp  = (grp["volume"] * tp).cumsum()
        cum_vol = grp["volume"].cumsum()
        vwap_vals += list((cum_vp / cum_vol).fillna(method="ffill"))
    df5["vwap"] = vwap_vals

    # Bullish Engulfing pattern
    op_prev   = df5["open"].shift(1)
    cl_prev   = df5["close"].shift(1)
    df5["bull_engulf"] = (op_prev > cl_prev) & (df5["open"] < cl_prev) & (df5["close"] > op_prev)

    latest = df5.iloc[-1]
    ts_latest = latest["timestamp"]

    # 15m EMA50 (forward‐filled)
    df15_idx = df15[df15["timestamp"] <= ts_latest]
    if df15_idx.empty:
        return None
    ema50_15_val = df15_idx.iloc[-1]["ema50_15"]

    # Daily pivot for that date
    pivot_val = df1.loc[df1.index.date == ts_latest.date(), "pivot"].values
    if len(pivot_val) == 0:
        pivot_val = np.nan
    else:
        pivot_val = float(pivot_val[0])

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
    """
    Given the latest indicators (dict), decide entry or exit.
    Prints a notification if entry conditions are checked and skipped.
    """
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

    # ── EXIT LOGIC ───────────────────────────────────────────────────────────────
    if bot_state["in_position"]:
        bot_state["bars_held"] += 1
        ep     = bot_state["entry_price"]
        tp     = bot_state["take_profit"]
        qty    = bot_state["quantity"]
        equity = bot_state["equity"]

        # 1) Take-profit check
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

        # 2) Breakeven exit after MAX_BARS_HELD
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

    # ── ENTRY LOGIC ───────────────────────────────────────────────────────────────
    if not bot_state["in_position"]:
        # Evaluate each condition
        cond1 = (ema9 > ema21 > ema50)           # EMA stacking on 5m
        cond2 = (adx > 20) and (rsi > 55)        # ADX + RSI momentum
        cond3 = (price > ema50_15)               # 15m EMA50 confirmation
        cond4 = (price > vwap)                   # VWAP filter
        cond5 = (price > pivot)                  # Daily pivot filter
        cond6 = bull                            # Bullish Engulfing pattern
        # All six must be true to enter
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
            # Print a notification showing which conditions failed
            tstr = ts.isoformat()
            print(
                f"[{tstr}] ❌ Entry conditions not met: "
                f"cond1={cond1}, cond2={cond2}, cond3={cond3}, "
                f"cond4={cond4}, cond5={cond5}, cond6={cond6} → skipping entry."
            )
            return

async def periodic_daily_refresh():
    """
    Periodically refresh the 1-day cache every REFRESH_1D_HRS hours.
    """
    while True:
        fetch_daily_cache()
        await asyncio.sleep(3600 * REFRESH_1D_HRS)

def on_message(ws, message):
    """
    Callback for incoming WebSocket messages.
    Parses 5m candle events and updates df5m.
    """
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

        # Resample 15m, recalc indicators, attempt entry/exit
        resample_15m_from_5m()
        ind = calculate_indicators()
        if ind is not None:
            calculate_entry_exit(ind)

def on_open(ws):
    """
    Called when WebSocket opens. Subscribe to 5m candles.
    """
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
    """
    Starts the Bitget WebSocket in a separate thread.
    """
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()

def main():
    # 1) Spin up the health‐check server on port 8000
    threading.Thread(target=start_health_server, daemon=True).start()

    # 2) Initial fetch of 1d cache
    fetch_daily_cache()

    # 3) Schedule periodic refresh of 1d candles
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_daily_refresh())

    # 4) Start WebSocket thread for 5m candles
    ws_thread = threading.Thread(target=start_websocket, daemon=True)
    ws_thread.start()

    # 5) Keep the main thread alive
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print("Shutting down bot...")

if __name__ == "__main__":
    main()
