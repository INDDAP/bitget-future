"""
bitget_bot.py

A live trading bot for BTCUSDT Perpetual Futures on Bitget.

Features:
- Fetches initial 5m, 15m, and 1d candle history via REST to seed data.
- Receives live 5-minute candles via WebSocket (with ping/pong to keep alive).
- Automatic WebSocket reconnect if the connection drops.
- Resamples 5-minute candles to 15-minute and computes EMA50.
- Caches 1-day candles (refreshed hourly) for daily pivot.
- Implements the “zero-loss, 0.5×ATR” strategy with multi-timeframe confluence.
- Prints debug notifications when entry conditions are checked and skipped.
- Places market orders via CCXT (Bitget futures).
- Attempts to detect your actual USDT balance (spot or futures) and prints the raw CCXT response for inspection.
- If no free USDT is found, falls back to a theoretical starting equity (START_EQUITY) so trades can still run.
- Saves bot_state (equity, open position, etc.) to disk for persistence.
- Exposes a dummy HTTP port (8000) for Render health‐checks.
- Prints a heartbeat every 5 minutes to show the bot is alive.
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

# WebSocket symbol (Bitget “mix” format) vs CCXT futures symbol
WS_SYMBOL    = "BTCUSDT_UMCBL"
CCXT_SYMBOL  = "BTCUSDT"

# Timeframes
TIMEFRAME_5M    = "5m"
TIMEFRAME_15M   = "15m"
TIMEFRAME_1D    = "1d"

# Local state and cache
STATE_FILE      = "bot_state.json"
DATA_DIR        = "data_cache"

# Strategy parameters
TARGET_LEVERAGE = 100            # 100× leverage
START_EQUITY    = 1000.0         # fallback theoretical equity if no real USDT found
MAX_BARS_HELD   = 10             # breakeven after 10 bars if TP not hit
ATR_MULTIPLIER  = 0.5            # TP = entry_price + 0.5×ATR
REFRESH_1D_HRS  = 1              # refresh 1d cache every 1 hour

# Bitget WebSocket endpoint (USDT-margined mix)
WS_URL = "wss://ws.bitgetapi.com/mix/v1/stream"

# Health‐check port (Render expects a listening port)
HEALTH_PORT = 8000

# How often to print a heartbeat
HEARTBEAT_INTERVAL_SECONDS = 300  # 5 minutes


# ─── 2) GLOBAL STATE ────────────────────────────────────────────────────────────

# Load or initialize bot_state
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

# These DataFrames will hold incoming candle data:
df5m  = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
df15m = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "ema50_15"])
df1d  = pd.DataFrame()  # will hold 1d OHLC + pivot

# Lock to protect concurrent updates from the WebSocket thread
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
    """
    Start a minimal HTTP server that listens on HEALTH_PORT and responds “OK” to HEAD/GET.
    This satisfies Render’s requirement for a listening port.
    """
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    print(f"[{datetime.utcnow().isoformat()}] Health-check server listening on port {HEALTH_PORT}", flush=True)
    server.serve_forever()


# ─── 4) UTILITY FUNCTIONS ───────────────────────────────────────────────────────

def save_state():
    """
    Persist bot_state to disk whenever something significant changes.
    """
    with open(STATE_FILE, "w") as f:
        json.dump(bot_state, f, indent=2)
    print(f"[{datetime.utcnow().isoformat()}] State saved to disk.", flush=True)

def get_ccxt_exchange():
    """
    Create and return a CCXT bitget instance configured for USDT-margined futures.
    """
    exchange = ccxt.bitget({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "password": PASSPHRASE,
        "enableRateLimit": True,
        "options": {
            "defaultType": "future",
            "defaultSubType": "UMCBL"  # USDT-margined mix
        }
    })
    return exchange

def print_initial_balance():
    """
    Attempt to fetch both spot and futures balances via CCXT.
    - Prints the raw 'info' response so the user can inspect exactly where their 10 USDT lives.
    - If we detect a nonzero “free” USDT, set bot_state['equity'] to that value.
    - Otherwise, fall back to START_EQUITY so the bot can still size trades.
    """
    try:
        exchange = get_ccxt_exchange()

        # 1) Try unified fetch_balance() without params
        bal_spot = exchange.fetch_balance()
        info_spot = bal_spot.get("info", {})
        print(f"[{datetime.utcnow().isoformat()}] Raw CCXT fetch_balance() response (spot) → info:", flush=True)
        print(json.dumps(info_spot, indent=2), flush=True)

        usdt_free = None
        if "USDT" in bal_spot and isinstance(bal_spot["USDT"], dict):
            free_val = bal_spot["USDT"].get("free", None)
            if free_val is not None:
                usdt_free = float(free_val)

        # 2) Try fetching as futures wallet
        if not usdt_free or usdt_free == 0.0:
            bal_fut = exchange.fetch_balance(params={"type": "future"})
            info_fut = bal_fut.get("info", {})
            print(f"[{datetime.utcnow().isoformat()}] Raw CCXT fetch_balance({'future'}) response → info:", flush=True)
            print(json.dumps(info_fut, indent=2), flush=True)

            if "USDT" in bal_fut and isinstance(bal_fut["USDT"], dict):
                free_val2 = bal_fut["USDT"].get("free", None)
                if free_val2 is not None:
                    usdt_free = float(free_val2)

        if not usdt_free:
            usdt_free = 0.0

        if usdt_free > 0:
            bot_state["equity"] = usdt_free
            print(f"[{datetime.utcnow().isoformat()}] Detected available USDT (used as equity): {usdt_free:.2f} USDT", flush=True)
        else:
            # No free USDT found → fall back to theoretical START_EQUITY
            bot_state["equity"] = START_EQUITY
            print(f"[{datetime.utcnow().isoformat()}] No free USDT detected. Falling back to START_EQUITY = {START_EQUITY:.2f}", flush=True)

    except Exception as e:
        print(f"[{datetime.utcnow().isoformat()}] ERROR fetching balance: {e}", flush=True)
        # Fall back to theoretical equity so the bot can still run:
        bot_state["equity"] = START_EQUITY
        print(f"[{datetime.utcnow().isoformat()}] Falling back to START_EQUITY = {START_EQUITY:.2f}", flush=True)

def fetch_initial_history():
    """
    Fetch the last ~100 5-minute candles and ~50 15-minute candles via CCXT REST.
    This seeds df5m and df15m so indicator calculations can begin immediately.
    """
    global df5m, df15m
    try:
        exchange = get_ccxt_exchange()

        # Last 100 x 5m bars
        ohlcv5 = exchange.fetch_ohlcv(CCXT_SYMBOL, timeframe=TIMEFRAME_5M, limit=100)
        df5 = pd.DataFrame(ohlcv5, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df5["timestamp"] = pd.to_datetime(df5["timestamp"], unit="ms", utc=True)
        df5m = df5[["timestamp", "open", "high", "low", "close", "volume"]]

        # Last 50 x 15m bars
        ohlcv15 = exchange.fetch_ohlcv(CCXT_SYMBOL, timeframe=TIMEFRAME_15M, limit=50)
        df15 = pd.DataFrame(ohlcv15, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df15["timestamp"] = pd.to_datetime(df15["timestamp"], unit="ms", utc=True)
        df15["ema50_15"] = df15["close"].ewm(span=50, adjust=False).mean()
        df15m = df15[["timestamp", "open", "high", "low", "close", "volume", "ema50_15"]]

        print(f"[{datetime.utcnow().isoformat()}] Fetched initial 5m and 15m history", flush=True)
    except Exception as e:
        print(f"[{datetime.utcnow().isoformat()}] ERROR fetching initial history: {e}", flush=True)

def fetch_daily_cache():
    """
    Fetch the last ~365 daily candles, compute the prior-day pivot, and store in df1d.
    """
    global df1d
    try:
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
        print(f"[{datetime.utcnow().isoformat()}] Refreshed 1d cache", flush=True)
    except Exception as e:
        print(f"[{datetime.utcnow().isoformat()}] ERROR fetching daily cache: {e}", flush=True)

def resample_15m_from_5m():
    """
    Resample df5m into 15-minute bars, compute EMA50 on 15m, and store in df15m.
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
        df15["ema50_15"] = df15["close"].ewm(span=50, adjust=False).mean()
        df15m = df15[["timestamp", "open", "high", "low", "close", "volume", "ema50_15"]]

def calculate_indicators():
    """
    Compute all necessary indicators for the most recent 5m bar:
    - EMA9, EMA21, EMA50 on 5m
    - RSI(14), ADX(14), ATR(14) on 5m
    - VWAP (intraday) on 5m
    - Bullish Engulfing flag on 5m
    - 15m EMA50 (forward‐filled)
    - Daily pivot (from df1d)

    Return a dict of indicators for the latest bar, or None if insufficient data.
    """
    global df5m, df15m, df1d
    with data_lock:
        df5 = df5m.copy()
        df15 = df15m.copy()
        df1 = df1d.copy()

    # Need at least 50 x 5m bars, 50 x 15m bars, and 2 x daily bars
    if len(df5) < 50 or len(df15) < 50 or len(df1) < 2:
        return None

    # 1) 5m EMAs
    df5["ema9"]  = df5["close"].ewm(span=9, adjust=False).mean()
    df5["ema21"] = df5["close"].ewm(span=21, adjust=False).mean()
    df5["ema50"] = df5["close"].ewm(span=50, adjust=False).mean()

    # 2) RSI(14)
    delta = df5["close"].diff()
    gain  = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    loss  = (-delta).clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    rs    = gain / loss
    df5["rsi"]  = 100 - (100 / (1 + rs))

    # 3) ADX(14)
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

    # 4) ATR(14)
    df5["atr"] = tr.ewm(alpha=1/14, adjust=False).mean()

    # 5) VWAP (intraday)
    df5["date"] = df5["timestamp"].dt.floor("D")
    vwap_vals = []
    for d, grp in df5.groupby("date"):
        tp     = (grp["high"] + grp["low"] + grp["close"]) / 3
        cum_vp = (grp["volume"] * tp).cumsum()
        cum_vol= grp["volume"].cumsum()
        vwap_vals += list((cum_vp / cum_vol).fillna(method="ffill"))
    df5["vwap"] = vwap_vals

    # 6) Bullish Engulfing
    op_prev = df5["open"].shift(1)
    cl_prev = df5["close"].shift(1)
    df5["bull_engulf"] = (op_prev > cl_prev) & (df5["open"] < cl_prev) & (df5["close"] > op_prev)

    latest     = df5.iloc[-1]
    ts_latest  = latest["timestamp"]

    # 7) 15m EMA50 (forward-filled)
    df15_idx = df15[df15["timestamp"] <= ts_latest]
    if df15_idx.empty:
        return None
    ema50_15_val = df15_idx.iloc[-1]["ema50_15"]

    # 8) Daily pivot
    pivot_val = df1.loc[df1.index.date == ts_latest.date(), "pivot"].values
    pivot_val = float(pivot_val[0]) if len(pivot_val) > 0 else np.nan

    # Debug: print out all indicators on each new 5m bar
    print(
        f"[{ts_latest.isoformat()}] Indicators → "
        f"EMA9={latest['ema9']:.2f}, EMA21={latest['ema21']:.2f}, EMA50={latest['ema50']:.2f}, "
        f"ADX={latest['adx']:.2f}, RSI={latest['rsi']:.2f}, ATR={latest['atr']:.2f}, "
        f"VWAP={latest['vwap']:.2f}, EMA50_15={ema50_15_val:.2f}, Pivot={pivot_val:.2f}, "
        f"Engulf={bool(latest['bull_engulf'])}",
        flush=True
    )

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
    Given the latest indicators, decide entry or exit.
    - Prints which conditions failed if skipping entry.
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
            print(f"[{ts.isoformat()}] ▶ TP hit. Sold {qty:.6f} BTC @ {tp:.2f}. P/L = {pnl:.2f} USDT.", flush=True)
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
            print(f"[{ts.isoformat()}] ▶ Breakeven exit. Sold {qty:.6f} BTC @ {ep:.2f}. P/L = 0.", flush=True)
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
        cond1 = (ema9 > ema21 > ema50)           # EMA stacking on 5m
        cond2 = (adx > 20) and (rsi > 55)        # ADX + RSI momentum
        cond3 = (price > ema50_15)               # 15m EMA50 confirmation
        cond4 = (price > vwap)                   # VWAP filter
        cond5 = (price > pivot)                  # daily pivot filter
        cond6 = bull                            # bullish engulfing
        all_ok = all([cond1, cond2, cond3, cond4, cond5, cond6, atr > 0])

        if all_ok:
            entry_price = price
            take_profit = entry_price + ATR_MULTIPLIER * atr
            equity      = bot_state["equity"]
            raw_qty     = (equity * TARGET_LEVERAGE) / entry_price
            step_size   = exchange.markets[CCXT_SYMBOL]["limits"]["cost"]["min"]
            qty         = math.floor(raw_qty / step_size) * step_size
            if qty <= 0:
                print(f"[{ts.isoformat()}] ⚠ Computed qty ≤ 0 (equity={equity:.2f}), skipping entry.", flush=True)
                return
            exchange.create_order(CCXT_SYMBOL, "market", "buy", qty)
            print(f"[{ts.isoformat()}] ▶ Enter LONG. Bought {qty:.6f} BTC @ {entry_price:.2f}. TP={take_profit:.2f}", flush=True)
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
                f"cond1={cond1}, cond2={cond2}, cond3={cond3}, cond4={cond4}, cond5={cond5}, cond6={cond6} → skipping entry.",
                flush=True
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
    Called whenever a WebSocket message arrives. We parse any 5-minute candles,
    update df5m, then resample to 15m & recalc indicators.
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
                # Overwrite if the timestamp matches the last row, else append
                if not df5m.empty and df5m.iloc[-1]["timestamp"] == ts:
                    df5m.iloc[-1] = entry
                else:
                    df5m.loc[len(df5m)] = entry

        # Resample to 15m and attempt entry/exit
        resample_15m_from_5m()
        ind = calculate_indicators()
        if ind is not None:
            calculate_entry_exit(ind)

def on_open(ws):
    """
    Called when WebSocket successfully opens. We now subscribe to 5m candles.
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
    print(f"[{datetime.utcnow().isoformat()}] WebSocket connected and subscribed to 5m candles.", flush=True)

def on_error(ws, error):
    print(f"[{datetime.utcnow().isoformat()}] WebSocket error: {error}", flush=True)

def on_close(ws, close_status_code, close_msg):
    print(f"[{datetime.utcnow().isoformat()}] WebSocket closed: {close_status_code} {close_msg}", flush=True)

def start_websocket():
    """
    Create and run the WebSocketApp. If the connection is lost,
    wait 5s and then reconnect (in a loop).
    We also specify ping_interval to keep the connection alive.
    """
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            # ping_interval=20s means send a ping frame every 20s to avoid timeouts
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"[{datetime.utcnow().isoformat()}] WebSocket run_forever exception: {e}", flush=True)
        print(f"[{datetime.utcnow().isoformat()}] Reconnecting WebSocket in 5 seconds...", flush=True)
        time.sleep(5)

async def heartbeat():
    """
    Prints a simple heartbeat message every HEARTBEAT_INTERVAL_SECONDS so you know
    the bot is still alive even if no candles/trades occur.
    """
    while True:
        print(f"[{datetime.utcnow().isoformat()}] 💓 Heartbeat: still running …", flush=True)
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)

def main():
    # 1) Print starting balance (spot & futures raw info, then pick a value or fallback)
    print_initial_balance()

    # 2) Seed initial 5m/15m history via REST
    fetch_initial_history()

    # 3) Spin up health-check server on port 8000
    threading.Thread(target=start_health_server, daemon=True).start()

    # 4) Fetch initial 1d cache
    fetch_daily_cache()

    # 5) Start asyncio tasks: periodic_daily_refresh() and heartbeat()
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_daily_refresh())
    loop.create_task(heartbeat())

    # 6) Start WebSocket in its own thread (auto-reconnect logic inside)
    ws_thread = threading.Thread(target=start_websocket, daemon=True)
    ws_thread.start()

    # 7) Keep the main thread alive
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print(f"[{datetime.utcnow().isoformat()}] Shutting down bot …", flush=True)

if __name__ == "__main__":
    main()
