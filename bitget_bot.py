import os
import time
import json
import math
import threading
import asyncio
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import ccxt
from flask import Flask, jsonify

# ─── 1) CONFIGURATION ─────────────────────────────────────────────────────────

# Load your Bitget API key/secret/passphrase from environment variables
API_KEY     = os.getenv("BITGET_API_KEY")
API_SECRET  = os.getenv("BITGET_API_SECRET")
PASSPHRASE  = os.getenv("BITGET_PASSPHRASE")

if not all([API_KEY, API_SECRET, PASSPHRASE]):
    raise ValueError("Please set BITGET_API_KEY, BITGET_API_SECRET, and BITGET_PASSPHRASE")

# Symbol definitions
CCXT_SYMBOL    = "BTCUSDT"       # CCXT uses this for Bitget USDT-m futures
TIMEFRAME_5M   = "5m"
TIMEFRAME_15M  = "15m"
TIMEFRAME_1D   = "1d"

# Strategy parameters
TARGET_LEVERAGE   = 100
START_EQUITY      = 1000.0        # fallback if no real USDT found
MAX_BARS_HELD     = 10
ATR_MULTIPLIER    = 0.5
REFRESH_1D_HRS    = 1             # refresh daily cache every hour
HEARTBEAT_SECONDS = 300           # 5-minute heartbeat
STATE_FILE        = "bot_state.json"
DATA_DIR          = "data_cache"

# Flask / Health‐check port
HEALTH_PORT = int(os.getenv("PORT", 8000))

# ─── 2) GLOBAL STATE ────────────────────────────────────────────────────────────

# Load or initialize bot state
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

# DataFrames to hold fetched candles
df5m  = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
df15m = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "ema50_15"])
df1d  = pd.DataFrame()  # will store daily candles + pivot

data_lock = threading.Lock()

# Flask application
app = Flask(__name__)

# CCXT Bitget futures exchange
def get_ccxt_exchange():
    exchange = ccxt.bitget({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "password": PASSPHRASE,
        "enableRateLimit": True,
        "options": {
            "defaultType": "future",
            "defaultSubType": "UMCBL",  # USDT-margined perpetual
        }
    })
    return exchange


# ─── 3) FLASK ENDPOINTS ─────────────────────────────────────────────────────────

@app.route("/")
def home():
    return "Bitget Futures Bot is running", 200

@app.route("/health")
def health():
    return "OK", 200

@app.route("/balance")
def get_balance():
    """
    Returns your USDT futures balance (free + locked) as JSON.
    """
    try:
        exchange = get_ccxt_exchange()
        balance = exchange.fetch_balance(params={"type": "future"})
        # Print only the USDT lines
        usdt = balance.get("USDT", {})
        return jsonify(usdt), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── 4) STATE PERSISTENCE ───────────────────────────────────────────────────────

def save_state():
    """Persist bot_state to disk."""
    with open(STATE_FILE, "w") as f:
        json.dump(bot_state, f, indent=2)
    print(f"[{datetime.utcnow().isoformat()}] State saved to {STATE_FILE}", flush=True)


# ─── 5) BALANCE FETCH & INITIALIZATION ──────────────────────────────────────────

def fetch_balance_and_set_equity():
    """
    Fetch the USDT futures balance via REST.
    Print raw JSON so you see exactly where your 10 USDT is.
    If free USDT > 0, use it as bot_state["equity"].
    Otherwise, fall back to START_EQUITY.
    """
    try:
        exchange = get_ccxt_exchange()
        bal = exchange.fetch_balance(params={"type": "future"})
        print(f"[{datetime.utcnow().isoformat()}] Raw fetch_balance({'future'}) response:", flush=True)
        print(json.dumps(bal.get("info", {}), indent=2), flush=True)

        usdt_free = bal.get("USDT", {}).get("free", None)
        if usdt_free is not None:
            usdt_free = float(usdt_free)
        else:
            usdt_free = 0.0

        if usdt_free > 0.0:
            bot_state["equity"] = usdt_free
            print(f"[{datetime.utcnow().isoformat()}] Detected free USDT: {usdt_free:.2f}. Using as equity.", flush=True)
        else:
            bot_state["equity"] = START_EQUITY
            print(f"[{datetime.utcnow().isoformat()}] No free USDT found. Falling back to START_EQUITY = {START_EQUITY:.2f}", flush=True)

    except Exception as e:
        print(f"[{datetime.utcnow().isoformat()}] ERROR fetching balance: {e}", flush=True)
        bot_state["equity"] = START_EQUITY
        print(f"[{datetime.utcnow().isoformat()}] Falling back to START_EQUITY = {START_EQUITY:.2f}", flush=True)

    save_state()


# ─── 6) FETCH CANDLES VIA REST ─────────────────────────────────────────────────

def fetch_ohlcv_ccxt(symbol, timeframe, limit):
    """
    Fetch recent OHLCV bars for the given symbol/timeframe via CCXT.
    Returns a DataFrame with columns: timestamp, open, high, low, close, volume.
    """
    exchange = get_ccxt_exchange()
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df[["timestamp", "open", "high", "low", "close", "volume"]].astype(float)
    return df


def resample_15m_from_5m():
    """
    Given df5m, resample to 15m and compute ema50_15, store in df15m.
    """
    global df5m, df15m
    with data_lock:
        if df5m.empty:
            return
        temp = df5m.set_index("timestamp").copy()
        df15 = temp.resample("15T").agg({
            "open":   "first",
            "high":   "max",
            "low":    "min",
            "close":  "last",
            "volume": "sum"
        }).dropna().reset_index()
        df15["ema50_15"] = df15["close"].ewm(span=50, adjust=False).mean()
        df15m = df15[["timestamp", "open", "high", "low", "close", "volume", "ema50_15"]]


# ─── 7) INDICATOR CALCULATIONS ─────────────────────────────────────────────────

def compute_indicators(df5, df15, df1d):
    """
    Compute all indicators for the latest 5m bar:
    - 5m EMAs (9,21,50), RSI14, ADX14, ATR14, VWAP, Bullish Engulfing.
    - 15m EMA50 (forward‐filled).
    - Daily pivot.
    Returns a dict of indicators for the latest bar, or None if insufficient data.
    """
    if len(df5) < 50 or len(df15) < 50 or len(df1d) < 2:
        return None

    # 1) 5m EMAs
    df5["ema9"]  = df5["close"].ewm(span=9,  adjust=False).mean()
    df5["ema21"] = df5["close"].ewm(span=21, adjust=False).mean()
    df5["ema50"] = df5["close"].ewm(span=50, adjust=False).mean()

    # 2) RSI(14)
    delta = df5["close"].diff()
    gain  = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    loss  = (-delta).clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    rs    = gain / loss
    df5["rsi"] = 100 - (100 / (1 + rs))

    # 3) ADX(14)
    high       = df5["high"]
    low        = df5["low"]
    prev_close = df5["close"].shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    tr14      = tr.ewm(alpha=1/14, adjust=False).mean()
    up_move   = high.diff()
    down_move = low.shift(1) - low
    plus_dm   = ((up_move > down_move) & (up_move > 0)) * up_move
    minus_dm  = ((down_move > up_move) & (down_move > 0)) * down_move
    plus_dm14  = plus_dm.ewm(alpha=1/14, adjust=False).mean()
    minus_dm14 = minus_dm.ewm(alpha=1/14, adjust=False).mean()
    plus_di   = 100 * (plus_dm14 / tr14)
    minus_di  = 100 * (minus_dm14 / tr14)
    dx        = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di))
    df5["adx"] = dx.ewm(alpha=1/14, adjust=False).mean()

    # 4) ATR(14)
    df5["atr"] = tr.ewm(alpha=1/14, adjust=False).mean()

    # 5) VWAP (intraday)
    df5["date"] = df5["timestamp"].dt.floor("D")
    vwap_list = []
    for d, grp in df5.groupby("date"):
        tp     = (grp["high"] + grp["low"] + grp["close"]) / 3
        cum_vp = (grp["volume"] * tp).cumsum()
        cum_vol= grp["volume"].cumsum()
        vwap_list += list((cum_vp / cum_vol).fillna(method="ffill"))
    df5["vwap"] = vwap_list

    # 6) Bullish Engulfing
    op_prev = df5["open"].shift(1)
    cl_prev = df5["close"].shift(1)
    df5["bull_engulf"] = (op_prev > cl_prev) & (df5["open"] < cl_prev) & (df5["close"] > op_prev)

    latest = df5.iloc[-1]
    ts_latest = latest["timestamp"]

    # 7) 15m EMA50 (forward-filled)
    df15["ema50_15"] = df15["close"].ewm(span=50, adjust=False).mean()
    df15_valid = df15[df15["timestamp"] <= ts_latest]
    if df15_valid.empty:
        return None
    ema50_15_val = df15_valid.iloc[-1]["ema50_15"]

    # 8) Daily pivot
    pivot_val = df1d.loc[df1d.index.date == ts_latest.date(), "pivot"].values
    pivot_val = float(pivot_val[0]) if len(pivot_val) > 0 else np.nan

    # Debug: print indicators
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


# ─── 8) ENTRY/EXIT LOGIC ───────────────────────────────────────────────────────

def calculate_entry_exit(ind):
    """
    Given the latest indicators, decide whether to enter or exit:
    1) If in_position:
       - bars_held += 1
       - If high >= take_profit -> market sell at TP + update equity
       - Else if bars_held >= MAX_BARS_HELD -> breakeven exit at entry_price

    2) If not in_position:
       - Evaluate 6 entry conditions on the latest bar
       - If all True and atr>0: calculate qty at 100× leverage, market buy
       - Else, print which conditions failed
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

        # 1a) Take-Profit check
        if high >= tp:
            try:
                exchange.create_order(CCXT_SYMBOL, "market", "sell", qty)
                R   = (tp - ep) / ep
                pnl = equity * (TARGET_LEVERAGE * R)
                bot_state["equity"] = equity + pnl
                print(
                    f"[{ts.isoformat()}] ▶ TP hit. Sold {qty:.6f} BTC @ {tp:.2f}. P/L = {pnl:.2f} USDT.",
                    flush=True
                )
            except Exception as e:
                print(f"[{ts.isoformat()}] ERROR placing TP SELL: {e}", flush=True)

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

        # 1b) Breakeven exit after MAX_BARS_HELD
        if bot_state["bars_held"] >= MAX_BARS_HELD:
            try:
                # Place limit sell at entry_price
                order = exchange.create_order(
                    CCXT_SYMBOL, "limit", "sell", qty, ep, {"timeInForce": "GTC"}
                )
                time.sleep(2)
                fetched = exchange.fetch_order(order["id"], CCXT_SYMBOL)
                if fetched["status"] != "closed":
                    exchange.cancel_order(order["id"], CCXT_SYMBOL)
                    exchange.create_order(CCXT_SYMBOL, "market", "sell", qty)
                print(
                    f"[{ts.isoformat()}] ▶ Breakeven exit. Sold {qty:.6f} BTC @ {ep:.2f}. P/L = 0.00 USDT.",
                    flush=True
                )
            except Exception as e:
                print(f"[{ts.isoformat()}] ERROR placing breakeven SELL: {e}", flush=True)

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
        cond1 = (ema9 > ema21 > ema50)          # 5m EMA stacking
        cond2 = (adx > 20) and (rsi > 55)       # ADX + RSI
        cond3 = (price > ema50_15)              # 15m EMA50 confirmation
        cond4 = (price > vwap)                  # VWAP filter
        cond5 = (price > pivot)                 # Daily pivot filter
        cond6 = bull                           # Bullish Engulfing
        all_ok = all([cond1, cond2, cond3, cond4, cond5, cond6, atr > 0])

        if all_ok:
            entry_price = price
            take_profit = entry_price + ATR_MULTIPLIER * atr
            equity      = bot_state["equity"]
            raw_qty     = (equity * TARGET_LEVERAGE) / entry_price

            # Determine step size for BTCUSDT from exchange info
            try:
                info = exchange.fetch_markets()
                sym_info = next((m for m in info if m["symbol"] == CCXT_SYMBOL), None)
                step_size = float(sym_info["limits"]["cost"]["min"])
            except Exception as e:
                print(f"[{ts.isoformat()}] ERROR fetching step size: {e}. Defaulting to 0.000001", flush=True)
                step_size = 0.000001

            qty = math.floor(raw_qty / step_size) * step_size
            qty = round(qty, 6)  # Binance/Bitget typically allow up to 6 decimal places

            if qty <= 0:
                print(
                    f"[{ts.isoformat()}] ⚠ Computed qty ≤ 0 (equity={equity:.2f}), skipping entry.",
                    flush=True
                )
                return

            try:
                exchange.create_order(CCXT_SYMBOL, "market", "buy", qty)
                print(
                    f"[{ts.isoformat()}] ▶ Enter LONG. Bought {qty:.6f} BTC @ {entry_price:.2f}. TP={take_profit:.2f}",
                    flush=True
                )
            except Exception as e:
                print(f"[{ts.isoformat()}] ERROR placing BUY order: {e}", flush=True)
                return

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


# ─── 9) TRADING LOOP (REST POLLING) ─────────────────────────────────────────────

def run_trading_loop():
    """
    Runs in a background thread:
    - Sleeps until the next 5-minute boundary + 1 second.
    - Fetches latest 5m, 15m, and 1d candles via REST.
    - Computes indicators, attempts entry/exit logic.
    - Loops forever.
    """
    while True:
        # 1) Sleep until next 5-minute close + 1 second
        now = datetime.utcnow().replace(second=0, microsecond=0)
        nxt_min = (now.minute // 5) * 5 + 5
        if nxt_min >= 60:
            nxt_hour = now.hour + 1
            if nxt_hour >= 24:
                nxt_hour = 0
                nxt_day = now + timedelta(days=1)
                nxt = datetime(nxt_day.year, nxt_day.month, nxt_day.day, nxt_hour, 0, 1)
            else:
                nxt = datetime(now.year, now.month, now.day, nxt_hour, 0, 1)
        else:
            nxt = datetime(now.year, now.month, now.day, now.hour, nxt_min, 1)

        sleep_secs = (nxt - datetime.utcnow()).total_seconds()
        if sleep_secs > 0:
            time.sleep(sleep_secs)

        # 2) Fetch latest candles
        try:
            df5  = fetch_ohlcv_ccxt(CCXT_SYMBOL, TIMEFRAME_5M,  100)
            df15 = fetch_ohlcv_ccxt(CCXT_SYMBOL, TIMEFRAME_15M, 50)

            df1_raw = fetch_ohlcv_ccxt(CCXT_SYMBOL, TIMEFRAME_1D, 2)
            df1_raw["pivot"] = (df1_raw["high"].shift(1) + df1_raw["low"].shift(1) + df1_raw["close"].shift(1)) / 3
            global df1d
            df1d = df1_raw.set_index("timestamp")[["open", "high", "low", "close", "volume", "pivot"]]

            # Compute indicators & run logic
            ind = compute_indicators(df5, df15, df1d)
            if ind is not None:
                calculate_entry_exit(ind)

        except Exception as e:
            print(f"[{datetime.utcnow().isoformat()}] ERROR in trading loop: {e}", flush=True)

        # Loop continues to next 5-minute boundary


# ─── 10) HEARTBEAT ──────────────────────────────────────────────────────────────

async def heartbeat():
    """
    Prints a “still running” message every HEARTBEAT_SECONDS.
    """
    while True:
        print(f"[{datetime.utcnow().isoformat()}] 💓 Heartbeat: still running …", flush=True)
        await asyncio.sleep(HEARTBEAT_SECONDS)


# ─── 11) APPLICATION ENTRYPOINT ─────────────────────────────────────────────────

def main():
    # (1) Fetch and print initial balance, set equity
    fetch_balance_and_set_equity()

    # (2) Start Flask server in a background thread
    flask_thread = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=HEALTH_PORT, debug=False, use_reloader=False),
        daemon=True
    )
    flask_thread.start()
    print(f"[{datetime.utcnow().isoformat()}] Flask health‐check listening on port {HEALTH_PORT}", flush=True)

    # (3) Start trading loop in background thread
    trading_thread = threading.Thread(target=run_trading_loop, daemon=True)
    trading_thread.start()
    print(f"[{datetime.utcnow().isoformat()}] Trading loop started.", flush=True)

    # (4) Run heartbeat in the main asyncio loop
    loop = asyncio.get_event_loop()
    loop.create_task(heartbeat())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print(f"[{datetime.utcnow().isoformat()}] KeyboardInterrupt caught. Shutting down.", flush=True)


if __name__ == "__main__":
    main()
