import os
import time
import json
import math
import threading
import asyncio

from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from flask import Flask, jsonify
from binance.client import Client
from binance.exceptions import BinanceAPIException

# ─── 1) CONFIGURATION ─────────────────────────────────────────────────────────

# Load your Binance API key/secret from environment variables
API_KEY    = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
if not API_KEY or not API_SECRET:
    raise ValueError("Please set BINANCE_API_KEY and BINANCE_API_SECRET in the environment.")

# Symbol for futures
SYMBOL       = "BTCUSDT"
INTERVAL_5M  = Client.KLINE_INTERVAL_5MINUTE
INTERVAL_15M = Client.KLINE_INTERVAL_15MINUTE
INTERVAL_1D  = Client.KLINE_INTERVAL_1DAY

# Strategy parameters
TARGET_LEVERAGE   = 100
START_EQUITY      = 1000.0       # fallback if no real USDT is found
MAX_BARS_HELD     = 10           # exit breakeven after 10 x 5m bars
ATR_MULTIPLIER    = 0.5          # Take profit = entry_price + 0.5 × ATR_at_entry
HEARTBEAT_SECONDS = 300          # 5-minute heartbeat
STATE_FILE        = "bot_state.json"

# Casino: If your account really has USDT, the bot will detect it and use it as equity.
# Otherwise it uses START_EQUITY so you can still simulate “paper trading.”
# Flask / Health‐check port:
HEALTH_PORT = int(os.getenv("PORT", 8000))  # Render requires a valid $PORT


# ─── 2) GLOBAL STATE ────────────────────────────────────────────────────────────

#  (Load or initialize bot_state)
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

# Flask app
app = Flask(__name__)

# Binance REST client (futures)
client = Client(API_KEY, API_SECRET)


# ─── 3) FLASK ENDPOINTS ─────────────────────────────────────────────────────────

@app.route("/")
def home():
    return "Binance Futures Bot is running", 200


@app.route("/health")
def health():
    # A simple “OK” endpoint so you know the service is live
    return "OK", 200


@app.route("/balance")
def get_balance():
    """
    Returns your USDT futures balance (free + locked) as JSON.
    This endpoint shows the raw Binance futures wallet response.
    """
    try:
        balances = client.futures_account_balance()
        # Only return the USDT entry
        usdt_line = [b for b in balances if b["asset"] == "USDT"]
        return jsonify(usdt_line), 200
    except BinanceAPIException as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── 4) UTILITIES & INDICATOR CALCULATIONS ──────────────────────────────────────

def save_state():
    """Persist bot_state to disk."""
    with open(STATE_FILE, "w") as f:
        json.dump(bot_state, f, indent=2)
    print(f"[{datetime.utcnow().isoformat()}] State saved to {STATE_FILE}", flush=True)


def fetch_balance_and_set_equity():
    """
    Fetch your futures balance via REST.
    If USDT is found under 'free', use it as bot_state["equity"].
    Otherwise, default to START_EQUITY.
    Also prints the raw response so you can see where your 10 USDT lives.
    """
    try:
        balances = client.futures_account_balance()
        print(
            f"[{datetime.utcnow().isoformat()}] Raw futures_account_balance() response:",
            flush=True,
        )
        print(json.dumps(balances, indent=2), flush=True)

        # Find USDT line
        usdt = next((b for b in balances if b["asset"] == "USDT"), None)
        if usdt:
            free_amt = float(usdt.get("withdrawAvailable", 0.0))  # or 'availableBalance'
            if free_amt > 0.0:
                bot_state["equity"] = free_amt
                print(
                    f"[{datetime.utcnow().isoformat()}] Detected available USDT balance: {free_amt:.2f} USDT → using as equity.",
                    flush=True,
                )
                save_state()
                return
        # If we get here, either USDT line missing or free_amt == 0
        bot_state["equity"] = START_EQUITY
        print(
            f"[{datetime.utcnow().isoformat()}] No free USDT found. Falling back to START_EQUITY = {START_EQUITY:.2f}",
            flush=True,
        )
        save_state()

    except Exception as e:
        print(f"[{datetime.utcnow().isoformat()}] ERROR fetching balance: {e}", flush=True)
        bot_state["equity"] = START_EQUITY
        print(
            f"[{datetime.utcnow().isoformat()}] Falling back to START_EQUITY = {START_EQUITY:.2f}",
            flush=True,
        )
        save_state()


def fetch_klines(interval, limit):
    """
    Fetch recent klines (candles) for SYMBOL at the given interval, limit, via REST.
    Returns a DataFrame with columns: [timestamp, open, high, low, close, volume].
    """
    raw = client.futures_klines(symbol=SYMBOL, interval=interval, limit=limit)
    df = pd.DataFrame(raw, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "qav", "num_trades", "taker_base", "taker_quote", "ignore"
    ])
    df["timestamp"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    df = df[["timestamp", "open", "high", "low", "close", "volume"]].astype(float)
    return df


def compute_indicators(df5, df15, df1d):
    """
    Given:
      - df5  : DataFrame of 5m candles (with timestamp, open, high, low, close, volume)
      - df15 : DataFrame of 15m candles (with timestamp, open, high, low, close, volume)
      - df1d : DataFrame of daily candles (with pivot computed)

    Returns a dictionary of indicators for the **most recent** 5m bar:
      {
        timestamp,
        close, high,
        ema9, ema21, ema50,
        adx, rsi, atr,
        vwap,
        bull_engulf (boolean),
        ema50_15,
        pivot
      }
    If insufficient data, returns None.
    """
    # Must have ≥50 bars of 5m, ≥50 bars of 15m, ≥2 bars of 1d
    if len(df5) < 50 or len(df15) < 50 or len(df1d) < 2:
        return None

    # ── 1) EMAs on 5m ───────────────────────────────────────────────────────────
    df5["ema9"]  = df5["close"].ewm(span=9, adjust=False).mean()
    df5["ema21"] = df5["close"].ewm(span=21, adjust=False).mean()
    df5["ema50"] = df5["close"].ewm(span=50, adjust=False).mean()

    # ── 2) RSI(14) on 5m ─────────────────────────────────────────────────────────
    delta = df5["close"].diff()
    gain  = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    loss  = (-delta).clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    rs    = gain / loss
    df5["rsi"] = 100 - (100 / (1 + rs))

    # ── 3) ADX(14) on 5m ─────────────────────────────────────────────────────────
    high = df5["high"]
    low  = df5["low"]
    prev_close = df5["close"].shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    tr14 = tr.ewm(alpha=1/14, adjust=False).mean()
    up_move = high.diff()
    down_move = low.shift(1) - low
    plus_dm = ((up_move > down_move) & (up_move > 0)) * up_move
    minus_dm = ((down_move > up_move) & (down_move > 0)) * down_move
    plus_dm14  = plus_dm.ewm(alpha=1/14, adjust=False).mean()
    minus_dm14 = minus_dm.ewm(alpha=1/14, adjust=False).mean()
    plus_di = 100 * (plus_dm14 / tr14)
    minus_di = 100 * (minus_dm14 / tr14)
    dx = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di))
    df5["adx"] = dx.ewm(alpha=1/14, adjust=False).mean()

    # ── 4) ATR(14) on 5m ─────────────────────────────────────────────────────────
    df5["atr"] = tr.ewm(alpha=1/14, adjust=False).mean()

    # ── 5) VWAP (intraday) on 5m ─────────────────────────────────────────────────
    df5["date"] = df5["timestamp"].dt.floor("D")
    vwap_list = []
    for d, grp in df5.groupby("date"):
        tp = (grp["high"] + grp["low"] + grp["close"]) / 3
        cum_vp  = (grp["volume"] * tp).cumsum()
        cum_vol = grp["volume"].cumsum()
        vwap_list += list((cum_vp / cum_vol).fillna(method="ffill"))
    df5["vwap"] = vwap_list

    # ── 6) Bullish Engulfing on 5m ────────────────────────────────────────────────
    op_prev = df5["open"].shift(1)
    cl_prev = df5["close"].shift(1)
    df5["bull_engulf"] = (op_prev > cl_prev) & (df5["open"] < cl_prev) & (df5["close"] > op_prev)

    # Take the last row in df5 as “latest”
    latest = df5.iloc[-1]
    ts_latest = latest["timestamp"]

    # ── 7) 15m EMA50 (forward‐filled) ─────────────────────────────────────────────
    df15["ema50_15"] = df15["close"].ewm(span=50, adjust=False).mean()
    df15_valid = df15[df15["timestamp"] <= ts_latest]
    if df15_valid.empty:
        return None
    ema50_15_val = df15_valid.iloc[-1]["ema50_15"]

    # ── 8) Daily pivot ────────────────────────────────────────────────────────────
    pivot_val = df1d.loc[df1d.index.date == ts_latest.date(), "pivot"].values
    pivot_val = float(pivot_val[0]) if len(pivot_val) > 0 else np.nan

    # Debug–print all indicators
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


# ─── 5) TRADING LOGIC (ENTRY/EXIT) ──────────────────────────────────────────────

def calculate_entry_exit(ind):
    """
    Given the latest indicators, decide whether to enter or exit.

    If already in_position:
      - Increment bars_held
      - If high >= take_profit → exit by market sell at TP, update equity
      - Else if bars_held >= MAX_BARS_HELD → exit breakeven by limit sell at entry_price

    If not in_position:
      - Check all 6 entry conditions. If all true and ATR>0:
           → compute quantity at 100× leverage using current equity
           → market buy → update bot_state
      - Else, print exactly which flags failed.
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

    # 1) EXIT LOGIC
    if bot_state["in_position"]:
        bot_state["bars_held"] += 1
        ep     = bot_state["entry_price"]
        tp     = bot_state["take_profit"]
        qty    = bot_state["quantity"]
        equity = bot_state["equity"]

        # 1a) Take Profit
        if high >= tp:
            try:
                client.futures_create_order(
                    symbol=SYMBOL, side="SELL", type="MARKET", quantity=qty
                )
                R       = (tp - ep) / ep
                pnl     = equity * (TARGET_LEVERAGE * R)
                bot_state["equity"] = equity + pnl
                print(
                    f"[{ts.isoformat()}] ▶ TP hit. Sold {qty:.6f} BTC @ {tp:.2f}. P/L = {pnl:.2f} USDT.",
                    flush=True,
                )
            except BinanceAPIException as e:
                print(f"[{ts.isoformat()}] ERROR placing TP SELL order: {e}", flush=True)
            except Exception as e:
                print(f"[{ts.isoformat()}] Unexpected error at TP: {e}", flush=True)

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

        # 1b) Breakeven after MAX_BARS_HELD
        if bot_state["bars_held"] >= MAX_BARS_HELD:
            try:
                # Place a limit sell at entry_price, GTC
                order = client.futures_create_order(
                    symbol=SYMBOL,
                    side="SELL",
                    type="LIMIT",
                    quantity=qty,
                    price=f"{ep:.2f}",
                    timeInForce="GTC"
                )
                time.sleep(2)
                status = client.futures_get_order(symbol=SYMBOL, orderId=order["orderId"])
                if status["status"] != "FILLED":
                    client.futures_cancel_order(symbol=SYMBOL, orderId=order["orderId"])
                    client.futures_create_order(symbol=SYMBOL, side="SELL", type="MARKET", quantity=qty)
                print(
                    f"[{ts.isoformat()}] ▶ Breakeven exit. Sold {qty:.6f} BTC @ {ep:.2f}. P/L = 0.",
                    flush=True,
                )
            except BinanceAPIException as e:
                print(f"[{ts.isoformat()}] ERROR placing breakeven SELL: {e}", flush=True)
            except Exception as e:
                print(f"[{ts.isoformat()}] Unexpected error at breakeven: {e}", flush=True)

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

    # 2) ENTRY LOGIC (only if not in_position)
    if not bot_state["in_position"]:
        cond1 = (ema9 > ema21 > ema50)           # 5m EMA stacking
        cond2 = (adx > 20) and (rsi > 55)        # ADX + RSI
        cond3 = (price > ema50_15)               # 15m EMA50 confirmation
        cond4 = (price > vwap)                   # VWAP
        cond5 = (price > pivot)                  # daily pivot
        cond6 = bull                            # bullish engulfing
        all_ok = all([cond1, cond2, cond3, cond4, cond5, cond6, atr > 0])

        if all_ok:
            entry_price = price
            take_profit = entry_price + ATR_MULTIPLIER * atr
            equity      = bot_state["equity"]
            raw_qty     = (equity * TARGET_LEVERAGE) / entry_price

            # Determine quantity precision (Step Size) from exchange info
            try:
                info = client.futures_exchange_info()
                sym_info = next((s for s in info["symbols"] if s["symbol"] == SYMBOL), None)
                step_size = float(sym_info["filters"][2]["stepSize"])  # LOT_SIZE filter index is usually 2
            except Exception as e:
                print(f"[{ts.isoformat()}] ERROR fetching step size: {e}. Defaulting to 0.000001", flush=True)
                step_size = 0.000001

            qty = math.floor(raw_qty / step_size) * step_size
            qty = round(qty, 6)  # Binance typically allows up to 6 decimal places for BTCUSDT

            if qty <= 0:
                print(
                    f"[{ts.isoformat()}] ⚠ Computed qty ≤ 0 (equity={equity:.2f}), skipping entry.",
                    flush=True,
                )
                return

            # Place market BUY
            try:
                client.futures_create_order(
                    symbol=SYMBOL, side="BUY", type="MARKET", quantity=qty
                )
                print(
                    f"[{ts.isoformat()}] ▶ Enter LONG. Bought {qty:.6f} BTC @ {entry_price:.2f}. TP={take_profit:.2f}",
                    flush=True,
                )
            except BinanceAPIException as e:
                print(f"[{ts.isoformat()}] ERROR placing BUY order: {e}", flush=True)
                return
            except Exception as e:
                print(f"[{ts.isoformat()}] Unexpected error placing BUY: {e}", flush=True)
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
            # Print exactly which flags failed
            tstr = ts.isoformat()
            print(
                f"[{tstr}] ❌ Entry conditions not met: "
                f"cond1={cond1}, cond2={cond2}, cond3={cond3}, cond4={cond4}, cond5={cond5}, cond6={cond6} → skipping entry.",
                flush=True,
            )
            return


# ─── 6) MAIN LOOP (POLLING) ─────────────────────────────────────────────────────

def run_trading_loop():
    """
    This runs in a background thread. It:
      - Sleeps until a new 5-minute candle has fully closed.
      - Fetches the latest klines (5m, 15m, 1d) via REST.
      - Computes indicators and runs entry/exit logic.
      - Loops forever.
    """
    while True:
        # 1) Align to the next 5m candle close + 1 second buffer
        now = datetime.utcnow().replace(second=0, microsecond=0)
        # e.g. if now = 12:07:xx, next 5m boundary is 12:10:00
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

        sleep_seconds = (nxt - datetime.utcnow()).total_seconds()
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        # 2) It’s now 1 second into a fresh 5m candle → fetch last 100 x 5m, 50 x 15m, 2 x 1d
        try:
            df5  = fetch_klines(INTERVAL_5M,  100)
            df15 = fetch_klines(INTERVAL_15M, 50)
            df1d_raw = fetch_klines(INTERVAL_1D, 2)

            # Compute daily pivot on df1d_raw
            df1d_raw["pivot"] = (df1d_raw["high"].shift(1) + df1d_raw["low"].shift(1) + df1d_raw["close"].shift(1)) / 3
            df1d = df1d_raw.set_index("timestamp")[["open", "high", "low", "close", "volume", "pivot"]]

            # Compute indicators for the latest 5m bar
            ind = compute_indicators(df5, df15, df1d)
            if ind is not None:
                calculate_entry_exit(ind)

        except Exception as e:
            print(f"[{datetime.utcnow().isoformat()}] ERROR in trading loop: {e}", flush=True)

        # Next iteration will align to the following 5m boundary

# ─── 7) HEARTBEAT ───────────────────────────────────────────────────────────────

async def heartbeat():
    """
    Prints a “still running” message every HEARTBEAT_SECONDS.
    """
    while True:
        print(f"[{datetime.utcnow().isoformat()}] 💓 Heartbeat: still running …", flush=True)
        await asyncio.sleep(HEARTBEAT_SECONDS)


# ─── 8) APPLICATION ENTRYPOINT ──────────────────────────────────────────────────

def main():
    # 1) Print detected balance (and set equity)
    fetch_balance_and_set_equity()

    # 2) Start the Flask server in a background thread
    flask_thread = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=HEALTH_PORT, debug=False, use_reloader=False),
        daemon=True
    )
    flask_thread.start()
    print(f"[{datetime.utcnow().isoformat()}] Flask health‐check running on port {HEALTH_PORT}", flush=True)

    # 3) Start the trading loop in a background thread
    trading_thread = threading.Thread(target=run_trading_loop, daemon=True)
    trading_thread.start()
    print(f"[{datetime.utcnow().isoformat()}] Trading loop started.", flush=True)

    # 4) Start asyncio event loop for heartbeat
    loop = asyncio.get_event_loop()
    loop.create_task(heartbeat())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print(f"[{datetime.utcnow().isoformat()}] KeyboardInterrupt → Shutting down gracefully.", flush=True)


if __name__ == "__main__":
    main()
