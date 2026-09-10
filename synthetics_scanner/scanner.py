import os
import json
import time
import math
import requests
import websocket
import pandas as pd
import numpy as np

# ============================================================
# CONFIG
# ============================================================

DERIV_APP_ID = os.getenv("DERIV_APP_ID", "1089")

DERIV_WS_URL = (
    f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

STATE_FILE = "state.json"

# Signal thresholds
SIGNAL_MIN = 7
WATCH_MIN = 5

# Instruments requested
TARGETS = {
    "CRASH 1000": [
        "Crash 1000",
        "Crash 1000 Index",
    ],
    "BOOM 1000": [
        "Boom 1000",
        "Boom 1000 Index",
    ],
    "CRASH 500": [
        "Crash 500",
        "Crash 500 Index",
    ],
}


# ============================================================
# LOGGING
# ============================================================

def log(message):
    print(message, flush=True)


# ============================================================
# STATE
# ============================================================

def load_state():
    try:
        if not os.path.exists(STATE_FILE):
            return {}

        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    except Exception:
        return {}


def save_state(state):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        log(f"STATE SAVE ERROR: {e}")


# ============================================================
# DERIV CONNECTION
# ============================================================

def deriv_connect():
    ws = websocket.create_connection(
        DERIV_WS_URL,
        timeout=30
    )

    return ws


def deriv_request(ws, payload, timeout=30):
    ws.settimeout(timeout)

    ws.send(json.dumps(payload))

    while True:
        raw = ws.recv()

        if not raw:
            continue

        data = json.loads(raw)

        if "error" in data:
            raise RuntimeError(
                data["error"].get(
                    "message",
                    str(data["error"])
                )
            )

        return data


# ============================================================
# ACTIVE SYMBOLS
# ============================================================

def get_active_symbols(ws):
    response = deriv_request(
        ws,
        {
            "active_symbols": "brief",
            "req_id": 1
        }
    )

    symbols = response.get("active_symbols", [])

    if not symbols:
        raise RuntimeError("Deriv returned no active symbols")

    return symbols


def get_symbol_name(item):
    return (
        item.get("display_name")
        or item.get("underlying_symbol_name")
        or item.get("name")
        or ""
    )


def get_symbol_code(item):
    return (
        item.get("symbol")
        or item.get("underlying_symbol")
        or ""
    )


def find_target_symbols(active_symbols):

    found = {}

    for target_name, possible_names in TARGETS.items():

        best = None

        for item in active_symbols:

            name = get_symbol_name(item)
            code = get_symbol_code(item)

            name_lower = name.lower()

            for wanted in possible_names:

                if wanted.lower() in name_lower:
                    best = code
                    break

            if best:
                break

        found[target_name] = best

    return found


# ============================================================
# HISTORICAL CANDLES
# ============================================================

def get_candles(ws, symbol, minutes, count=250):

    granularity = minutes * 60

    response = deriv_request(
        ws,
        {
            "ticks_history": symbol,
            "adjust_start_time": 1,
            "count": count,
            "end": "latest",
            "style": "candles",
            "granularity": granularity,
            "req_id": int(time.time() * 1000) % 100000000
        },
        timeout=40
    )

    candles = response.get("candles", [])

    if not candles:
        raise RuntimeError(
            f"No candles returned for {symbol} {minutes}m"
        )

    rows = []

    for candle in candles:

        try:
            rows.append({
                "time": pd.to_datetime(
                    int(candle["epoch"]),
                    unit="s",
                    utc=True
                ),
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"]),
            })

        except Exception:
            continue

    df = pd.DataFrame(rows)

    if df.empty:
        raise RuntimeError(
            f"Could not parse candles for {symbol}"
        )

    df = df.sort_values("time").drop_duplicates("time")

    # Remove the currently-forming candle.
    now = pd.Timestamp.now(tz="UTC")

    candle_delta = pd.Timedelta(minutes=minutes)

    cutoff = now.floor(f"{minutes}min")

    df = df[df["time"] < cutoff]

    if len(df) < 60:
        raise RuntimeError(
            f"Not enough completed candles for {symbol} {minutes}m"
        )

    return df.reset_index(drop=True)


# ============================================================
# INDICATORS
# ============================================================

def ema(series, length):
    return series.ewm(
        span=length,
        adjust=False
    ).mean()


def rsi(series, length=14):

    delta = series.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(
        alpha=1 / length,
        adjust=False
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / length,
        adjust=False
    ).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)

    result = 100 - (100 / (1 + rs))

    return result.fillna(50)


def atr(df, length=14):

    previous_close = df["close"].shift(1)

    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - previous_close).abs()
    tr3 = (df["low"] - previous_close).abs()

    true_range = pd.concat(
        [tr1, tr2, tr3],
        axis=1
    ).max(axis=1)

    return true_range.ewm(
        alpha=1 / length,
        adjust=False
    ).mean()


def supertrend(df, period=10, multiplier=3.0):

    high = df["high"]
    low = df["low"]
    close = df["close"]

    atr_value = atr(df, period)

    hl2 = (high + low) / 2

    upper_basic = hl2 + multiplier * atr_value
    lower_basic = hl2 - multiplier * atr_value

    upper = upper_basic.copy()
    lower = lower_basic.copy()

    direction = pd.Series(
        index=df.index,
        dtype="int64"
    )

    direction.iloc[0] = 1

    for i in range(1, len(df)):

        if (
            lower_basic.iloc[i] > lower.iloc[i - 1]
            or close.iloc[i - 1] < lower.iloc[i - 1]
        ):
            lower.iloc[i] = lower_basic.iloc[i]
        else:
            lower.iloc[i] = lower.iloc[i - 1]

        if (
            upper_basic.iloc[i] < upper.iloc[i - 1]
            or close.iloc[i - 1] > upper.iloc[i - 1]
        ):
            upper.iloc[i] = upper_basic.iloc[i]
        else:
            upper.iloc[i] = upper.iloc[i - 1]

        if direction.iloc[i - 1] == -1:

            if close.iloc[i] > upper.iloc[i]:
                direction.iloc[i] = 1
            else:
                direction.iloc[i] = -1

        else:

            if close.iloc[i] < lower.iloc[i]:
                direction.iloc[i] = -1
            else:
                direction.iloc[i] = 1

    return direction


# ============================================================
# TIMEFRAME ANALYSIS
# ============================================================

def timeframe_analysis(df):

    df = df.copy()

    df["ema20"] = ema(df["close"], 20)
    df["ema50"] = ema(df["close"], 50)
    df["rsi"] = rsi(df["close"], 14)
    df["atr"] = atr(df, 14)
    df["st"] = supertrend(df, 10, 3.0)

    last = df.iloc[-1]
    previous = df.iloc[-2]

    score = 0
    reasons = []

    # EMA trend
    if last["close"] > last["ema20"] > last["ema50"]:
        score += 1
        reasons.append("EMA bullish")

    elif last["close"] < last["ema20"] < last["ema50"]:
        score -= 1
        reasons.append("EMA bearish")

    # Supertrend
    if last["st"] == 1:
        score += 1
        reasons.append("Supertrend bullish")

    else:
        score -= 1
        reasons.append("Supertrend bearish")

    # RSI broad bias
    if last["rsi"] >= 55:
        score += 1
        reasons.append(
            f"RSI bullish {last['rsi']:.1f}"
        )

    elif last["rsi"] <= 45:
        score -= 1
        reasons.append(
            f"RSI bearish {last['rsi']:.1f}"
        )

    return {
        "score": score,
        "close": float(last["close"]),
        "ema20": float(last["ema20"]),
        "ema50": float(last["ema50"]),
        "rsi": float(last["rsi"]),
        "supertrend": int(last["st"]),
        "candle_time": last["time"].isoformat(),
        "reasons": reasons,
    }


# ============================================================
# 5M ENTRY CONFIRMATION
# ============================================================

def five_min_confirmation(df):

    df = df.copy()

    df["ema20"] = ema(df["close"], 20)
    df["st"] = supertrend(df, 10, 3.0)

    last = df.iloc[-1]
    previous = df.iloc[-2]

    bullish = False
    bearish = False

    reasons = []

    # Bullish confirmation
    if last["close"] > last["ema20"]:
        bullish = True
        reasons.append("5M above EMA20")

    if last["st"] == 1:
        bullish = True
        reasons.append("5M Supertrend bullish")

    if last["close"] > previous["high"]:
        bullish = True
        reasons.append("5M broke previous high")

    # Bearish confirmation
    bearish_reasons = []

    if last["close"] < last["ema20"]:
        bearish_reasons.append("5M below EMA20")

    if last["st"] == -1:
        bearish_reasons.append("5M Supertrend bearish")

    if last["close"] < previous["low"]:
        bearish_reasons.append("5M broke previous low")

    # We need at least 2 confirmations
    bull_count = 0

    if last["close"] > last["ema20"]:
        bull_count += 1

    if last["st"] == 1:
        bull_count += 1

    if last["close"] > previous["high"]:
        bull_count += 1

    bear_count = 0

    if last["close"] < last["ema20"]:
        bear_count += 1

    if last["st"] == -1:
        bear_count += 1

    if last["close"] < previous["low"]:
        bear_count += 1

    bullish = bull_count >= 2
    bearish = bear_count >= 2

    if bullish:
        reasons.extend([
            "5M bullish confirmation"
        ] + [
            x for x in [
                "5M above EMA20"
                if last["close"] > last["ema20"]
                else None,
                "5M Supertrend bullish"
                if last["st"] == 1
                else None,
                "5M broke previous high"
                if last["close"] > previous["high"]
                else None,
            ]
            if x
        ])

    if bearish:
        reasons.extend(
            ["5M bearish confirmation"] +
            bearish_reasons
        )

    return {
        "bullish": bullish,
        "bearish": bearish,
        "bull_count": bull_count,
        "bear_count": bear_count,
        "reasons": list(dict.fromkeys(reasons)),
        "candle_time": last["time"].isoformat(),
    }


# ============================================================
# MAIN SCORING ENGINE
# ============================================================

def score_market(
    h12,
    h4,
    h1,
    m15,
    m5
):

    buy_score = 0
    sell_score = 0

    buy_reasons = []
    sell_reasons = []

    # --------------------------------------------------------
    # 12H
    # --------------------------------------------------------

    if h12["score"] > 0:
        buy_score += 1
        buy_reasons.append("12H bullish context")

    elif h12["score"] < 0:
        sell_score += 1
        sell_reasons.append("12H bearish context")

    # --------------------------------------------------------
    # 4H
    # --------------------------------------------------------

    if h4["score"] > 0:
        buy_score += 1
        buy_reasons.append("4H bullish context")

    elif h4["score"] < 0:
        sell_score += 1
        sell_reasons.append("4H bearish context")

    # --------------------------------------------------------
    # 1H
    # --------------------------------------------------------

    if h1["score"] > 0:
        buy_score += 2
        buy_reasons.append("1H bullish bias")

    elif h1["score"] < 0:
        sell_score += 2
        sell_reasons.append("1H bearish bias")

    # --------------------------------------------------------
    # 15M
    # --------------------------------------------------------

    if m15["score"] > 0:
        buy_score += 3
        buy_reasons.append("15M bullish setup")

    elif m15["score"] < 0:
        sell_score += 3
        sell_reasons.append("15M bearish setup")

    # --------------------------------------------------------
    # 5M
    # --------------------------------------------------------

    if m5["bullish"]:
        buy_score += 2
        buy_reasons.append("5M entry confirmation")

    elif m5["bearish"]:
        sell_score += 2
        sell_reasons.append("5M entry confirmation")

    # --------------------------------------------------------
    # Decide technical direction
    # --------------------------------------------------------

    direction = "NONE"
    score = max(buy_score, sell_score)
    reasons = []

    if buy_score > sell_score:
        direction = "BUY"
        reasons = buy_reasons

    elif sell_score > buy_score:
        direction = "SELL"
        reasons = sell_reasons

    else:
        direction = "NONE"

    # Do not allow a weak 15M setup to become a signal
    if direction == "BUY" and m15["score"] <= 0:
        direction = "NONE"

    if direction == "SELL" and m15["score"] >= 0:
        direction = "NONE"

    # Technical status
    if direction != "NONE" and score >= SIGNAL_MIN:
        status = "SIGNAL"

    elif direction != "NONE" and score >= WATCH_MIN:
        status = "WATCH"

    else:
        status = "PASS"

    return {
        "direction": direction,
        "status": status,
        "buy_score": buy_score,
        "sell_score": sell_score,
        "score": score,
        "reasons": reasons,
    }


# ============================================================
# GROQ REVIEW
# ============================================================

def groq_review(
    instrument,
    technical,
    h12,
    h4,
    h1,
    m15,
    m5
):

    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY missing")

    prompt = f"""
You are the final reviewer for a synthetic-index technical scanner.

IMPORTANT:
- This is analysis only.
- Do NOT place trades.
- Do NOT invent market data.
- Do NOT change the technical direction unless the evidence clearly contradicts it.
- The scanner should NOT be excessively strict.
- A WATCH is allowed when the setup is developing.
- A SIGNAL is allowed when the technical score is strong enough.
- Return exactly one decision: BUY, SELL, WATCH, or PASS.

Instrument:
{instrument}

Technical proposal:
{json.dumps(technical, indent=2)}

12H:
{json.dumps(h12, indent=2)}

4H:
{json.dumps(h4, indent=2)}

1H:
{json.dumps(h1, indent=2)}

15M:
{json.dumps(m15, indent=2)}

5M:
{json.dumps(m5, indent=2)}

Rules:

1. If technical direction is BUY:
   - You may return BUY, WATCH, or PASS.
   - Do not return SELL.

2. If technical direction is SELL:
   - You may return SELL, WATCH, or PASS.
   - Do not return BUY.

3. If technical direction is NONE:
   - Return PASS.

4. Prefer WATCH when the setup is developing but not strong enough
   for a full signal.

5. Prefer PASS when the timeframes strongly conflict.

6. Never invent price levels.

Return JSON only:

{{
  "decision": "BUY|SELL|WATCH|PASS",
  "confidence": 0,
  "reason": "short explanation"
}}
"""

    from groq import Groq

    client = Groq(
        api_key=GROQ_API_KEY
    )

    response = client.chat.completions.create(
        model=GROQ_MODEL,
        temperature=0.1,
        max_tokens=300,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a disciplined technical market "
                    "reviewer. Return valid JSON only."
                ),
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
    )

    content = response.choices[0].message.content.strip()

    # Remove markdown fences if model adds them
    content = content.replace("```json", "")
    content = content.replace("```", "")
    content = content.strip()

    result = json.loads(content)

    decision = str(
        result.get("decision", "PASS")
    ).upper()

    if decision not in {
        "BUY",
        "SELL",
        "WATCH",
        "PASS",
    }:
        decision = "PASS"

    return {
        "decision": decision,
        "confidence": result.get("confidence", 0),
        "reason": result.get(
            "reason",
            "No reason provided"
        ),
    }


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):

    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN missing"
        )

    if not TELEGRAM_CHAT_ID:
        raise RuntimeError(
            "TELEGRAM_CHAT_ID missing"
        )

    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    response = requests.post(
        url,
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
        },
        timeout=20,
    )

    response.raise_for_status()


# ============================================================
# FORMAT ALERT
# ============================================================

def format_alert(
    instrument,
    groq_result,
    technical,
    h12,
    h4,
    h1,
    m15,
    m5
):

    decision = groq_result["decision"]

    emoji = {
        "BUY": "🟢",
        "SELL": "🔴",
        "WATCH": "🟡",
        "PASS": "⚪",
    }.get(decision, "⚪")

    lines = [
        f"{emoji} {decision} — {instrument}",
        "",
        f"Score: {technical['score']}",
        f"Buy score: {technical['buy_score']}",
        f"Sell score: {technical['sell_score']}",
        "",
        f"12H: {h12['score']}",
        f"4H: {h4['score']}",
        f"1H: {h1['score']}",
        f"15M: {m15['score']}",
        "",
        (
            f"5M confirmation: "
            f"Bull={m5['bull_count']} "
            f"Bear={m5['bear_count']}"
        ),
        "",
        f"AI confidence: {groq_result['confidence']}",
        f"AI review: {groq_result['reason']}",
        "",
        "Analysis only — no auto trading.",
    ]

    return "\n".join(lines)


# ============================================================
# SCAN ONE INSTRUMENT
# ============================================================

def scan_instrument(
    ws,
    instrument,
    symbol,
    state
):

    log("")
    log("=" * 60)
    log(f"SCANNING {instrument}")
    log(f"SYMBOL: {symbol}")
    log("=" * 60)

    # --------------------------------------------------------
    # Fetch all timeframes
    # --------------------------------------------------------

    h12_df = get_candles(
        ws,
        symbol,
        720,
        250
    )

    h4_df = get_candles(
        ws,
        symbol,
        240,
        250
    )

    h1_df = get_candles(
        ws,
        symbol,
        60,
        250
    )

    m15_df = get_candles(
        ws,
        symbol,
        15,
        250
    )

    m5_df = get_candles(
        ws,
        symbol,
        5,
        250
    )

    # --------------------------------------------------------
    # Indicators
    # --------------------------------------------------------

    h12 = timeframe_analysis(h12_df)
    h4 = timeframe_analysis(h4_df)
    h1 = timeframe_analysis(h1_df)
    m15 = timeframe_analysis(m15_df)

    m5 = five_min_confirmation(m5_df)

    # --------------------------------------------------------
    # Technical score
    # --------------------------------------------------------

    technical = score_market(
        h12,
        h4,
        h1,
        m15,
        m5
    )

    log(
        f"{instrument} "
        f"TECHNICAL "
        f"direction={technical['direction']} "
        f"status={technical['status']} "
        f"buy={technical['buy_score']} "
        f"sell={technical['sell_score']}"
    )

    # --------------------------------------------------------
    # If no technical setup, don't waste Groq call
    # --------------------------------------------------------

    if technical["status"] == "PASS":

        log(
            f"{instrument} PASS - "
            f"no valid technical setup"
        )

        return

    # --------------------------------------------------------
    # Groq is REQUIRED for every WATCH/SIGNAL
    # --------------------------------------------------------

    groq_result = groq_review(
        instrument,
        technical,
        h12,
        h4,
        h1,
        m15,
        m5
    )

    log(
        f"{instrument} GROQ "
        f"decision={groq_result['decision']} "
        f"confidence={groq_result['confidence']}"
    )

    decision = groq_result["decision"]

    # --------------------------------------------------------
    # Safety: AI must agree with technical direction
    # --------------------------------------------------------

    if decision in {"BUY", "SELL"}:

        if decision != technical["direction"]:

            log(
                f"{instrument} AI direction rejected: "
                f"technical={technical['direction']} "
                f"AI={decision}"
            )

            return

    # --------------------------------------------------------
    # PASS = nothing
    # --------------------------------------------------------

    if decision == "PASS":
        log(f"{instrument} GROQ PASS")
        return

    # --------------------------------------------------------
    # Deduplication
    # --------------------------------------------------------

    candle_time = m5["candle_time"]

    previous = state.get(instrument, {})

    previous_decision = previous.get(
        "decision"
    )

    previous_candle = previous.get(
        "candle_time"
    )

    if (
        previous_decision == decision
        and previous_candle == candle_time
    ):

        log(
            f"{instrument} DUPLICATE ALERT "
            f"- not sending"
        )

        return

    # --------------------------------------------------------
    # Send Telegram
    # --------------------------------------------------------

    message = format_alert(
        instrument,
        groq_result,
        technical,
        h12,
        h4,
        h1,
        m15,
        m5
    )

    send_telegram(message)

    log(
        f"{instrument} TELEGRAM ALERT SENT: "
        f"{decision}"
    )

    # --------------------------------------------------------
    # Save state
    # --------------------------------------------------------

    state[instrument] = {
        "decision": decision,
        "candle_time": candle_time,
        "timestamp": pd.Timestamp.now(
            tz="UTC"
        ).isoformat(),
    }

    save_state(state)


# ============================================================
# MAIN
# ============================================================

def main():

    log("")
    log("==============================================")
    log("SYNTHETIC INDICES SCANNER")
    log("==============================================")

    log(
        f"Deriv App ID: {DERIV_APP_ID}"
    )

    # --------------------------------------------------------
    # Check required environment variables
    # --------------------------------------------------------

    missing = []

    if not TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")

    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")

    if not GROQ_API_KEY:
        missing.append("GROQ_API_KEY")

    if missing:

        for item in missing:
            log(
                f"ERROR: {item} missing"
            )

        return

    # --------------------------------------------------------
    # Connect
    # --------------------------------------------------------

    ws = None

    try:

        log(
            "Connecting to Deriv..."
        )

        ws = deriv_connect()

        log(
            "Deriv WebSocket connected."
        )

        # ----------------------------------------------------
        # Discover symbols
        # ----------------------------------------------------

        active_symbols = get_active_symbols(
            ws
        )

        log(
            f"Deriv returned "
            f"{len(active_symbols)} active symbols."
        )

        symbols = find_target_symbols(
            active_symbols
        )

        for name, symbol in symbols.items():

            if symbol:
                log(
                    f"{name} -> {symbol}"
                )
            else:
                log(
                    f"{name} -> NOT FOUND"
                )

        # ----------------------------------------------------
        # State
        # ----------------------------------------------------

        state = load_state()

        # ----------------------------------------------------
        # Scan all targets
        # ----------------------------------------------------

        for instrument, symbol in symbols.items():

            if not symbol:

                log(
                    f"{instrument} SKIPPED - "
                    f"symbol not found"
                )

                continue

            try:

                scan_instrument(
                    ws,
                    instrument,
                    symbol,
                    state
                )

            except Exception as e:

                log(
                    f"{instrument} ERROR: {e}"
                )

        log("")
        log(
            "SCAN COMPLETED"
        )

    except Exception as e:

        log(
            f"FATAL ERROR: {e}"
        )

    finally:

        if ws:

            try:
                ws.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
