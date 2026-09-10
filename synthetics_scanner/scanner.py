import os
import json
import time
import requests
import websocket
import pandas as pd
import numpy as np


# ============================================================
# CONFIG
# ============================================================

# Public Deriv market-data WebSocket.
# No Deriv token or App ID is required.
DERIV_WS_URL = "wss://ws.binaryws.com/websockets/v3"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv(
    "GROQ_MODEL",
    "llama-3.3-70b-versatile"
)

STATE_FILE = "state.json"

# Score thresholds
WATCH_MIN = 5
SIGNAL_MIN = 7


# Requested indices.
# The scanner discovers their real Deriv symbol codes automatically.
TARGETS = {
    "CRASH 1000": [
        "crash 1000",
        "crash1000",
        "crash 1000 index",
    ],

    "BOOM 1000": [
        "boom 1000",
        "boom1000",
        "boom 1000 index",
    ],

    "CRASH 500": [
        "crash 500",
        "crash500",
        "crash 500 index",
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

        with open(
            STATE_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            return json.load(f)

    except Exception as e:

        log(f"STATE LOAD ERROR: {e}")

        return {}


def save_state(state):

    try:

        with open(
            STATE_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                state,
                f,
                indent=2
            )

    except Exception as e:

        log(f"STATE SAVE ERROR: {e}")


# ============================================================
# DERIV WEBSOCKET
# ============================================================

def connect_deriv():

    log("Connecting to Deriv...")

    ws = websocket.create_connection(
        DERIV_WS_URL,
        timeout=30
    )

    log("Deriv WebSocket connected.")

    return ws


def deriv_request(
    ws,
    payload,
    timeout=30
):

    ws.settimeout(timeout)

    ws.send(
        json.dumps(payload)
    )

    while True:

        raw = ws.recv()

        if not raw:
            continue

        data = json.loads(raw)

        if "error" in data:

            error = data["error"]

            raise RuntimeError(
                error.get(
                    "message",
                    str(error)
                )
            )

        return data


# ============================================================
# ACTIVE SYMBOLS
# ============================================================

def get_active_symbols(ws):

    log("Requesting Deriv active symbols...")

    # IMPORTANT:
    # Do NOT send product_type.
    # Current Deriv API removed that parameter.
    response = deriv_request(
        ws,
        {
            "active_symbols": "brief",
            "req_id": 1
        }
    )

    symbols = response.get(
        "active_symbols",
        []
    )

    if not symbols:

        raise RuntimeError(
            "Deriv returned no active symbols. "
            f"Full response: {response}"
        )

    log(
        f"Deriv returned "
        f"{len(symbols)} active symbols."
    )

    return symbols


# ============================================================
# SYMBOL HELPERS
# ============================================================

def symbol_code(item):

    # New API field
    if item.get("underlying_symbol"):
        return item["underlying_symbol"]

    # Legacy API field
    if item.get("symbol"):
        return item["symbol"]

    return ""


def symbol_name(item):

    # New API field
    if item.get("underlying_symbol_name"):
        return item["underlying_symbol_name"]

    # Legacy API field
    if item.get("display_name"):
        return item["display_name"]

    return ""


def find_symbol(
    active_symbols,
    target
):

    wanted_names = TARGETS[target]

    for item in active_symbols:

        code = symbol_code(item)
        name = symbol_name(item)

        combined = (
            f"{name} {code}"
        ).lower()

        for wanted in wanted_names:

            if wanted in combined:

                return code, name

    return None, None


def discover_symbols(
    active_symbols
):

    found = {}

    for target in TARGETS:

        code, name = find_symbol(
            active_symbols,
            target
        )

        found[target] = {
            "code": code,
            "name": name
        }

        if code:

            log(
                f"{target} -> "
                f"{code} "
                f"({name})"
            )

        else:

            log(
                f"{target} -> NOT FOUND"
            )

    return found


# ============================================================
# HISTORICAL CANDLES
# ============================================================

def get_candles(
    ws,
    symbol,
    minutes,
    count=250
):

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
            "req_id": int(
                time.time() * 1000
            ) % 100000000
        },
        timeout=45
    )

    candles = response.get(
        "candles",
        []
    )

    if not candles:

        raise RuntimeError(
            f"No candles returned for "
            f"{symbol} {minutes}m. "
            f"Response: {response}"
        )

    rows = []

    for candle in candles:

        try:

            rows.append(
                {
                    "time": pd.to_datetime(
                        int(candle["epoch"]),
                        unit="s",
                        utc=True
                    ),

                    "open": float(
                        candle["open"]
                    ),

                    "high": float(
                        candle["high"]
                    ),

                    "low": float(
                        candle["low"]
                    ),

                    "close": float(
                        candle["close"]
                    ),
                }
            )

        except Exception:
            continue

    df = pd.DataFrame(rows)

    if df.empty:

        raise RuntimeError(
            f"Could not parse candles "
            f"for {symbol}"
        )

    df = (
        df
        .sort_values("time")
        .drop_duplicates("time")
        .reset_index(drop=True)
    )

    # Remove the currently forming candle.
    now = pd.Timestamp.now(
        tz="UTC"
    )

    current_candle_start = (
        now.floor(f"{minutes}min")
    )

    df = df[
        df["time"] <
        current_candle_start
    ]

    if len(df) < 60:

        raise RuntimeError(
            f"Not enough completed candles "
            f"for {symbol} {minutes}m: "
            f"{len(df)}"
        )

    return df


# ============================================================
# INDICATORS
# ============================================================

def ema(
    series,
    length
):

    return series.ewm(
        span=length,
        adjust=False
    ).mean()


def rsi(
    series,
    length=14
):

    delta = series.diff()

    gain = delta.clip(
        lower=0
    )

    loss = -delta.clip(
        upper=0
    )

    avg_gain = gain.ewm(
        alpha=1 / length,
        adjust=False
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / length,
        adjust=False
    ).mean()

    rs = (
        avg_gain /
        avg_loss.replace(
            0,
            np.nan
        )
    )

    result = (
        100 -
        (100 / (1 + rs))
    )

    return result.fillna(50)


def atr(
    df,
    length=14
):

    previous_close = (
        df["close"].shift(1)
    )

    tr1 = (
        df["high"] -
        df["low"]
    )

    tr2 = (
        df["high"] -
        previous_close
    ).abs()

    tr3 = (
        df["low"] -
        previous_close
    ).abs()

    true_range = pd.concat(
        [tr1, tr2, tr3],
        axis=1
    ).max(axis=1)

    return true_range.ewm(
        alpha=1 / length,
        adjust=False
    ).mean()


def supertrend(
    df,
    period=10,
    multiplier=3.0
):

    high = df["high"]
    low = df["low"]
    close = df["close"]

    atr_value = atr(
        df,
        period
    )

    hl2 = (
        high + low
    ) / 2

    upper_basic = (
        hl2 +
        multiplier * atr_value
    )

    lower_basic = (
        hl2 -
        multiplier * atr_value
    )

    upper = upper_basic.copy()
    lower = lower_basic.copy()

    direction = pd.Series(
        index=df.index,
        dtype="int64"
    )

    direction.iloc[0] = 1

    for i in range(
        1,
        len(df)
    ):

        if (
            lower_basic.iloc[i]
            > lower.iloc[i - 1]
            or
            close.iloc[i - 1]
            < lower.iloc[i - 1]
        ):

            lower.iloc[i] = (
                lower_basic.iloc[i]
            )

        else:

            lower.iloc[i] = (
                lower.iloc[i - 1]
            )

        if (
            upper_basic.iloc[i]
            < upper.iloc[i - 1]
            or
            close.iloc[i - 1]
            > upper.iloc[i - 1]
        ):

            upper.iloc[i] = (
                upper_basic.iloc[i]
            )

        else:

            upper.iloc[i] = (
                upper.iloc[i - 1]
            )

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

def analyze_timeframe(df):

    df = df.copy()

    df["ema20"] = ema(
        df["close"],
        20
    )

    df["ema50"] = ema(
        df["close"],
        50
    )

    df["rsi"] = rsi(
        df["close"],
        14
    )

    df["atr"] = atr(
        df,
        14
    )

    df["st"] = supertrend(
        df,
        10,
        3.0
    )

    last = df.iloc[-1]

    score = 0

    reasons = []

    # EMA
    if (
        last["close"]
        >
        last["ema20"]
        >
        last["ema50"]
    ):

        score += 1

        reasons.append(
            "EMA bullish"
        )

    elif (
        last["close"]
        <
        last["ema20"]
        <
        last["ema50"]
    ):

        score -= 1

        reasons.append(
            "EMA bearish"
        )

    # Supertrend
    if last["st"] == 1:

        score += 1

        reasons.append(
            "Supertrend bullish"
        )

    else:

        score -= 1

        reasons.append(
            "Supertrend bearish"
        )

    # RSI
    if last["rsi"] >= 55:

        score += 1

        reasons.append(
            f"RSI bullish "
            f"{last['rsi']:.1f}"
        )

    elif last["rsi"] <= 45:

        score -= 1

        reasons.append(
            f"RSI bearish "
            f"{last['rsi']:.1f}"
        )

    return {
        "score": score,

        "close": float(
            last["close"]
        ),

        "ema20": float(
            last["ema20"]
        ),

        "ema50": float(
            last["ema50"]
        ),

        "rsi": float(
            last["rsi"]
        ),

        "supertrend": int(
            last["st"]
        ),

        "candle_time":
            last["time"].isoformat(),

        "reasons": reasons
    }


# ============================================================
# 5 MINUTE CONFIRMATION
# ============================================================

def analyze_5m(df):

    df = df.copy()

    df["ema20"] = ema(
        df["close"],
        20
    )

    df["st"] = supertrend(
        df,
        10,
        3.0
    )

    last = df.iloc[-1]
    previous = df.iloc[-2]

    bull_count = 0
    bear_count = 0

    bull_reasons = []
    bear_reasons = []

    # EMA
    if last["close"] > last["ema20"]:

        bull_count += 1

        bull_reasons.append(
            "above EMA20"
        )

    elif last["close"] < last["ema20"]:

        bear_count += 1

        bear_reasons.append(
            "below EMA20"
        )

    # Supertrend
    if last["st"] == 1:

        bull_count += 1

        bull_reasons.append(
            "Supertrend bullish"
        )

    else:

        bear_count += 1

        bear_reasons.append(
            "Supertrend bearish"
        )

    # Break
    if last["close"] > previous["high"]:

        bull_count += 1

        bull_reasons.append(
            "broke previous high"
        )

    if last["close"] < previous["low"]:

        bear_count += 1

        bear_reasons.append(
            "broke previous low"
        )

    bullish = (
        bull_count >= 2
    )

    bearish = (
        bear_count >= 2
    )

    return {
        "bullish": bullish,
        "bearish": bearish,

        "bull_count":
            bull_count,

        "bear_count":
            bear_count,

        "bull_reasons":
            bull_reasons,

        "bear_reasons":
            bear_reasons,

        "candle_time":
            last["time"].isoformat()
    }


# ============================================================
# SCORE ENGINE
# ============================================================

def calculate_score(
    h12,
    h4,
    h1,
    m15,
    m5
):

    buy = 0
    sell = 0

    buy_reasons = []
    sell_reasons = []

    # 12H = 1 point
    if h12["score"] > 0:

        buy += 1

        buy_reasons.append(
            "12H bullish"
        )

    elif h12["score"] < 0:

        sell += 1

        sell_reasons.append(
            "12H bearish"
        )

    # 4H = 1 point
    if h4["score"] > 0:

        buy += 1

        buy_reasons.append(
            "4H bullish"
        )

    elif h4["score"] < 0:

        sell += 1

        sell_reasons.append(
            "4H bearish"
        )

    # 1H = 2 points
    if h1["score"] > 0:

        buy += 2

        buy_reasons.append(
            "1H bullish"
        )

    elif h1["score"] < 0:

        sell += 2

        sell_reasons.append(
            "1H bearish"
        )

    # 15M = 3 points
    if m15["score"] > 0:

        buy += 3

        buy_reasons.append(
            "15M bullish setup"
        )

    elif m15["score"] < 0:

        sell += 3

        sell_reasons.append(
            "15M bearish setup"
        )

    # 5M = 2 points
    if m5["bullish"]:

        buy += 2

        buy_reasons.append(
            "5M bullish confirmation"
        )

    elif m5["bearish"]:

        sell += 2

        sell_reasons.append(
            "5M bearish confirmation"
        )

    # Direction
    if buy > sell:

        direction = "BUY"
        score = buy
        reasons = buy_reasons

    elif sell > buy:

        direction = "SELL"
        score = sell
        reasons = sell_reasons

    else:

        direction = "NONE"
        score = 0
        reasons = []

    # 15M must agree with direction.
    if direction == "BUY":

        if m15["score"] <= 0:

            direction = "NONE"
            score = 0
            reasons = []

    elif direction == "SELL":

        if m15["score"] >= 0:

            direction = "NONE"
            score = 0
            reasons = []

    # Status
    if (
        direction != "NONE"
        and score >= SIGNAL_MIN
    ):

        status = "SIGNAL"

    elif (
        direction != "NONE"
        and score >= WATCH_MIN
    ):

        status = "WATCH"

    else:

        status = "PASS"

    return {
        "direction": direction,
        "status": status,

        "score": score,

        "buy_score": buy,
        "sell_score": sell,

        "reasons": reasons
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

        raise RuntimeError(
            "GROQ_API_KEY missing"
        )

    from groq import Groq

    client = Groq(
        api_key=GROQ_API_KEY
    )

    prompt = f"""
You are the final technical reviewer
for a synthetic-indices scanner.

This system is analysis only.
Do NOT place trades.
Do NOT invent data.

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

- If technical direction is BUY,
  AI may return BUY, WATCH or PASS.
  Never return SELL.

- If technical direction is SELL,
  AI may return SELL, WATCH or PASS.
  Never return BUY.

- If technical direction is NONE,
  return PASS.

- Do not be excessively strict.

- WATCH is allowed when a setup is developing.

- PASS when timeframes strongly conflict.

Return JSON only:

{{
  "decision": "BUY|SELL|WATCH|PASS",
  "confidence": 0,
  "reason": "short explanation"
}}
"""

    response = client.chat.completions.create(
        model=GROQ_MODEL,
        temperature=0.1,
        max_tokens=250,

        messages=[
            {
                "role": "system",
                "content":
                    "Return valid JSON only."
            },

            {
                "role": "user",
                "content": prompt
            }
        ]
    )

    text = (
        response
        .choices[0]
        .message
        .content
        .strip()
    )

    text = text.replace(
        "```json",
        ""
    )

    text = text.replace(
        "```",
        ""
    ).strip()

    result = json.loads(text)

    decision = str(
        result.get(
            "decision",
            "PASS"
        )
    ).upper()

    if decision not in {
        "BUY",
        "SELL",
        "WATCH",
        "PASS"
    }:

        decision = "PASS"

    return {
        "decision": decision,

        "confidence":
            result.get(
                "confidence",
                0
            ),

        "reason":
            result.get(
                "reason",
                ""
            )
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
        "https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    response = requests.post(
        url,

        json={
            "chat_id":
                TELEGRAM_CHAT_ID,

            "text":
                message
        },

        timeout=20
    )

    response.raise_for_status()


# ============================================================
# TELEGRAM MESSAGE
# ============================================================

def create_message(
    instrument,
    technical,
    ai,
    h12,
    h4,
    h1,
    m15,
    m5
):

    decision = ai["decision"]

    if decision == "BUY":
        icon = "🟢"

    elif decision == "SELL":
        icon = "🔴"

    else:
        icon = "🟡"

    return (
        f"{icon} {decision} — {instrument}\n"
        f"\n"
        f"Score: {technical['score']}\n"
        f"Buy score: {technical['buy_score']}\n"
        f"Sell score: {technical['sell_score']}\n"
        f"\n"
        f"12H: {h12['score']}\n"
        f"4H: {h4['score']}\n"
        f"1H: {h1['score']}\n"
        f"15M: {m15['score']}\n"
        f"\n"
        f"5M Bull: {m5['bull_count']}\n"
        f"5M Bear: {m5['bear_count']}\n"
        f"\n"
        f"AI confidence: "
        f"{ai['confidence']}\n"
        f"\n"
        f"AI review: "
        f"{ai['reason']}\n"
        f"\n"
        f"Analysis only — "
        f"no auto trading."
    )


# ============================================================
# SCAN ONE INDEX
# ============================================================

def scan_index(
    ws,
    instrument,
    symbol,
    state
):

    log("")
    log("=" * 60)

    log(
        f"SCANNING {instrument}"
    )

    log(
        f"Symbol: {symbol}"
    )

    log("=" * 60)

    # --------------------------------------------------------
    # Get all required timeframes
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
    # Analyze
    # --------------------------------------------------------

    h12 = analyze_timeframe(
        h12_df
    )

    h4 = analyze_timeframe(
        h4_df
    )

    h1 = analyze_timeframe(
        h1_df
    )

    m15 = analyze_timeframe(
        m15_df
    )

    m5 = analyze_5m(
        m5_df
    )

    # --------------------------------------------------------
    # Score
    # --------------------------------------------------------

    technical = calculate_score(
        h12,
        h4,
        h1,
        m15,
        m5
    )

    log(
        f"{instrument} "
        f"direction={technical['direction']} "
        f"status={technical['status']} "
        f"buy={technical['buy_score']} "
        f"sell={technical['sell_score']}"
    )

    # --------------------------------------------------------
    # No setup
    # --------------------------------------------------------

    if technical["status"] == "PASS":

        log(
            f"{instrument} NO SETUP"
        )

        return

    # --------------------------------------------------------
    # Groq review
    # --------------------------------------------------------

    log(
        f"{instrument} "
        f"sending setup to Groq..."
    )

    ai = groq_review(
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
        f"decision={ai['decision']} "
        f"confidence={ai['confidence']}"
    )

    # --------------------------------------------------------
    # AI cannot reverse technical direction
    # --------------------------------------------------------

    if ai["decision"] in {
        "BUY",
        "SELL"
    }:

        if (
            ai["decision"]
            != technical["direction"]
        ):

            log(
                f"{instrument} "
                f"AI direction rejected."
            )

            return

    # --------------------------------------------------------
    # PASS
    # --------------------------------------------------------

    if ai["decision"] == "PASS":

        log(
            f"{instrument} GROQ PASS"
        )

        return

    # --------------------------------------------------------
    # Duplicate prevention
    # --------------------------------------------------------

    candle_time = m5[
        "candle_time"
    ]

    previous = state.get(
        instrument,
        {}
    )

    if (
        previous.get("decision")
        == ai["decision"]
        and
        previous.get("candle_time")
        == candle_time
    ):

        log(
            f"{instrument} "
            f"DUPLICATE - not sending"
        )

        return

    # --------------------------------------------------------
    # Telegram
    # --------------------------------------------------------

    message = create_message(
        instrument,
        technical,
        ai,
        h12,
        h4,
        h1,
        m15,
        m5
    )

    send_telegram(
        message
    )

    log(
        f"{instrument} "
        f"TELEGRAM ALERT SENT: "
        f"{ai['decision']}"
    )

    # --------------------------------------------------------
    # Save state
    # --------------------------------------------------------

    state[instrument] = {
        "
