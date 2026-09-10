import os
import json
import time
import requests
import websocket
import pandas as pd
import numpy as np


# ============================================================
# CONFIGURATION
# ============================================================

DERIV_WS_URL = (
    "wss://api.derivws.com/"
    "trading/v1/options/ws/public"
)

STATE_FILE = "state.json"

TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    ""
)

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    ""
)

GROQ_API_KEY = os.getenv(
    "GROQ_API_KEY",
    ""
)

GROQ_MODEL = os.getenv(
    "GROQ_MODEL",
    "openai/gpt-oss-120b"
)

# ------------------------------------------------------------
# SCORE SETTINGS
# ------------------------------------------------------------

WATCH_MIN = 5
SIGNAL_MIN = 7


# ============================================================
# TARGET INDICES
# ============================================================

TARGETS = {
    "CRASH 1000": [
        "crash 1000",
        "crash1000"
    ],

    "BOOM 1000": [
        "boom 1000",
        "boom1000"
    ],

    "CRASH 500": [
        "crash 500",
        "crash500"
    ]
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

        if not os.path.exists(
            STATE_FILE
        ):
            return {}

        with open(
            STATE_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            return json.load(f)

    except Exception as e:

        log(
            f"STATE LOAD ERROR: {e}"
        )

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

        log(
            f"STATE SAVE ERROR: {e}"
        )


# ============================================================
# DERIV CONNECTION
# ============================================================

def connect_deriv():

    log(
        "Connecting to Deriv..."
    )

    ws = websocket.create_connection(
        DERIV_WS_URL,
        timeout=30
    )

    log(
        "Deriv WebSocket connected."
    )

    return ws


# ============================================================
# DERIV REQUEST
# ============================================================

def deriv_request(
    ws,
    payload,
    timeout=60
):

    ws.settimeout(timeout)

    ws.send(
        json.dumps(payload)
    )

    request_id = payload.get(
        "req_id"
    )

    deadline = (
        time.time() + timeout
    )

    while time.time() < deadline:

        raw = ws.recv()

        if not raw:
            continue

        data = json.loads(raw)

        if data.get("error"):

            raise RuntimeError(
                str(data["error"])
            )

        if data.get("errors"):

            raise RuntimeError(
                str(data["errors"])
            )

        if (
            request_id is None
            or
            data.get("req_id")
            == request_id
        ):

            return data

    raise TimeoutError(
        "Deriv response timeout"
    )


# ============================================================
# ACTIVE SYMBOLS
# ============================================================

def get_active_symbols(ws):

    log(
        "Requesting active symbols..."
    )

    response = deriv_request(
        ws,
        {
            "active_symbols": "brief",
            "req_id": 1001
        }
    )

    symbols = response.get(
        "active_symbols",
        []
    )

    if not symbols:

        raise RuntimeError(
            "Deriv returned no active "
            f"symbols: {response}"
        )

    log(
        f"Deriv returned "
        f"{len(symbols)} active symbols."
    )

    return symbols


# ============================================================
# SYMBOL HELPERS
# ============================================================

def get_symbol_code(item):

    return str(
        item.get(
            "underlying_symbol"
        )
        or
        item.get(
            "symbol"
        )
        or
        ""
    )


def get_symbol_name(item):

    return str(
        item.get(
            "underlying_symbol_name"
        )
        or
        item.get(
            "display_name"
        )
        or
        ""
    )


# ============================================================
# DISCOVER TARGET SYMBOLS
# ============================================================

def discover_symbols(
    active_symbols
):

    results = {}

    for target, aliases in TARGETS.items():

        found = None

        for item in active_symbols:

            code = get_symbol_code(
                item
            )

            name = get_symbol_name(
                item
            )

            combined = (
                name
                + " "
                + code
            ).lower()

            for alias in aliases:

                if alias in combined:

                    found = (
                        code,
                        name
                    )

                    break

            if found:
                break

        results[target] = found

        if found:

            log(
                f"{target} -> "
                f"{found[0]} "
                f"({found[1]})"
            )

        else:

            log(
                f"{target} -> NOT FOUND"
            )

    return results


# ============================================================
# GET CANDLES
# ============================================================

def get_candles(
    ws,
    symbol,
    minutes,
    count=250
):

    # Deriv currently supports these
    # candle granularities.

    supported = {
        5: 300,
        15: 900,
        60: 3600,
        240: 14400
    }

    if minutes not in supported:

        raise RuntimeError(
            f"Unsupported timeframe: "
            f"{minutes} minutes"
        )

    granularity = supported[
        minutes
    ]

    req_id = (
        int(
            time.time() * 1000
        )
        % 1000000000
    )

    payload = {

        "ticks_history":
            symbol,

        "end":
            "latest",

        "style":
            "candles",

        "granularity":
            granularity,

        "count":
            count,

        "adjust_start_time":
            1,

        "req_id":
            req_id
    }

    response = deriv_request(
        ws,
        payload,
        timeout=60
    )

    candles = response.get(
        "candles",
        []
    )

    if not candles:

        raise RuntimeError(
            f"No candles returned "
            f"for {symbol} "
            f"{minutes}m: "
            f"{response}"
        )

    rows = []

    for candle in candles:

        try:

            rows.append(
                {
                    "time":
                        pd.to_datetime(
                            int(
                                candle[
                                    "epoch"
                                ]
                            ),
                            unit="s",
                            utc=True
                        ),

                    "open":
                        float(
                            candle["open"]
                        ),

                    "high":
                        float(
                            candle["high"]
                        ),

                    "low":
                        float(
                            candle["low"]
                        ),

                    "close":
                        float(
                            candle["close"]
                        )
                }
            )

        except Exception:

            continue

    if not rows:

        raise RuntimeError(
            f"Could not parse candles "
            f"for {symbol}"
        )

    df = pd.DataFrame(
        rows
    )

    df = (
        df
        .sort_values("time")
        .drop_duplicates(
            "time"
        )
        .reset_index(
            drop=True
        )
    )

    # --------------------------------------------------------
    # REMOVE CURRENT INCOMPLETE CANDLE
    # --------------------------------------------------------

    now = pd.Timestamp.now(
        tz="UTC"
    )

    current_start = now.floor(
        f"{minutes}min"
    )

    df = df[
        df["time"]
        <
        current_start
    ].reset_index(
        drop=True
    )

    if len(df) < 60:

        raise RuntimeError(
            f"Only {len(df)} completed "
            f"candles for {symbol} "
            f"{minutes}m"
        )

    return df


# ============================================================
# BUILD 12H FROM 4H
# ============================================================

def resample_12h(df):

    data = df.copy()

    data = data.sort_values(
        "time"
    )

    data = data.set_index(
        "time"
    )

    result = (
        data
        .resample(
            "12h",
            origin="epoch"
        )
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last"
            }
        )
    )

    result = result.dropna()

    result = result.reset_index()

    # --------------------------------------------------------
    # REMOVE CURRENT INCOMPLETE 12H CANDLE
    # --------------------------------------------------------

    now = pd.Timestamp.now(
        tz="UTC"
    )

    current_12h_start = (
        now.normalize()
    )

    if now.hour >= 12:

        current_12h_start += (
            pd.to_timedelta(
                12,
                unit="h"
            )
        )

    result = result[
        result["time"]
        <
        current_12h_start
    ].reset_index(
        drop=True
    )

    if len(result) < 60:

        raise RuntimeError(
            "Not enough 12H candles "
            "after 4H aggregation"
        )

    return result


# ============================================================
# EMA
# ============================================================

def ema(
    series,
    length
):

    return series.ewm(
        span=length,
        adjust=False
    ).mean()


# ============================================================
# RSI
# ============================================================

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
        avg_gain
        /
        avg_loss.replace(
            0,
            np.nan
        )
    )

    result = (
        100
        -
        (
            100
            /
            (1 + rs)
        )
    )

    return result.fillna(
        50
    )


# ============================================================
# ATR
# ============================================================

def atr(
    df,
    length=14
):

    previous_close = (
        df["close"].shift(1)
    )

    tr1 = (
        df["high"]
        -
        df["low"]
    )

    tr2 = (
        df["high"]
        -
        previous_close
    ).abs()

    tr3 = (
        df["low"]
        -
        previous_close
    ).abs()

    true_range = pd.concat(
        [
            tr1,
            tr2,
            tr3
        ],
        axis=1
    ).max(
        axis=1
    )

    return true_range.ewm(
        alpha=1 / length,
        adjust=False
    ).mean()


# ============================================================
# SUPERTREND
# ============================================================

def supertrend(
    df,
    period=10,
    multiplier=3.0
):

    atr_value = atr(
        df,
        period
    )

    hl2 = (
        df["high"]
        +
        df["low"]
    ) / 2

    upper_basic = (
        hl2
        +
        multiplier
        *
        atr_value
    )

    lower_basic = (
        hl2
        -
        multiplier
        *
        atr_value
    )

    upper = upper_basic.copy()

    lower = lower_basic.copy()

    direction = pd.Series(
        1,
        index=df.index,
        dtype=int
    )

    for i in range(
        1,
        len(df)
    ):

        if (
            lower_basic.iloc[i]
            >
            lower.iloc[i - 1]
            or
            df["close"].iloc[i - 1]
            <
            lower.iloc[i - 1]
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
            <
            upper.iloc[i - 1]
            or
            df["close"].iloc[i - 1]
            >
            upper.iloc[i - 1]
        ):

            upper.iloc[i] = (
                upper_basic.iloc[i]
            )

        else:

            upper.iloc[i] = (
                upper.iloc[i - 1]
            )

        if (
            direction.iloc[i - 1]
            == -1
        ):

            if (
                df["close"].iloc[i]
                >
                upper.iloc[i]
            ):

                direction.iloc[i] = 1

            else:

                direction.iloc[i] = -1

        else:

            if (
                df["close"].iloc[i]
                <
                lower.iloc[i]
            ):

                direction.iloc[i] = -1

            else:

                direction.iloc[i] = 1

    return direction


# ============================================================
# TIMEFRAME ANALYSIS
# ============================================================

def analyze_timeframe(
    df
):

    data = df.copy()

    data["ema20"] = ema(
        data["close"],
        20
    )

    data["ema50"] = ema(
        data["close"],
        50
    )

    data["rsi"] = rsi(
        data["close"],
        14
    )

    data["atr"] = atr(
        data,
        14
    )

    data["st"] = supertrend(
        data,
        10,
        3.0
    )

    last = data.iloc[-1]

    score = 0

    reasons = []

    # --------------------------------------------------------
    # EMA
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # SUPERTREND
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

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

        "score":
            int(score),

        "close":
            float(
                last["close"]
            ),

        "ema20":
            float(
                last["ema20"]
            ),

        "ema50":
            float(
                last["ema50"]
            ),

        "rsi":
            float(
                last["rsi"]
            ),

        "supertrend":
            int(
                last["st"]
            ),

        "candle_time":
            last[
                "time"
            ].isoformat(),

        "reasons":
            reasons
    }


# ============================================================
# 5M ENTRY CONFIRMATION
# ============================================================

def analyze_5m(
    df
):

    data = df.copy()

    data["ema20"] = ema(
        data["close"],
        20
    )

    data["st"] = supertrend(
        data,
        10,
        3.0
    )

    last = data.iloc[-1]

    previous = data.iloc[-2]

    bull_count = 0

    bear_count = 0

    bull_reasons = []

    bear_reasons = []

    # --------------------------------------------------------
    # EMA
    # --------------------------------------------------------

    if (
        last["close"]
        >
        last["ema20"]
    ):

        bull_count += 1

        bull_reasons.append(
            "above EMA20"
        )

    elif (
        last["close"]
        <
        last["ema20"]
    ):

        bear_count += 1

        bear_reasons.append(
            "below EMA20"
        )

    # --------------------------------------------------------
    # SUPERTREND
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # PREVIOUS CANDLE BREAK
    # --------------------------------------------------------

    if (
        last["close"]
        >
        previous["high"]
    ):

        bull_count += 1

        bull_reasons.append(
            "broke previous high"
        )

    if (
        last["close"]
        <
        previous["low"]
    ):

        bear_count += 1

        bear_reasons.append(
            "broke previous low"
        )

    return {

        "bullish":
            bull_count >= 2,

        "bearish":
            bear_count >= 2,

        "bull_count":
            bull_count,

        "bear_count":
            bear_count,

        "bull_reasons":
            bull_reasons,

        "bear_reasons":
            bear_reasons,

        "candle_time":
            last[
                "time"
            ].isoformat()
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

    # --------------------------------------------------------
    # 12H = 1 POINT
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # 4H = 1 POINT
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # 1H = 2 POINTS
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # 15M = 3 POINTS
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # 5M = 2 POINTS
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # FINAL DIRECTION
    # --------------------------------------------------------

    if (
        buy > sell
        and
        m15["score"] > 0
    ):

        direction = "BUY"

        score = buy

        reasons = buy_reasons

    elif (
        sell > buy
        and
        m15["score"] < 0
    ):

        direction = "SELL"

        score = sell

        reasons = sell_reasons

    else:

        direction = "NONE"

        score = 0

        reasons = []

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    if score >= SIGNAL_MIN:

        status = "SIGNAL"

    elif score >= WATCH_MIN:

        status = "WATCH"

    else:

        status = "PASS"

    return {

        "direction":
            direction,

        "status":
            status,

        "score":
            score,

        "buy_score":
            buy,

        "sell_score":
            sell,

        "reasons":
            reasons
    }


# ============================================================
# SAFE JSON EXTRACTION
# ============================================================

def extract_json_object(
    text
):

    if not text:

        return None

    text = str(
        text
    ).strip()

    # Remove markdown fences.

    text = text.replace(
        "```json",
        ""
    )

    text = text.replace(
        "```JSON",
        ""
    )

    text = text.replace(
        "```",
        ""
    )

    text = text.strip()

    # --------------------------------------------------------
    # DIRECT JSON
    # --------------------------------------------------------

    try:

        return json.loads(
            text
        )

    except Exception:

        pass

    # --------------------------------------------------------
    # FIND JSON OBJECT INSIDE RESPONSE
    # --------------------------------------------------------

    start = text.find(
        "{"
    )

    end = text.rfind(
        "}"
    )

    if (
        start >= 0
        and
        end > start
    ):

        candidate = text[
            start:end + 1
        ]

        try:

            return json.loads(
                candidate
            )

        except Exception:

            pass

    return None


# ============================================================
# GROQ FINAL REVIEW
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
for a synthetic indices signal scanner.

This system is analysis only.
Never place trades.

Instrument:
{instrument}

Technical proposal:
{json.dumps(technical)}

12H:
{json.dumps(h12)}

4H:
{json.dumps(h4)}

1H:
{json.dumps(h1)}

15M:
{json.dumps(m15)}

5M:
{json.dumps(m5)}

Rules:

1. If technical direction is BUY,
you may return BUY, WATCH, or PASS.

2. If technical direction is SELL,
you may return SELL, WATCH, or PASS.

3. If technical direction is NONE,
return PASS.

4. Do not reverse BUY into SELL.

5. Do not reverse SELL into BUY.

6. Do not be excessively strict.

7. WATCH is acceptable when a setup
is developing but is not yet strong enough.

Return ONLY a JSON object.

Example:

{{
  "decision": "BUY",
  "confidence": 80,
  "reason": "Higher timeframes and the 15M setup support the direction."
}}

The decision must be exactly:
BUY, SELL, WATCH, or PASS.

Confidence must be a number from 0 to 100.
"""

    # --------------------------------------------------------
    # GROQ API CALL
    # --------------------------------------------------------

    try:

        response = (
            client
            .chat
            .completions
            .create(

                model=GROQ_MODEL,

                messages=[
                    {
                        "role":
                            "system",

                        "content":
                            "You are a technical "
                            "analysis assistant. "
                            "Return only valid JSON."
                    },

                    {
                        "role":
                            "user",

                        "content":
                            prompt
                    }
                ],

                temperature=0.1,

                max_tokens=300,

                response_format={
                    "type":
                        "json_object"
                }
            )
        )

    except Exception as e:

        log(
            f"GROQ API ERROR: {e}"
        )

        return {

            "decision":
                "PASS",

            "confidence":
                0,

            "reason":
                "Groq API error"
        }

    # --------------------------------------------------------
    # GET GROQ TEXT
    # --------------------------------------------------------

    try:

        message = (
            response
            .choices[0]
            .message
        )

        text = (
            message.content
            or
            ""
        )

    except Exception as e:

        log(
            f"GROQ RESPONSE ERROR: {e}"
        )

        return {

            "decision":
                "PASS",

            "confidence":
                0,

            "reason":
                "Empty Groq response"
        }

    # --------------------------------------------------------
    # PARSE JSON
    # --------------------------------------------------------

    result = extract_json_object(
        text
    )

    if result is None:

        log(
            "GROQ returned invalid "
            "or empty JSON."
        )

        log(
            "GROQ RAW RESPONSE: "
            f"{repr(text[:500])}"
        )

        return {

            "decision":
                "PASS",

            "confidence":
                0,

            "reason":
                "Groq returned invalid JSON"
        }

    # --------------------------------------------------------
    # VALIDATE DECISION
    # --------------------------------------------------------

    decision = str(
        result.get(
            "decision",
            "PASS"
        )
    ).upper().strip()

    if decision not in {
        "BUY",
        "SELL",
        "WATCH",
        "PASS"
    }:

        decision = "PASS"

    # --------------------------------------------------------
    # CONFIDENCE
    # --------------------------------------------------------

    try:

        confidence = float(
            result.get(
                "confidence",
                0
            )
        )

    except Exception:

        confidence = 0

    confidence = max(
        0,
        min(
            100,
            confidence
        )
    )

    # --------------------------------------------------------
    # REASON
    # --------------------------------------------------------

    reason = str(
        result.get(
            "reason",
            ""
        )
    ).strip()

    if not reason:

        reason = (
            "No AI explanation returned."
        )

    return {

        "decision":
            decision,

        "confidence":
            confidence,

        "reason":
            reason
    }


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(
    message
):

    if not TELEGRAM_BOT_TOKEN:

        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN missing"
        )

    if not TELEGRAM_CHAT_ID:

        raise RuntimeError(
            "TELEGRAM_CHAT_ID missing"
        )

    token = (
        TELEGRAM_BOT_TOKEN
        .strip()
    )

    chat_id = (
        TELEGRAM_CHAT_ID
        .strip()
    )

    url = (
        "https://api.telegram.org/"
        f"bot{token}"
        "/sendMessage"
    )

    payload = {

        "chat_id":
            chat_id,

        "text":
            str(message)
    }

    try:

        response = requests.post(
            url,
            json=payload,
            timeout=20
        )

    except Exception as e:

        raise RuntimeError(
            "Telegram connection error: "
            f"{e}"
        )

    # --------------------------------------------------------
    # TELEGRAM ERROR
    # --------------------------------------------------------

    if not response.ok:

        try:

            details = (
                response.json()
            )

            error_code = details.get(
                "error_code",
                response.status_code
            )

            description = details.get(
                "description",
                "Unknown Telegram error"
            )

        except Exception:

            error_code = (
                response.status_code
            )

            description = (
                response.text
                or
                "Unknown Telegram error"
            )

        raise RuntimeError(
            "Telegram rejected message: "
            f"error_code={error_code}, "
            f"description={description}"
        )

    # --------------------------------------------------------
    # PARSE TELEGRAM RESPONSE
    # --------------------------------------------------------

    try:

        result = response.json()

    except Exception:

        raise RuntimeError(
            "Telegram returned an "
            "invalid response."
        )

    if result.get("ok") is not True:

        raise RuntimeError(
            "Telegram API error: "
            f"{result.get('description', 'Unknown error')}"
        )

    return result


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

    log(
        "=" * 60
    )

    log(
        f"SCANNING {instrument}"
    )

    log(
        f"Symbol: {symbol}"
    )

    log(
        "=" * 60
    )

    # --------------------------------------------------------
    # 4H
    # --------------------------------------------------------

    h4_df = get_candles(
        ws,
        symbol,
        240,
        300
    )

    # --------------------------------------------------------
    # 12H FROM 4H
    # --------------------------------------------------------

    h12_df = resample_12h(
        h4_df
    )

    # --------------------------------------------------------
    # 1H
    # --------------------------------------------------------

    h1_df = get_candles(
        ws,
        symbol,
        60,
        250
    )

    # --------------------------------------------------------
    # 15M
    # --------------------------------------------------------

    m15_df = get_candles(
        ws,
        symbol,
        15,
        250
    )

    # --------------------------------------------------------
    # 5M
    # --------------------------------------------------------

    m5_df = get_candles(
        ws,
        symbol,
        5,
        250
    )

    # --------------------------------------------------------
    # ANALYSIS
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
    # SCORE
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
        f"DIRECTION="
        f"{technical['direction']} "
        f"STATUS="
        f"{technical['status']} "
        f"BUY="
        f"{technical['buy_score']} "
        f"SELL="
        f"{technical['sell_score']}"
    )

    log(
        f"{instrument} "
        f"12H={h12['score']} "
        f"4H={h4['score']} "
        f"1H={h1['score']} "
        f"15M={m15['score']} "
        f"5M_BULL={m5['bull_count']} "
        f"5M_BEAR={m5['bear_count']}"
    )

    # --------------------------------------------------------
    # NO SETUP
    # --------------------------------------------------------

    if (
        technical["status"]
        ==
        "PASS"
    ):

        log(
            f"{instrument} NO SETUP"
        )

        return

    # --------------------------------------------------------
    # GROQ
    # --------------------------------------------------------

    log(
        f"{instrument} "
        "sending setup to Groq..."
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
        f"{instrument} "
        f"GROQ="
        f"{ai['decision']} "
        f"CONFIDENCE="
        f"{ai['confidence']}"
    )

    # --------------------------------------------------------
    # GROQ PASS
    # --------------------------------------------------------

    if ai["decision"] == "PASS":

        log(
            f"{instrument} "
            "GROQ PASS"
        )

        return

    # --------------------------------------------------------
    # PREVENT AI DIRECTION REVERSAL
    # --------------------------------------------------------

    if ai["decision"] in {
        "BUY",
        "SELL"
    }:

        if (
            technical["direction"]
            !=
            ai["decision"]
        ):

            log(
                f"{instrument} "
                "AI direction rejected "
                "because it conflicts "
                "with technical direction."
            )

            return

    # --------------------------------------------------------
    # DUPLICATE PREVENTION
    # --------------------------------------------------------

    candle_time = (
        m5["candle_time"]
    )

    previous = state.get(
        instrument,
        {}
    )

    if (
        previous.get(
            "decision"
        )
        ==
        ai["decision"]
        and
        previous.get(
            "candle_time"
        )
        ==
        candle_time
    ):

        log(
            f"{instrument} "
            "DUPLICATE - "
            "not sending Telegram."
        )

        return

    # --------------------------------------------------------
    # ICON
    # --------------------------------------------------------

    if ai["decision"] == "BUY":

        icon = "🟢"

    elif ai["decision"] == "SELL":

        icon = "🔴"

    else:

        icon = "🟡"

    # --------------------------------------------------------
    # TELEGRAM MESSAGE
    # --------------------------------------------------------

    message = (
        f"{icon} "
        f"{ai['decision']} — "
        f"{instrument}\n\n"

        f"Technical Score: "
        f"{technical['score']}\n"

        f"Buy Score: "
        f"{technical['buy_score']}\n"

        f"Sell Score: "
        f"{technical['sell_score']}\n\n"

        f"12H: "
        f"{h12['score']}\n"

        f"4H: "
        f"{h4['score']}\n"

        f"1H: "
        f"{h1['score']}\n"

        f"15M: "
        f"{m15['score']}\n\n"

        f"5M Bull: "
        f"{m5['bull_count']}\n"

        f"5M Bear: "
        f"{m5['bear_count']}\n\n"

        f"AI Confidence: "
        f"{ai['confidence']:.0f}%\n\n"

        f"AI Review:\n"
        f"{ai['reason']}\n\n"

        "Analysis only — "
        "no auto trading."
    )

    # --------------------------------------------------------
    # SEND TELEGRAM
    # --------------------------------------------------------

    try:

        send_telegram(
            message
        )

    except Exception as e:

        log(
            f"{instrument} "
            f"TELEGRAM ERROR: {e}"
        )

        return

    log(
        f"{instrument} "
        f"TELEGRAM ALERT SENT: "
        f"{ai['decision']}"
    )

    # --------------------------------------------------------
    # SAVE STATE
    # --------------------------------------------------------

    state[instrument] = {

        "decision":
            ai["decision"],

        "candle_time":
            candle_time,

        "timestamp":
            pd.Timestamp.now(
                tz="UTC"
            ).isoformat()
    }

    save_state(
        state
    )


# ============================================================
# MAIN
# ============================================================

def main():

    log("")

    log(
        "=" * 60
    )

    log(
        "SYNTHETIC INDICES SIGNAL SCANNER"
    )

    log(
        "Deriv + Multi-Timeframe + Groq"
    )

    log(
        "=" * 60
    )

    # --------------------------------------------------------
    # CHECK REQUIRED VARIABLES
    # --------------------------------------------------------

    missing = []

    if not TELEGRAM_BOT_TOKEN:

        missing.append(
            "TELEGRAM_BOT_TOKEN"
        )

    if not TELEGRAM_CHAT_ID:

        missing.append(
            "TELEGRAM_CHAT_ID"
        )

    if not GROQ_API_KEY:

        missing.append(
            "GROQ_API_KEY"
        )

    if missing:

        for variable in missing:

            log(
                f"ERROR: "
                f"{variable} missing"
            )

        return

    log(
        f"Groq model: "
        f"{GROQ_MODEL}"
    )

    ws = None

    try:

        # ----------------------------------------------------
        # CONNECT TO DERIV
        # ----------------------------------------------------

        ws = connect_deriv()

        # ----------------------------------------------------
        # GET ACTIVE SYMBOLS
        # ----------------------------------------------------

        active_symbols = (
            get_active_symbols(
                ws
            )
        )

        # ----------------------------------------------------
        # DISCOVER TARGETS
        # ----------------------------------------------------

        symbols = (
            discover_symbols(
                active_symbols
            )
        )

        # ----------------------------------------------------
        # LOAD STATE
        # ----------------------------------------------------

        state = load_state()

        # ----------------------------------------------------
        # SCAN ALL TARGETS
        # ----------------------------------------------------

        for (
            instrument,
            info
        ) in symbols.items():

            if not info:

                log(
                    f"{instrument} "
                    "SKIPPED - "
                    "symbol not found"
                )

                continue

            symbol = info[0]

            try:

                scan_index(
                    ws,
                    instrument,
                    symbol,
                    state
                )

            except Exception as e:

                log(
                    f"{instrument} "
                    f"ERROR: {e}"
                )

        # ----------------------------------------------------
        # COMPLETE
        # ----------------------------------------------------

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


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()
