import os
import json
import time
import logging
from datetime import datetime, timezone

import pandas as pd
import requests
from websocket import create_connection

try:
    from groq import Groq
except ImportError:
    Groq = None


# ============================================================
# CONFIG
# ============================================================

DERIV_WS_URL = os.getenv(
    "DERIV_WS_URL",
    "wss://api.derivws.com/trading/v1/options/ws/public"
)

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

# ============================================================
# SCAN INTERVAL
# ============================================================

# 300 seconds = 5 minutes
SCAN_INTERVAL_SECONDS = int(
    os.getenv(
        "SCAN_INTERVAL_SECONDS",
        "300"
    )
)


# ============================================================
# TARGET SYNTHETIC INDICES
# ============================================================

TARGET_NAMES = {
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
# DERIV TIMEFRAMES
# ============================================================

# Deriv supports these candle granularities.
# 12H is constructed from 3 completed 4H candles.

TIMEFRAMES = {
    "4H": 14400,
    "1H": 3600,
    "15M": 900,
    "5M": 300,
}


# ============================================================
# SCORING
# ============================================================

# Deliberately not too strict.

SIGNAL_MIN = 7
WATCH_MIN = 5


# ============================================================
# INDICATOR SETTINGS
# ============================================================

EMA_FAST = 20
EMA_SLOW = 50

RSI_LEN = 14

ATR_LEN = 14

ST_ATR_LEN = 10
ST_FACTOR = 3.0


# ============================================================
# STATE
# ============================================================

STATE_FILE = "state.json"

LOG_LEVEL = os.getenv(
    "LOG_LEVEL",
    "INFO"
).upper()


logging.basicConfig(
    level=getattr(
        logging,
        LOG_LEVEL,
        logging.INFO
    ),
    format="%(asctime)s %(levelname)s %(message)s"
)

log = logging.getLogger(
    "synthetic-scanner"
)


# ============================================================
# STATE FUNCTIONS
# ============================================================

def load_state():

    try:

        with open(
            STATE_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            return json.load(f)

    except Exception:

        return {}


def save_state(state):

    temp_file = STATE_FILE + ".tmp"

    with open(
        temp_file,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            state,
            f,
            indent=2
        )

    os.replace(
        temp_file,
        STATE_FILE
    )


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):

    """
    Sends Telegram alert.

    The bot token is never printed in logs.
    """

    if not TELEGRAM_BOT_TOKEN:

        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN missing"
        )

    if not TELEGRAM_CHAT_ID:

        raise RuntimeError(
            "TELEGRAM_CHAT_ID missing"
        )

    token = TELEGRAM_BOT_TOKEN.strip()

    chat_id = TELEGRAM_CHAT_ID.strip()

    url = (
        "https://api.telegram.org/"
        f"bot{token}"
        "/sendMessage"
    )

    payload = {
        "chat_id": chat_id,
        "text": str(message)
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

    if not response.ok:

        try:

            details = response.json()

            error_code = details.get(
                "error_code",
                response.status_code
            )

            description = details.get(
                "description",
                "Unknown Telegram error"
            )

        except Exception:

            error_code = response.status_code

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

    try:

        result = response.json()

    except Exception:

        raise RuntimeError(
            "Telegram returned invalid JSON."
        )

    if result.get("ok") is not True:

        raise RuntimeError(
            "Telegram API error: "
            f"{result.get('description', 'Unknown error')}"
        )

    return result


# ============================================================
# DERIV WEBSOCKET
# ============================================================

def deriv_request(ws, payload):

    """
    Send one request and wait for its response.
    """

    ws.send(
        json.dumps(payload)
    )

    while True:

        raw = ws.recv()

        data = json.loads(raw)

        if data.get("error"):

            error = data["error"]

            message = error.get(
                "message",
                "Deriv API error"
            )

            raise RuntimeError(
                message
            )

        return data


# ============================================================
# ACTIVE SYMBOLS
# ============================================================

def get_active_symbols():

    ws = create_connection(
        DERIV_WS_URL,
        timeout=20
    )

    try:

        data = deriv_request(
            ws,
            {
                "active_symbols": "brief",
                "req_id": 1
            }
        )

    finally:

        ws.close()

    symbols = data.get(
        "active_symbols",
        []
    )

    found = {}

    for item in symbols:

        name = (
            item.get(
                "underlying_symbol_name"
            )
            or
            item.get(
                "display_name"
            )
            or
            item.get(
                "symbol"
            )
            or
            ""
        )

        symbol = (
            item.get(
                "underlying_symbol"
            )
            or
            item.get(
                "symbol"
            )
        )

        if not symbol:

            continue

        normalized = (
            " ".join(
                str(name)
                .upper()
                .split()
            )
        )

        for target, aliases in TARGET_NAMES.items():

            if target in normalized:

                found[target] = symbol

                break

            for alias in aliases:

                normalized_alias = (
                    " ".join(
                        alias.upper().split()
                    )
                )

                if normalized_alias in normalized:

                    found[target] = symbol

                    break

    return found


# ============================================================
# GET DERIV CANDLES
# ============================================================

def get_candles(
    symbol,
    granularity,
    count=250
):

    """
    Get completed OHLC candles.

    IMPORTANT:
    No subscribe=0 is sent because the current
    Deriv API rejects that value for candle requests.
    """

    ws = create_connection(
        DERIV_WS_URL,
        timeout=30
    )

    try:

        payload = {
            "ticks_history": symbol,
            "end": "latest",
            "count": count,
            "style": "candles",
            "granularity": granularity,
            "req_id": 2
        }

        data = deriv_request(
            ws,
            payload
        )

    finally:

        ws.close()

    candles = data.get(
        "candles",
        []
    )

    if not candles:

        raise RuntimeError(
            f"No candles returned for "
            f"{symbol} / {granularity}"
        )

    df = pd.DataFrame(
        candles
    )

    required = [
        "epoch",
        "open",
        "high",
        "low",
        "close"
    ]

    for column in required:

        if column not in df.columns:

            raise RuntimeError(
                f"Missing {column} "
                "in Deriv candle response"
            )

    for column in [
        "open",
        "high",
        "low",
        "close"
    ]:

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    df["time"] = pd.to_datetime(
        df["epoch"],
        unit="s",
        utc=True
    )

    df = (
        df
        .dropna(
            subset=[
                "open",
                "high",
                "low",
                "close"
            ]
        )
        .drop_duplicates(
            "epoch"
        )
        .sort_values(
            "epoch"
        )
        .set_index(
            "time"
        )
    )

    # --------------------------------------------------------
    # REMOVE CURRENTLY FORMING CANDLE
    # --------------------------------------------------------

    now = pd.Timestamp.now(
        tz="UTC"
    )

    if len(df):

        last_time = df.index[-1]

        age_seconds = (
            now - last_time
        ).total_seconds()

        if age_seconds < granularity:

            df = df.iloc[:-1]

    if len(df) < 80:

        raise RuntimeError(
            f"Not enough completed candles "
            f"for {symbol}: {len(df)}"
        )

    return df


# ============================================================
# BUILD 12H FROM 4H
# ============================================================

def build_12h_from_4h(df_4h):

    """
    Deriv does not provide 12H in the supported
    candle granularity list.

    Therefore:

        3 x completed 4H candles
        =
        1 x 12H candle
    """

    if df_4h.empty:

        raise RuntimeError(
            "Cannot build 12H from empty 4H data"
        )

    df = df_4h.copy()

    result = (
        df.resample(
            "12h",
            origin="epoch",
            label="left",
            closed="left"
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

    counts = (
        df["close"]
        .resample(
            "12h",
            origin="epoch",
            label="left",
            closed="left"
        )
        .count()
    )

    result["count"] = counts

    result = result[
        result["count"] >= 3
    ]

    result = result.drop(
        columns=["count"]
    )

    result = result.dropna(
        subset=[
            "open",
            "high",
            "low",
            "close"
        ]
    )

    if len(result) < 80:

        raise RuntimeError(
            "Not enough completed 12H "
            "candles after 4H aggregation"
        )

    return result


# ============================================================
# INDICATORS
# ============================================================

def rma(
    series,
    length
):

    return series.ewm(
        alpha=1 / length,
        adjust=False
    ).mean()


def atr(
    df,
    length=ATR_LEN
):

    previous_close = (
        df["close"]
        .shift(1)
    )

    true_range = pd.concat(
        [
            df["high"] - df["low"],

            (
                df["high"]
                - previous_close
            ).abs(),

            (
                df["low"]
                - previous_close
            ).abs()
        ],
        axis=1
    ).max(
        axis=1
    )

    return rma(
        true_range,
        length
    )


def rsi(
    series,
    length=RSI_LEN
):

    delta = series.diff()

    gain = delta.clip(
        lower=0
    )

    loss = -delta.clip(
        upper=0
    )

    avg_gain = rma(
        gain,
        length
    )

    avg_loss = rma(
        loss,
        length
    )

    rs = (
        avg_gain
        /
        avg_loss.replace(
            0,
            float("nan")
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
# SUPERTREND
# ============================================================

def supertrend(
    df,
    atr_len=ST_ATR_LEN,
    factor=ST_FACTOR
):

    a = atr(
        df,
        atr_len
    )

    hl2 = (
        df["high"]
        +
        df["low"]
    ) / 2

    upper_basic = (
        hl2
        +
        factor * a
    )

    lower_basic = (
        hl2
        -
        factor * a
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
            direction.iloc[i - 1] == -1
            and
            df["close"].iloc[i]
            >
            upper.iloc[i]
        ):

            direction.iloc[i] = 1

        elif (
            direction.iloc[i - 1] == 1
            and
            df["close"].iloc[i]
            <
            lower.iloc[i]
        ):

            direction.iloc[i] = -1

        else:

            direction.iloc[i] = (
                direction.iloc[i - 1]
            )

    return direction


# ============================================================
# INDICATOR SNAPSHOT
# ============================================================

def indicator_snapshot(df):

    if len(df) < 80:

        raise RuntimeError(
            "Not enough candles for indicators"
        )

    ema20 = (
        df["close"]
        .ewm(
            span=EMA_FAST,
            adjust=False
        )
        .mean()
    )

    ema50 = (
        df["close"]
        .ewm(
            span=EMA_SLOW,
            adjust=False
        )
        .mean()
    )

    rsi_values = rsi(
        df["close"]
    )

    atr_values = atr(
        df
    )

    st = supertrend(
        df
    )

    last = df.iloc[-1]

    previous = df.iloc[-2]

    ema_bullish = bool(
        ema20.iloc[-1]
        >
        ema50.iloc[-1]
    )

    ema_bearish = bool(
        ema20.iloc[-1]
        <
        ema50.iloc[-1]
    )

    st_bullish = bool(
        st.iloc[-1] == 1
    )

    st_bearish = bool(
        st.iloc[-1] == -1
    )

    current_rsi = float(
        rsi_values.iloc[-1]
    )

    rsi_bullish = bool(
        current_rsi >= 52
    )

    rsi_bearish = bool(
        current_rsi <= 48
    )

    current_atr = float(
        atr_values.iloc[-1]
    )

    current_close = float(
        last["close"]
    )

    if current_close:

        atr_pct = (
            current_atr
            /
            current_close
            *
            100
        )

    else:

        atr_pct = 0.0

    break_up = bool(
        last["close"]
        >
        previous["high"]
    )

    break_down = bool(
        last["close"]
        <
        previous["low"]
    )

    if ema_bullish:

        ema_trend = "BULLISH"

    else:

        ema_trend = "BEARISH"

    if st_bullish:

        st_direction = "BULLISH"

    else:

        st_direction = "BEARISH"

    if rsi_bullish:

        rsi_bias = "BULLISH"

    elif rsi_bearish:

        rsi_bias = "BEARISH"

    else:

        rsi_bias = "NEUTRAL"

    return {

        "close": current_close,

        "ema20": float(
            ema20.iloc[-1]
        ),

        "ema50": float(
            ema50.iloc[-1]
        ),

        "rsi": current_rsi,

        "atr": current_atr,

        "atr_pct": float(
            atr_pct
        ),

        "supertrend": st_direction,

        "ema_trend": ema_trend,

        "rsi_bias": rsi_bias,

        "break_up": break_up,

        "break_down": break_down,

        "candle_time": (
            df.index[-1]
            .isoformat()
        )
    }


# ============================================================
# SCORE ENGINE
# ============================================================

def score_market(snaps):

    """
    Scoring:

    12H = 1 point
    4H  = 1 point
    1H  = 2 points

    15M:
        EMA        = 1
        Supertrend = 1
        RSI        = 1

    5M:
        Supertrend = 1
        Break/RSI  = 1

    Maximum = 10 points.

    SIGNAL = 7+
    WATCH  = 5+
    """

    buy = 0
    sell = 0

    # --------------------------------------------------------
    # 12H
    # --------------------------------------------------------

    if (
        snaps["12H"]["ema_trend"]
        ==
        "BULLISH"
    ):

        buy += 1

    elif (
        snaps["12H"]["ema_trend"]
        ==
        "BEARISH"
    ):

        sell += 1

    # --------------------------------------------------------
    # 4H
    # --------------------------------------------------------

    if (
        snaps["4H"]["ema_trend"]
        ==
        "BULLISH"
    ):

        buy += 1

    elif (
        snaps["4H"]["ema_trend"]
        ==
        "BEARISH"
    ):

        sell += 1

    # --------------------------------------------------------
    # 1H
    # --------------------------------------------------------

    if (
        snaps["1H"]["ema_trend"]
        ==
        "BULLISH"
    ):

        buy += 2

    elif (
        snaps["1H"]["ema_trend"]
        ==
        "BEARISH"
    ):

        sell += 2

    # --------------------------------------------------------
    # 15M
    # --------------------------------------------------------

    m15 = snaps["15M"]

    if (
        m15["ema_trend"]
        ==
        "BULLISH"
    ):

        buy += 1

    elif (
        m15["ema_trend"]
        ==
        "BEARISH"
    ):

        sell += 1

    if (
        m15["supertrend"]
        ==
        "BULLISH"
    ):

        buy += 1

    elif (
        m15["supertrend"]
        ==
        "BEARISH"
    ):

        sell += 1

    if (
        m15["rsi_bias"]
        ==
        "BULLISH"
    ):

        buy += 1

    elif (
        m15["rsi_bias"]
        ==
        "BEARISH"
    ):

        sell += 1

    # --------------------------------------------------------
    # 5M
    # --------------------------------------------------------

    m5 = snaps["5M"]

    if (
        m5["supertrend"]
        ==
        "BULLISH"
    ):

        buy += 1

    elif (
        m5["supertrend"]
        ==
        "BEARISH"
    ):

        sell += 1

    if (
        m5["break_up"]
        and
        m5["rsi"] >= 50
    ):

        buy += 1

    if (
        m5["break_down"]
        and
        m5["rsi"] <= 50
    ):

        sell += 1

    return buy, sell


# ============================================================
# GROQ REVIEW
# ============================================================

def groq_review(
    name,
    snaps,
    buy,
    sell
):

    if not GROQ_API_KEY:

        raise RuntimeError(
            "GROQ_API_KEY is required"
        )

    if Groq is None:

        raise RuntimeError(
            "groq package is not installed"
        )

    client = Groq(
        api_key=GROQ_API_KEY
    )

    compact = {}

    for tf, snapshot in snaps.items():

        compact[tf] = {

            "close": round(
                snapshot["close"],
                8
            ),

            "ema20": round(
                snapshot["ema20"],
                8
            ),

            "ema50": round(
                snapshot["ema50"],
                8
            ),

            "rsi": round(
                snapshot["rsi"],
                2
            ),

            "atr_pct": round(
                snapshot["atr_pct"],
                4
            ),

            "supertrend":
                snapshot["supertrend"],

            "ema_trend":
                snapshot["ema_trend"],

            "rsi_bias":
                snapshot["rsi_bias"],

            "break_up":
                snapshot["break_up"],

            "break_down":
                snapshot["break_down"]
        }

    prompt = f"""
You are the final AI filter for a synthetic-index
signal scanner.

This is NOT an auto-trading system.

Instrument:
{name}

Technical score:
BUY = {buy}
SELL = {sell}

Multi-timeframe data:
{json.dumps(compact, indent=2)}

Evaluate the evidence across 12H, 4H, 1H, 15M and 5M.

Rules:

1. Do not invent market data.
2. Do not blindly follow the technical score.
3. Prefer BUY only when bullish evidence is reasonably strong.
4. Prefer SELL only when bearish evidence is reasonably strong.
5. If evidence is mixed, use WATCH or PASS.
6. Do not require perfect alignment.
7. This scanner should not be excessively strict.
8. Keep the reason short.

Return ONLY the required JSON object.
"""

    try:

        response = (
            client
            .chat
            .completions
            .create(

                model=GROQ_MODEL,

                messages=[

                    {
                        "role": "system",
                        "content": (
                            "Return ONLY one valid "
                            "JSON object. "
                            "Use exactly these "
                            "fields: decision, "
                            "confidence, reason."
                        )
                    },

                    {
                        "role": "user",
                        "content": prompt
                    }
                ],

                temperature=0.1,

                max_completion_tokens=1024,

                reasoning_effort="low",

                response_format={
                    "type": "json_object"
                }
            )
        )

        text = (
            response
            .choices[0]
            .message
            .content
            .strip()
        )

        # Remove accidental markdown fences.

        if text.startswith("```"):

            text = (
                text
                .replace(
                    "```json",
                    ""
                )
                .replace(
                    "```",
                    ""
                )
                .strip()
            )

        result = json.loads(
            text
        )

        # Normalize decision.

        decision = str(
            result.get(
                "decision",
                "PASS"
            )
        ).upper().strip()

        if decision not in (
            "BUY",
            "SELL",
            "WATCH",
            "PASS"
        ):

            decision = "PASS"

        confidence = result.get(
            "confidence"
        )

        try:

            if confidence is not None:

                confidence = float(
                    confidence
                )

                confidence = max(
                    0,
                    min(
                        100,
                        confidence
                    )
                )

        except Exception:

            confidence = None

        reason = str(
            result.get(
                "reason",
                ""
            )
        ).strip()

        return {

            "decision": decision,

            "confidence": confidence,

            "reason": reason
        }

    except Exception as e:

        log.error(
            "GROQ API ERROR: %s",
            e
        )

        return {

            "decision": "PASS",

            "confidence": None,

            "reason": "Groq review failed"
        }


# ============================================================
# TELEGRAM SIGNAL MESSAGE
# ============================================================

def build_signal_message(
    name,
    symbol,
    direction,
    score,
    groq_result,
    snaps
):

    if direction == "BUY":

        emoji = "🟢"

    else:

        emoji = "🔴"

    confidence = groq_result.get(
        "confidence"
    )

    reason = groq_result.get(
        "reason",
        ""
    )

    lines = [

        f"{emoji} {name} — {direction} SIGNAL",

        f"Technical score: {score}/10",

        f"Symbol: {symbol}",

        f"Price: {snaps['5M']['close']}",

        "",

        "MULTI-TIMEFRAME:",

        (
            f"12H: "
            f"{snaps['12H']['ema_trend']}"
        ),

        (
            f"4H: "
            f"{snaps['4H']['ema_trend']}"
        ),

        (
            f"1H: "
            f"{snaps['1H']['ema_trend']}"
        ),

        (
            f"15M: "
            f"{snaps['15M']['supertrend']}"
        ),

        (
            f"5M: "
            f"{snaps['5M']['supertrend']}"
        ),

        (
            f"5M RSI: "
            f"{snaps['5M']['rsi']:.1f}"
        )
    ]

    if confidence is not None:

        lines.append(
            f"Groq confidence: "
            f"{confidence:.0f}%"
        )

    if reason:

        lines.append(
            f"AI note: {reason}"
        )

    lines.extend(
        [

            "",

            "Scanner only — "
            "no automatic trading."
        ]
    )

    return "\n".join(
        lines
    )


# ============================================================
# WATCH MESSAGE
# ============================================================

def build_watch_message(
    name,
    symbol,
    proposal,
    score,
    groq_result,
    snaps
):

    confidence = groq_result.get(
        "confidence"
    )

    if confidence is None:

        confidence_text = "N/A"

    else:

        confidence_text = (
            f"{confidence:.0f}%"
        )

    reason = groq_result.get(
        "reason",
        ""
    )

    lines = [

        f"👀 {name} — WATCH",

        f"Direction developing: {proposal}",

        f"Technical score: {score}/10",

        f"Symbol: {symbol}",

        f"Price: {snaps['5M']['close']}",

        "",

        "MULTI-TIMEFRAME:",

        (
            f"12H: "
            f"{snaps['12H']['ema_trend']}"
        ),

        (
            f"4H: "
            f"{snaps['4H']['ema_trend']}"
        ),

        (
            f"1H: "
            f"{snaps['1H']['ema_trend']}"
        ),

        (
            f"15M: "
            f"{snaps['15M']['supertrend']}"
        ),

        (
            f"5M: "
            f"{snaps['5M']['supertrend']}"
        ),

        (
            f"5M RSI: "
            f"{snaps['5M']['rsi']:.1f}"
        ),

        f"Groq confidence: {confidence_text}"
    ]

    if reason:

        lines.append(
            f"AI note: {reason}"
        )

    lines.extend(
        [

            "",

            "Developing setup — "
            "not a confirmed signal.",

            "Scanner only — "
            "no automatic trading."
        ]
    )

    return "\n".join(
        lines
    )


# ============================================================
# SCAN ONE INDEX
# ============================================================

def scan_one(
    name,
    symbol,
    state
):

    log.info(
        "Scanning %s (%s)",
        name,
        symbol
    )

    # --------------------------------------------------------
    # GET 4H
    # --------------------------------------------------------

    df_4h = get_candles(
        symbol,
        14400,
        count=300
    )

    # Build 12H from completed 4H.

    df_12h = build_12h_from_4h(
        df_4h
    )

    # --------------------------------------------------------
    # GET OTHER TIMEFRAMES
    # --------------------------------------------------------

    df_1h = get_candles(
        symbol,
        3600,
        count=250
    )

    df_15m = get_candles(
        symbol,
        900,
        count=250
    )

    df_5m = get_candles(
        symbol,
        300,
        count=250
    )

    # --------------------------------------------------------
    # INDICATOR SNAPSHOTS
    # --------------------------------------------------------

    snaps = {

        "12H":
            indicator_snapshot(
                df_12h
            ),

        "4H":
            indicator_snapshot(
                df_4h
            ),

        "1H":
            indicator_snapshot(
                df_1h
            ),

        "15M":
            indicator_snapshot(
                df_15m
            ),

        "5M":
            indicator_snapshot(
                df_5m
            )
    }

    # --------------------------------------------------------
    # SCORE
    # --------------------------------------------------------

    buy, sell = score_market(
        snaps
    )

    log.info(
        (
            "%s "
            "12H=%s "
            "4H=%s "
            "1H=%s "
            "15M=%s "
            "5M=%s"
        ),

        name,

        snaps["12H"]["ema_trend"],

        snaps["4H"]["ema_trend"],

        snaps["1H"]["ema_trend"],

        snaps["15M"]["supertrend"],

        snaps["5M"]["supertrend"]
    )

    log.info(
        "%s SCORE BUY=%d SELL=%d",
        name,
        buy,
        sell
    )

    # --------------------------------------------------------
    # TECHNICAL PROPOSAL
    # --------------------------------------------------------

    proposal = None

    score = 0

    if (
        buy >= SIGNAL_MIN
        and
        buy > sell
    ):

        proposal = "BUY"

        score = buy

    elif (
        sell >= SIGNAL_MIN
        and
        sell > buy
    ):

        proposal = "SELL"

        score = sell

    elif (
        max(buy, sell)
        >= WATCH_MIN
        and
        buy != sell
    ):

        if buy > sell:

            proposal = "BUY"

            score = buy

        else:

            proposal = "SELL"

            score = sell

    else:

        log.info(
            (
                "%s NO SETUP "
                "BUY=%d SELL=%d"
            ),

            name,

            buy,

            sell
        )

        return

    log.info(
        "%s technical proposal=%s score=%d",
        name,
        proposal,
        score
    )

    # --------------------------------------------------------
    # GROQ FINAL REVIEW
    # --------------------------------------------------------

    log.info(
        "%s sending setup to Groq...",
        name
    )

    groq_result = groq_review(
        name,
        snaps,
        buy,
        sell
    )

    ai_decision = str(
        groq_result.get(
            "decision",
            "PASS"
        )
    ).upper()

    confidence = groq_result.get(
        "confidence"
    )

    log.info(
        "%s GROQ=%s CONFIDENCE=%s",
        name,
        ai_decision,
        confidence
    )

    # --------------------------------------------------------
    # DUPLICATE PREVENTION
    # --------------------------------------------------------

    candle_key = (
        snaps["5M"]["candle_time"]
    )

    state_key = (
        f"{name}:{ai_decision}"
    )

    if (
        state.get(state_key)
        ==
        candle_key
    ):

        log.info(
            "%s duplicate %s ignored",
            name,
            ai_decision
        )

        return

    # --------------------------------------------------------
    # WATCH
    # --------------------------------------------------------

    if ai_decision == "WATCH":

        text = build_watch_message(
            name,
            symbol,
            proposal,
            score,
            groq_result,
            snaps
        )

        try:

            send_telegram(
                text
            )

            state[state_key] = (
                candle_key
            )

            log.info(
                "%s WATCH sent to Telegram",
                name
            )

        except Exception as e:

            log.error(
                "%s TELEGRAM ERROR: %s",
                name,
                e
            )

        return

    # --------------------------------------------------------
    # BUY / SELL SIGNAL
    # --------------------------------------------------------

    if ai_decision in (
        "BUY",
        "SELL"
    ):

        # AI must agree with technical proposal.

        if ai_decision != proposal:

            log.info(
                (
                    "%s Groq direction "
                    "%s conflicts with "
                    "technical proposal %s; "
                    "no alert."
                ),

                name,

                ai_decision,

                proposal
            )

            return

        text = build_signal_message(
            name,
            symbol,
            ai_decision,
            score,
            groq_result,
            snaps
        )

        try:

            send_telegram(
                text
            )

            state[state_key] = (
                candle_key
            )

            log.info(
                "%s %s SIGNAL sent to Telegram",
                name,
                ai_decision
            )

        except Exception as e:

            log.error(
                "%s TELEGRAM ERROR: %s",
                name,
                e
            )

        return

    # --------------------------------------------------------
    # PASS
    # --------------------------------------------------------

    log.info(
        "%s Groq returned PASS; no alert.",
        name
    )


# ============================================================
# RUN ONE COMPLETE SCAN
# ============================================================

def run_scan():

    log.info(
        "======================================"
    )

    log.info(
        "Starting scan cycle"
    )

    log.info(
        "Targets: Crash 1000, Boom 1000, Crash 500"
    )

    log.info(
        "12H is constructed from completed 4H candles"
    )

    log.info(
        "Groq model: %s",
        GROQ_MODEL
    )

    log.info(
        "Signal threshold: %d",
        SIGNAL_MIN
    )

    log.info(
        "Watch threshold: %d",
        WATCH_MIN
    )

    log.info(
        "Scan interval: %d seconds (%d minutes)",
        SCAN_INTERVAL_SECONDS,
        SCAN_INTERVAL_SECONDS // 60
    )

    log.info(
        "No automatic trading"
    )

    log.info(
        "======================================"
    )

    state = load_state()

    # --------------------------------------------------------
    # GET SYMBOLS
    # --------------------------------------------------------

    try:

        symbols = get_active_symbols()

    except Exception as e:

        log.exception(
            "Could not get Deriv symbols: %s",
            e
        )

        return

    log.info(
        "Discovered symbols: %s",
        symbols
    )

    missing = [
        name
        for name in TARGET_NAMES
        if name not in symbols
    ]

    if missing:

        log.warning(
            "Could not discover: %s",
            ", ".join(missing)
        )

    # --------------------------------------------------------
    # SCAN OUR THREE INDICES
    # --------------------------------------------------------

    for name in TARGET_NAMES:

        symbol = symbols.get(
            name
        )

        if not symbol:

            continue

        try:

            scan_one(
                name,
                symbol,
                state
            )

        except Exception as e:

            log.exception(
                "%s ERROR: %s",
                name,
                e
            )

    # --------------------------------------------------------
    # SAVE STATE
    # --------------------------------------------------------

    try:

        save_state(
            state
        )

    except Exception as e:

        log.exception(
            "Could not save state: %s",
            e
        )

    log.info(
        "Scan cycle finished."
    )


# ============================================================
# MAIN — CONTINUOUS 5-MINUTE LOOP
# ============================================================

def main():

    log.info(
        "======================================"
    )

    log.info(
        "SYNTHETIC SCANNER STARTING"
    )

    log.info(
        "Continuous scanning ENABLED"
    )

    log.info(
        "Scan interval: %d seconds (%d minutes)",
        SCAN_INTERVAL_SECONDS,
        SCAN_INTERVAL_SECONDS // 60
    )

    log.info(
        "======================================"
    )

    while True:

        cycle_started = time.time()

        try:

            run_scan()

        except Exception as e:

            log.exception(
                "Unexpected scanner error: %s",
                e
            )

        elapsed = (
            time.time()
            -
            cycle_started
        )

        wait_seconds = max(
            1,
            SCAN_INTERVAL_SECONDS
        )

        log.info(
            (
                "Next scan in %d seconds "
                "(%d minutes). "
                "Current cycle took %.1f seconds."
            ),

            wait_seconds,

            wait_seconds // 60,

            elapsed
        )

        try:

            time.sleep(
                wait_seconds
            )

        except KeyboardInterrupt:

            log.info(
                "Scanner stopped."
            )

            break


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()
