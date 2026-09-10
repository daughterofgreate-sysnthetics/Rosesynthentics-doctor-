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
    "CRASH 300": [
        "Crash 300",
        "Crash 300 Index"
    ],

    "CRASH 500": [
        "Crash 500",
        "Crash 500 Index"
    ],

    "CRASH 600": [
        "Crash 600",
        "Crash 600 Index"
    ],

    "CRASH 900": [
        "Crash 900",
        "Crash 900 Index"
    ],

    "CRASH 1000": [
        "Crash 1000",
        "Crash 1000 Index"
    ],

    "BOOM 300": [
        "Boom 300",
        "Boom 300 Index"
    ],

    "BOOM 500": [
        "Boom 500",
        "Boom 500 Index"
    ],

    "BOOM 600": [
        "Boom 600",
        "Boom 600 Index"
    ],

    "BOOM 900": [
        "Boom 900",
        "Boom 900 Index"
    ],

    "BOOM 1000": [
        "Boom 1000",
        "Boom 1000 Index"
    ],
}


# ============================================================
# DERIV TIMEFRAMES
# ============================================================

TIMEFRAMES = {

    "4H": 14400,

    "1H": 3600,

    "15M": 900,

    "5M": 300,
}


# ============================================================
# SIGNAL SETTINGS
# ============================================================

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
# SPIKE / PULLBACK SETTINGS
# ============================================================

# Lower number = more spike detections.
# Higher number = fewer, stronger spike detections.

SPIKE_ATR_MULT = 1.20


# Number of recent 5M candles to inspect
# for a possible spike.

SPIKE_LOOKBACK = 12


# Minimum pullback size measured in ATR.

PULLBACK_MIN_ATR = 0.20


# Maximum pullback size measured in ATR.

PULLBACK_MAX_ATR = 1.50


# Maximum age of a spike that can still create
# a reversal signal.

MAX_SPIKE_AGE_BARS = 12


# ============================================================
# STATE
# ============================================================

STATE_FILE = "state.json"


# ============================================================
# LOGGING
# ============================================================

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

            matched = False

            for alias in aliases:

                normalized_alias = (
                    " ".join(
                        alias.upper().split()
                    )
                )

                if normalized_alias in normalized:

                    found[target] = symbol

                    matched = True

                    break

            if matched:

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
    # REMOVE CURRENT FORMING CANDLE
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
# RMA
# ============================================================

def rma(
    series,
    length
):

    return series.ewm(
        alpha=1 / length,
        adjust=False
    ).mean()


# ============================================================
# ATR
# ============================================================

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
                -
                previous_close
            ).abs(),

            (
                df["low"]
                -
                previous_close
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


# ============================================================
# RSI
# ============================================================

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
# SYNTHETIC TYPE
# ============================================================

def synthetic_direction(name):

    normalized = (
        name
        .upper()
        .strip()
    )

    if "CRASH" in normalized:

        return "CRASH"

    if "BOOM" in normalized:

        return "BOOM"

    return None


# ============================================================
# SPIKE DETECTION
# ============================================================

def detect_recent_spike(
    df,
    direction
):

    if len(df) < 60:

        return None

    atr_values = atr(
        df,
        ATR_LEN
    )

    start = max(
        1,
        len(df)
        -
        SPIKE_LOOKBACK
        -
        1
    )

    best = None

    for i in range(
        start,
        len(df) - 1
    ):

        candle = df.iloc[i]

        candle_atr = float(
            atr_values.iloc[i]
        )

        if candle_atr <= 0:

            continue

        candle_range = (
            float(candle["high"])
            -
            float(candle["low"])
        )

        candle_body = abs(
            float(candle["close"])
            -
            float(candle["open"])
        )

        movement = max(
            candle_range,
            candle_body
        )

        size_atr = (
            movement
            /
            candle_atr
        )

        # ----------------------------------------------------
        # CRASH = RED / DOWN SPIKE
        # ----------------------------------------------------

        if direction == "CRASH":

            is_spike = (
                float(candle["close"])
                <
                float(candle["open"])
                and
                size_atr
                >=
                SPIKE_ATR_MULT
            )

        # ----------------------------------------------------
        # BOOM = GREEN / UP SPIKE
        # ----------------------------------------------------

        elif direction == "BOOM":

            is_spike = (
                float(candle["close"])
                >
                float(candle["open"])
                and
                size_atr
                >=
                SPIKE_ATR_MULT
            )

        else:

            is_spike = False

        if not is_spike:

            continue

        candidate = {

            "index": i,

            "time": (
                df.index[i]
                .isoformat()
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

            "size_atr": float(
                size_atr
            )
        }

        if (
            best is None
            or
            i > best["index"]
        ):

            best = candidate

    return best


# ============================================================
# SPIKE → PULLBACK → REVERSAL
# ============================================================

def detect_spike_pullback_reversal(
    df,
    direction
):

    result = {

        "valid": False,

        "phase": "WAITING",

        "spike": False,

        "pullback": False,

        "reversal": False,

        "spike_age": None,

        "spike_size_atr": 0.0,

        "pullback_atr": 0.0,

        "spike_time": None
    }

    spike = detect_recent_spike(
        df,
        direction
    )

    if spike is None:

        result["phase"] = (
            "WAITING_FOR_SPIKE"
        )

        return result

    result["spike"] = True

    result["spike_age"] = (
        len(df)
        -
        1
        -
        spike["index"]
    )

    result["spike_size_atr"] = (
        spike["size_atr"]
    )

    result["spike_time"] = (
        spike["time"]
    )

    # --------------------------------------------------------
    # REJECT STALE SPIKES
    # --------------------------------------------------------

    if (
        result["spike_age"]
        >
        MAX_SPIKE_AGE_BARS
    ):

        result["spike"] = False

        result["phase"] = (
            "SPIKE_TOO_OLD"
        )

        return result

    # --------------------------------------------------------
    # Get candles after spike
    # --------------------------------------------------------

    after = df.iloc[
        spike["index"] + 1:
    ]

    if after.empty:

        result["phase"] = (
            "SPIKE_DETECTED"
        )

        return result

    current = df.iloc[-1]

    previous = df.iloc[-2]

    current_atr = float(
        atr(
            df,
            ATR_LEN
        ).iloc[-1]
    )

    if current_atr <= 0:

        result["phase"] = (
            "NO_ATR"
        )

        return result

    # ========================================================
    # CRASH
    # ========================================================

    if direction == "CRASH":

        highest_after = float(
            after["high"].max()
        )

        pullback_distance = (
            highest_after
            -
            spike["low"]
        )

        pullback_atr = (
            pullback_distance
            /
            current_atr
        )

        result["pullback_atr"] = (
            float(pullback_atr)
        )

        pullback_started = (
            highest_after
            >
            spike["close"]
        )

        pullback = (
            pullback_started
            and
            pullback_atr
            >=
            PULLBACK_MIN_ATR
            and
            pullback_atr
            <=
            PULLBACK_MAX_ATR
        )

        result["pullback"] = (
            bool(pullback)
        )

        if not pullback:

            result["phase"] = (
                "WAITING_FOR_PULLBACK"
            )

            return result

        # ----------------------------------------------------
        # Bearish reversal
        # ----------------------------------------------------

        bearish_candle = (
            float(current["close"])
            <
            float(current["open"])
        )

        break_previous_low = (
            float(current["close"])
            <
            float(previous["low"])
        )

        lower_close = (
            float(current["close"])
            <
            float(previous["close"])
        )

        reversal = (
            bearish_candle
            and
            (
                break_previous_low
                or
                lower_close
            )
        )

        result["reversal"] = (
            bool(reversal)
        )

        if reversal:

            result["valid"] = True

            result["phase"] = (
                "SELL_READY"
            )

        else:

            result["phase"] = (
                "WAITING_FOR_SELL_REVERSAL"
            )

        return result

    # ========================================================
    # BOOM
    # ========================================================

    if direction == "BOOM":

        lowest_after = float(
            after["low"].min()
        )

        pullback_distance = (
            spike["high"]
            -
            lowest_after
        )

        pullback_atr = (
            pullback_distance
            /
            current_atr
        )

        result["pullback_atr"] = (
            float(pullback_atr)
        )

        pullback_started = (
            lowest_after
            <
            spike["close"]
        )

        pullback = (
            pullback_started
            and
            pullback_atr
            >=
            PULLBACK_MIN_ATR
            and
            pullback_atr
            <=
            PULLBACK_MAX_ATR
        )

        result["pullback"] = (
            bool(pullback)
        )

        if not pullback:

            result["phase"] = (
                "WAITING_FOR_PULLBACK"
            )

            return result

        # ----------------------------------------------------
        # Bullish reversal
        # ----------------------------------------------------

        bullish_candle = (
            float(current["close"])
            >
            float(current["open"])
        )

        break_previous_high = (
            float(current["close"])
            >
            float(previous["high"])
        )

        higher_close = (
            float(current["close"])
            >
            float(previous["close"])
        )

        reversal = (
            bullish_candle
            and
            (
                break_previous_high
                or
                higher_close
            )
        )

        result["reversal"] = (
            bool(reversal)
        )

        if reversal:

            result["valid"] = True

            result["phase"] = (
                "BUY_READY"
            )

        else:

            result["phase"] = (
                "WAITING_FOR_BUY_REVERSAL"
            )

        return result

    result["phase"] = (
        "UNKNOWN_DIRECTION"
    )

    return result


# ============================================================
# SYNTHETIC SETUP SCORE
# ============================================================

def score_synthetic_setup(
    name,
    snaps,
    structure
):

    direction = synthetic_direction(
        name
    )

    if direction == "CRASH":

        target = "SELL"

    elif direction == "BOOM":

        target = "BUY"

    else:

        return 0, None

    score = 0

    # --------------------------------------------------------
    # SPIKE
    # --------------------------------------------------------

    if structure.get(
        "spike"
    ):

        score += 2

    # --------------------------------------------------------
    # PULLBACK
    # --------------------------------------------------------

    if structure.get(
        "pullback"
    ):

        score += 2

    # --------------------------------------------------------
    # REVERSAL
    # --------------------------------------------------------

    if structure.get(
        "reversal"
    ):

        score += 2

    # --------------------------------------------------------
    # 15M
    # --------------------------------------------------------

    m15 = snaps["15M"]

    if target == "SELL":

        if (
            m15["supertrend"]
            ==
            "BEARISH"
        ):

            score += 1

        if (
            m15["ema_trend"]
            ==
            "BEARISH"
        ):

            score += 1

        if (
            m15["rsi"]
            <=
            50
        ):

            score += 1

    else:

        if (
            m15["supertrend"]
            ==
            "BULLISH"
        ):

            score += 1

        if (
            m15["ema_trend"]
            ==
            "BULLISH"
        ):

            score += 1

        if (
            m15["rsi"]
            >=
            50
        ):

            score += 1

    # --------------------------------------------------------
    # 5M
    # --------------------------------------------------------

    m5 = snaps["5M"]

    if target == "SELL":

        if (
            m5["supertrend"]
            ==
            "BEARISH"
        ):

            score += 1

    else:

        if (
            m5["supertrend"]
            ==
            "BULLISH"
        ):

            score += 1

    return score, target


# ============================================================
# GROQ REVIEW
# ============================================================

def groq_review(
    name,
    snaps,
    buy,
    sell,
    structure
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

    synthetic_type = synthetic_direction(
        name
    )

    if synthetic_type == "CRASH":

        allowed_direction = "SELL ONLY"

    elif synthetic_type == "BOOM":

        allowed_direction = "BUY ONLY"

    else:

        allowed_direction = "NONE"

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

    structure_compact = {

        "phase":
            structure.get(
                "phase"
            ),

        "spike":
            structure.get(
                "spike"
            ),

        "pullback":
            structure.get(
                "pullback"
            ),

        "reversal":
            structure.get(
                "reversal"
            ),

        "spike_age":
            structure.get(
                "spike_age"
            ),

        "spike_size_atr":
            round(
                structure.get(
                    "spike_size_atr",
                    0
                ),
                2
            ),

        "pullback_atr":
            round(
                structure.get(
                    "pullback_atr",
                    0
                ),
                2
            )
    }

    prompt = f"""
You are the final AI review layer for a
synthetic-index signal scanner.

This is NOT an auto-trading system.

Instrument:
{name}

Synthetic type:
{synthetic_type}

ALLOWED SIGNAL:
{allowed_direction}

IMPORTANT:

CRASH indices are handled as:

DOWN/RED SPIKE
->
UPWARD PULLBACK
->
BEARISH REVERSAL
->
SELL

BOOM indices are handled as:

UP/GREEN SPIKE
->
DOWNWARD PULLBACK
->
BULLISH REVERSAL
->
BUY

Never reverse this direction.

A CRASH BUY is invalid.

A BOOM SELL is invalid.

The scanner is specifically looking for:

SPIKE -> PULLBACK -> REVERSAL

Technical BUY score:
{buy}

Technical SELL score:
{sell}

Detected structure:
{json.dumps(structure_compact, indent=2)}

Multi-timeframe indicators:
{json.dumps(compact, indent=2)}

Rules:

1. Do not invent market data.

2. Respect the synthetic direction.

3. CRASH can only produce SELL.

4. BOOM can only produce BUY.

5. Do not issue a signal simply because
   higher-timeframe indicators agree.

6. The spike/pullback/reversal structure
   is more important than generic trend scoring.

7. If the structure is not properly confirmed,
   use WATCH or PASS.

8. Mixed timeframes are acceptable for a setup,
   but do not ignore strong contradiction.

9. Keep the reason short.

10. Confidence must represent the quality of
    the current evidence, not certainty about
    the future.

Return ONLY valid JSON:

{{
    "decision": "BUY|SELL|WATCH|PASS",
    "confidence": 0,
    "reason": "short explanation"
}}
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
                            "JSON object."
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

        # ----------------------------------------------------
        # HARD AI DIRECTION NORMALIZATION
        # ----------------------------------------------------

        if synthetic_type == "CRASH":

            if decision == "BUY":

                log.warning(
                    (
                        "%s Groq attempted "
                        "BUY on CRASH. "
                        "Blocked."
                    ),
                    name
                )

                decision = "PASS"

        elif synthetic_type == "BOOM":

            if decision == "SELL":

                log.warning(
                    (
                        "%s Groq attempted "
                        "SELL on BOOM. "
                        "Blocked."
                    ),
                    name
                )

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
# SIGNAL MESSAGE
# ============================================================

def build_signal_message(
    name,
    symbol,
    direction,
    score,
    groq_result,
    snaps,
    structure
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

    if confidence is None:

        confidence_text = "N/A"

    else:

        confidence_text = (
            f"{confidence:.0f}%"
        )

    lines = [

        (
            f"{emoji} {name} — "
            f"{direction} PULLBACK SIGNAL"
        ),

        f"Technical score: {score}/10",

        f"Symbol: {symbol}",

        f"Price: {snaps['5M']['close']}",

        "",

        "SETUP:",

        "Spike → Pullback → Reversal",

        (
            f"Spike size: "
            f"{structure.get('spike_size_atr', 0):.2f} ATR"
        ),

        (
            f"Pullback: "
            f"{structure.get('pullback_atr', 0):.2f} ATR"
        ),

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

        (
            f"Groq confidence: "
            f"{confidence_text}"
        )
    ]

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
    snaps,
    structure
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

    phase = structure.get(
        "phase",
        "UNKNOWN"
    )

    lines = [

        f"👀 {name} — WATCH",

        f"Direction developing: {proposal}",

        f"Technical score: {score}/10",

        f"Symbol: {symbol}",

        f"Price: {snaps['5M']['close']}",

        "",

        "SETUP:",

        f"Phase: {phase}",

        (
            f"Spike: "
            f"{'YES' if structure.get('spike') else 'NO'}"
        ),

        (
            f"Pullback: "
            f"{'YES' if structure.get('pullback') else 'NO'}"
        ),

        (
            f"Reversal: "
            f"{'YES' if structure.get('reversal') else 'NO'}"
        ),

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

        (
            f"Groq confidence: "
            f"{confidence_text}"
        )
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

    synthetic_type = synthetic_direction(
        name
    )

    if synthetic_type is None:

        log.warning(
            "%s unknown synthetic type",
            name
        )

        return

    # --------------------------------------------------------
    # GET DATA
    # --------------------------------------------------------

    df_4h = get_candles(
        symbol,
        TIMEFRAMES["4H"],
        count=300
    )

    df_12h = build_12h_from_4h(
        df_4h
    )

    df_1h = get_candles(
        symbol,
        TIMEFRAMES["1H"],
        count=250
    )

    df_15m = get_candles(
        symbol,
        TIMEFRAMES["15M"],
        count=250
    )

    df_5m = get_candles(
        symbol,
        TIMEFRAMES["5M"],
        count=250
    )

    # --------------------------------------------------------
    # SNAPSHOTS
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
    # DETECT SPIKE/PULLBACK/REVERSAL
    # --------------------------------------------------------

    structure = detect_spike_pullback_reversal(
        df_5m,
        synthetic_type
    )

    log.info(
        (
            "%s STRUCTURE "
            "type=%s "
            "phase=%s "
            "spike=%s "
            "pullback=%s "
            "reversal=%s "
            "spike_age=%s "
            "spike_atr=%.2f "
            "pullback_atr=%.2f"
        ),

        name,

        synthetic_type,

        structure.get(
            "phase"
        ),

        structure.get(
            "spike"
        ),

        structure.get(
            "pullback"
        ),

        structure.get(
            "reversal"
        ),

        structure.get(
            "spike_age"
        ),

        structure.get(
            "spike_size_atr",
            0
        ),

        structure.get(
            "pullback_atr",
            0
        )
    )

    # --------------------------------------------------------
    # SCORE
    # --------------------------------------------------------

    score, proposal = score_synthetic_setup(
        name,
        snaps,
        structure
    )

    log.info(
        "%s SETUP SCORE=%d PROPOSAL=%s",
        name,
        score,
        proposal
    )

    # --------------------------------------------------------
    # WAITING FOR SPIKE
    # --------------------------------------------------------

    if not structure.get(
        "spike"
    ):

        log.info(
            "%s WAITING FOR %s SPIKE",
            name,
            synthetic_type
        )

        return

    # --------------------------------------------------------
    # WAITING FOR PULLBACK
    # --------------------------------------------------------

    if not structure.get(
        "pullback"
    ):

        log.info(
            "%s SPIKE DETECTED - WAITING FOR PULLBACK",
            name
        )

        return

    # --------------------------------------------------------
    # WAITING FOR REVERSAL
    # --------------------------------------------------------

    if not structure.get(
        "reversal"
    ):

        log.info(
            "%s PULLBACK DETECTED - WAITING FOR REVERSAL",
            name
        )

        return

    # --------------------------------------------------------
    # HARD DIRECTION
    # --------------------------------------------------------

    if synthetic_type == "CRASH":

        proposal = "SELL"

    elif synthetic_type == "BOOM":

        proposal = "BUY"

    else:

        return

    # --------------------------------------------------------
    # SCORE FILTER
    # --------------------------------------------------------

    if score < SIGNAL_MIN:

        log.info(
            (
                "%s valid %s structure "
                "but score %d < signal threshold %d"
            ),

            name,

            proposal,

            score,

            SIGNAL_MIN
        )

        return

    log.info(
        (
            "%s VALID %s PULLBACK SETUP "
            "score=%d"
        ),

        name,

        proposal,

        score
    )

    # --------------------------------------------------------
    # GROQ
    # --------------------------------------------------------

    log.info(
        "%s sending setup to Groq...",
        name
    )

    # IMPORTANT:
    # BUY setup -> buy gets the score
    # SELL setup -> sell gets the score

    if proposal == "BUY":

        buy_score = score

        sell_score = 0

    else:

        buy_score = 0

        sell_score = score

    groq_result = groq_review(
        name,
        snaps,
        buy_score,
        sell_score,
        structure
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
    # HARD AI DIRECTION PROTECTION
    # --------------------------------------------------------

    if (
        synthetic_type == "CRASH"
        and
        ai_decision != "SELL"
        and
        ai_decision not in (
            "WATCH",
            "PASS"
        )
    ):

        log.warning(
            (
                "%s BLOCKED %s: "
                "CRASH can only SELL"
            ),

            name,

            ai_decision
        )

        return

    if (
        synthetic_type == "BOOM"
        and
        ai_decision != "BUY"
        and
        ai_decision not in (
            "WATCH",
            "PASS"
        )
    ):

        log.warning(
            (
                "%s BLOCKED %s: "
                "BOOM can only BUY"
            ),

            name,

            ai_decision
        )

        return

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
            snaps,
            structure
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
    # SIGNAL
    # --------------------------------------------------------

    if ai_decision in (
        "BUY",
        "SELL"
    ):

        # ====================================================
        # CRASH HARD SAFETY
        # ====================================================

        if (
            synthetic_type == "CRASH"
            and
            ai_decision != "SELL"
        ):

            log.warning(
                (
                    "%s BLOCKED %s: "
                    "CRASH only allows SELL"
                ),

                name,

                ai_decision
            )

            return

        # ====================================================
        # BOOM HARD SAFETY
        # ====================================================

        if (
            synthetic_type == "BOOM"
            and
            ai_decision != "BUY"
        ):

            log.warning(
                (
                    "%s BLOCKED %s: "
                    "BOOM only allows BUY"
                ),

                name,

                ai_decision
            )

            return

        # ====================================================
        # STRUCTURE DIRECTION
        # ====================================================

        if ai_decision != proposal:

            log.warning(
                (
                    "%s AI direction %s "
                    "does not match "
                    "required structure %s"
                ),

                name,

                ai_decision,

                proposal
            )

            return

        # ====================================================
        # SEND SIGNAL
        # ====================================================

        text = build_signal_message(
            name,
            symbol,
            ai_decision,
            score,
            groq_result,
            snaps,
            structure
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
        (
            "Targets: CRASH 300, CRASH 500, "
            "CRASH 600, CRASH 900, CRASH 1000, "
            "BOOM 300, BOOM 500, BOOM 600, "
            "BOOM 900, BOOM 1000"
        )
    )

    log.info(
        "Strategy: SPIKE -> PULLBACK -> REVERSAL"
    )

    log.info(
        "CRASH direction: SELL ONLY"
    )

    log.info(
        "BOOM direction: BUY ONLY"
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
        "Spike ATR threshold: %.2f",
        SPIKE_ATR_MULT
    )

    log.info(
        "Spike lookback: %d candles",
        SPIKE_LOOKBACK
    )

    log.info(
        "Pullback range: %.2f - %.2f ATR",
        PULLBACK_MIN_ATR,
        PULLBACK_MAX_ATR
    )

    log.info(
        "Maximum spike age: %d 5M candles",
        MAX_SPIKE_AGE_BARS
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
        "Discovered target symbols: %s",
        symbols
    )

    # --------------------------------------------------------
    # REPORT MISSING
    # --------------------------------------------------------

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
    # REPORT FOUND
    # --------------------------------------------------------

    found = [

        name

        for name in TARGET_NAMES

        if name in symbols
    ]

    log.info(
        "Found %d/%d target indices.",
        len(found),
        len(TARGET_NAMES)
    )

    # --------------------------------------------------------
    # SCAN TARGETS
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
# MAIN
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
        "Strategy: SPIKE -> PULLBACK -> REVERSAL"
    )

    log.info(
        "CRASH = SELL ONLY"
    )

    log.info(
        "BOOM = BUY ONLY"
    )

    log.info(
        "10 TARGET INDICES ENABLED"
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
