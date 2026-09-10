import os
import json
import time
import logging
from datetime import timezone

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

SCAN_INTERVAL_SECONDS = int(
    os.getenv(
        "SCAN_INTERVAL_SECONDS",
        "300"
    )
)


# ============================================================
# TARGETS
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
# TIMEFRAMES
# ============================================================

TIMEFRAMES = {
    "4H": 14400,
    "1H": 3600,
    "15M": 900,
    "5M": 300,
}


# ============================================================
# INDICATORS
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

SPIKE_ATR_MULT = 1.20

SPIKE_LOOKBACK = 20

PULLBACK_MIN_ATR = 0.20
PULLBACK_MAX_ATR = 1.50

MAX_SPIKE_AGE_BARS = 10

# The reversal candle must not be another giant spike.
MAX_REVERSAL_ATR = 1.25


# ============================================================
# SIGNAL SETTINGS
# ============================================================

SIGNAL_MIN = 8
WATCH_MIN = 6

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
# STATE
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

    url = (
        "https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}"
        "/sendMessage"
    )

    response = requests.post(
        url,
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message
        },
        timeout=20
    )

    if not response.ok:

        raise RuntimeError(
            f"Telegram HTTP error "
            f"{response.status_code}: "
            f"{response.text}"
        )

    result = response.json()

    if not result.get("ok"):

        raise RuntimeError(
            result.get(
                "description",
                "Telegram error"
            )
        )

    return result


# ============================================================
# DERIV REQUEST
# ============================================================

def deriv_request(
    ws,
    payload
):

    ws.send(
        json.dumps(payload)
    )

    while True:

        raw = ws.recv()

        data = json.loads(raw)

        if data.get("error"):

            error = data["error"]

            raise RuntimeError(
                error.get(
                    "message",
                    "Deriv API error"
                )
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

    active = data.get(
        "active_symbols",
        []
    )

    found = {}

    for item in active:

        display_name = str(
            item.get(
                "display_name",
                ""
            )
        )

        underlying_name = str(
            item.get(
                "underlying_symbol_name",
                ""
            )
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

        candidates = [
            display_name,
            underlying_name
        ]

        for target, aliases in TARGET_NAMES.items():

            matched = False

            for candidate in candidates:

                normalized = (
                    " ".join(
                        candidate.upper().split()
                    )
                )

                if target in normalized:

                    found[target] = symbol

                    matched = True

                    break

                for alias in aliases:

                    alias_normalized = (
                        " ".join(
                            alias.upper().split()
                        )
                    )

                    if (
                        alias_normalized
                        in normalized
                    ):

                        found[target] = symbol

                        matched = True

                        break

                if matched:

                    break

            if matched:

                break

    return found


# ============================================================
# GET CANDLES
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

        data = deriv_request(
            ws,
            {
                "ticks_history": symbol,
                "end": "latest",
                "count": count,
                "style": "candles",
                "granularity": granularity,
                "req_id": 2
            }
        )

    finally:

        ws.close()

    candles = data.get(
        "candles",
        []
    )

    if not candles:

        raise RuntimeError(
            f"No candles returned for {symbol}"
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
                f"Missing candle field: {column}"
            )

    df["epoch"] = pd.to_numeric(
        df["epoch"],
        errors="coerce"
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

    df = df.dropna(
        subset=required
    )

    df["time"] = pd.to_datetime(
        df["epoch"],
        unit="s",
        utc=True
    )

    df = (
        df
        .drop_duplicates("epoch")
        .sort_values("epoch")
        .set_index("time")
    )

    # --------------------------------------------------------
    # REMOVE FORMING CANDLE
    # --------------------------------------------------------

    now = pd.Timestamp.now(
        tz="UTC"
    )

    if len(df):

        last_time = df.index[-1]

        age = (
            now - last_time
        ).total_seconds()

        if age < granularity:

            df = df.iloc[:-1]

    if len(df) < 80:

        raise RuntimeError(
            f"Not enough completed candles "
            f"for {symbol}: {len(df)}"
        )

    return df


# ============================================================
# BUILD 12H
# ============================================================

def build_12h_from_4h(
    df_4h
):

    result = (
        df_4h
        .resample(
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
        df_4h["close"]
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

    result = result.dropna()

    if len(result) < 80:

        raise RuntimeError(
            "Not enough 12H candles"
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
        df["close"].shift(1)
    )

    tr = pd.concat(
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
    ).max(axis=1)

    return rma(
        tr,
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

    value = (
        100
        -
        (
            100
            /
            (1 + rs)
        )
    )

    return value.fillna(50)


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

def indicator_snapshot(
    df
):

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

    current = df.iloc[-1]

    previous = df.iloc[-2]

    ema_bullish = (
        ema20.iloc[-1]
        >
        ema50.iloc[-1]
    )

    if ema_bullish:

        ema_trend = "BULLISH"

    else:

        ema_trend = "BEARISH"

    if st.iloc[-1] == 1:

        st_direction = "BULLISH"

    else:

        st_direction = "BEARISH"

    current_rsi = float(
        rsi_values.iloc[-1]
    )

    current_atr = float(
        atr_values.iloc[-1]
    )

    current_close = float(
        current["close"]
    )

    if current_close != 0:

        atr_pct = (
            current_atr
            /
            current_close
            *
            100
        )

    else:

        atr_pct = 0.0

    if current_rsi >= 52:

        rsi_bias = "BULLISH"

    elif current_rsi <= 48:

        rsi_bias = "BEARISH"

    else:

        rsi_bias = "NEUTRAL"

    return {

        "close":
            current_close,

        "ema20":
            float(
                ema20.iloc[-1]
            ),

        "ema50":
            float(
                ema50.iloc[-1]
            ),

        "rsi":
            current_rsi,

        "atr":
            current_atr,

        "atr_pct":
            float(atr_pct),

        "supertrend":
            st_direction,

        "ema_trend":
            ema_trend,

        "rsi_bias":
            rsi_bias,

        "break_up":
            bool(
                current["close"]
                >
                previous["high"]
            ),

        "break_down":
            bool(
                current["close"]
                <
                previous["low"]
            ),

        "candle_time":
            df.index[-1].isoformat()
    }


# ============================================================
# SYNTHETIC TYPE
# ============================================================

def synthetic_direction(
    name
):

    value = name.upper()

    if "CRASH" in value:

        return "CRASH"

    if "BOOM" in value:

        return "BOOM"

    return None


# ============================================================
# FIND RECENT SPIKE
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

        body = abs(
            float(candle["close"])
            -
            float(candle["open"])
        )

        movement = max(
            candle_range,
            body
        )

        size_atr = (
            movement
            /
            candle_atr
        )

        if direction == "CRASH":

            valid = (
                candle["close"]
                <
                candle["open"]
                and
                size_atr
                >=
                SPIKE_ATR_MULT
            )

        elif direction == "BOOM":

            valid = (
                candle["close"]
                >
                candle["open"]
                and
                size_atr
                >=
                SPIKE_ATR_MULT
            )

        else:

            valid = False

        if not valid:

            continue

        candidate = {

            "index":
                i,

            "time":
                df.index[i].isoformat(),

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
                ),

            "size_atr":
                float(
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

        "valid":
            False,

        "phase":
            "WAITING",

        "spike":
            False,

        "pullback":
            False,

        "reversal":
            False,

        "spike_age":
            None,

        "spike_size_atr":
            0.0,

        "pullback_atr":
            0.0,

        "reversal_size_atr":
            0.0,

        "spike_time":
            None
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

    age = (
        len(df)
        -
        1
        -
        spike["index"]
    )

    if age > MAX_SPIKE_AGE_BARS:

        result["phase"] = (
            "SPIKE_TOO_OLD"
        )

        return result

    result["spike"] = True

    result["spike_age"] = age

    result["spike_size_atr"] = (
        spike["size_atr"]
    )

    result["spike_time"] = (
        spike["time"]
    )

    # --------------------------------------------------------
    # We need candles after the spike.
    # --------------------------------------------------------

    after = df.iloc[
        spike["index"] + 1:
    ]

    if len(after) < 2:

        result["phase"] = (
            "SPIKE_DETECTED"
        )

        return result

    current = df.iloc[-1]

    previous = df.iloc[-2]

    atr_values = atr(
        df,
        ATR_LEN
    )

    current_atr = float(
        atr_values.iloc[-1]
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

        # ----------------------------------------------------
        # The pullback must move upward from the spike low.
        # ----------------------------------------------------

        highest_pullback = float(
            after["high"].max()
        )

        pullback_distance = (
            highest_pullback
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

        pullback = (
            highest_pullback
            >
            spike["close"]
            and
            PULLBACK_MIN_ATR
            <=
            pullback_atr
            <=
            PULLBACK_MAX_ATR
        )

        if not pullback:

            result["phase"] = (
                "WAITING_FOR_PULLBACK"
            )

            return result

        result["pullback"] = True

        # ----------------------------------------------------
        # Current candle must be a controlled bearish reversal.
        # ----------------------------------------------------

        current_range = (
            float(current["high"])
            -
            float(current["low"])
        )

        current_body = abs(
            float(current["close"])
            -
            float(current["open"])
        )

        reversal_size_atr = (
            max(
                current_range,
                current_body
            )
            /
            current_atr
        )

        result["reversal_size_atr"] = (
            float(reversal_size_atr)
        )

        bearish_candle = (
            current["close"]
            <
            current["open"]
        )

        lower_close = (
            current["close"]
            <
            previous["close"]
        )

        reversal = (
            bearish_candle
            and
            lower_close
            and
            reversal_size_atr
            <=
            MAX_REVERSAL_ATR
        )

        if not reversal:

            result["phase"] = (
                "WAITING_FOR_CONTROLLED_BEARISH_REVERSAL"
            )

            return result

        result["reversal"] = True

        result["valid"] = True

        result["phase"] = (
            "SELL_READY"
        )

        return result

    # ========================================================
    # BOOM
    # ========================================================

    if direction == "BOOM":

        # ----------------------------------------------------
        # Pullback must move downward from spike high.
        # ----------------------------------------------------

        lowest_pullback = float(
            after["low"].min()
        )

        pullback_distance = (
            spike["high"]
            -
            lowest_pullback
        )

        pullback_atr = (
            pullback_distance
            /
            current_atr
        )

        result["pullback_atr"] = (
            float(pullback_atr)
        )

        pullback = (
            lowest_pullback
            <
            spike["close"]
            and
            PULLBACK_MIN_ATR
            <=
            pullback_atr
            <=
            PULLBACK_MAX_ATR
        )

        if not pullback:

            result["phase"] = (
                "WAITING_FOR_PULLBACK"
            )

            return result

        result["pullback"] = True

        # ----------------------------------------------------
        # Current candle must be a controlled bullish reversal.
        # ----------------------------------------------------

        current_range = (
            float(current["high"])
            -
            float(current["low"])
        )

        current_body = abs(
            float(current["close"])
            -
            float(current["open"])
        )

        reversal_size_atr = (
            max(
                current_range,
                current_body
            )
            /
            current_atr
        )

        result["reversal_size_atr"] = (
            float(reversal_size_atr)
        )

        bullish_candle = (
            current["close"]
            >
            current["open"]
        )

        higher_close = (
            current["close"]
            >
            previous["close"]
        )

        reversal = (
            bullish_candle
            and
            higher_close
            and
            reversal_size_atr
            <=
            MAX_REVERSAL_ATR
        )

        if not reversal:

            result["phase"] = (
                "WAITING_FOR_CONTROLLED_BULLISH_REVERSAL"
            )

            return result

        result["reversal"] = True

        result["valid"] = True

        result["phase"] = (
            "BUY_READY"
        )

        return result

    return result


# ============================================================
# HIGHER-TIMEFRAME ALIGNMENT
# ============================================================

def higher_timeframe_alignment(
    synthetic_type,
    snaps
):

    h12 = snaps["12H"]
    h4 = snaps["4H"]
    h1 = snaps["1H"]
    m15 = snaps["15M"]
    m5 = snaps["5M"]

    # ========================================================
    # CRASH SELL
    # ========================================================

    if synthetic_type == "CRASH":

        h12_bearish = (
            h12["ema_trend"] == "BEARISH"
            and
            h12["supertrend"] == "BEARISH"
        )

        h4_bearish = (
            h4["ema_trend"] == "BEARISH"
            and
            h4["supertrend"] == "BEARISH"
        )

        h1_bearish = (
            h1["ema_trend"] == "BEARISH"
            and
            h1["supertrend"] == "BEARISH"
        )

        m15_bearish = (
            m15["ema_trend"] == "BEARISH"
            and
            m15["supertrend"] == "BEARISH"
        )

        m5_bearish = (
            m5["ema_trend"] == "BEARISH"
            or
            m5["supertrend"] == "BEARISH"
            or
            m5["rsi"] <= 48
        )

        return {

            "h12":
                h12_bearish,

            "h4":
                h4_bearish,

            "h1":
                h1_bearish,

            "m15":
                m15_bearish,

            "m5":
                m5_bearish,

            "all_higher":
                (
                    h12_bearish
                    and
                    h4_bearish
                    and
                    h1_bearish
                    and
                    m15_bearish
                ),

            "all_confirmed":
                (
                    h12_bearish
                    and
                    h4_bearish
                    and
                    h1_bearish
                    and
                    m15_bearish
                    and
                    m5_bearish
                )
        }

    # ========================================================
    # BOOM BUY
    # ========================================================

    if synthetic_type == "BOOM":

        h12_bullish = (
            h12["ema_trend"] == "BULLISH"
            and
            h12["supertrend"] == "BULLISH"
        )

        h4_bullish = (
            h4["ema_trend"] == "BULLISH"
            and
            h4["supertrend"] == "BULLISH"
        )

        h1_bullish = (
            h1["ema_trend"] == "BULLISH"
            and
            h1["supertrend"] == "BULLISH"
        )

        m15_bullish = (
            m15["ema_trend"] == "BULLISH"
            and
            m15["supertrend"] == "BULLISH"
        )

        m5_bullish = (
            m5["ema_trend"] == "BULLISH"
            or
            m5["supertrend"] == "BULLISH"
            or
            m5["rsi"] >= 52
        )

        return {

            "h12":
                h12_bullish,

            "h4":
                h4_bullish,

            "h1":
                h1_bullish,

            "m15":
                m15_bullish,

            "m5":
                m5_bullish,

            "all_higher":
                (
                    h12_bullish
                    and
                    h4_bullish
                    and
                    h1_bullish
                    and
                    m15_bullish
                ),

            "all_confirmed":
                (
                    h12_bullish
                    and
                    h4_bullish
                    and
                    h1_bullish
                    and
                    m15_bullish
                    and
                    m5_bullish
                )
        }

    return {

        "h12": False,
        "h4": False,
        "h1": False,
        "m15": False,
        "m5": False,
        "all_higher": False,
        "all_confirmed": False
    }


# ============================================================
# SCORE
# ============================================================

def score_synthetic_setup(
    name,
    snaps,
    structure,
    alignment
):

    synthetic_type = synthetic_direction(
        name
    )

    if synthetic_type == "CRASH":

        direction = "SELL"

    elif synthetic_type == "BOOM":

        direction = "BUY"

    else:

        return 0, None

    score = 0

    # --------------------------------------------------------
    # STRUCTURE
    # --------------------------------------------------------

    if structure["spike"]:

        score += 2

    if structure["pullback"]:

        score += 2

    if structure["reversal"]:

        score += 2

    # --------------------------------------------------------
    # HTF
    # --------------------------------------------------------

    if alignment["h12"]:

        score += 1

    if alignment["h4"]:

        score += 1

    if alignment["h1"]:

        score += 1

    if alignment["m15"]:

        score += 1

    # --------------------------------------------------------
    # Maximum = 10
    # --------------------------------------------------------

    return score, direction


# ============================================================
# GROQ REVIEW
# ============================================================

def groq_review(
    name,
    snaps,
    score,
    structure,
    alignment
):

    if not GROQ_API_KEY:

        return {

            "decision":
                "PASS",

            "confidence":
                0,

            "reason":
                "Groq API key missing"
        }

    if Groq is None:

        return {

            "decision":
                "PASS",

            "confidence":
                0,

            "reason":
                "Groq package missing"
        }

    synthetic_type = synthetic_direction(
        name
    )

    if synthetic_type == "CRASH":

        allowed = "SELL ONLY"

        expected = "SELL"

    else:

        allowed = "BUY ONLY"

        expected = "BUY"

    data = {

        "12H":
            snaps["12H"]["ema_trend"],

        "4H":
            snaps["4H"]["ema_trend"],

        "1H":
            snaps["1H"]["ema_trend"],

        "15M":
            snaps["15M"]["ema_trend"],

        "5M":
            snaps["5M"]["supertrend"],

        "5M_RSI":
            round(
                snaps["5M"]["rsi"],
                1
            ),

        "spike":
            structure["spike"],

        "pullback":
            structure["pullback"],

        "reversal":
            structure["reversal"],

        "spike_size_atr":
            round(
                structure["spike_size_atr"],
                2
            ),

        "pullback_atr":
            round(
                structure["pullback_atr"],
                2
            ),

        "reversal_size_atr":
            round(
                structure["reversal_size_atr"],
                2
            ),

        "alignment":
            alignment["all_confirmed"]
    }

    prompt = f"""
You are a strict quality-control layer for a
synthetic-index pullback scanner.

This scanner does NOT automatically trade.

Instrument:
{name}

Synthetic type:
{synthetic_type}

Allowed direction:
{allowed}

Expected direction:
{expected}

The ONLY acceptable pattern is:

CRASH:
DOWN SPIKE
-> UPWARD PULLBACK
-> CONTROLLED BEARISH REVERSAL
-> SELL

BOOM:
UP SPIKE
-> DOWNWARD PULLBACK
-> CONTROLLED BULLISH REVERSAL
-> BUY

IMPORTANT:

12H, 4H, 1H and 15M MUST ALL agree.

If even ONE of these higher timeframes disagrees,
the final decision MUST be PASS.

Do not approve a setup simply because 5M is bearish
or bullish.

Do not approve a setup after a huge continuation candle.

The reversal must be controlled.

Technical score:
{score}/10

Data:
{json.dumps(data)}

Return ONLY JSON:

{{
  "decision": "BUY|SELL|WATCH|PASS",
  "confidence": 0,
  "reason": "short explanation"
}}
"""

    try:

        client = Groq(
            api_key=GROQ_API_KEY
        )

        response = (
            client
            .chat
            .completions
            .create(
                model=GROQ_MODEL,

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
                ],

                temperature=0.1,

                max_completion_tokens=512,

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
        # HARD DIRECTION PROTECTION
        # ----------------------------------------------------

        if (
            synthetic_type == "CRASH"
            and
            decision == "BUY"
        ):

            decision = "PASS"

        if (
            synthetic_type == "BOOM"
            and
            decision == "SELL"
        ):

            decision = "PASS"

        # ----------------------------------------------------
        # HARD HIGHER-TIMEFRAME PROTECTION
        # ----------------------------------------------------

        if not alignment["all_higher"]:

            decision = "PASS"

        # ----------------------------------------------------
        # EXPECTED DIRECTION
        # ----------------------------------------------------

        if (
            decision in
            ("BUY", "SELL")
            and
            decision != expected
        ):

            decision = "PASS"

        confidence = result.get(
            "confidence",
            0
        )

        try:

            confidence = float(
                confidence
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

        reason = str(
            result.get(
                "reason",
                ""
            )
        ).strip()

        return {

            "decision":
                decision,

            "confidence":
                confidence,

            "reason":
                reason
        }

    except Exception as e:

        log.error(
            "%s GROQ ERROR: %s",
            name,
            e
        )

        return {

            "decision":
                "PASS",

            "confidence":
                0,

            "reason":
                "Groq review failed"
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

    if direction == "SELL":

        emoji = "🔴"

    else:

        emoji = "🟢"

    return (
        f"{emoji} {name} — "
        f"{direction} PULLBACK SIGNAL\n\n"

        f"Technical score: "
        f"{score}/10\n"

        f"Symbol: {symbol}\n"

        f"Price: "
        f"{snaps['5M']['close']}\n\n"

        f"SETUP:\n"

        f"Spike → Pullback → Reversal\n"

        f"Spike size: "
        f"{structure['spike_size_atr']:.2f} ATR\n"

        f"Pullback: "
        f"{structure['pullback_atr']:.2f} ATR\n"

        f"Reversal: "
        f"{structure['reversal_size_atr']:.2f} ATR\n\n"

        f"MULTI-TIMEFRAME:\n"

        f"12H: "
        f"{snaps['12H']['ema_trend']}\n"

        f"4H: "
        f"{snaps['4H']['ema_trend']}\n"

        f"1H: "
        f"{snaps['1H']['ema_trend']}\n"

        f"15M: "
        f"{snaps['15M']['ema_trend']}\n"

        f"5M: "
        f"{snaps['5M']['supertrend']}\n"

        f"5M RSI: "
        f"{snaps['5M']['rsi']:.1f}\n\n"

        f"Groq confidence: "
        f"{groq_result['confidence']:.0f}%\n"

        f"AI note: "
        f"{groq_result['reason']}\n\n"

        f"Scanner only — "
        f"no automatic trading."
    )


# ============================================================
# WATCH MESSAGE
# ============================================================

def build_watch_message(
    name,
    symbol,
    direction,
    score,
    groq_result,
    snaps,
    structure
):

    return (
        f"👀 {name} — WATCH\n\n"

        f"Potential direction: "
        f"{direction}\n"

        f"Technical score: "
        f"{score}/10\n"

        f"Symbol: {symbol}\n"

        f"Price: "
        f"{snaps['5M']['close']}\n\n"

        f"PHASE:\n"
        f"{structure['phase']}\n\n"

        f"Spike: "
        f"{'YES' if structure['spike'] else 'NO'}\n"

        f"Pullback: "
        f"{'YES' if structure['pullback'] else 'NO'}\n"

        f"Reversal: "
        f"{'YES' if structure['reversal'] else 'NO'}\n\n"

        f"12H: "
        f"{snaps['12H']['ema_trend']}\n"

        f"4H: "
        f"{snaps['4H']['ema_trend']}\n"

        f"1H: "
        f"{snaps['1H']['ema_trend']}\n"

        f"15M: "
        f"{snaps['15M']['ema_trend']}\n"

        f"5M: "
        f"{snaps['5M']['supertrend']}\n\n"

        f"Groq confidence: "
        f"{groq_result['confidence']:.0f}%\n"

        f"AI note: "
        f"{groq_result['reason']}\n\n"

        f"Not a confirmed signal."
    )


# ============================================================
# SCAN ONE
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

    # ========================================================
    # FETCH DATA
    # ========================================================

    df_4h = get_candles(
        symbol,
        TIMEFRAMES["4H"],
        300
    )

    df_12h = build_12h_from_4h(
        df_4h
    )

    df_1h = get_candles(
        symbol,
        TIMEFRAMES["1H"],
        250
    )

    df_15m = get_candles(
        symbol,
        TIMEFRAMES["15M"],
        250
    )

    df_5m = get_candles(
        symbol,
        TIMEFRAMES["5M"],
        250
    )

    # ========================================================
    # SNAPSHOTS
    # ========================================================

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

    # ========================================================
    # STRUCTURE
    # ========================================================

    structure = (
        detect_spike_pullback_reversal(
            df_5m,
            synthetic_type
        )
    )

    log.info(
        (
            "%s STRUCTURE "
            "phase=%s "
            "spike=%s "
            "pullback=%s "
            "reversal=%s "
            "age=%s "
            "spike=%.2f ATR "
            "pullback=%.2f ATR "
            "reversal=%.2f ATR"
        ),

        name,

        structure["phase"],

        structure["spike"],

        structure["pullback"],

        structure["reversal"],

        structure["spike_age"],

        structure["spike_size_atr"],

        structure["pullback_atr"],

        structure["reversal_size_atr"]
    )

    # ========================================================
    # HIGHER-TIMEFRAME ALIGNMENT
    # ========================================================

    alignment = higher_timeframe_alignment(
        synthetic_type,
        snaps
    )

    log.info(
        (
            "%s ALIGNMENT "
            "12H=%s "
            "4H=%s "
            "1H=%s "
            "15M=%s "
            "5M=%s "
            "ALL_HTF=%s"
        ),

        name,

        alignment["h12"],

        alignment["h4"],

        alignment["h1"],

        alignment["m15"],

        alignment["m5"],

        alignment["all_higher"]
    )

    # ========================================================
    # WAITING FOR SPIKE
    # ========================================================

    if not structure["spike"]:

        log.info(
            "%s WAITING FOR %s SPIKE",
            name,
            synthetic_type
        )

        return

    # ========================================================
    # WAITING FOR PULLBACK
    # ========================================================

    if not structure["pullback"]:

        log.info(
            "%s SPIKE DETECTED - WAITING FOR PULLBACK",
            name
        )

        return

    # ========================================================
    # WAITING FOR REVERSAL
    # ========================================================

    if not structure["reversal"]:

        log.info(
            "%s PULLBACK DETECTED - WAITING FOR REVERSAL",
            name
        )

        return

    # ========================================================
    # REQUIRED HIGHER-TIMEFRAME ALIGNMENT
    # ========================================================

    if not alignment["all_higher"]:

        log.info(
            (
                "%s BLOCKED - "
                "12H/4H/1H/15M NOT FULLY ALIGNED"
            ),
            name
        )

        return

    # ========================================================
    # SCORE
    # ========================================================

    score, proposal = (
        score_synthetic_setup(
            name,
            snaps,
            structure,
            alignment
        )
    )

    log.info(
        "%s VALID STRUCTURE score=%d direction=%s",
        name,
        score,
        proposal
    )

    # ========================================================
    # SIGNAL SCORE
    # ========================================================

    if score < SIGNAL_MIN:

        log.info(
            (
                "%s score=%d below "
                "signal threshold=%d"
            ),

            name,

            score,

            SIGNAL_MIN
        )

        return

    # ========================================================
    # GROQ
    # ========================================================

    groq_result = groq_review(
        name,
        snaps,
        score,
        structure,
        alignment
    )

    ai_decision = str(
        groq_result.get(
            "decision",
            "PASS"
        )
    ).upper()

    log.info(
        (
            "%s GROQ=%s "
            "confidence=%.0f%% "
            "reason=%s"
        ),

        name,

        ai_decision,

        groq_result.get(
            "confidence",
            0
        ),

        groq_result.get(
            "reason",
            ""
        )
    )

    # ========================================================
    # FINAL HARD DIRECTION
    # ========================================================

    if synthetic_type == "CRASH":

        if proposal != "SELL":

            log.warning(
                "%s invalid CRASH direction",
                name
            )

            return

        if ai_decision == "BUY":

            log.warning(
                "%s BLOCKED CRASH BUY",
                name
            )

            return

    elif synthetic_type == "BOOM":

        if proposal != "BUY":

            log.warning(
                "%s invalid BOOM direction",
                name
            )

            return

        if ai_decision == "SELL":

            log.warning(
                "%s BLOCKED BOOM SELL",
                name
            )

            return

    # ========================================================
    # DUPLICATE CONTROL
    # ========================================================

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

    # ========================================================
    # FINAL SIGNAL
    # ========================================================

    if ai_decision in (
        "BUY",
        "SELL"
    ):

        if ai_decision != proposal:

            log.warning(
                (
                    "%s AI direction=%s "
                    "doesn't match proposal=%s"
                ),

                name,

                ai_decision,

                proposal
            )

            return

        message = build_signal_message(
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
                message
            )

            state[state_key] = (
                candle_key
            )

            log.info(
                "%s %s SIGNAL SENT",
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

    # ========================================================
    # WATCH
    # ========================================================

    if ai_decision == "WATCH":

        message = build_watch_message(
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
                message
            )

            state[state_key] = (
                candle_key
            )

            log.info(
                "%s WATCH SENT",
                name
            )

        except Exception as e:

            log.error(
                "%s TELEGRAM ERROR: %s",
                name,
                e
            )

        return

    # ========================================================
    # PASS
    # ========================================================

    log.info(
        "%s PASS - no signal",
        name
    )


# ============================================================
# RUN SCAN
# ============================================================

def run_scan():

    log.info(
        "======================================"
    )

    log.info(
        "STARTING SCAN CYCLE"
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
        "Pattern: SPIKE -> PULLBACK -> REVERSAL"
    )

    log.info(
        "CRASH = SELL ONLY"
    )

    log.info(
        "BOOM = BUY ONLY"
    )

    log.info(
        "Required HTF alignment: 12H + 4H + 1H + 15M"
    )

    log.info(
        "5M = reversal trigger"
    )

    log.info(
        "Automatic trading = DISABLED"
    )

    state = load_state()

    # ========================================================
    # SYMBOL DISCOVERY
    # ========================================================

    try:

        symbols = get_active_symbols()

    except Exception as e:

        log.exception(
            "SYMBOL DISCOVERY ERROR: %s",
            e
        )

        return

    log.info(
        "Discovered symbols: %s",
        symbols
    )

    # ========================================================
    # MISSING TARGETS
    # ========================================================

    missing = [

        name

        for name in TARGET_NAMES

        if name not in symbols
    ]

    if missing:

        log.warning(
            "Missing targets: %s",
            ", ".join(missing)
        )

    # ========================================================
    # FOUND TARGETS
    # ========================================================

    found = [

        name

        for name in TARGET_NAMES

        if name in symbols
    ]

    log.info(
        "Found %d/%d target indices",
        len(found),
        len(TARGET_NAMES)
    )

    # ========================================================
    # SCAN ALL TARGETS
    # ========================================================

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

    # ========================================================
    # SAVE STATE
    # ========================================================

    try:

        save_state(
            state
        )

    except Exception as e:

        log.error(
            "STATE SAVE ERROR: %s",
            e
        )

    log.info(
        "SCAN CYCLE FINISHED"
    )

    log.info(
        "======================================"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    log.info(
        "======================================"
    )

    log.info(
        "SYNTHETIC PULLBACK SCANNER STARTING"
    )

    log.info(
        "10 TARGETS ENABLED"
    )

    log.info(
        "CRASH = SELL ONLY"
    )

    log.info(
        "BOOM = BUY ONLY"
    )

    log.info(
        "Pattern = SPIKE -> PULLBACK -> REVERSAL"
    )

    log.info(
        "HTF = 12H + 4H + 1H + 15M MUST AGREE"
    )

    log.info(
        "5M = ENTRY REVERSAL"
    )

    log.info(
        "Scan interval = %d seconds",
        SCAN_INTERVAL_SECONDS
    )

    log.info(
        "Automatic trading = DISABLED"
    )

    log.info(
        "======================================"
    )

    while True:

        started = time.time()

        try:

            run_scan()

        except Exception as e:

            log.exception(
                "UNEXPECTED SCANNER ERROR: %s",
                e
            )

        elapsed = (
            time.time()
            -
            started
        )

        log.info(
            (
                "Next scan in %d seconds "
                "(cycle took %.1f seconds)"
            ),

            SCAN_INTERVAL_SECONDS,

            elapsed
        )

        try:

            time.sleep(
                SCAN_INTERVAL_SECONDS
            )

        except KeyboardInterrupt:

            log.info(
                "Scanner stopped."
            )

            break


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()
