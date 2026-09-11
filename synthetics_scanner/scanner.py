import os
import json
import time
import logging

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
    os.getenv("SCAN_INTERVAL_SECONDS", "300")
)


# ============================================================
# TARGETS
# ============================================================

TARGET_NAMES = {
    "CRASH 300": ["Crash 300", "Crash 300 Index"],
    "CRASH 500": ["Crash 500", "Crash 500 Index"],
    "CRASH 600": ["Crash 600", "Crash 600 Index"],
    "CRASH 900": ["Crash 900", "Crash 900 Index"],
    "CRASH 1000": ["Crash 1000", "Crash 1000 Index"],

    "BOOM 300": ["Boom 300", "Boom 300 Index"],
    "BOOM 500": ["Boom 500", "Boom 500 Index"],
    "BOOM 600": ["Boom 600", "Boom 600 Index"],
    "BOOM 900": ["Boom 900", "Boom 900 Index"],
    "BOOM 1000": ["Boom 1000", "Boom 1000 Index"],
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
# SPIKE / PULLBACK
# ============================================================

SPIKE_ATR_MULT = 1.20

# We can recognize a spike over up to 3 candles.
SPIKE_MAX_CANDLES = 3

# Search this many recent candles for a new spike.
SPIKE_LOOKBACK = 24

# IMPORTANT:
# The setup is remembered for longer than one scan.
MAX_SETUP_AGE_BARS = 18

PULLBACK_MIN_ATR = 0.15
PULLBACK_MAX_ATR = 1.80

MAX_REVERSAL_ATR = 1.50

# Minimum candle body for early pullback weakening.
WEAKENING_BODY_ATR = 0.12


# ============================================================
# SIGNAL
# ============================================================

SIGNAL_MIN = 8

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

            data = json.load(f)

            if isinstance(data, dict):
                return data

    except Exception:
        pass

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
# DERIV
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
            item.get("underlying_symbol")
            or
            item.get("symbol")
        )

        if not symbol:
            continue

        candidates = [
            display_name,
            underlying_name
        ]

        for target, aliases in TARGET_NAMES.items():

            for candidate in candidates:

                normalized = " ".join(
                    candidate.upper().split()
                )

                if target in normalized:

                    found[target] = symbol
                    break

                for alias in aliases:

                    alias_normalized = " ".join(
                        alias.upper().split()
                    )

                    if alias_normalized in normalized:

                        found[target] = symbol
                        break

                if target in found:
                    break

            if target in found:
                break

    return found


# ============================================================
# CANDLES
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

    # Remove forming candle.
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

def build_12h_from_4h(df_4h):

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

    if len(result) < 20:

        raise RuntimeError(
            "Not enough 12H candles"
        )

    return result


# ============================================================
# RMA
# ============================================================

def rma(series, length):

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
# SNAPSHOT
# ============================================================

def indicator_snapshot(df):

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

    ema_trend = (
        "BULLISH"
        if ema20.iloc[-1] > ema50.iloc[-1]
        else
        "BEARISH"
    )

    st_direction = (
        "BULLISH"
        if st.iloc[-1] == 1
        else
        "BEARISH"
    )

    current_rsi = float(
        rsi_values.iloc[-1]
    )

    current_atr = float(
        atr_values.iloc[-1]
    )

    close = float(
        current["close"]
    )

    return {

        "close": close,

        "ema20":
            float(ema20.iloc[-1]),

        "ema50":
            float(ema50.iloc[-1]),

        "rsi":
            current_rsi,

        "atr":
            current_atr,

        "supertrend":
            st_direction,

        "ema_trend":
            ema_trend,

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

def synthetic_direction(name):

    value = name.upper()

    if "CRASH" in value:
        return "CRASH"

    if "BOOM" in value:
        return "BOOM"

    return None


# ============================================================
# NEW: MULTI-CANDLE SPIKE DETECTOR
# ============================================================

def detect_new_spike(
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
        2,
        len(df)
        -
        SPIKE_LOOKBACK
    )

    candidates = []

    # Look for 1, 2 or 3 candle impulses.
    for i in range(
        start,
        len(df)
    ):

        for span in range(
            1,
            SPIKE_MAX_CANDLES + 1
        ):

            end = i + span - 1

            if end >= len(df):
                continue

            window = df.iloc[
                i:end + 1
            ]

            base_atr = float(
                atr_values.iloc[i]
            )

            if base_atr <= 0:
                continue

            first_open = float(
                window["open"].iloc[0]
            )

            last_close = float(
                window["close"].iloc[-1]
            )

            highest = float(
                window["high"].max()
            )

            lowest = float(
                window["low"].min()
            )

            total_range = (
                highest - lowest
            )

            bullish_body = float(
                (
                    window["close"]
                    -
                    window["open"]
                )
                .clip(lower=0)
                .sum()
            )

            bearish_body = float(
                (
                    window["open"]
                    -
                    window["close"]
                )
                .clip(lower=0)
                .sum()
            )

            if direction == "BOOM":

                move = (
                    highest
                    -
                    first_open
                )

                move_atr = (
                    move
                    /
                    base_atr
                )

                body_atr = (
                    bullish_body
                    /
                    base_atr
                )

                valid = (
                    move_atr >= SPIKE_ATR_MULT
                    and
                    body_atr >= 0.55
                    and
                    last_close > first_open
                )

            elif direction == "CRASH":

                move = (
                    first_open
                    -
                    lowest
                )

                move_atr = (
                    move
                    /
                    base_atr
                )

                body_atr = (
                    bearish_body
                    /
                    base_atr
                )

                valid = (
                    move_atr >= SPIKE_ATR_MULT
                    and
                    body_atr >= 0.55
                    and
                    last_close < first_open
                )

            else:

                valid = False
                move_atr = 0

            if not valid:
                continue

            candidates.append(
                {
                    "index": end,
                    "start_index": i,
                    "time":
                        df.index[end].isoformat(),
                    "start_time":
                        df.index[i].isoformat(),
                    "open":
                        first_open,
                    "high":
                        highest,
                    "low":
                        lowest,
                    "close":
                        last_close,
                    "size_atr":
                        float(move_atr)
                }
            )

    if not candidates:
        return None

    # Prefer the newest spike.
    candidates.sort(
        key=lambda x: (
            x["index"],
            x["size_atr"]
        )
    )

    return candidates[-1]


# ============================================================
# NEW: STATEFUL SPIKE/PULLBACK/WEAKENING
# ============================================================

def detect_spike_pullback_reversal(
    df,
    direction,
    setup_state
):

    result = {

        "valid": False,

        "phase": "WAITING_FOR_SPIKE",

        "spike": False,

        "pullback": False,

        "weakening": False,

        "reversal": False,

        "entry_trigger": False,

        "spike_age": None,

        "spike_size_atr": 0.0,

        "pullback_atr": 0.0,

        "reversal_size_atr": 0.0,

        "spike_time": None
    }

    # ========================================================
    # FIND / RECOVER SPIKE
    # ========================================================

    spike = None

    saved_spike_time = setup_state.get(
        "spike_time"
    )

    if saved_spike_time:

        matching = [
            i
            for i, value in enumerate(df.index)
            if value.isoformat() == saved_spike_time
        ]

        if matching:

            i = matching[-1]

            spike = {
                "index": i,
                "time":
                    saved_spike_time,
                "open":
                    float(df.iloc[i]["open"]),
                "high":
                    float(df.iloc[i]["high"]),
                "low":
                    float(df.iloc[i]["low"]),
                "close":
                    float(df.iloc[i]["close"]),
                "size_atr":
                    float(
                        setup_state.get(
                            "spike_size_atr",
                            0
                        )
                    )
            }

    # If there is no remembered spike,
    # search for a new one.
    if spike is None:

        spike = detect_new_spike(
            df,
            direction
        )

        if spike is not None:

            # Save it immediately.
            setup_state.clear()

            setup_state.update(
                {
                    "direction": direction,
                    "spike_time":
                        spike["time"],
                    "spike_size_atr":
                        spike["size_atr"],
                    "phase":
                        "SPIKE_DETECTED"
                }
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

    result["spike"] = True
    result["spike_age"] = age
    result["spike_size_atr"] = (
        spike["size_atr"]
    )
    result["spike_time"] = (
        spike["time"]
    )

    # ========================================================
    # SPIKE TOO OLD
    # ========================================================

    if age > MAX_SETUP_AGE_BARS:

        setup_state.clear()

        result["phase"] = (
            "SPIKE_EXPIRED"
        )

        return result

    # Need at least one candle after spike.
    after = df.iloc[
        spike["index"] + 1:
    ]

    if len(after) < 1:

        result["phase"] = (
            "SPIKE_DETECTED"
        )

        return result

    atr_values = atr(
        df,
        ATR_LEN
    )

    current_atr = float(
        atr_values.iloc[-1]
    )

    if current_atr <= 0:

        result["phase"] = "NO_ATR"

        return result

    current = df.iloc[-1]
    previous = df.iloc[-2]

    # ========================================================
    # BOOM
    # ========================================================

    if direction == "BOOM":

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

            setup_state["phase"] = (
                "WAITING_FOR_PULLBACK"
            )

            result["phase"] = (
                "WAITING_FOR_PULLBACK"
            )

            return result

        result["pullback"] = True

        # ----------------------------------------------------
        # Find the pullback candles.
        # ----------------------------------------------------

        pullback_df = after[
            after["low"]
            <=
            lowest_pullback
        ]

        # ----------------------------------------------------
        # Current candle measurements.
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

        current_body_atr = (
            current_body
            /
            current_atr
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

        # ----------------------------------------------------
        # Pullback weakening.
        #
        # We don't require a huge green candle.
        # We want the DOWNWARD pullback to lose strength.
        # ----------------------------------------------------

        prev_body = abs(
            float(previous["close"])
            -
            float(previous["open"])
        )

        prev_body_atr = (
            prev_body
            /
            current_atr
        )

        current_bullish = (
            current["close"]
            >
            current["open"]
        )

        current_higher_close = (
            current["close"]
            >
            previous["close"]
        )

        pullback_low_recent = (
            float(current["low"])
            <=
            lowest_pullback
            +
            current_atr * 0.20
        )

        weakening = (
            (
                current_bullish
                and
                current_higher_close
            )
            or
            (
                not current_bullish
                and
                current_body_atr
                <=
                max(
                    WEAKENING_BODY_ATR,
                    prev_body_atr * 0.80
                )
                and
                pullback_low_recent
            )
        )

        result["weakening"] = bool(
            weakening
        )

        # ----------------------------------------------------
        # Controlled bullish reversal.
        # ----------------------------------------------------

        controlled_reversal = (
            current_bullish
            and
            current_higher_close
            and
            current_body_atr
            >=
            WEAKENING_BODY_ATR
            and
            reversal_size_atr
            <=
            MAX_REVERSAL_ATR
        )

        result["reversal"] = bool(
            controlled_reversal
        )

        # ----------------------------------------------------
        # EARLY ENTRY
        #
        # This is the important change.
        #
        # If the pullback is weakening, we can trigger
        # before the next giant BOOM spike.
        # ----------------------------------------------------

        early_entry = (
            weakening
            and
            pullback_atr
            >=
            PULLBACK_MIN_ATR
        )

        if controlled_reversal:

            result["entry_trigger"] = True
            result["valid"] = True
            result["phase"] = "BUY_READY"

            setup_state["phase"] = (
                "BUY_READY"
            )

        elif early_entry:

            result["entry_trigger"] = True
            result["valid"] = True
            result["phase"] = (
                "PULLBACK_WEAKENING_BUY"
            )

            setup_state["phase"] = (
                "PULLBACK_WEAKENING"
            )

        else:

            setup_state["phase"] = (
                "PULLBACK_DEVELOPING"
            )

            result["phase"] = (
                "PULLBACK_DEVELOPING"
            )

        return result

    # ========================================================
    # CRASH
    # ========================================================

    if direction == "CRASH":

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

            setup_state["phase"] = (
                "WAITING_FOR_PULLBACK"
            )

            result["phase"] = (
                "WAITING_FOR_PULLBACK"
            )

            return result

        result["pullback"] = True

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

        current_body_atr = (
            current_body
            /
            current_atr
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

        prev_body = abs(
            float(previous["close"])
            -
            float(previous["open"])
        )

        prev_body_atr = (
            prev_body
            /
            current_atr
        )

        current_bearish = (
            current["close"]
            <
            current["open"]
        )

        current_lower_close = (
            current["close"]
            <
            previous["close"]
        )

        pullback_high_recent = (
            float(current["high"])
            >=
            highest_pullback
            -
            current_atr * 0.20
        )

        weakening = (
            (
                current_bearish
                and
                current_lower_close
            )
            or
            (
                not current_bearish
                and
                current_body_atr
                <=
                max(
                    WEAKENING_BODY_ATR,
                    prev_body_atr * 0.80
                )
                and
                pullback_high_recent
            )
        )

        result["weakening"] = bool(
            weakening
        )

        controlled_reversal = (
            current_bearish
            and
            current_lower_close
            and
            current_body_atr
            >=
            WEAKENING_BODY_ATR
            and
            reversal_size_atr
            <=
            MAX_REVERSAL_ATR
        )

        result["reversal"] = bool(
            controlled_reversal
        )

        early_entry = (
            weakening
            and
            pullback_atr
            >=
            PULLBACK_MIN_ATR
        )

        if controlled_reversal:

            result["entry_trigger"] = True
            result["valid"] = True
            result["phase"] = "SELL_READY"

            setup_state["phase"] = (
                "SELL_READY"
            )

        elif early_entry:

            result["entry_trigger"] = True
            result["valid"] = True
            result["phase"] = (
                "PULLBACK_WEAKENING_SELL"
            )

            setup_state["phase"] = (
                "PULLBACK_WEAKENING"
            )

        else:

            setup_state["phase"] = (
                "PULLBACK_DEVELOPING"
            )

            result["phase"] = (
                "PULLBACK_DEVELOPING"
            )

        return result

    return result


# ============================================================
# HIGHER TIMEFRAME ALIGNMENT
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

    # --------------------------------------------------------
    # We keep 12H mandatory.
    #
    # But we no longer require BOTH EMA and Supertrend
    # on every timeframe.
    #
    # This prevents the scanner from becoming unnecessarily
    # strict.
    # --------------------------------------------------------

    if synthetic_type == "CRASH":

        h12_bearish = (
            h12["ema_trend"] == "BEARISH"
            or
            h12["supertrend"] == "BEARISH"
        )

        h4_bearish = (
            h4["ema_trend"] == "BEARISH"
            or
            h4["supertrend"] == "BEARISH"
        )

        h1_bearish = (
            h1["ema_trend"] == "BEARISH"
            or
            h1["supertrend"] == "BEARISH"
        )

        m15_bearish = (
            m15["ema_trend"] == "BEARISH"
            or
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

            "h12": h12_bearish,
            "h4": h4_bearish,
            "h1": h1_bearish,
            "m15": m15_bearish,
            "m5": m5_bearish,

            "all_higher": (
                h12_bearish
                and
                h4_bearish
                and
                h1_bearish
                and
                m15_bearish
            ),

            "all_confirmed": (
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

    if synthetic_type == "BOOM":

        h12_bullish = (
            h12["ema_trend"] == "BULLISH"
            or
            h12["supertrend"] == "BULLISH"
        )

        h4_bullish = (
            h4["ema_trend"] == "BULLISH"
            or
            h4["supertrend"] == "BULLISH"
        )

        h1_bullish = (
            h1["ema_trend"] == "BULLISH"
            or
            h1["supertrend"] == "BULLISH"
        )

        m15_bullish = (
            m15["ema_trend"] == "BULLISH"
            or
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

            "h12": h12_bullish,
            "h4": h4_bullish,
            "h1": h1_bullish,
            "m15": m15_bullish,
            "m5": m5_bullish,

            "all_higher": (
                h12_bullish
                and
                h4_bullish
                and
                h1_bullish
                and
                m15_bullish
            ),

            "all_confirmed": (
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

    if structure["spike"]:
        score += 2

    if structure["pullback"]:
        score += 2

    if (
        structure["weakening"]
        or
        structure["reversal"]
    ):
        score += 2

    if alignment["h12"]:
        score += 1

    if alignment["h4"]:
        score += 1

    if alignment["h1"]:
        score += 1

    if alignment["m15"]:
        score += 1

    return score, direction


# ============================================================
# GROQ
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
            "decision": "PASS",
            "confidence": 0,
            "reason": "Groq API key missing"
        }

    if Groq is None:
        return {
            "decision": "PASS",
            "confidence": 0,
            "reason": "Groq package missing"
        }

    synthetic_type = synthetic_direction(
        name
    )

    expected = (
        "SELL"
        if synthetic_type == "CRASH"
        else
        "BUY"
    )

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

        "weakening":
            structure["weakening"],

        "reversal":
            structure["reversal"],

        "phase":
            structure["phase"],

        "score":
            score
    }

    prompt = f"""
You are reviewing a synthetic-index scanner signal.

Instrument: {name}
Synthetic type: {synthetic_type}
Expected direction: {expected}

CRASH:
down spike -> upward pullback -> weakening -> SELL

BOOM:
up spike -> downward pullback -> weakening -> BUY

12H must agree with the intended direction.
4H, 1H and 15M must also agree directionally.

Do not reverse CRASH into BUY.
Do not reverse BOOM into SELL.

This is scanner-only.
There is NO automatic trading.

Technical data:
{json.dumps(data)}

Return JSON only:

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

        # Direction protection.
        if synthetic_type == "CRASH" and decision == "BUY":
            decision = "PASS"

        if synthetic_type == "BOOM" and decision == "SELL":
            decision = "PASS"

        # AI cannot override 12H/HTF protection.
        if not alignment["all_higher"]:
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

        return {
            "decision": decision,
            "confidence": confidence,
            "reason": str(
                result.get(
                    "reason",
                    ""
                )
            ).strip()
        }

    except Exception as e:

        log.error(
            "%s GROQ ERROR: %s",
            name,
            e
        )

        return {
            "decision": "PASS",
            "confidence": 0,
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

    emoji = (
        "🔴"
        if direction == "SELL"
        else
        "🟢"
    )

    return (
        f"{emoji} {name} — "
        f"{direction} PULLBACK SIGNAL\n\n"

        f"Technical score: {score}/10\n"
        f"Symbol: {symbol}\n"
        f"Price: {snaps['5M']['close']}\n\n"

        f"SETUP:\n"
        f"Spike → Pullback → "
        f"Weakening → Reversal\n\n"

        f"Phase: {structure['phase']}\n"
        f"Spike age: "
        f"{structure['spike_age']} candles\n"

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

    # --------------------------------------------------------
    # Persistent setup state
    # --------------------------------------------------------

    setups = state.setdefault(
        "setups",
        {}
    )

    setup_state = setups.setdefault(
        name,
        {}
    )

    # --------------------------------------------------------
    # DATA
    # --------------------------------------------------------

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
    # STRUCTURE
    # --------------------------------------------------------

    structure = detect_spike_pullback_reversal(
        df_5m,
        synthetic_type,
        setup_state
    )

    log.info(
        (
            "%s STRUCTURE "
            "phase=%s "
            "spike=%s "
            "pullback=%s "
            "weakening=%s "
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
        structure["weakening"],
        structure["reversal"],
        structure["spike_age"],
        structure["spike_size_atr"],
        structure["pullback_atr"],
        structure["reversal_size_atr"]
    )

    # --------------------------------------------------------
    # ALIGNMENT
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # HTF BLOCK
    #
    # We check this before sending anything.
    # --------------------------------------------------------

    if not alignment["all_higher"]:

        log.info(
            (
                "%s BLOCKED - "
                "12H/4H/1H/15M not aligned"
            ),
            name
        )

        # Important:
        # Keep the spike state alive.
        # We don't throw away the setup just because
        # HTF alignment isn't ready on this candle.

        save_state(state)

        return

    # --------------------------------------------------------
    # Need spike + pullback + weakening/reversal
    # --------------------------------------------------------

    if not structure["spike"]:

        log.info(
            "%s WAITING FOR %s SPIKE",
            name,
            synthetic_type
        )

        save_state(state)

        return

    if not structure["pullback"]:

        log.info(
            "%s SPIKE REMEMBERED - WAITING FOR PULLBACK",
            name
        )

        save_state(state)

        return

    if not structure["entry_trigger"]:

        log.info(
            "%s PULLBACK DEVELOPING - WAITING FOR WEAKENING",
            name
        )

        save_state(state)

        return

    # --------------------------------------------------------
    # SCORE
    # --------------------------------------------------------

    score, proposal = score_synthetic_setup(
        name,
        snaps,
        structure,
        alignment
    )

    log.info(
        "%s ENTRY TRIGGER score=%d direction=%s",
        name,
        score,
        proposal
    )

    if score < SIGNAL_MIN:

        log.info(
            "%s score=%d below threshold=%d",
            name,
            score,
            SIGNAL_MIN
        )

        save_state(state)

        return

    # --------------------------------------------------------
    # GROQ
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # HARD DIRECTION
    # --------------------------------------------------------

    if synthetic_type == "CRASH":

        proposal = "SELL"

    elif synthetic_type == "BOOM":

        proposal = "BUY"

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # Groq is now advisory.
    #
    # If technical structure is valid, we don't allow
    # an AI PASS to make us miss the setup.
    #
    # Groq can still block an opposite direction.
    # --------------------------------------------------------

    if ai_decision not in (
        "BUY",
        "SELL"
    ):

        ai_decision = proposal

    if synthetic_type == "CRASH":

        if ai_decision == "BUY":
            ai_decision = "SELL"

    elif synthetic_type == "BOOM":

        if ai_decision == "SELL":
            ai_decision = "BUY"

    # --------------------------------------------------------
    # DUPLICATE CONTROL
    # --------------------------------------------------------

    candle_key = (
        snaps["5M"]["candle_time"]
    )

    state_key = (
        f"{name}:SIGNAL"
    )

    if (
        state.get(state_key)
        ==
        candle_key
    ):

        log.info(
            "%s duplicate signal ignored",
            name
        )

        return

    # --------------------------------------------------------
    # SEND
    # --------------------------------------------------------

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

        # Clear setup after signal so the scanner
        # waits for a genuinely new spike.
        setups.pop(
            name,
            None
        )

        save_state(
            state
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
        "Pattern: SPIKE -> PULLBACK -> WEAKENING -> REVERSAL"
    )

    log.info(
        "CRASH = SELL ONLY"
    )

    log.info(
        "BOOM = BUY ONLY"
    )

    log.info(
        "12H direction is mandatory"
    )

    log.info(
        "Automatic trading = DISABLED"
    )

    state = load_state()

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

    for name in TARGET_NAMES:

        symbol = symbols.get(
            name
        )

        if not symbol:

            log.warning(
                "%s symbol not found",
                name
            )

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
        "Pattern = SPIKE -> PULLBACK -> WEAKENING -> REVERSAL"
    )

    log.info(
        "12H = BIG PICTURE FILTER"
    )

    log.info(
        "5M = ENTRY MONITOR"
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
