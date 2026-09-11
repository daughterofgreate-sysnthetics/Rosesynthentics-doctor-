import os
import json
import time
import traceback
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import requests
import websocket


# ============================================================
# VERSION
# ============================================================

VERSION = "2026-09-BALANCED-PULLBACK-V6"


# ============================================================
# SETTINGS
# ============================================================

PUBLIC_WS_URL = (
    "wss://api.derivws.com/"
    "trading/v1/options/ws/public"
)

DERIV_WS_URL = os.getenv(
    "DERIV_WS_URL",
    PUBLIC_WS_URL
).strip()

# Safety: never use the old legacy endpoint.
if "ws.binaryws.com/websockets/v3" in DERIV_WS_URL:
    print("OLD DERIV ENDPOINT DETECTED")
    print("FORCING CURRENT PUBLIC MARKET DATA ENDPOINT")
    DERIV_WS_URL = PUBLIC_WS_URL


TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    ""
).strip()

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    ""
).strip()


SCAN_INTERVAL_SECONDS = int(
    os.getenv(
        "SCAN_INTERVAL_SECONDS",
        "60"
    )
)


# ============================================================
# TARGETS
# ============================================================
#
# IMPORTANT:
# Each TARGET contains THREE values:
#
# display name
# expected symbol
# direction
#
# CRASH = SELL ONLY
# BOOM  = BUY ONLY
#
# ============================================================

TARGETS = [
    ("CRASH 300", "CRASH300", "SELL"),
    ("CRASH 500", "CRASH500", "SELL"),
    ("CRASH 600", "CRASH600", "SELL"),
    ("CRASH 900", "CRASH900", "SELL"),
    ("CRASH 1000", "CRASH1000", "SELL"),

    ("BOOM 300", "BOOM300", "BUY"),
    ("BOOM 500", "BOOM500", "BUY"),
    ("BOOM 600", "BOOM600", "BUY"),
    ("BOOM 900", "BOOM900", "BUY"),
    ("BOOM 1000", "BOOM1000", "BUY"),
]


# ============================================================
# TIMEFRAMES
# ============================================================

TF_4H = 14400
TF_1H = 3600
TF_15M = 900
TF_5M = 300


# ============================================================
# STRATEGY SETTINGS
# ============================================================

EMA_FAST = 20
EMA_SLOW = 50

ATR_LEN = 14
RSI_LEN = 14


# ------------------------------------------------------------
# SPIKE
# ------------------------------------------------------------

SPIKE_MIN_ATR = 1.15
SPIKE_LOOKBACK = 30
MAX_SPIKE_AGE = 8


# ------------------------------------------------------------
# PULLBACK
# ------------------------------------------------------------

PULLBACK_MIN_ATR = 0.15
PULLBACK_MAX_ATR = 1.80


# ------------------------------------------------------------
# REVERSAL
# ------------------------------------------------------------

MAX_REVERSAL_ATR = 1.80


# ------------------------------------------------------------
# SIGNAL
# ------------------------------------------------------------

MIN_SCORE = 8

REQUIRE_5M_TRIGGER = True

REQUIRE_12H_DIRECTION = True

MIN_LOWER_TF_AGREEMENT = 2


# ------------------------------------------------------------
# STATE
# ------------------------------------------------------------

STATE_FILE = "state.json"


# ============================================================
# GENERAL HELPERS
# ============================================================

def now_utc():
    return datetime.now(timezone.utc)


def utc_iso(dt):
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()

    if dt.tzinfo is None:
        dt = dt.replace(
            tzinfo=timezone.utc
        )

    return dt.astimezone(
        timezone.utc
    ).isoformat()


def safe_float(
    value,
    default=np.nan
):
    try:
        return float(value)
    except Exception:
        return default


def normalize_symbol(text):
    if text is None:
        return ""

    s = str(text).upper()

    replacements = [
        " ",
        "_",
        "-",
        "/",
        "INDEX",
        "VOLATILITY",
    ]

    for item in replacements:
        s = s.replace(
            item,
            ""
        )

    return s


# ============================================================
# STATE
# ============================================================

def empty_state():
    return {
        "version": VERSION,
        "setups": {},
        "alerts": {}
    }


def load_state():

    if not os.path.exists(
        STATE_FILE
    ):
        return empty_state()

    try:

        with open(
            STATE_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            state = json.load(f)

        if (
            state.get("version")
            != VERSION
        ):

            print(
                "OLD STATE VERSION DETECTED"
            )

            print(
                "RESETTING STATE"
            )

            return empty_state()

        if "setups" not in state:
            state["setups"] = {}

        if "alerts" not in state:
            state["alerts"] = {}

        return state

    except Exception as e:

        print(
            f"STATE LOAD ERROR: {e}"
        )

        return empty_state()


def save_state(state):

    try:

        temp_file = (
            STATE_FILE + ".tmp"
        )

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

    except Exception as e:

        print(
            f"STATE SAVE ERROR: {e}"
        )


STATE = load_state()


# ============================================================
# DERIV REQUEST
# ============================================================

def deriv_request(
    payload,
    timeout=30
):
    """
    One public Deriv market-data request.

    IMPORTANT:
    No subscribe field is used.

    The payload is also printed so Railway logs show
    exactly what is being sent to Deriv.
    """

    payload = dict(payload)

    # --------------------------------------------------------
    # HARD PROTECTION AGAINST OLD subscribe PROBLEM
    # --------------------------------------------------------

    if (
        payload.get("ticks_history")
        is not None
    ):

        payload.pop(
            "subscribe",
            None
        )

    if (
        payload.get("active_symbols")
        is not None
    ):

        payload.pop(
            "subscribe",
            None
        )

    print(
        "DERIV REQUEST:",
        json.dumps(
            payload,
            separators=(",", ":")
        )
    )

    ws = None

    try:

        ws = websocket.create_connection(
            DERIV_WS_URL,
            timeout=timeout
        )

        message = json.dumps(
            payload
        )

        ws.send(message)

        while True:

            raw = ws.recv()

            if not raw:
                continue

            data = json.loads(raw)

            if "error" in data:

                raise RuntimeError(
                    data["error"]
                )

            return data

    finally:

        if ws is not None:

            try:
                ws.close()
            except Exception:
                pass


# ============================================================
# SYMBOL DISCOVERY
# ============================================================

def discover_symbols():

    print(
        "DISCOVERING DERIV SYMBOLS..."
    )

    payload = {
        "active_symbols": "brief"
    }

    # Absolutely no subscribe.
    payload.pop(
        "subscribe",
        None
    )

    data = deriv_request(
        payload
    )

    symbols = data.get(
        "active_symbols",
        []
    )

    if not symbols:

        raise RuntimeError(
            "Deriv returned no active symbols"
        )

    discovered = {}

    for item in symbols:

        symbol = (
            item.get(
                "underlying_symbol"
            )
            or
            item.get(
                "symbol"
            )
        )

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
                "name"
            )
            or
            ""
        )

        if not symbol:
            continue

        key_symbol = normalize_symbol(
            symbol
        )

        key_name = normalize_symbol(
            name
        )

        discovered[
            key_symbol
        ] = symbol

        if key_name:

            discovered[
                key_name
            ] = symbol

    resolved = {}

    # ========================================================
    # IMPORTANT FIX
    #
    # TARGETS has 3 values:
    #
    # display_name
    # expected
    # direction
    #
    # ========================================================

    for (
        display_name,
        expected,
        direction
    ) in TARGETS:

        expected_key = normalize_symbol(
            expected
        )

        found = None

        # ----------------------------------------------------
        # Exact match
        # ----------------------------------------------------

        if expected_key in discovered:

            found = discovered[
                expected_key
            ]

        # ----------------------------------------------------
        # Partial match
        # ----------------------------------------------------

        if found is None:

            for (
                discovered_key,
                discovered_symbol
            ) in discovered.items():

                if (
                    expected_key
                    in discovered_key
                    or
                    discovered_key
                    in expected_key
                ):

                    found = (
                        discovered_symbol
                    )

                    break

        if found:

            resolved[
                expected
            ] = found

            print(
                f"FOUND {display_name}: "
                f"{found} "
                f"[{direction}]"
            )

        else:

            print(
                f"NOT FOUND: "
                f"{display_name} "
                f"(expected {expected})"
            )

    if not resolved:

        raise RuntimeError(
            "None of the requested "
            "synthetic indices were found"
        )

    return resolved


# ============================================================
# CANDLE FETCHING
# ============================================================

def fetch_candles(
    symbol,
    granularity,
    count=220
):
    """
    Fetch candles from Deriv.

    No subscribe field.

    Removes only candles that are actually incomplete.
    """

    payload = {
        "ticks_history": symbol,
        "end": "latest",
        "style": "candles",
        "granularity": int(
            granularity
        ),
        "count": int(count)
    }

    # Absolute protection.
    payload.pop(
        "subscribe",
        None
    )

    data = deriv_request(
        payload,
        timeout=30
    )

    candles = data.get(
        "candles"
    )

    if not candles:

        raise RuntimeError(
            f"No candles returned for "
            f"{symbol} "
            f"granularity={granularity}"
        )

    rows = []

    for candle in candles:

        epoch = candle.get(
            "epoch"
        )

        if epoch is None:
            continue

        try:
            epoch = int(epoch)
        except Exception:
            continue

        rows.append(
            {
                "time": pd.to_datetime(
                    epoch,
                    unit="s",
                    utc=True
                ),

                "open": safe_float(
                    candle.get(
                        "open"
                    )
                ),

                "high": safe_float(
                    candle.get(
                        "high"
                    )
                ),

                "low": safe_float(
                    candle.get(
                        "low"
                    )
                ),

                "close": safe_float(
                    candle.get(
                        "close"
                    )
                ),
            }
        )

    if not rows:

        raise RuntimeError(
            f"Invalid candle data "
            f"for {symbol}"
        )

    df = pd.DataFrame(
        rows
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
        subset=[
            "time",
            "open",
            "high",
            "low",
            "close"
        ]
    )

    df = df.sort_values(
        "time"
    )

    df = df.drop_duplicates(
        subset=["time"],
        keep="last"
    )

    df = df.reset_index(
        drop=True
    )

    # --------------------------------------------------------
    # REMOVE ONLY THE CURRENT INCOMPLETE CANDLE
    # --------------------------------------------------------

    current_time = pd.Timestamp.now(
        tz="UTC"
    )

    candle_seconds = int(
        granularity
    )

    candle_duration = pd.to_timedelta(
        candle_seconds,
        unit="s"
    )

    # A candle is complete when:
    #
    # candle open time + candle duration <= now
    #
    completed_mask = (
        df["time"]
        + candle_duration
        <= current_time
    )

    df = df[
        completed_mask
    ].copy()

    df = df.reset_index(
        drop=True
    )

    if len(df) < 60:

        raise RuntimeError(
            f"Not enough completed "
            f"candles for {symbol} "
            f"TF={granularity}. "
            f"Got {len(df)}"
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
            np.nan
        )
    )

    result = (
        100
        -
        (
            100
            /
            (
                1 + rs
            )
        )
    )

    return result.fillna(
        50
    )


def add_indicators(df):

    d = df.copy()

    d["ema20"] = ema(
        d["close"],
        EMA_FAST
    )

    d["ema50"] = ema(
        d["close"],
        EMA_SLOW
    )

    d["atr"] = atr(
        d,
        ATR_LEN
    )

    d["rsi"] = rsi(
        d["close"],
        RSI_LEN
    )

    d["body"] = (
        d["close"]
        -
        d["open"]
    ).abs()

    d["range"] = (
        d["high"]
        -
        d["low"]
    )

    d["bull"] = (
        d["close"]
        >
        d["open"]
    )

    d["bear"] = (
        d["close"]
        <
        d["open"]
    )

    return d


# ============================================================
# SUPERTREND
# ============================================================

def supertrend_direction(
    df,
    atr_period=10,
    factor=3.0
):

    if len(df) < (
        atr_period + 5
    ):

        return 0

    high = df["high"]
    low = df["low"]
    close = df["close"]

    atr_value = atr(
        df,
        atr_period
    )

    hl2 = (
        high + low
    ) / 2

    upper = (
        hl2
        +
        factor * atr_value
    )

    lower = (
        hl2
        -
        factor * atr_value
    )

    final_upper = upper.copy()
    final_lower = lower.copy()

    direction = pd.Series(
        0,
        index=df.index,
        dtype=int
    )

    for i in range(
        1,
        len(df)
    ):

        if (
            upper.iloc[i]
            <
            final_upper.iloc[i - 1]
            or
            close.iloc[i - 1]
            >
            final_upper.iloc[i - 1]
        ):

            final_upper.iloc[i] = (
                upper.iloc[i]
            )

        else:

            final_upper.iloc[i] = (
                final_upper.iloc[i - 1]
            )

        if (
            lower.iloc[i]
            >
            final_lower.iloc[i - 1]
            or
            close.iloc[i - 1]
            <
            final_lower.iloc[i - 1]
        ):

            final_lower.iloc[i] = (
                lower.iloc[i]
            )

        else:

            final_lower.iloc[i] = (
                final_lower.iloc[i - 1]
            )

        previous_direction = (
            direction.iloc[i - 1]
        )

        if previous_direction == 0:

            if (
                close.iloc[i]
                >=
                hl2.iloc[i]
            ):

                direction.iloc[i] = 1

            else:

                direction.iloc[i] = -1

        elif previous_direction == 1:

            if (
                close.iloc[i]
                <=
                final_lower.iloc[i]
            ):

                direction.iloc[i] = -1

            else:

                direction.iloc[i] = 1

        else:

            if (
                close.iloc[i]
                >=
                final_upper.iloc[i]
            ):

                direction.iloc[i] = 1

            else:

                direction.iloc[i] = -1

    return int(
        direction.iloc[-1]
    )


# ============================================================
# 12H RESAMPLING
# ============================================================

def build_12h_from_4h(
    df4
):

    if df4.empty:
        return pd.DataFrame()

    x = df4.copy()

    x = x.set_index(
        "time"
    )

    result = x.resample(
        "12h",
        label="right",
        closed="right"
    ).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last"
        }
    )

    result = result.dropna()

    result = result.reset_index()

    return result


# ============================================================
# TIMEFRAME DIRECTION
# ============================================================

def timeframe_direction(
    df
):
    """
    Direction uses 3 measurements:

    1. Price vs EMA20
    2. EMA20 vs EMA50
    3. Supertrend

    2 of 3 = directional agreement.
    """

    if len(df) < 60:
        return 0

    d = add_indicators(
        df
    )

    last = d.iloc[-1]

    bull_score = 0
    bear_score = 0

    # --------------------------------------------------------
    # Price vs EMA20
    # --------------------------------------------------------

    if (
        last["close"]
        >
        last["ema20"]
    ):

        bull_score += 1

    elif (
        last["close"]
        <
        last["ema20"]
    ):

        bear_score += 1

    # --------------------------------------------------------
    # EMA20 vs EMA50
    # --------------------------------------------------------

    if (
        last["ema20"]
        >
        last["ema50"]
    ):

        bull_score += 1

    elif (
        last["ema20"]
        <
        last["ema50"]
    ):

        bear_score += 1

    # --------------------------------------------------------
    # Supertrend
    # --------------------------------------------------------

    st = supertrend_direction(
        d
    )

    if st > 0:
        bull_score += 1

    elif st < 0:
        bear_score += 1

    if bull_score >= 2:
        return 1

    if bear_score >= 2:
        return -1

    return 0


# ============================================================
# 12H DIRECTION
# ============================================================

def h12_direction(
    df4
):
    """
    Broad 12H regime.

    Uses:

    1. Price vs EMA20
    2. EMA20 vs EMA50
    3. EMA20 slope

    Requires 2/3.
    """

    h12 = build_12h_from_4h(
        df4
    )

    if len(h12) < 55:
        return 0

    d = add_indicators(
        h12
    )

    last = d.iloc[-1]
    previous = d.iloc[-2]

    bull_score = 0
    bear_score = 0

    # --------------------------------------------------------
    # Price vs EMA20
    # --------------------------------------------------------

    if (
        last["close"]
        >
        last["ema20"]
    ):

        bull_score += 1

    elif (
        last["close"]
        <
        last["ema20"]
    ):

        bear_score += 1

    # --------------------------------------------------------
    # EMA20 vs EMA50
    # --------------------------------------------------------

    if (
        last["ema20"]
        >
        last["ema50"]
    ):

        bull_score += 1

    elif (
        last["ema20"]
        <
        last["ema50"]
    ):

        bear_score += 1

    # --------------------------------------------------------
    # EMA20 slope
    # --------------------------------------------------------

    if (
        last["ema20"]
        >
        previous["ema20"]
    ):

        bull_score += 1

    elif (
        last["ema20"]
        <
        previous["ema20"]
    ):

        bear_score += 1

    if bull_score >= 2:
        return 1

    if bear_score >= 2:
        return -1

    return 0


# ============================================================
# HIGHER TIMEFRAME ANALYSIS
# ============================================================

def analyze_higher_timeframes(
    h12,
    h4,
    h1,
    m15,
    direction
):

    expected = (
        -1
        if direction == "SELL"
        else 1
    )

    dirs = {
        "12H": h12,
        "4H": timeframe_direction(
            h4
        ),
        "1H": timeframe_direction(
            h1
        ),
        "15M": timeframe_direction(
            m15
        ),
    }

    h12_ok = (
        dirs["12H"]
        ==
        expected
    )

    lower_ok_count = sum(
        1
        for tf in [
            "4H",
            "1H",
            "15M"
        ]
        if dirs[tf] == expected
    )

    lower_ok = (
        lower_ok_count
        >=
        MIN_LOWER_TF_AGREEMENT
    )

    all_higher = (
        h12_ok
        and
        lower_ok
    )

    return {
        "dirs": dirs,
        "h12_ok": h12_ok,
        "lower_ok_count": lower_ok_count,
        "lower_ok": lower_ok,
        "all_higher": all_higher,
    }


# ============================================================
# SPIKE DETECTION
# ============================================================

def detect_spike(
    df,
    direction
):
    """
    Detects recent directional spike.

    CRASH:
        downward spike

    BOOM:
        upward spike

    Allows 1-3 candle structures.
    """

    if len(df) < (
        SPIKE_LOOKBACK + 5
    ):

        return None

    d = add_indicators(
        df
    )

    latest_index = (
        len(d) - 1
    )

    start = max(
        EMA_SLOW + 2,
        latest_index
        -
        SPIKE_LOOKBACK
    )

    candidates = []

    # --------------------------------------------------------
    # 1, 2 and 3 candle spike structures
    # --------------------------------------------------------

    for end in range(
        start,
        latest_index + 1
    ):

        for width in [
            1,
            2,
            3
        ]:

            first = (
                end
                -
                width
                +
                1
            )

            if first < start:
                continue

            section = d.iloc[
                first:end + 1
            ]

            if section.empty:
                continue

            atr_ref = safe_float(
                section["atr"].iloc[-1]
            )

            if (
                not np.isfinite(
                    atr_ref
                )
                or
                atr_ref <= 0
            ):

                continue

            high = float(
                section["high"].max()
            )

            low = float(
                section["low"].min()
            )

            total_range = (
                high - low
            )

            total_body = (
                float(
                    section["close"].iloc[-1]
                )
                -
                float(
                    section["open"].iloc[0]
                )
            )

            size_atr = (
                total_range
                /
                atr_ref
            )

            if (
                size_atr
                <
                SPIKE_MIN_ATR
            ):

                continue

            # ------------------------------------------------
            # CRASH
            # ------------------------------------------------

            if direction == "SELL":

                if total_body >= 0:
                    continue

                if not bool(
                    section["bear"].iloc[-1]
                ):
                    continue

                candidates.append(
                    {
                        "index": end,
                        "time": section[
                            "time"
                        ].iloc[-1],
                        "high": high,
                        "low": low,
                        "size_atr": float(
                            size_atr
                        ),
                        "width": width,
                        "atr": float(
                            atr_ref
                        ),
                    }
                )

            # ------------------------------------------------
            # BOOM
            # ------------------------------------------------

            else:

                if total_body <= 0:
                    continue

                if not bool(
                    section["bull"].iloc[-1]
                ):
                    continue

                candidates.append(
                    {
                        "index": end,
                        "time": section[
                            "time"
                        ].iloc[-1],
                        "high": high,
                        "low": low,
                        "size_atr": float(
                            size_atr
                        ),
                        "width": width,
                        "atr": float(
                            atr_ref
                        ),
                    }
                )

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: x["time"]
    )

    spike = candidates[-1]

    age = (
        latest_index
        -
        spike["index"]
    )

    if age < 0:
        return None

    if age > MAX_SPIKE_AGE:
        return None

    spike["age"] = int(
        age
    )

    return spike


# ============================================================
# PULLBACK ANALYSIS
# ============================================================

def analyze_pullback(
    df,
    spike,
    direction
):
    """
    CRASH SELL:

        Down spike
             ↓
        upward pullback
             ↑
        pullback weakens
             ↓
        bearish reversal

    BOOM BUY:

        Up spike
             ↑
        downward pullback
             ↓
        pullback weakens
             ↑
        bullish reversal
    """

    if spike is None:
        return None

    d = add_indicators(
        df
    )

    spike_index = int(
        spike["index"]
    )

    if spike_index >= len(d):
        return None

    current = d.iloc[-1]

    after = d.iloc[
        spike_index + 1:
    ].copy()

    if len(after) < 1:

        return {
            "phase":
                "WAITING_FOR_PULLBACK",

            "pullback_atr": 0.0,

            "reversal_atr": 0.0,

            "pullback_high":
                float(
                    spike["high"]
                ),

            "pullback_low":
                float(
                    spike["low"]
                ),

            "zone_ok": False,

            "reversal_ok": False,

            "weakening": False,

            "trigger_started": False,
        }

    spike_atr = max(
        float(
            spike["atr"]
        ),
        1e-10
    )

    # ========================================================
    # CRASH SELL
    # ========================================================

    if direction == "SELL":

        pullback_high = float(
            max(
                spike["high"],
                after["high"].max()
            )
        )

        pullback_distance = (
            pullback_high
            -
            float(
                spike["low"]
            )
        )

        pullback_atr = (
            pullback_distance
            /
            spike_atr
        )

        reversal_distance = (
            pullback_high
            -
            float(
                current["close"]
            )
        )

        reversal_atr = (
            reversal_distance
            /
            spike_atr
        )

        pullback_ok = (
            PULLBACK_MIN_ATR
            <=
            pullback_atr
            <=
            PULLBACK_MAX_ATR
        )

        zone_ok = (
            float(
                current["close"]
            )
            <
            pullback_high
        )

        weakening = False

        if len(after) >= 2:

            last2 = after.iloc[
                -2:
            ]

            bearish_count = int(
                last2["bear"].sum()
            )

            lower_high = (
                float(
                    last2["high"].iloc[-1]
                )
                <=
                float(
                    last2["high"].iloc[-2]
                )
            )

            below_ema = (
                float(
                    current["close"]
                )
                <
                float(
                    current["ema20"]
                )
            )

            weakening = (
                bearish_count >= 1
                or
                lower_high
                or
                below_ema
            )

        reversal_ok = (
            reversal_atr
            <=
            MAX_REVERSAL_ATR
        )

        trigger_started = (
            bool(
                current["bear"]
            )
            and
            float(
                current["close"]
            )
            <
            float(
                d["close"].iloc[-2]
            )
        )

        if not pullback_ok:

            phase = (
                "PULLBACK_TOO_SMALL_OR_LARGE"
            )

        elif not weakening:

            phase = (
                "PULLBACK_WEAKENING_WAIT"
            )

        elif not reversal_ok:

            phase = (
                "REVERSAL_TOO_LARGE"
            )

        elif trigger_started:

            phase = (
                "REVERSAL_SELL"
            )

        else:

            phase = (
                "PULLBACK_WEAKENING_SELL"
            )

        return {
            "phase": phase,

            "pullback_atr":
                float(
                    pullback_atr
                ),

            "reversal_atr":
                float(
                    reversal_atr
                ),

            "pullback_high":
                pullback_high,

            "pullback_low":
                float(
                    spike["low"]
                ),

            "zone_ok":
                bool(
                    zone_ok
                ),

            "reversal_ok":
                bool(
                    reversal_ok
                ),

            "weakening":
                bool(
                    weakening
                ),

            "trigger_started":
                bool(
                    trigger_started
                ),
        }

    # ========================================================
    # BOOM BUY
    # ========================================================

    pullback_low = float(
        min(
            spike["low"],
            after["low"].min()
        )
    )

    pullback_distance = (
        float(
            spike["high"]
        )
        -
        pullback_low
    )

    pullback_atr = (
        pullback_distance
        /
        spike_atr
    )

    reversal_distance = (
        float(
            current["close"]
        )
        -
        pullback_low
    )

    reversal_atr = (
        reversal_distance
        /
        spike_atr
    )

    pullback_ok = (
        PULLBACK_MIN_ATR
        <=
        pullback_atr
        <=
        PULLBACK_MAX_ATR
    )

    zone_ok = (
        float(
            current["close"]
        )
        >
        pullback_low
    )

    weakening = False

    if len(after) >= 2:

        last2 = after.iloc[
            -2:
        ]

        bullish_count = int(
            last2["bull"].sum()
        )

        higher_low = (
            float(
                last2["low"].iloc[-1]
            )
            >=
            float(
                last2["low"].iloc[-2]
            )
        )

        above_ema = (
            float(
                current["close"]
            )
            >
            float(
                current["ema20"]
            )
        )

        weakening = (
            bullish_count >= 1
            or
            higher_low
            or
            above_ema
        )

    reversal_ok = (
        reversal_atr
        <=
        MAX_REVERSAL_ATR
    )

    trigger_started = (
        bool(
            current["bull"]
        )
        and
        float(
            current["close"]
        )
        >
        float(
            d["close"].iloc[-2]
        )
    )

    if not pullback_ok:

        phase = (
            "PULLBACK_TOO_SMALL_OR_LARGE"
        )

    elif not weakening:

        phase = (
            "PULLBACK_WEAKENING_WAIT"
        )

    elif not reversal_ok:

        phase = (
            "REVERSAL_TOO_LARGE"
        )

    elif trigger_started:

        phase = (
            "REVERSAL_BUY"
        )

    else:

        phase = (
            "PULLBACK_WEAKENING_BUY"
        )

    return {
        "phase": phase,

        "pullback_atr":
            float(
                pullback_atr
            ),

        "reversal_atr":
            float(
                reversal_atr
            ),

        "pullback_high":
            float(
                spike["high"]
            ),

        "pullback_low":
            pullback_low,

        "zone_ok":
            bool(
                zone_ok
            ),

        "reversal_ok":
            bool(
                reversal_ok
            ),

        "weakening":
            bool(
                weakening
            ),

        "trigger_started":
            bool(
                trigger_started
            ),
    }


# ============================================================
# 5M ENTRY TRIGGER
# ============================================================

def five_minute_trigger(
    df,
    direction
):

    d = add_indicators(
        df
    )

    if len(d) < 25:

        return {
            "ok": False,
            "bull": False,
            "bear": False,
            "rsi": 50.0,
            "range_atr": 0.0,
            "time": d["time"].iloc[-1]
            if len(d)
            else now_utc(),
        }

    current = d.iloc[-1]

    previous = d.iloc[-2]

    atr_value = max(
        float(
            current["atr"]
        ),
        1e-10
    )

    candle_range_atr = (
        float(
            current["high"]
            -
            current["low"]
        )
        /
        atr_value
    )

    controlled = (
        candle_range_atr <= 2.0
    )

    # ========================================================
    # CRASH SELL
    # ========================================================

    if direction == "SELL":

        bearish = (
            current["close"]
            <
            current["ema20"]
            and
            current["close"]
            <
            current["open"]
        )

        break_low = (
            current["close"]
            <
            previous["low"]
        )

        ok = (
            bearish
            and
            break_low
            and
            controlled
        )

        return {
            "ok": bool(ok),

            "bull": False,

            "bear": bool(
                bearish
                and
                break_low
            ),

            "rsi": float(
                current["rsi"]
            ),

            "range_atr": float(
                candle_range_atr
            ),

            "time":
                current["time"],
        }

    # ========================================================
    # BOOM BUY
    # ========================================================

    bullish = (
        current["close"]
        >
        current["ema20"]
        and
        current["close"]
        >
        current["open"]
    )

    break_high = (
        current["close"]
        >
        previous["high"]
    )

    ok = (
        bullish
        and
        break_high
        and
        controlled
    )

    return {
        "ok": bool(ok),

        "bull": bool(
            bullish
            and
            break_high
        ),

        "bear": False,

        "rsi": float(
            current["rsi"]
        ),

        "range_atr": float(
            candle_range_atr
        ),

        "time":
            current["time"],
    }


# ============================================================
# SCORE
# ============================================================

def calculate_score(
    higher,
    pullback,
    trigger,
    direction
):

    score = 0

    reasons = []

    # --------------------------------------------------------
    # 12H
    # --------------------------------------------------------

    if higher["h12_ok"]:

        score += 3

        reasons.append(
            "12H agrees"
        )

    # --------------------------------------------------------
    # Lower timeframes
    # --------------------------------------------------------

    if (
        higher["lower_ok_count"]
        >= 2
    ):

        score += 3

        reasons.append(
            f"{higher['lower_ok_count']}/3 "
            "lower TFs agree"
        )

    # --------------------------------------------------------
    # 5M
    # --------------------------------------------------------

    if trigger["ok"]:

        score += 2

        reasons.append(
            "5M trigger"
        )

    # --------------------------------------------------------
    # Pullback
    # --------------------------------------------------------

    if (
        PULLBACK_MIN_ATR
        <=
        pullback["pullback_atr"]
        <=
        PULLBACK_MAX_ATR
    ):

        score += 2

        reasons.append(
            "valid pullback"
        )

    # --------------------------------------------------------
    # Weakening
    # --------------------------------------------------------

    if pullback["weakening"]:

        score += 1

        reasons.append(
            "pullback weakening"
        )

    # --------------------------------------------------------
    # Controlled reversal
    # --------------------------------------------------------

    if pullback["reversal_ok"]:

        score += 1

        reasons.append(
            "controlled reversal"
        )

    return (
        score,
        reasons
    )


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(
    text
):

    if not TELEGRAM_BOT_TOKEN:

        print(
            "TELEGRAM_BOT_TOKEN NOT SET"
        )

        return False

    if not TELEGRAM_CHAT_ID:

        print(
            "TELEGRAM_CHAT_ID NOT SET"
        )

        return False

    url = (
        "https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id":
            TELEGRAM_CHAT_ID,

        "text":
            text
    }

    try:

        response = requests.post(
            url,
            json=payload,
            timeout=20
        )

        if response.status_code != 200:

            print(
                "TELEGRAM ERROR:",
                response.status_code,
                response.text
            )

            return False

        print(
            "TELEGRAM SENT"
        )

        return True

    except Exception as e:

        print(
            "TELEGRAM SEND ERROR:",
            e
        )

        return False


# ============================================================
# FORMATTERS
# ============================================================

def direction_name(
    value
):

    if value > 0:
        return "BULLISH"

    if value < 0:
        return "BEARISH"

    return "NEUTRAL"


def price_string(
    value
):

    try:

        value = float(
            value
        )

        if value >= 1000:
            return f"{value:.3f}"

        if value >= 100:
            return f"{value:.3f}"

        if value >= 10:
            return f"{value:.5f}"

        return f"{value:.5f}"

    except Exception:

        return str(
            value
        )


# ============================================================
# SIGNAL MESSAGE
# ============================================================

def create_signal_message(
    display_name,
    direction,
    current_price,
    score,
    spike,
    pullback,
    higher,
    trigger
):

    emoji = (
        "🔴"
        if direction == "SELL"
        else "🟢"
    )

    action = direction

    dirs = higher[
        "dirs"
    ]

    five_direction = (
        "BULLISH"
        if trigger["bull"]
        else
        "BEARISH"
        if trigger["bear"]
        else
        "NEUTRAL"
    )

    return (
        f"{emoji} "
        f"{display_name} — "
        f"{action} PULLBACK SIGNAL\n"
        f"\n"
        f"Technical score: "
        f"{score}/13\n"
        f"Symbol: "
        f"{display_name.replace(' ', '')}\n"
        f"Price: "
        f"{price_string(current_price)}\n"
        f"\n"
        f"SETUP:\n"
        f"Spike → Pullback → "
        f"Weakening → Reversal\n"
        f"\n"
        f"Phase: "
        f"{pullback['phase']}\n"
        f"Spike age: "
        f"{spike['age']} candles\n"
        f"Spike size: "
        f"{spike['size_atr']:.2f} ATR\n"
        f"Pullback: "
        f"{pullback['pullback_atr']:.2f} ATR\n"
        f"Reversal: "
        f"{pullback['reversal_atr']:.2f} ATR\n"
        f"\n"
        f"MULTI-TIMEFRAME:\n"
        f"12H: "
        f"{direction_name(dirs['12H'])}\n"
        f"4H: "
        f"{direction_name(dirs['4H'])}\n"
        f"1H: "
        f"{direction_name(dirs['1H'])}\n"
        f"15M: "
        f"{direction_name(dirs['15M'])}\n"
        f"5M: "
        f"{five_direction}\n"
        f"5M RSI: "
        f"{trigger['rsi']:.1f}\n"
        f"\n"
        f"ENTRY TYPE:\n"
        f"{action} after pullback "
        f"confirmation\n"
        f"\n"
        f"⚠️ Signal scanner only "
        f"— NO AUTO TRADING"
    )


# ============================================================
# WATCH MESSAGE
# ============================================================

def create_watch_message(
    display_name,
    direction,
    spike,
    pullback,
    higher
):

    return (
        f"👀 {display_name} — WATCH\n"
        f"\n"
        f"Potential {direction} "
        f"pullback setup\n"
        f"\n"
        f"Spike: "
        f"{spike['size_atr']:.2f} ATR\n"
        f"Age: "
        f"{spike['age']} candles\n"
        f"Pullback: "
        f"{pullback['pullback_atr']:.2f} ATR\n"
        f"Phase: "
        f"{pullback['phase']}\n"
        f"\n"
        f"12H: "
        f"{direction_name(higher['dirs']['12H'])}\n"
        f"4H: "
        f"{direction_name(higher['dirs']['4H'])}\n"
        f"1H: "
        f"{direction_name(higher['dirs']['1H'])}\n"
        f"15M: "
        f"{direction_name(higher['dirs']['15M'])}\n"
        f"\n"
        f"Waiting for 5M confirmation."
    )


# ============================================================
# STRUCTURE LOGGING
# ============================================================

def log_structure(
    display_name,
    direction,
    spike,
    pullback,
    higher,
    trigger
):

    print(
        f"{display_name} STRUCTURE "
        f"direction={direction} "
        f"phase={pullback['phase']} "
        f"age={spike['age']} "
        f"spike={spike['size_atr']:.2f}ATR "
        f"pullback={pullback['pullback_atr']:.2f}ATR "
        f"reversal={pullback['reversal_atr']:.2f}ATR"
    )

    print(
        f"{display_name} ALIGNMENT "
        f"12H={higher['h12_ok']} "
        f"4H={higher['dirs']['4H']} "
        f"1H={higher['dirs']['1H']} "
        f"15M={higher['dirs']['15M']} "
        f"lower={higher['lower_ok_count']}/3 "
        f"5M={trigger['ok']}"
    )


# ============================================================
# ONE SYMBOL SCAN
# ============================================================

def scan_one(
    display_name,
    symbol,
    direction
):

    try:

        print(
            f"\nScanning "
            f"{display_name} "
            f"({symbol})..."
        )

        # ====================================================
        # FETCH DATA
        # ====================================================

        h4 = fetch_candles(
            symbol,
            TF_4H,
            220
        )

        h1 = fetch_candles(
            symbol,
            TF_1H,
            220
        )

        m15 = fetch_candles(
            symbol,
            TF_15M,
            260
        )

        m5 = fetch_candles(
            symbol,
            TF_5M,
            260
        )

        # ====================================================
        # PRICE
        # ====================================================

        current_price = float(
            m5["close"].iloc[-1]
        )

        # ====================================================
        # 12H + LOWER TF
        # ====================================================

        h12 = h12_direction(
            h4
        )

        higher = analyze_higher_timeframes(
            h12,
            h4,
            h1,
            m15,
            direction
        )

        # ====================================================
        # SPIKE
        # ====================================================

        spike = detect_spike(
            m15,
            direction
        )

        if spike is None:

            print(
                f"{display_name} "
                f"NO RECENT "
                f"{direction} SPIKE"
            )

            return

        # ====================================================
        # PULLBACK
        # ====================================================

        pullback = analyze_pullback(
            m15,
            spike,
            direction
        )

        if pullback is None:

            print(
                f"{display_name} "
                f"PULLBACK ANALYSIS FAILED"
            )

            return

        # ====================================================
        # 5M
        # ====================================================

        trigger = five_minute_trigger(
            m5,
            direction
        )

        # ====================================================
        # LOG
        # ====================================================

        log_structure(
            display_name,
            direction,
            spike,
            pullback,
            higher,
            trigger
        )

        # ====================================================
        # 12H DIRECTION PROTECTION
        # ====================================================

        expected_h12 = (
            -1
            if direction == "SELL"
            else 1
        )

        if REQUIRE_12H_DIRECTION:

            if h12 != expected_h12:

                print(
                    f"{display_name} "
                    f"BLOCKED - "
                    f"12H does not agree "
                    f"with {direction}"
                )

                return

        # ====================================================
        # LOWER TF AGREEMENT
        # ====================================================

        if (
            higher[
                "lower_ok_count"
            ]
            <
            MIN_LOWER_TF_AGREEMENT
        ):

            print(
                f"{display_name} "
                f"BLOCKED - only "
                f"{higher['lower_ok_count']}/3 "
                f"lower TFs agree"
            )

            return

        # ====================================================
        # PULLBACK QUALITY
        # ====================================================

        if not (
            PULLBACK_MIN_ATR
            <=
            pullback["pullback_atr"]
            <=
            PULLBACK_MAX_ATR
        ):

            print(
                f"{display_name} "
                f"PULLBACK NOT READY "
                f"{pullback['pullback_atr']:.2f} ATR"
            )

            return

        # ====================================================
        # REVERSAL PROTECTION
        # ====================================================

        if not pullback[
            "reversal_ok"
        ]:

            print(
                f"{display_name} "
                f"BLOCKED - reversal "
                f"too large "
                f"{pullback['reversal_atr']:.2f} ATR"
            )

            return

        # ====================================================
        # WEAKENING
        # ====================================================

        if not pullback[
            "weakening"
        ]:

            print(
                f"{display_name} "
                f"PULLBACK WEAKENING - "
                f"WAITING..."
            )

            return

        # ====================================================
        # 5M TRIGGER
        # ====================================================

        if REQUIRE_5M_TRIGGER:

            if not trigger[
                "ok"
            ]:

                print(
                    f"{display_name} "
                    f"WAITING FOR 5M "
                    f"{direction} TRIGGER"
                )

                return

        # ====================================================
        # SCORE
        # ====================================================

        score, reasons = (
            calculate_score(
                higher,
                pullback,
                trigger,
                direction
            )
        )

        print(
            f"{display_name} "
            f"SCORE={score}/13 "
            f"{', '.join(reasons)}"
        )

        if score < MIN_SCORE:

            print(
                f"{display_name} "
                f"BLOCKED - score below "
                f"{MIN_SCORE}"
            )

            return

        # ====================================================
        # DUPLICATE PROTECTION
        # ====================================================

        signal_time = utc_iso(
            trigger["time"]
        )

        last_alert = (
            STATE[
                "alerts"
            ].get(
                display_name
            )
        )

        if (
            last_alert
            ==
            signal_time
        ):

            print(
                f"{display_name} "
                f"DUPLICATE BLOCKED"
            )

            return

        # ====================================================
        # MESSAGE
        # ====================================================

        message = create_signal_message(
            display_name,
            direction,
            current_price,
            score,
            spike,
            pullback,
            higher,
            trigger
        )

        # ====================================================
        # TELEGRAM
        # ====================================================

        sent = send_telegram(
            message
        )

        if sent:

            STATE[
                "alerts"
            ][
                display_name
            ] = signal_time

            STATE[
                "setups"
            ][
                display_name
            ] = {
                "direction":
                    direction,

                "spike_time":
                    utc_iso(
                        spike["time"]
                    ),

                "signal_time":
                    signal_time,

                "phase":
                    pullback["phase"],

                "score":
                    score,
            }

            save_state(
                STATE
            )

            print(
                f"{display_name} "
                f"{direction} "
                f"SIGNAL SENT"
            )

    except Exception as e:

        print(
            f"{display_name} ERROR: "
            f"{e}"
        )

        traceback.print_exc()


# ============================================================
# SCAN CYCLE
# ============================================================

def scan_cycle(
    symbols
):

    start = time.time()

    print("")
    print(
        "=========================================="
    )

    print(
        "STARTING SCAN "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    print(
        "=========================================="
    )

    # IMPORTANT:
    # Three-value unpacking.
    for (
        display_name,
        expected,
        direction
    ) in TARGETS:

        symbol = symbols.get(
            expected
        )

        if not symbol:

            print(
                f"{display_name} "
                f"SKIPPED - SYMBOL NOT FOUND"
            )

            continue

        scan_one(
            display_name,
            symbol,
            direction
        )

    elapsed = (
        time.time()
        -
        start
    )

    print(
        f"CYCLE FINISHED "
        f"in {elapsed:.1f} seconds"
    )

    return elapsed


# ============================================================
# STARTUP CHECKS
# ============================================================

def startup_checks():

    print("")
    print(
        "=========================================="
    )

    print(
        f"DERIV SYNTHETIC PULLBACK SCANNER "
        f"{VERSION}"
    )

    print(
        "CRASH = SELL ONLY | "
        "BOOM = BUY ONLY"
    )

    print(
        "NO AUTO TRADING"
    )

    print(
        "=========================================="
    )

    print(
        f"SCANNER VERSION: {VERSION}"
    )

    print(
        f"RUNNING FILE: "
        f"{os.path.abspath(__file__)}"
    )

    print(
        f"DERIV WS: "
        f"{DERIV_WS_URL}"
    )

    print(
        f"SCAN INTERVAL: "
        f"{SCAN_INTERVAL_SECONDS} seconds"
    )

    print(
        "STRATEGY: "
        "SPIKE -> PULLBACK -> "
        "WEAKENING -> 5M REVERSAL"
    )

    print(
        f"SPIKE MIN: "
        f"{SPIKE_MIN_ATR:.2f} ATR"
    )

    print(
        f"PULLBACK RANGE: "
        f"{PULLBACK_MIN_ATR:.2f} - "
        f"{PULLBACK_MAX_ATR:.2f} ATR"
    )

    print(
        f"MIN SCORE: "
        f"{MIN_SCORE}/13"
    )

    print(
        f"LOWER TF AGREEMENT: "
        f"{MIN_LOWER_TF_AGREEMENT}/3"
    )

    print(
        f"TELEGRAM TOKEN: "
        f"{'SET' if TELEGRAM_BOT_TOKEN else 'MISSING'}"
    )

    print(
        f"TELEGRAM CHAT ID: "
        f"{'SET' if TELEGRAM_CHAT_ID else 'MISSING'}"
    )

    print("")

    if (
        DERIV_WS_URL
        != PUBLIC_WS_URL
    ):

        print(
            "WARNING: DERIV_WS_URL "
            "is not the expected "
            "current public endpoint."
        )

    if not TELEGRAM_BOT_TOKEN:

        print(
            "WARNING: "
            "TELEGRAM_BOT_TOKEN "
            "is missing."
        )

    if not TELEGRAM_CHAT_ID:

        print(
            "WARNING: "
            "TELEGRAM_CHAT_ID "
            "is missing."
        )


# ============================================================
# MAIN
# ============================================================

def main():

    startup_checks()

    # ========================================================
    # TEST DERIV CONNECTION
    # ========================================================

    try:

        test_payload = {
            "active_symbols": "brief"
        }

        test_payload.pop(
            "subscribe",
            None
        )

        deriv_request(
            test_payload
        )

        print(
            "DERIV WEBSOCKET CONNECTED"
        )

    except Exception as e:

        print(
            "DERIV CONNECTION ERROR:"
        )

        print(e)

        traceback.print_exc()

        raise

    # ========================================================
    # DISCOVER SYMBOLS
    # ========================================================

    symbols = discover_symbols()

    print("")
    print(
        f"READY: "
        f"{len(symbols)} "
        f"symbol mappings found"
    )
    print("")

    # ========================================================
    # MAIN LOOP
    # ========================================================

    while True:

        cycle_start = time.time()

        try:

            scan_cycle(
                symbols
            )

        except Exception as e:

            print(
                "SCAN CYCLE ERROR:",
                e
            )

            traceback.print_exc()

        elapsed = (
            time.time()
            -
            cycle_start
        )

        sleep_time = max(
            5,
            SCAN_INTERVAL_SECONDS
            -
            elapsed
        )

        print(
            f"NEXT SCAN IN "
            f"{sleep_time:.0f} seconds"
        )

        time.sleep(
            sleep_time
        )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
