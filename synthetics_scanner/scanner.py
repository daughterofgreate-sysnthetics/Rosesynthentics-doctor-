# ============================================================
# DERIV SYNTHETIC PULLBACK SCANNER
# VERSION: 2026-09-BALANCED-PULLBACK-V4
#
# CRASH  -> SELL ONLY
# BOOM   -> BUY ONLY
#
# NO AUTO TRADING
# TELEGRAM ALERTS ONLY
# ============================================================

import os
import json
import time
import traceback
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import requests

try:
    import websocket
except ImportError:
    websocket = None


# ============================================================
# VERSION
# ============================================================

VERSION = "2026-09-BALANCED-PULLBACK-V4"

print("=" * 65)
print(f"DERIV SYNTHETIC PULLBACK SCANNER {VERSION}")
print("CRASH = SELL ONLY | BOOM = BUY ONLY")
print("NO AUTO TRADING")
print("=" * 65)


# ============================================================
# DERIV CONNECTION
# ============================================================

PUBLIC_WS_URL = (
    "wss://api.derivws.com/trading/v1/options/ws/public"
)

DERIV_WS_URL = os.getenv(
    "DERIV_WS_URL",
    PUBLIC_WS_URL
).strip()

# Protect against accidentally leaving old endpoint
if "ws.binaryws.com/websockets/v3" in DERIV_WS_URL:
    print(
        "OLD DERIV ENDPOINT DETECTED."
    )
    print(
        "USING CURRENT PUBLIC MARKET DATA ENDPOINT."
    )
    DERIV_WS_URL = PUBLIC_WS_URL


# ============================================================
# TELEGRAM
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    ""
).strip()

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    ""
).strip()


# ============================================================
# OPTIONAL GROQ
# ============================================================

GROQ_API_KEY = os.getenv(
    "GROQ_API_KEY",
    ""
).strip()

GROQ_MODEL = os.getenv(
    "GROQ_MODEL",
    "openai/gpt-oss-120b"
).strip()


# ============================================================
# SCAN INTERVAL
# ============================================================

SCAN_INTERVAL_SECONDS = int(
    os.getenv(
        "SCAN_INTERVAL_SECONDS",
        "60"
    )
)


# ============================================================
# STATE
# ============================================================

STATE_FILE = "state.json"


# ============================================================
# TARGETS
# ============================================================

TARGETS = [
    "CRASH 300",
    "CRASH 500",
    "CRASH 600",
    "CRASH 900",
    "CRASH 1000",

    "BOOM 300",
    "BOOM 500",
    "BOOM 600",
    "BOOM 900",
    "BOOM 1000",
]


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
# STRATEGY SETTINGS
# ============================================================

EMA_FAST = 20
EMA_SLOW = 50

RSI_LENGTH = 14
ATR_LENGTH = 14

# Spike
SPIKE_ATR_MULT = 1.10
SPIKE_LOOKBACK = 20
SPIKE_MAX_AGE = 8

# Pullback
PULLBACK_MIN_ATR = 0.15
PULLBACK_MAX_ATR = 1.80

# Reject enormous continuation candle
MAX_REVERSAL_ATR = 1.80

# Lower timeframe agreement
MIN_LOWER_TF_AGREEMENT = 2

# 5M entry candle
ENTRY_MIN_BODY_ATR = 0.08

# Prevent repeated alerts
DUPLICATE_PROTECTION = True


# ============================================================
# TIME
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


# ============================================================
# BASIC HELPERS
# ============================================================

def normalize_name(value):

    if value is None:
        return ""

    return (
        str(value)
        .upper()
        .replace(" ", "")
        .replace("_", "")
        .replace("-", "")
        .replace("/", "")
        .replace("INDEX", "")
    )


def safe_float(value):

    try:
        return float(value)
    except Exception:
        return np.nan


# ============================================================
# STATE MANAGEMENT
# ============================================================

def load_state():

    default = {
        "version": VERSION,
        "alerts": {},
        "setups": {}
    }

    try:

        if not os.path.exists(
            STATE_FILE
        ):
            return default

        with open(
            STATE_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            state = json.load(f)

        # Important:
        # Old scanner states should not interfere
        # with the new strategy.
        if state.get("version") != VERSION:

            print(
                "OLD STATE DETECTED - RESETTING"
            )

            return default

        state.setdefault(
            "alerts",
            {}
        )

        state.setdefault(
            "setups",
            {}
        )

        return state

    except Exception as e:

        print(
            "STATE LOAD ERROR:",
            e
        )

        return default


STATE = load_state()


def save_state():

    try:

        temp_file = STATE_FILE + ".tmp"

        with open(
            temp_file,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                STATE,
                f,
                indent=2
            )

        os.replace(
            temp_file,
            STATE_FILE
        )

    except Exception as e:

        print(
            "STATE SAVE ERROR:",
            e
        )


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):

    if not TELEGRAM_BOT_TOKEN:

        print(
            "TELEGRAM_BOT_TOKEN missing"
        )

        return False

    if not TELEGRAM_CHAT_ID:

        print(
            "TELEGRAM_CHAT_ID missing"
        )

        return False

    url = (
        "https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": True
    }

    try:

        response = requests.post(
            url,
            json=payload,
            timeout=20
        )

        if response.ok:

            return True

        print(
            "TELEGRAM ERROR:",
            response.status_code,
            response.text[:500]
        )

    except Exception as e:

        print(
            "TELEGRAM REQUEST ERROR:",
            e
        )

    return False


# ============================================================
# DERIV WEBSOCKET
# ============================================================

def ws_connect():

    if websocket is None:

        raise RuntimeError(
            "websocket-client is not installed"
        )

    print(
        "Connecting to Deriv..."
    )

    ws = websocket.create_connection(
        DERIV_WS_URL,
        timeout=25
    )

    print(
        "DERIV WEBSOCKET CONNECTED"
    )

    return ws


# ============================================================
# DERIV REQUEST
# ============================================================

def deriv_request(
    ws,
    payload,
    expected_type,
    timeout=30
):

    ws.send(
        json.dumps(payload)
    )

    deadline = (
        time.time() +
        timeout
    )

    while time.time() < deadline:

        remaining = max(
            1,
            int(
                deadline -
                time.time()
            )
        )

        ws.settimeout(
            remaining
        )

        raw = ws.recv()

        if not raw:
            continue

        try:

            data = json.loads(
                raw
            )

        except Exception:

            continue

        if "error" in data:

            raise RuntimeError(
                str(
                    data["error"]
                )
            )

        if data.get(
            "msg_type"
        ) == expected_type:

            return data

    raise TimeoutError(
        f"Timed out waiting for "
        f"{expected_type}"
    )


# ============================================================
# SYMBOL DISCOVERY
# ============================================================

def discover_symbols():

    ws = None

    try:

        ws = ws_connect()

        # IMPORTANT:
        # No subscribe parameter here.
        response = deriv_request(
            ws,
            {
                "active_symbols": "brief"
            },
            "active_symbols",
            timeout=30
        )

        items = response.get(
            "active_symbols",
            []
        )

        if not items:

            raise RuntimeError(
                "Deriv returned no active symbols"
            )

        discovered = {}

        for item in items:

            # Current API
            symbol = (
                item.get(
                    "underlying_symbol"
                )
                or
                # Legacy fallback
                item.get(
                    "symbol"
                )
            )

            display = (
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
                symbol
            )

            if not symbol:
                continue

            discovered[
                normalize_name(
                    symbol
                )
            ] = symbol

            discovered[
                normalize_name(
                    display
                )
            ] = symbol

        result = {}

        for target in TARGETS:

            target_normalized = (
                normalize_name(
                    target
                )
            )

            found = None

            # Exact match first
            for key, symbol in discovered.items():

                if key == target_normalized:

                    found = symbol
                    break

            # Partial match
            if found is None:

                for key, symbol in discovered.items():

                    if (
                        target_normalized in key
                        or
                        key in target_normalized
                    ):

                        found = symbol
                        break

            if found:

                result[target] = found

                print(
                    f"FOUND: "
                    f"{target} -> {found}"
                )

            else:

                print(
                    f"NOT FOUND: {target}"
                )

        if not result:

            raise RuntimeError(
                "None of the requested "
                "synthetic indices were found."
            )

        return result

    finally:

        if ws is not None:

            try:
                ws.close()
            except Exception:
                pass


# ============================================================
# CANDLE FETCH
# ============================================================

def fetch_candles(
    symbol,
    granularity,
    count=250
):

    ws = None

    try:

        ws = ws_connect()

        # ====================================================
        # IMPORTANT FIX
        #
        # DO NOT SEND:
        # "subscribe": 1
        # "subscribe": 0
        #
        # The current endpoint rejects that parameter.
        # ====================================================

        payload = {
            "ticks_history": symbol,
            "end": "latest",
            "style": "candles",
            "granularity": int(
                granularity
            ),
            "count": int(
                count
            )
        }

        response = deriv_request(
            ws,
            payload,
            "candles",
            timeout=30
        )

        candles = response.get(
            "candles",
            []
        )

        if not candles:

            raise RuntimeError(
                f"No candles returned "
                f"for {symbol}"
            )

        rows = []

        for candle in candles:

            epoch = candle.get(
                "epoch"
            )

            if epoch is None:
                continue

            rows.append({

                "time":
                    pd.to_datetime(
                        int(epoch),
                        unit="s",
                        utc=True
                    ),

                "open":
                    safe_float(
                        candle.get(
                            "open"
                        )
                    ),

                "high":
                    safe_float(
                        candle.get(
                            "high"
                        )
                    ),

                "low":
                    safe_float(
                        candle.get(
                            "low"
                        )
                    ),

                "close":
                    safe_float(
                        candle.get(
                            "close"
                        )
                    )
            })

        df = pd.DataFrame(
            rows
        )

        if df.empty:

            raise RuntimeError(
                f"Empty candle data "
                f"for {symbol}"
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
                "open",
                "high",
                "low",
                "close"
            ]
        )

        df = (
            df
            .sort_values(
                "time"
            )
            .drop_duplicates(
                "time"
            )
            .reset_index(
                drop=True
            )
        )

        # Remove incomplete/current candle.
        now = pd.Timestamp.now(
            tz="UTC"
        )

        candle_seconds = int(
            granularity
        )

        cutoff = now.floor(
            f"{candle_seconds}s"
        )

        df = df[
            df["time"] < cutoff
        ].copy()

        if len(df) < 60:

            raise RuntimeError(
                f"Not enough completed "
                f"candles for {symbol}: "
                f"{len(df)}"
            )

        return df.reset_index(
            drop=True
        )

    finally:

        if ws is not None:

            try:
                ws.close()
            except Exception:
                pass


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

def calculate_rsi(
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
        (
            100 /
            (1 + rs)
        )
    )

    return result.fillna(
        50
    )


# ============================================================
# ATR
# ============================================================

def calculate_atr(
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
# ADD INDICATORS
# ============================================================

def add_indicators(df):

    df = df.copy()

    df["ema20"] = ema(
        df["close"],
        EMA_FAST
    )

    df["ema50"] = ema(
        df["close"],
        EMA_SLOW
    )

    df["rsi"] = calculate_rsi(
        df["close"],
        RSI_LENGTH
    )

    df["atr"] = calculate_atr(
        df,
        ATR_LENGTH
    )

    df["body"] = (
        df["close"] -
        df["open"]
    )

    df["body_abs"] = (
        df["body"].abs()
    )

    df["range"] = (
        df["high"] -
        df["low"]
    )

    df["body_atr"] = (
        df["body_abs"] /
        df["atr"].replace(
            0,
            np.nan
        )
    )

    return df


# ============================================================
# BUILD 12H
# ============================================================

def build_12h(
    df_4h
):

    df = df_4h.copy()

    df = df.set_index(
        "time"
    )

    result = df.resample(
        "12h",
        origin="start_day"
    ).agg({

        "open": "first",

        "high": "max",

        "low": "min",

        "close": "last"

    })

    result = result.dropna()

    result = result.reset_index()

    return add_indicators(
        result
    )


# ============================================================
# TIMEFRAME DIRECTION
# ============================================================

def timeframe_direction(
    df
):

    if len(df) < 55:

        return 0

    last = df.iloc[-1]

    close = float(
        last["close"]
    )

    ema20 = float(
        last["ema20"]
    )

    ema50 = float(
        last["ema50"]
    )

    old_ema20 = float(
        df["ema20"].iloc[-4]
    )

    bullish_points = 0
    bearish_points = 0

    # Price vs EMA20
    if close > ema20:

        bullish_points += 1

    elif close < ema20:

        bearish_points += 1

    # EMA20 vs EMA50
    if ema20 > ema50:

        bullish_points += 1

    elif ema20 < ema50:

        bearish_points += 1

    # EMA slope
    if ema20 > old_ema20:

        bullish_points += 1

    elif ema20 < old_ema20:

        bearish_points += 1

    if bullish_points >= 2:

        return 1

    if bearish_points >= 2:

        return -1

    return 0


# ============================================================
# ALIGNMENT
# ============================================================

def get_alignment(
    df_12h,
    df_4h,
    df_1h,
    df_15m,
    df_5m,
    expected_direction
):

    d12 = timeframe_direction(
        df_12h
    )

    d4 = timeframe_direction(
        df_4h
    )

    d1 = timeframe_direction(
        df_1h
    )

    d15 = timeframe_direction(
        df_15m
    )

    d5 = timeframe_direction(
        df_5m
    )

    lower_agreement = sum(
        1
        for direction in [
            d4,
            d1,
            d15
        ]
        if direction ==
        expected_direction
    )

    return {

        "12H": d12,

        "4H": d4,

        "1H": d1,

        "15M": d15,

        "5M": d5,

        "h12_ok":
            d12 ==
            expected_direction,

        "lower_agreement":
            lower_agreement,

        "lower_ok":
            lower_agreement >=
            MIN_LOWER_TF_AGREEMENT,

        "five_ok":
            d5 ==
            expected_direction
    }


# ============================================================
# SPIKE DETECTION
# ============================================================

def detect_spike(
    df,
    expected_direction
):

    start = max(
        2,
        len(df) -
        SPIKE_LOOKBACK
    )

    candidates = []

    # Do not use the currently forming candle.
    end = len(df) - 1

    for i in range(
        start,
        end
    ):

        row = df.iloc[i]

        atr_value = float(
            row["atr"]
        )

        if (
            not np.isfinite(
                atr_value
            )
            or
            atr_value <= 0
        ):

            continue

        body = float(
            row["close"] -
            row["open"]
        )

        body_abs = abs(
            body
        )

        candle_range = float(
            row["high"] -
            row["low"]
        )

        if candle_range <= 0:
            continue

        body_atr = (
            body_abs /
            atr_value
        )

        range_atr = (
            candle_range /
            atr_value
        )

        # ====================================================
        # CRASH
        # ====================================================

        if expected_direction == -1:

            bearish = (
                body < 0
            )

            close_near_low = (
                row["close"] <=
                row["low"] +
                candle_range * 0.40
            )

            valid = (
                bearish
                and
                (
                    body_atr >=
                    SPIKE_ATR_MULT
                    or
                    (
                        range_atr >= 1.35
                        and
                        close_near_low
                    )
                )
            )

        # ====================================================
        # BOOM
        # ====================================================

        else:

            bullish = (
                body > 0
            )

            close_near_high = (
                row["close"] >=
                row["high"] -
                candle_range * 0.40
            )

            valid = (
                bullish
                and
                (
                    body_atr >=
                    SPIKE_ATR_MULT
                    or
                    (
                        range_atr >= 1.35
                        and
                        close_near_high
                    )
                )
            )

        if valid:

            candidates.append({

                "index": i,

                "time":
                    row["time"],

                "open":
                    float(
                        row["open"]
                    ),

                "high":
                    float(
                        row["high"]
                    ),

                "low":
                    float(
                        row["low"]
                    ),

                "close":
                    float(
                        row["close"]
                    ),

                "atr":
                    atr_value,

                "size_atr":
                    max(
                        body_atr,
                        range_atr
                    )
            })

    if not candidates:

        return None

    return candidates[-1]


# ============================================================
# PULLBACK ANALYSIS
# ============================================================

def analyze_pullback(
    df,
    spike,
    expected_direction
):

    if spike is None:

        return None

    spike_index = (
        spike["index"]
    )

    current_index = (
        len(df) - 1
    )

    age = (
        current_index -
        spike_index
    )

    if age <= 0:

        return {

            "phase":
                "SPIKE",

            "age":
                age,

            "pullback_atr":
                0.0,

            "reversal_atr":
                0.0
        }

    if age > SPIKE_MAX_AGE:

        return {

            "phase":
                "EXPIRED",

            "age":
                age,

            "pullback_atr":
                0.0,

            "reversal_atr":
                0.0
        }

    after = df.iloc[
        spike_index + 1:
    ]

    if after.empty:

        return None

    current = df.iloc[-1]

    atr_value = float(
        current["atr"]
    )

    if (
        not np.isfinite(
            atr_value
        )
        or
        atr_value <= 0
    ):

        return None

    # ========================================================
    # CRASH
    # ========================================================

    if expected_direction == -1:

        # Price spiked DOWN.
        # Pullback must travel UP.

        pullback_high = float(
            after["high"].max()
        )

        spike_low = float(
            spike["low"]
        )

        pullback_distance = (
            pullback_high -
            spike_low
        )

        pullback_atr = (
            pullback_distance /
            atr_value
        )

        current_close = float(
            current["close"]
        )

        reversal_distance = (
            pullback_high -
            current_close
        )

    # ========================================================
    # BOOM
    # ========================================================

    else:

        # Price spiked UP.
        # Pullback must travel DOWN.

        pullback_low = float(
            after["low"].min()
        )

        spike_high = float(
            spike["high"]
        )

        pullback_distance = (
            spike_high -
            pullback_low
        )

        pullback_atr = (
            pullback_distance /
            atr_value
        )

        current_close = float(
            current["close"]
        )

        reversal_distance = (
            current_close -
            pullback_low
        )

    # ========================================================
    # NOT ENOUGH PULLBACK
    # ========================================================

    if (
        pullback_atr <
        PULLBACK_MIN_ATR
    ):

        return {

            "phase":
                "WAITING_FOR_PULLBACK",

            "age":
                age,

            "pullback_atr":
                pullback_atr,

            "reversal_atr":
                0.0
        }

    # ========================================================
    # TOO DEEP
    # ========================================================

    if (
        pullback_atr >
        PULLBACK_MAX_ATR
    ):

        return {

            "phase":
                "PULLBACK_TOO_DEEP",

            "age":
                age,

            "pullback_atr":
                pullback_atr,

            "reversal_atr":
                0.0
        }

    reversal_atr = (
        abs(
            reversal_distance
        ) /
        atr_value
    )

    # ========================================================
    # TOO STRONG = PROBABLY CONTINUATION
    # ========================================================

    if (
        reversal_atr >
        MAX_REVERSAL_ATR
    ):

        return {

            "phase":
                "CONTINUATION_TOO_STRONG",

            "age":
                age,

            "pullback_atr":
                pullback_atr,

            "reversal_atr":
                reversal_atr
        }

    # ========================================================
    # VALID PULLBACK
    # ========================================================

    return {

        "phase":
            "PULLBACK",

        "age":
            age,

        "pullback_atr":
            pullback_atr,

        "reversal_atr":
            reversal_atr
    }


# ============================================================
# 5M ENTRY
# ============================================================

def five_minute_trigger(
    df,
    expected_direction
):

    if len(df) < 60:

        return (
            False,
            "not enough 5M candles"
        )

    last = df.iloc[-1]

    previous = df.iloc[-2]

    atr_value = float(
        last["atr"]
    )

    if (
        not np.isfinite(
            atr_value
        )
        or
        atr_value <= 0
    ):

        return (
            False,
            "invalid 5M ATR"
        )

    body = float(
        last["close"] -
        last["open"]
    )

    body_atr = (
        abs(body) /
        atr_value
    )

    # Reject huge continuation candle
    if (
        body_atr >
        MAX_REVERSAL_ATR
    ):

        return (
            False,
            "5M candle too large"
        )

    # ========================================================
    # CRASH SELL
    # ========================================================

    if expected_direction == -1:

        bearish = (
            last["close"] <
            last["open"]
        )

        below_ema = (
            last["close"] <
            last["ema20"]
        )

        rsi_ok = (
            float(
                last["rsi"]
            ) < 55
        )

        body_ok = (
            body_atr >=
            ENTRY_MIN_BODY_ATR
        )

        break_low = (
            last["close"] <
            previous["low"]
        )

        # Strong trigger
        if (
            bearish
            and
            below_ema
            and
            rsi_ok
            and
            body_ok
            and
            break_low
        ):

            return (
                True,
                "5M bearish reversal "
                "broke previous low"
            )

        # Softer trigger
        if (
            bearish
            and
            below_ema
            and
            rsi_ok
            and
            body_ok
        ):

            return (
                True,
                "5M controlled bearish "
                "reversal below EMA20"
            )

        return (
            False,
            "no bearish 5M trigger"
        )

    # ========================================================
    # BOOM BUY
    # ========================================================

    bullish = (
        last["close"] >
        last["open"]
    )

    above_ema = (
        last["close"] >
        last["ema20"]
    )

    rsi_ok = (
        float(
            last["rsi"]
        ) > 45
    )

    body_ok = (
        body_atr >=
        ENTRY_MIN_BODY_ATR
    )

    break_high = (
        last["close"] >
        previous["high"]
    )

    # Strong trigger
    if (
        bullish
        and
        above_ema
        and
        rsi_ok
        and
        body_ok
        and
        break_high
    ):

        return (
            True,
            "5M bullish reversal "
            "broke previous high"
        )

    # Softer trigger
    if (
        bullish
        and
        above_ema
        and
        rsi_ok
        and
        body_ok
    ):

        return (
            True,
            "5M controlled bullish "
            "reversal above EMA20"
        )

    return (
        False,
        "no bullish 5M trigger"
    )


# ============================================================
# SCORE
# ============================================================

def calculate_score(
    alignment,
    pullback,
    trigger
):

    score = 0

    # 12H
    if alignment["h12_ok"]:
        score += 3

    # 4H/1H/15M
    score += alignment[
        "lower_agreement"
    ]

    # 5M
    if alignment["five_ok"]:
        score += 2

    # Pullback
    if (
        pullback["phase"] ==
        "PULLBACK"
    ):
        score += 2

    # Good pullback zone
    if (
        0.15 <=
        pullback["pullback_atr"]
        <= 1.20
    ):
        score += 1

    # Entry trigger
    if trigger:
        score += 2

    return score


# ============================================================
# DIRECTION TEXT
# ============================================================

def direction_text(
    direction
):

    if direction == 1:
        return "BULLISH"

    if direction == -1:
        return "BEARISH"

    return "NEUTRAL"


# ============================================================
# SIGNAL MESSAGE
# ============================================================

def build_signal_message(
    target,
    symbol,
    expected_direction,
    score,
    price,
    spike,
    pullback,
    alignment,
    trigger_reason,
    rsi_5m
):

    if expected_direction == -1:

        emoji = "🔴"
        action = "SELL"

    else:

        emoji = "🟢"
        action = "BUY"

    return (
        f"{emoji} {target} — "
        f"{action} PULLBACK SIGNAL\n\n"

        f"Technical score: "
        f"{score}/13\n"

        f"Symbol: {symbol}\n"

        f"Price: {price:.5f}\n\n"

        f"SETUP:\n"

        f"Spike → Pullback → "
        f"Controlled Reversal\n\n"

        f"Spike age: "
        f"{pullback['age']} candles\n"

        f"Spike size: "
        f"{spike['size_atr']:.2f} ATR\n"

        f"Pullback: "
        f"{pullback['pullback_atr']:.2f} ATR\n"

        f"Reversal: "
        f"{pullback['reversal_atr']:.2f} ATR\n\n"

        f"MULTI-TIMEFRAME:\n"

        f"12H: "
        f"{direction_text(alignment['12H'])}\n"

        f"4H: "
        f"{direction_text(alignment['4H'])}\n"

        f"1H: "
        f"{direction_text(alignment['1H'])}\n"

        f"15M: "
        f"{direction_text(alignment['15M'])}\n"

        f"5M: "
        f"{direction_text(alignment['5M'])}\n"

        f"5M RSI: "
        f"{rsi_5m:.1f}\n\n"

        f"ENTRY:\n"
        f"{trigger_reason}\n\n"

        f"Scanner:\n"
        f"{VERSION}\n\n"

        f"Time:\n"
        f"{utc_now().strftime('%Y-%m-%d %H:%M UTC')}"
    )


# ============================================================
# DUPLICATE PROTECTION
# ============================================================

def alert_key(
    target,
    direction
):

    return (
        f"{target}|"
        f"{direction}"
    )


def already_alerted(
    target,
    direction,
    candle_time
):

    key = alert_key(
        target,
        direction
    )

    previous = STATE[
        "alerts"
    ].get(key)

    return (
        previous ==
        str(candle_time)
    )


def mark_alerted(
    target,
    direction,
    candle_time
):

    key = alert_key(
        target,
        direction
    )

    STATE[
        "alerts"
    ][key] = str(
        candle_time
    )

    save_state()


# ============================================================
# SETUP STATE
# ============================================================

def setup_key(
    target,
    direction
):

    return (
        f"{target}|"
        f"{direction}"
    )


def save_setup(
    target,
    direction,
    spike
):

    key = setup_key(
        target,
        direction
    )

    STATE[
        "setups"
    ][key] = {

        "spike_time":
            str(
                spike["time"]
            ),

        "spike_size_atr":
            float(
                spike["size_atr"]
            )
    }

    save_state()


def clear_setup(
    target,
    direction
):

    key = setup_key(
        target,
        direction
    )

    if key in STATE[
        "setups"
    ]:

        del STATE[
            "setups"
        ][key]

        save_state()


# ============================================================
# SCAN ONE
# ============================================================

def scan_one(
    target,
    symbol
):

    print()
    print("-" * 65)

    print(
        f"SCANNING {target} -> {symbol}"
    )

    try:

        # ====================================================
        # DATA
        # ====================================================

        df_4h = fetch_candles(
            symbol,
            TIMEFRAMES["4H"],
            220
        )

        df_1h = fetch_candles(
            symbol,
            TIMEFRAMES["1H"],
            220
        )

        df_15m = fetch_candles(
            symbol,
            TIMEFRAMES["15M"],
            250
        )

        df_5m = fetch_candles(
            symbol,
            TIMEFRAMES["5M"],
            250
        )

        # ====================================================
        # INDICATORS
        # ====================================================

        df_4h = add_indicators(
            df_4h
        )

        df_1h = add_indicators(
            df_1h
        )

        df_15m = add_indicators(
            df_15m
        )

        df_5m = add_indicators(
            df_5m
        )

        df_12h = build_12h(
            df_4h
        )

        if len(df_12h) < 55:

            print(
                f"{target}: "
                f"NOT ENOUGH 12H DATA"
            )

            return

        # ====================================================
        # DIRECTION
        # ====================================================

        if "CRASH" in target.upper():

            expected_direction = -1

        elif "BOOM" in target.upper():

            expected_direction = 1

        else:

            print(
                f"{target}: "
                f"UNKNOWN INDEX"
            )

            return

        # ====================================================
        # SPIKE
        # ====================================================

        spike = detect_spike(
            df_15m,
            expected_direction
        )

        if spike is None:

            print(
                f"{target}: NO RECENT SPIKE"
            )

            return

        # ====================================================
        # PULLBACK
        # ====================================================

        pullback = analyze_pullback(
            df_15m,
            spike,
            expected_direction
        )

        if pullback is None:

            print(
                f"{target}: "
                f"PULLBACK ANALYSIS FAILED"
            )

            return

        print(
            f"{target}: "
            f"PHASE={pullback['phase']} "
            f"AGE={pullback['age']} "
            f"SPIKE="
            f"{spike['size_atr']:.2f}ATR "
            f"PULLBACK="
            f"{pullback['pullback_atr']:.2f}ATR"
        )

        # ====================================================
        # SAVE VALID SETUP
        # ====================================================

        if pullback["phase"] in [
            "WAITING_FOR_PULLBACK",
            "PULLBACK"
        ]:

            save_setup(
                target,
                expected_direction,
                spike
            )

        # ====================================================
        # INVALID SETUP
        # ====================================================

        if pullback["phase"] in [
            "EXPIRED",
            "PULLBACK_TOO_DEEP",
            "CONTINUATION_TOO_STRONG"
        ]:

            clear_setup(
                target,
                expected_direction
            )

            print(
                f"{target}: "
                f"SETUP INVALID - "
                f"{pullback['phase']}"
            )

            return

        # ====================================================
        # STILL WAITING FOR PULLBACK
        # ====================================================

        if (
            pullback["phase"] ==
            "WAITING_FOR_PULLBACK"
        ):

            print(
                f"{target}: "
                f"WAITING FOR MORE PULLBACK"
            )

            return

        # ====================================================
        # ALIGNMENT
        # ====================================================

        alignment = get_alignment(
            df_12h,
            df_4h,
            df_1h,
            df_15m,
            df_5m,
            expected_direction
        )

        print(
            f"{target}: "
            f"12H={direction_text(alignment['12H'])} "
            f"4H={direction_text(alignment['4H'])} "
            f"1H={direction_text(alignment['1H'])} "
            f"15M={direction_text(alignment['15M'])} "
            f"5M={direction_text(alignment['5M'])} "
            f"LOWER="
            f"{alignment['lower_agreement']}/3"
        )

        # ====================================================
        # 12H MUST AGREE
        # ====================================================

        if not alignment["h12_ok"]:

            print(
                f"{target}: "
                f"BLOCKED - 12H "
                f"DOES NOT AGREE"
            )

            return

        # ====================================================
        # AT LEAST 2 LOWER TFs
        # ====================================================

        if not alignment["lower_ok"]:

            print(
                f"{target}: "
                f"BLOCKED - ONLY "
                f"{alignment['lower_agreement']}/3 "
                f"LOWER TIMEFRAMES AGREE"
            )

            return

        # ====================================================
        # 5M TRIGGER
        # ====================================================

        trigger, trigger_reason = (
            five_minute_trigger(
                df_5m,
                expected_direction
            )
        )

        if not trigger:

            print(
                f"{target}: "
                f"PULLBACK READY - "
                f"WAITING FOR 5M "
                f"TRIGGER"
            )

            print(
                f"Reason: "
                f"{trigger_reason}"
            )

            return

        # ====================================================
        # 5M DIRECTION
        # ====================================================

        if not alignment["five_ok"]:

            print(
                f"{target}: "
                f"5M TRIGGER REJECTED - "
                f"DIRECTION NOT ALIGNED"
            )

            return

        # ====================================================
        # SCORE
        # ====================================================

        score = calculate_score(
            alignment,
            pullback,
            trigger
        )

        # ====================================================
        # CURRENT 5M
        # ====================================================

        last_5m = df_5m.iloc[-1]

        price = float(
            last_5m["close"]
        )

        rsi_5m = float(
            last_5m["rsi"]
        )

        candle_time = (
            last_5m["time"]
        )

        # ====================================================
        # DUPLICATE CHECK
        # ====================================================

        if DUPLICATE_PROTECTION:

            if already_alerted(
                target,
                expected_direction,
                candle_time
            ):

                print(
                    f"{target}: "
                    f"DUPLICATE ALERT BLOCKED"
                )

                return

        # ====================================================
        # BUILD SIGNAL
        # ====================================================

        message = build_signal_message(
            target=
                target,

            symbol=
                symbol,

            expected_direction=
                expected_direction,

            score=
                score,

            price=
                price,

            spike=
                spike,

            pullback=
                pullback,

            alignment=
                alignment,

            trigger_reason=
                trigger_reason,

            rsi_5m=
                rsi_5m
        )

        print()
        print(
            f"🚨 {target} "
            f"SIGNAL READY"
        )

        print(
            message
        )

        # ====================================================
        # TELEGRAM
        # ====================================================

        sent = send_telegram(
            message
        )

        if sent:

            mark_alerted(
                target,
                expected_direction,
                candle_time
            )

            print(
                f"{target}: "
                f"TELEGRAM SENT"
            )

        else:

            print(
                f"{target}: "
                f"TELEGRAM FAILED"
            )

    except Exception as e:

        print()
        print(
            f"{target} ERROR: {e}"
        )

        traceback.print_exc()


# ============================================================
# FULL SCAN
# ============================================================

def run_scan(
    symbols
):

    start_time = time.time()

    print()
    print("=" * 65)

    print(
        "STARTING SCAN"
    )

    print(
        utc_now().strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    )

    print(
        f"VERSION: {VERSION}"
    )

    print("=" * 65)

    for target in TARGETS:

        symbol = symbols.get(
            target
        )

        if not symbol:

            print(
                f"{target}: "
                f"SKIPPED - NO SYMBOL"
            )

            continue

        scan_one(
            target,
            symbol
        )

    elapsed = (
        time.time() -
        start_time
    )

    print()
    print("=" * 65)

    print(
        f"CYCLE FINISHED "
        f"in {elapsed:.1f} seconds"
    )

    print("=" * 65)


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        f"Scan interval: "
        f"{SCAN_INTERVAL_SECONDS} seconds"
    )

    print(
        f"Deriv WebSocket: "
        f"{DERIV_WS_URL}"
    )

    # ========================================================
    # ENV CHECK
    # ========================================================

    if not TELEGRAM_BOT_TOKEN:

        print(
            "WARNING: "
            "TELEGRAM_BOT_TOKEN missing"
        )

    if not TELEGRAM_CHAT_ID:

        print(
            "WARNING: "
            "TELEGRAM_CHAT_ID missing"
        )

    # ========================================================
    # DISCOVER SYMBOLS
    # ========================================================

    while True:

        try:

            symbols = (
                discover_symbols()
            )

            break

        except Exception as e:

            print()
            print(
                "SYMBOL DISCOVERY ERROR:",
                e
            )

            print(
                "Retrying in 30 seconds..."
            )

            time.sleep(
                30
            )

    # ========================================================
    # SYMBOL MAP
    # ========================================================

    print()
    print("=" * 65)
    print("SYMBOL MAP")
    print("=" * 65)

    for target in TARGETS:

        print(
            f"{target}: "
            f"{symbols.get(target, 'NOT FOUND')}"
        )

    print("=" * 65)

    # ========================================================
    # CONTINUOUS SCAN
    # ========================================================

    while True:

        cycle_start = time.time()

        try:

            run_scan(
                symbols
            )

        except Exception as e:

            print(
                "SCAN CYCLE ERROR:",
                e
            )

            traceback.print_exc()

        elapsed = (
            time.time() -
            cycle_start
        )

        sleep_time = max(
            5,
            SCAN_INTERVAL_SECONDS -
            elapsed
        )

        print(
            f"Next scan in "
            f"{sleep_time:.0f} seconds..."
        )

        time.sleep(
            sleep_time
        )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()
