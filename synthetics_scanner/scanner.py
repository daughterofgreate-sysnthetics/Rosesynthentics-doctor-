# ============================================================
# DERIV CRASH / BOOM PULLBACK SCANNER
# VERSION: 2026-09-BALANCED-PULLBACK-V2
#
# IMPORTANT:
# - SCANNER ONLY
# - NO AUTOMATIC TRADING
# - Telegram alerts only
#
# CRASH  -> SELL setups only
# BOOM   -> BUY setups only
#
# Strategy:
#   Spike -> Pullback -> Weakening -> Controlled reversal
#
# Higher timeframe:
#   12H = broad regime filter
#   4H/1H/15M = 2-of-3 confirmation
#   5M = entry trigger
#
# ============================================================

import os
import json
import time
import math
import traceback
from datetime import datetime, timezone

import requests
import pandas as pd
import numpy as np

try:
    import websocket
except ImportError:
    websocket = None


# ============================================================
# VERSION
# ============================================================

VERSION = "2026-09-BALANCED-PULLBACK-V2"


# ============================================================
# ENVIRONMENT
# ============================================================

DERIV_WS_URL = os.getenv(
    "DERIV_WS_URL",
    "wss://ws.binaryws.com/websockets/v3"
)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv(
    "GROQ_MODEL",
    "openai/gpt-oss-120b"
)

# IMPORTANT:
# 60 seconds is recommended because the actual entry is on 5M.
SCAN_INTERVAL_SECONDS = int(
    os.getenv("SCAN_INTERVAL_SECONDS", "60")
)

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
# STRATEGY SETTINGS
# ============================================================

# Spike detection
SPIKE_MIN_ATR = 1.20
SPIKE_MAX_LOOKBACK_BARS = 3

# Spike must have meaningful directional movement
SPIKE_MIN_BODY_ATR = 0.55

# How long a spike remains usable
MAX_SPIKE_AGE_BARS = 8

# Pullback
PULLBACK_MIN_ATR = 0.15
PULLBACK_MAX_ATR = 1.80

# Pullback extreme should not become ancient
MAX_PULLBACK_AGE_BARS = 4

# Reversal candle protection
MAX_REVERSAL_ATR = 1.80

# Early entry
ENTRY_ON_WEAKENING = True

# Minimum score
SIGNAL_MIN = 8

# 5M trigger must agree with desired direction
REQUIRE_5M_TRIGGER = True

# HTF confirmation
LOWER_TF_REQUIRED = 2


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
# HTTP SESSION
# ============================================================

HTTP = requests.Session()

HTTP.headers.update({
    "User-Agent": "Deriv-Pullback-Scanner/2026"
})


# ============================================================
# STATE
# ============================================================

def default_state():
    return {
        "version": VERSION,
        "setups": {},
        "sent": {},
        "symbol_map": {},
    }


def load_state():
    try:
        if not os.path.exists(STATE_FILE):
            return default_state()

        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)

        if not isinstance(state, dict):
            return default_state()

        # Old scanner states are deliberately discarded.
        # This prevents old setup structures from interfering
        # with this new strategy.
        if state.get("version") != VERSION:
            print("STATE VERSION CHANGED -> RESETTING OLD SETUPS")
            return default_state()

        state.setdefault("setups", {})
        state.setdefault("sent", {})
        state.setdefault("symbol_map", {})

        return state

    except Exception as e:
        print("STATE LOAD ERROR:", e)
        return default_state()


def save_state(state):
    try:
        tmp = STATE_FILE + ".tmp"

        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                state,
                f,
                indent=2,
                default=str
            )

        os.replace(tmp, STATE_FILE)

    except Exception as e:
        print("STATE SAVE ERROR:", e)


STATE = load_state()


# ============================================================
# BASIC HELPERS
# ============================================================

def now_utc():
    return datetime.now(timezone.utc)


def iso_now():
    return now_utc().isoformat()


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def clamp(value, low, high):
    return max(low, min(high, value))


def direction_label(direction):
    if direction == "BUY":
        return "BULLISH"
    if direction == "SELL":
        return "BEARISH"
    return "NEUTRAL"


def opposite(direction):
    return "SELL" if direction == "BUY" else "BUY"


# ============================================================
# DERIV WEBSOCKET
# ============================================================

def ws_connect():
    if websocket is None:
        raise RuntimeError(
            "websocket-client is not installed"
        )

    ws = websocket.create_connection(
        DERIV_WS_URL,
        timeout=20
    )

    return ws


# ============================================================
# ACTIVE SYMBOL DISCOVERY
# ============================================================

def normalise_name(value):
    return (
        str(value)
        .upper()
        .replace("_", " ")
        .replace("-", " ")
        .replace("/", " ")
        .replace("INDEX", "")
    )


def target_matches(display_name, target):
    a = normalise_name(display_name)
    b = normalise_name(target)

    # Remove repeated whitespace
    a = " ".join(a.split())
    b = " ".join(b.split())

    return b in a or a in b


def discover_symbols():
    """
    Find the actual Deriv symbols corresponding to:
      CRASH 300
      CRASH 500
      ...
      BOOM 1000
    """

    try:
        ws = ws_connect()

        request = {
            "active_symbols": "brief",
            "product_type": "basic",
        }

        ws.send(json.dumps(request))

        response = json.loads(ws.recv())

        ws.close()

        if response.get("error"):
            raise RuntimeError(
                response["error"].get("message", "active_symbols error")
            )

        symbols = response.get("active_symbols", [])

        found = {}

        for item in symbols:
            symbol = item.get("symbol")
            display = (
                item.get("display_name")
                or item.get("name")
                or ""
            )

            if not symbol:
                continue

            for target in TARGETS:
                if target in found:
                    continue

                if target_matches(display, target):
                    found[target] = symbol

        # Sometimes display_name is not exactly what we expect.
        # Try a second flexible matching pass.
        for target in TARGETS:
            if target in found:
                continue

            target_norm = normalise_name(target)

            for item in symbols:
                symbol = item.get("symbol")
                display = normalise_name(
                    item.get("display_name")
                    or item.get("name")
                    or ""
                )

                digits = "".join(
                    c for c in target_norm
                    if c.isdigit()
                )

                if (
                    digits
                    and digits in display
                    and (
                        "CRASH" in target_norm
                        and "CRASH" in display
                        or
                        "BOOM" in target_norm
                        and "BOOM" in display
                    )
                ):
                    found[target] = symbol
                    break

        if found:
            print("SYMBOL MAP:")
            for k, v in found.items():
                print(f"  {k} -> {v}")

        else:
            print("WARNING: NO TARGET SYMBOLS FOUND")

        return found

    except Exception as e:
        print("SYMBOL DISCOVERY ERROR:", e)
        return {}


def get_symbol_map():
    existing = STATE.get("symbol_map", {})

    if len(existing) >= len(TARGETS):
        return existing

    fresh = discover_symbols()

    if fresh:
        STATE["symbol_map"] = fresh
        save_state(STATE)

    return fresh


# ============================================================
# CANDLE DATA
# ============================================================

def get_candles(symbol, granularity, count=200):
    """
    Fetch completed candles from Deriv.

    Uses one websocket connection for one request.
    """

    ws = None

    try:
        ws = ws_connect()

        request = {
            "ticks_history": symbol,
            "adjust_start_time": 1,
            "count": count,
            "end": "latest",
            "style": "candles",
            "granularity": granularity,
            "subscribe": 0,
        }

        ws.send(json.dumps(request))

        while True:
            raw = ws.recv()

            if not raw:
                continue

            response = json.loads(raw)

            if response.get("error"):
                raise RuntimeError(
                    response["error"].get(
                        "message",
                        "Deriv candle error"
                    )
                )

            if response.get("msg_type") == "candles":
                candles = response.get("candles", [])
                break

        if not candles:
            raise RuntimeError(
                f"No candles returned for {symbol} {granularity}"
            )

        rows = []

        for c in candles:
            try:
                epoch = int(c["epoch"])

                rows.append({
                    "time": pd.to_datetime(
                        epoch,
                        unit="s",
                        utc=True
                    ),
                    "open": float(c["open"]),
                    "high": float(c["high"]),
                    "low": float(c["low"]),
                    "close": float(c["close"]),
                })

            except Exception:
                continue

        if not rows:
            raise RuntimeError(
                f"No valid candles for {symbol}"
            )

        df = pd.DataFrame(rows)

        df = df.dropna()

        df = df.sort_values("time")

        df = df.drop_duplicates(
            subset=["time"]
        )

        # Remove currently forming candle.
        current_epoch = int(
            now_utc().timestamp()
        )

        if len(df) > 0:
            last_epoch = int(
                df.iloc[-1]["time"].timestamp()
            )

            if last_epoch + granularity > current_epoch:
                df = df.iloc[:-1].copy()

        if len(df) < 30:
            raise RuntimeError(
                f"Too few completed candles: "
                f"{symbol} {granularity} "
                f"{len(df)}"
            )

        return df.reset_index(drop=True)

    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass


# ============================================================
# INDICATORS
# ============================================================

def ema(series, length):
    return series.ewm(
        span=length,
        adjust=False
    ).mean()


def atr(df, length=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]

    prev_close = close.shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1
    ).max(axis=1)

    return tr.ewm(
        alpha=1 / length,
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

    rs = avg_gain / avg_loss.replace(
        0,
        np.nan
    )

    result = 100 - (
        100 / (1 + rs)
    )

    return result.fillna(50)


def supertrend_direction(df, atr_length=10, factor=3.0):
    """
    Returns:
        1  bullish
       -1  bearish
        0  unavailable
    """

    if len(df) < atr_length + 5:
        return 0

    h = df["high"].values
    l = df["low"].values
    c = df["close"].values

    atr_values = atr(
        df,
        atr_length
    ).values

    upper = (
        (h + l) / 2
        + factor * atr_values
    )

    lower = (
        (h + l) / 2
        - factor * atr_values
    )

    final_upper = upper.copy()
    final_lower = lower.copy()

    trend = np.ones(len(df))

    for i in range(1, len(df)):

        if (
            upper[i] < final_upper[i - 1]
            or c[i - 1] > final_upper[i - 1]
        ):
            final_upper[i] = upper[i]
        else:
            final_upper[i] = final_upper[i - 1]

        if (
            lower[i] > final_lower[i - 1]
            or c[i - 1] < final_lower[i - 1]
        ):
            final_lower[i] = lower[i]
        else:
            final_lower[i] = final_lower[i - 1]

        if trend[i - 1] == -1:
            if c[i] > final_upper[i]:
                trend[i] = 1
            else:
                trend[i] = -1
        else:
            if c[i] < final_lower[i]:
                trend[i] = -1
            else:
                trend[i] = 1

    return int(trend[-1])


# ============================================================
# SNAPSHOT
# ============================================================

def indicator_snapshot(df):
    if len(df) < 55:
        return None

    close = df["close"]

    ema20 = ema(close, 20)
    ema50 = ema(close, 50)

    atr14 = atr(df, 14)

    st = supertrend_direction(
        df,
        10,
        3.0
    )

    current = float(close.iloc[-1])

    ema20_now = float(ema20.iloc[-1])
    ema50_now = float(ema50.iloc[-1])

    # Three broad trend components
    price_above_ema20 = current > ema20_now
    ema20_above_ema50 = ema20_now > ema50_now

    if len(ema20) >= 4:
        ema20_slope_up = (
            ema20.iloc[-1] > ema20.iloc[-4]
        )
    else:
        ema20_slope_up = False

    bullish_score = sum([
        price_above_ema20,
        ema20_above_ema50,
        ema20_slope_up,
    ])

    bearish_score = sum([
        not price_above_ema20,
        not ema20_above_ema50,
        not ema20_slope_up,
    ])

    if bullish_score >= 2:
        broad_direction = "BULLISH"

    elif bearish_score >= 2:
        broad_direction = "BEARISH"

    else:
        broad_direction = "NEUTRAL"

    return {
        "close": current,
        "ema20": ema20_now,
        "ema50": ema50_now,
        "atr": float(atr14.iloc[-1]),
        "supertrend": st,

        "price_above_ema20": bool(
            price_above_ema20
        ),

        "ema20_above_ema50": bool(
            ema20_above_ema50
        ),

        "ema20_slope_up": bool(
            ema20_slope_up
        ),

        "bullish_score": int(
            bullish_score
        ),

        "bearish_score": int(
            bearish_score
        ),

        "direction": broad_direction,
    }


# ============================================================
# 12H DATA
# ============================================================

def build_12h_from_4h(df4h):
    """
    Build completed 12H candles from completed 4H candles.
    """

    if len(df4h) < 20:
        return pd.DataFrame()

    x = df4h.copy()

    x = x.set_index("time")

    result = x.resample(
        "12h",
        label="left",
        closed="left"
    ).agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
    })

    result = result.dropna()

    result = result.reset_index()

    # Only accept a fully completed 12H block.
    current_time = now_utc()

    result = result[
        result["time"] + pd.Timedelta(
            hours=12
        ) <= current_time
    ]

    return result.reset_index(drop=True)


# ============================================================
# TIMEFRAME DIRECTION
# ============================================================

def tf_direction(snapshot):
    """
    Each timeframe gets a balanced 2-of-3 direction:

    1. price vs EMA20
    2. EMA20 vs EMA50
    3. Supertrend

    This avoids requiring every indicator to agree.
    """

    if not snapshot:
        return "NEUTRAL"

    bullish = sum([
        snapshot["price_above_ema20"],
        snapshot["ema20_above_ema50"],
        snapshot["supertrend"] == 1,
    ])

    bearish = sum([
        not snapshot["price_above_ema20"],
        not snapshot["ema20_above_ema50"],
        snapshot["supertrend"] == -1,
    ])

    if bullish >= 2:
        return "BULLISH"

    if bearish >= 2:
        return "BEARISH"

    return "NEUTRAL"


# ============================================================
# SPIKE DETECTION
# ============================================================

def candle_stats(row):
    candle_range = max(
        safe_float(row["high"]) -
        safe_float(row["low"]),
        0.0
    )

    body = abs(
        safe_float(row["close"]) -
        safe_float(row["open"])
    )

    return candle_range, body


def detect_recent_spike(df5m, desired_direction):
    """
    Detect a spike over 1-3 candles.

    CRASH:
        downward impulse

    BOOM:
        upward impulse
    """

    if len(df5m) < 30:
        return None

    atr_series = atr(
        df5m,
        14
    )

    latest = len(df5m) - 1

    # Search recent candles, but don't search indefinitely.
    start = max(
        15,
        latest - 6
    )

    candidates = []

    for end_idx in range(
        start,
        latest + 1
    ):

        for bars in range(
            1,
            SPIKE_MAX_LOOKBACK_BARS + 1
        ):

            begin_idx = end_idx - bars + 1

            if begin_idx < 15:
                continue

            section = df5m.iloc[
                begin_idx:end_idx + 1
            ]

            start_price = float(
                section["open"].iloc[0]
            )

            end_price = float(
                section["close"].iloc[-1]
            )

            move = end_price - start_price

            atr_ref = float(
                atr_series.iloc[end_idx]
            )

            if atr_ref <= 0:
                continue

            total_body = (
                section["close"]
                - section["open"]
            ).abs().sum()

            move_atr = abs(move) / atr_ref
            body_atr = (
                float(total_body)
                / atr_ref
            )

            if desired_direction == "SELL":
                correct_move = move < 0

            else:
                correct_move = move > 0

            if not correct_move:
                continue

            if move_atr < SPIKE_MIN_ATR:
                continue

            if body_atr < SPIKE_MIN_BODY_ATR:
                continue

            # Calculate strongest candle in this impulse.
            max_single_atr = 0.0

            for j in range(
                begin_idx,
                end_idx + 1
            ):
                r, b = candle_stats(
                    df5m.iloc[j]
                )

                a = float(
                    atr_series.iloc[j]
                )

                if a > 0:
                    max_single_atr = max(
                        max_single_atr,
                        r / a
                    )

            candidates.append({
                "start_idx": begin_idx,
                "end_idx": end_idx,
                "bars": bars,
                "move_atr": move_atr,
                "body_atr": body_atr,
                "max_single_atr": max_single_atr,
                "time": df5m.iloc[
                    end_idx
                ]["time"].isoformat(),
            })

    if not candidates:
        return None

    # Strongest / most recent useful candidate
    candidates.sort(
        key=lambda x: (
            x["move_atr"],
            x["end_idx"]
        ),
        reverse=True
    )

    return candidates[0]


# ============================================================
# STATEFUL SETUP DETECTION
# ============================================================

def setup_key(target):
    return target


def get_setup(target):
    return STATE["setups"].get(
        setup_key(target)
    )


def clear_setup(target):
    STATE["setups"].pop(
        setup_key(target),
        None
    )


def store_setup(target, setup):
    STATE["setups"][
        setup_key(target)
    ] = setup


def setup_age(df5m, setup):
    spike_time = setup.get("spike_time")

    if not spike_time:
        return 999

    matches = np.where(
        df5m["time"].astype(str).values
        == spike_time
    )[0]

    if len(matches) == 0:
        return 999

    idx = int(matches[-1])

    return (
        len(df5m) - 1 - idx
    )


def locate_spike_index(df5m, setup):
    spike_time = setup.get(
        "spike_time"
    )

    if not spike_time:
        return None

    times = df5m["time"].astype(str)

    matches = np.where(
        times.values == spike_time
    )[0]

    if len(matches) == 0:
        return None

    return int(matches[-1])


def create_spike_state(
    df5m,
    candidate,
    direction
):
    idx = candidate["end_idx"]

    row = df5m.iloc[idx]

    return {
        "direction": direction,
        "spike_time": row["time"].isoformat(),
        "spike_close": float(row["close"]),
        "spike_high": float(row["high"]),
        "spike_low": float(row["low"]),
        "spike_size_atr": float(
            candidate["move_atr"]
        ),
        "spike_body_atr": float(
            candidate["body_atr"]
        ),
        "spike_bars": int(
            candidate["bars"]
        ),
        "created_at": iso_now(),
        "phase": "SPIKE_DETECTED",
    }


def analyse_active_setup(
    df5m,
    setup,
    direction
):
    """
    Turn an active spike into:

      WAITING_FOR_PULLBACK
      PULLBACK_WEAKENING
      CONTROLLED_REVERSAL
      READY
    """

    spike_idx = locate_spike_index(
        df5m,
        setup
    )

    if spike_idx is None:
        return {
            "valid": False,
            "phase": "SPIKE_NOT_FOUND"
        }

    age = (
        len(df5m) - 1 - spike_idx
    )

    if age > MAX_SPIKE_AGE_BARS:
        return {
            "valid": False,
            "phase": "SPIKE_TOO_OLD",
            "age": age,
        }

    if age < 1:
        return {
            "valid": True,
            "phase": "SPIKE_DETECTED",
            "age": age,
        }

    after = df5m.iloc[
        spike_idx + 1:
    ].copy()

    if len(after) < 1:
        return {
            "valid": True,
            "phase": "WAITING_FOR_PULLBACK",
            "age": age,
        }

    spike_close = float(
        setup["spike_close"]
    )

    # --------------------------------------------------------
    # ATR reference
    # --------------------------------------------------------

    atr_series = atr(
        df5m,
        14
    )

    current_atr = float(
        atr_series.iloc[-1]
    )

    if current_atr <= 0:
        return {
            "valid": True,
            "phase": "WAITING_FOR_PULLBACK",
            "age": age,
        }

    # --------------------------------------------------------
    # PULLBACK
    # --------------------------------------------------------

    if direction == "SELL":

        pullback_extreme = float(
            after["high"].max()
        )

        pullback_distance = (
            pullback_extreme -
            spike_close
        )

        pullback_atr = (
            pullback_distance /
            current_atr
        )

        extreme_positions = np.where(
            after["high"].values
            == pullback_extreme
        )[0]

    else:

        pullback_extreme = float(
            after["low"].min()
        )

        pullback_distance = (
            spike_close -
            pullback_extreme
        )

        pullback_atr = (
            pullback_distance /
            current_atr
        )

        extreme_positions = np.where(
            after["low"].values
            == pullback_extreme
        )[0]

    if len(extreme_positions) == 0:
        return {
            "valid": True,
            "phase": "WAITING_FOR_PULLBACK",
            "age": age,
        }

    pullback_relative_idx = int(
        extreme_positions[-1]
    )

    pullback_age = (
        len(after) - 1
        - pullback_relative_idx
    )

    pullback_exists = (
        pullback_atr >= PULLBACK_MIN_ATR
        and
        pullback_atr <= PULLBACK_MAX_ATR
    )

    if not pullback_exists:

        if (
            pullback_atr
            > PULLBACK_MAX_ATR
        ):
            return {
                "valid": False,
                "phase": "PULLBACK_TOO_DEEP",
                "age": age,
                "pullback_atr": pullback_atr,
            }

        return {
            "valid": True,
            "phase": "WAITING_FOR_PULLBACK",
            "age": age,
            "pullback_atr": pullback_atr,
        }

    # --------------------------------------------------------
    # PULLBACK TOO OLD
    # --------------------------------------------------------

    if pullback_age > MAX_PULLBACK_AGE_BARS:
        return {
            "valid": True,
            "phase": "PULLBACK_STALE",
            "age": age,
            "pullback_atr": pullback_atr,
        }

    # --------------------------------------------------------
    # CURRENT CANDLE
    # --------------------------------------------------------

    current = df5m.iloc[-1]
    previous = df5m.iloc[-2]

    current_range, current_body = candle_stats(
        current
    )

    previous_range, previous_body = candle_stats(
        previous
    )

    current_atr = max(
        current_atr,
        1e-9
    )

    current_size_atr = (
        current_range /
        current_atr
    )

    current_body_atr = (
        current_body /
        current_atr
    )

    # Huge candle = not a controlled entry.
    if current_size_atr > MAX_REVERSAL_ATR:
        return {
            "valid": True,
            "phase": "PULLBACK_BUT_CANDLE_TOO_LARGE",
            "age": age,
            "pullback_atr": pullback_atr,
            "trigger_size_atr": current_size_atr,
        }

    # --------------------------------------------------------
    # REVERSAL DIRECTION
    # --------------------------------------------------------

    if direction == "SELL":

        reversal_direction = (
            float(current["close"])
            < float(current["open"])
        )

        close_moving_direction = (
            float(current["close"])
            < float(previous["close"])
        )

    else:

        reversal_direction = (
            float(current["close"])
            > float(current["open"])
        )

        close_moving_direction = (
            float(current["close"])
            > float(previous["close"])
        )

    # --------------------------------------------------------
    # PULLBACK WEAKENING
    # --------------------------------------------------------

    recent_start = max(
        spike_idx + 1,
        len(df5m) - 4
    )

    recent = df5m.iloc[
        recent_start:
    ].copy()

    if len(recent) >= 2:

        bodies = []

        for _, row in recent.iterrows():
            _, b = candle_stats(row)
            bodies.append(b)

        # Countertrend body weakening.
        weakening = (
            len(bodies) >= 2
            and
            bodies[-1] <=
            bodies[-2]
        )

    else:
        weakening = False

    # Another useful condition:
    # the pullback extreme happened recently and price
    # has now started moving back in the spike direction.
    early_turn = (
        pullback_age <= 2
        and
        reversal_direction
        and
        close_moving_direction
    )

    controlled_reversal = (
        reversal_direction
        and
        close_moving_direction
        and
        current_size_atr
        <= MAX_REVERSAL_ATR
        and
        current_body_atr >= 0.08
    )

    if (
        ENTRY_ON_WEAKENING
        and
        (weakening or early_turn)
        and
        controlled_reversal
    ):
        return {
            "valid": True,
            "phase": "PULLBACK_WEAKENING",
            "age": age,
            "pullback_atr": pullback_atr,
            "pullback_age": pullback_age,
            "trigger_size_atr": current_size_atr,
            "trigger_body_atr": current_body_atr,
        }

    if controlled_reversal:
        return {
            "valid": True,
            "phase": "CONTROLLED_REVERSAL",
            "age": age,
            "pullback_atr": pullback_atr,
            "pullback_age": pullback_age,
            "trigger_size_atr": current_size_atr,
            "trigger_body_atr": current_body_atr,
        }

    return {
        "valid": True,
        "phase": "WAITING_FOR_CONTROLLED_REVERSAL",
        "age": age,
        "pullback_atr": pullback_atr,
        "pullback_age": pullback_age,
        "trigger_size_atr": current_size_atr,
    }


# ============================================================
# 5M ENTRY TRIGGER
# ============================================================

def five_minute_trigger(df5m, direction):
    if len(df5m) < 25:
        return False, 50.0

    close = df5m["close"]

    ema20 = ema(
        close,
        20
    )

    r = rsi(
        close,
        14
    )

    current = df5m.iloc[-1]
    previous = df5m.iloc[-2]

    current_close = float(
        current["close"]
    )

    previous_close = float(
        previous["close"]
    )

    current_high = float(
        current["high"]
    )

    current_low = float(
        current["low"]
    )

    previous_high = float(
        previous["high"]
    )

    previous_low = float(
        previous["low"]
    )

    current_ema = float(
        ema20.iloc[-1]
    )

    current_rsi = float(
        r.iloc[-1]
    )

    if direction == "SELL":

        # For Crash we want the pullback to start turning down.
        conditions = [
            current_close < current_ema,
            current_close < previous_close,
            current_close < previous_high,
            current_low <= previous_low,
        ]

        score = sum(conditions)

        # Don't demand all 4.
        trigger = score >= 2

    else:

        conditions = [
            current_close > current_ema,
            current_close > previous_close,
            current_close > previous_low,
            current_high >= previous_high,
        ]

        score = sum(conditions)

        trigger = score >= 2

    return trigger, current_rsi


# ============================================================
# HIGHER TIMEFRAME ALIGNMENT
# ============================================================

def higher_timeframe_alignment(
    snapshots,
    direction
):
    wanted = direction_label(
        direction
    )

    h12 = snapshots.get("12H")

    if not h12:
        return {
            "h12_ok": False,
            "h12_direction": "NEUTRAL",
            "lower_agreement": 0,
            "lower_total": 3,
            "all_higher": False,
            "directions": {},
        }

    h12_direction = h12["direction"]

    h12_ok = (
        h12_direction == wanted
    )

    lower_names = [
        "4H",
        "1H",
        "15M",
    ]

    directions = {}

    agreement = 0

    for name in lower_names:

        snap = snapshots.get(name)

        if not snap:
            directions[name] = "NEUTRAL"
            continue

        d = tf_direction(snap)

        directions[name] = d

        if d == wanted:
            agreement += 1

    lower_ok = (
        agreement >= LOWER_TF_REQUIRED
    )

    return {
        "h12_ok": h12_ok,
        "h12_direction": h12_direction,
        "h12_bull_score": h12.get(
            "bullish_score",
            0
        ),
        "h12_bear_score": h12.get(
            "bearish_score",
            0
        ),
        "lower_agreement": agreement,
        "lower_total": len(lower_names),
        "lower_ok": lower_ok,
        "all_higher": (
            h12_ok and lower_ok
        ),
        "directions": directions,
    }


# ============================================================
# TECHNICAL SCORE
# ============================================================

def calculate_score(
    structure,
    alignment,
    five_trigger,
):
    score = 0

    # Spike
    if structure.get(
        "spike_size_atr",
        0
    ) >= SPIKE_MIN_ATR:
        score += 2

    # Pullback
    pullback = structure.get(
        "pullback_atr",
        0
    )

    if (
        PULLBACK_MIN_ATR
        <= pullback
        <= PULLBACK_MAX_ATR
    ):
        score += 2

    # Reversal
    phase = structure.get(
        "phase",
        ""
    )

    if phase == "PULLBACK_WEAKENING":
        score += 2

    elif phase == "CONTROLLED_REVERSAL":
        score += 2

    # H12
    if alignment.get(
        "h12_ok",
        False
    ):
        score += 2

    # Lower timeframe
    if alignment.get(
        "lower_ok",
        False
    ):
        score += 1

    # 5M
    if five_trigger:
        score += 1

    return min(
        score,
        10
    )


# ============================================================
# GROQ ADVISORY
# ============================================================

def groq_review(
    target,
    direction,
    score,
    structure,
    alignment,
    five_trigger,
):
    """
    Groq is ADVISORY ONLY.

    It cannot reverse:
      CRASH SELL
      BOOM BUY

    It can provide an opinion/note.
    """

    if not GROQ_API_KEY:
        return {
            "decision": "NO_AI",
            "confidence": 0,
            "note": "Groq not configured",
        }

    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are reviewing a Deriv Crash/Boom "
                    "pullback scanner. You are advisory only. "
                    "Do not invent market data. "
                    "CRASH setups can only be SELL. "
                    "BOOM setups can only be BUY. "
                    "Evaluate whether the technical setup is "
                    "coherent. Return JSON only with keys: "
                    "decision, confidence, note. "
                    "decision must be BUY, SELL, WATCH, or PASS."
                ),
            },
            {
                "role": "user",
                "content": json.dumps({
                    "symbol": target,
                    "technical_direction": direction,
                    "technical_score": score,
                    "phase": structure.get(
                        "phase"
                    ),
                    "spike_size_atr": structure.get(
                        "spike_size_atr"
                    ),
                    "pullback_atr": structure.get(
                        "pullback_atr"
                    ),
                    "trigger_size_atr": structure.get(
                        "trigger_size_atr"
                    ),
                    "h12": alignment.get(
                        "h12_direction"
                    ),
                    "lower_timeframes": alignment.get(
                        "directions"
                    ),
                    "lower_agreement": alignment.get(
                        "lower_agreement"
                    ),
                    "five_minute_trigger": five_trigger,
                }),
            },
        ],
        "temperature": 0.1,
        "max_completion_tokens": 300,
    }

    try:
        response = HTTP.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization":
                    f"Bearer {GROQ_API_KEY}",
                "Content-Type":
                    "application/json",
            },
            json=payload,
            timeout=15,
        )

        response.raise_for_status()

        data = response.json()

        content = (
            data["choices"][0]
            ["message"]["content"]
            .strip()
        )

        # Strip markdown code fences if model uses them.
        content = content.replace(
            "```json",
            ""
        ).replace(
            "```",
            ""
        ).strip()

        result = json.loads(content)

        return {
            "decision": str(
                result.get(
                    "decision",
                    "WATCH"
                )
            ).upper(),

            "confidence": int(
                safe_float(
                    result.get(
                        "confidence",
                        0
                    )
                )
            ),

            "note": str(
                result.get(
                    "note",
                    ""
                )
            )[:500],
        }

    except Exception as e:
        print(
            f"{target} GROQ ERROR:",
            e
        )

        return {
            "decision": "AI_ERROR",
            "confidence": 0,
            "note": "AI review unavailable",
        }


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN:
        print("TELEGRAM ERROR: missing TELEGRAM_BOT_TOKEN")
        return False

    if not TELEGRAM_CHAT_ID:
        print("TELEGRAM ERROR: missing TELEGRAM_CHAT_ID")
        return False

    url = (
        "https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    try:
        response = HTTP.post(
            url,
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
            },
            timeout=15,
        )

        response.raise_for_status()

        return True

    except Exception as e:
        print(
            "TELEGRAM SEND ERROR:",
            e
        )

        return False


# ============================================================
# FORMAT MESSAGE
# ============================================================

def build_signal_message(
    target,
    direction,
    score,
    structure,
    alignment,
    five_rsi,
    groq,
):
    if target.startswith("CRASH"):
        emoji = "🔴"
        action = "SELL"
    else:
        emoji = "🟢"
        action = "BUY"

    phase = structure.get(
        "phase",
        "UNKNOWN"
    )

    spike_age = structure.get(
        "age",
        0
    )

    spike_size = structure.get(
        "spike_size_atr",
        0
    )

    pullback = structure.get(
        "pullback_atr",
        0
    )

    trigger_size = structure.get(
        "trigger_size_atr",
        0
    )

    lower = alignment.get(
        "directions",
        {}
    )

    h12 = alignment.get(
        "h12_direction",
        "NEUTRAL"
    )

    lower_agreement = alignment.get(
        "lower_agreement",
        0
    )

    lower_total = alignment.get(
        "lower_total",
        3
    )

    ai_decision = groq.get(
        "decision",
        "NO_AI"
    )

    ai_confidence = groq.get(
        "confidence",
        0
    )

    ai_note = groq.get(
        "note",
        ""
    )

    lines = [
        f"{emoji} {target} — {action} PULLBACK SIGNAL",
        "",
        f"Technical score: {score}/10",
        f"Symbol: {target}",
        "",
        "SETUP:",
        "Spike → Pullback → Weakening/Reversal",
        "",
        f"Phase: {phase}",
        f"Spike age: {spike_age} candles",
        f"Spike size: {spike_size:.2f} ATR",
        f"Pullback: {pullback:.2f} ATR",
        f"Trigger candle: {trigger_size:.2f} ATR",
        "",
        "MULTI-TIMEFRAME:",
        f"12H: {h12}",
        f"4H: {lower.get('4H', 'NEUTRAL')}",
        f"1H: {lower.get('1H', 'NEUTRAL')}",
        f"15M: {lower.get('15M', 'NEUTRAL')}",
        f"5M: {direction_label(direction)}",
        f"5M RSI: {five_rsi:.1f}",
        "",
        f"HTF agreement: "
        f"{lower_agreement}/{lower_total}",
        "",
        f"Groq advisory: "
        f"{ai_decision} "
        f"({ai_confidence}%)",
    ]

    if ai_note:
        lines.extend([
            f"AI note: {ai_note}",
        ])

    lines.extend([
        "",
        "Scanner only — no automatic trade."
    ])

    return "\n".join(lines)


# ============================================================
# WATCH MESSAGE
# ============================================================

def build_watch_message(
    target,
    direction,
    structure,
    alignment,
    five_trigger,
):
    if target.startswith("CRASH"):
        emoji = "👀"
        action = "SELL"
    else:
        emoji = "👀"
        action = "BUY"

    lower = alignment.get(
        "directions",
        {}
    )

    return "\n".join([
        f"{emoji} {target} — WATCH {action}",
        "",
        f"Phase: {structure.get('phase')}",
        f"Spike age: {structure.get('age', 0)} candles",
        (
            f"Spike: "
            f"{structure.get('spike_size_atr', 0):.2f} ATR"
        ),
        (
            f"Pullback: "
            f"{structure.get('pullback_atr', 0):.2f} ATR"
        ),
        "",
        f"12H: "
        f"{alignment.get('h12_direction', 'NEUTRAL')}",
        f"4H: {lower.get('4H', 'NEUTRAL')}",
        f"1H: {lower.get('1H', 'NEUTRAL')}",
        f"15M: {lower.get('15M', 'NEUTRAL')}",
        "",
        f"5M trigger: "
        f"{'YES' if five_trigger else 'NO'}",
    ])


# ============================================================
# DUPLICATE CONTROL
# ============================================================

def was_sent(target, candle_time):
    return STATE["sent"].get(
        target
    ) == candle_time


def mark_sent(target, candle_time):
    STATE["sent"][target] = candle_time

    # Keep state from growing forever.
    if len(STATE["sent"]) > 100:
        items = list(
            STATE["sent"].items()
        )

        STATE["sent"] = dict(
            items[-50:]
        )


# ============================================================
# ONE TARGET SCAN
# ============================================================

def scan_one(
    target,
    symbol
):
    print("")
    print(
        f"========== {target} =========="
    )
    print(
        f"{target} symbol={symbol}"
    )

    is_crash = target.startswith(
        "CRASH"
    )

    direction = (
        "SELL"
        if is_crash
        else
        "BUY"
    )

    # --------------------------------------------------------
    # FETCH TIMEFRAMES
    # --------------------------------------------------------

    data = {}

    try:
        for name, granularity in TIMEFRAMES.items():

            count = 220

            if name == "5M":
                count = 300

            df = get_candles(
                symbol,
                granularity,
                count
            )

            data[name] = df

        # 12H from 4H
        data["12H"] = build_12h_from_4h(
            data["4H"]
        )

    except Exception as e:

        print(
            f"{target} DATA ERROR:",
            e
        )

        return

    # --------------------------------------------------------
    # SNAPSHOTS
    # --------------------------------------------------------

    snapshots = {}

    for tf in [
        "12H",
        "4H",
        "1H",
        "15M",
    ]:

        snap = indicator_snapshot(
            data[tf]
        )

        snapshots[tf] = snap

    if not snapshots["12H"]:
        print(
            f"{target} 12H unavailable"
        )
        return

    # --------------------------------------------------------
    # 12H DEBUG
    # --------------------------------------------------------

    h12 = snapshots["12H"]

    print(
        f"{target} 12H "
        f"regime={h12['direction']} "
        f"bull_score={h12['bullish_score']}/3 "
        f"bear_score={h12['bearish_score']}/3"
    )

    # --------------------------------------------------------
    # HTF ALIGNMENT
    # --------------------------------------------------------

    alignment = higher_timeframe_alignment(
        snapshots,
        direction
    )

    lower = alignment[
        "directions"
    ]

    print(
        f"{target} ALIGNMENT "
        f"12H={alignment['h12_ok']} "
        f"4H={lower.get('4H')} "
        f"1H={lower.get('1H')} "
        f"15M={lower.get('15M')} "
        f"agreement="
        f"{alignment['lower_agreement']}/3"
    )

    # --------------------------------------------------------
    # CURRENT SETUP STATE
    # --------------------------------------------------------

    setup = get_setup(
        target
    )

    # --------------------------------------------------------
    # IF NO ACTIVE SPIKE:
    # SEARCH FOR ONE
    # --------------------------------------------------------

    if not setup:

        candidate = detect_recent_spike(
            data["5M"],
            direction
        )

        if candidate:

            setup = create_spike_state(
                data["5M"],
                candidate,
                direction
            )

            store_setup(
                target,
                setup
            )

            save_state(
                STATE
            )

            print(
                f"{target} NEW SPIKE "
                f"size="
                f"{candidate['move_atr']:.2f} ATR "
                f"bars="
                f"{candidate['bars']}"
            )

        else:

            print(
                f"{target} WAITING FOR SPIKE"
            )

            return

    # --------------------------------------------------------
    # ANALYSE ACTIVE SETUP
    # --------------------------------------------------------

    structure = analyse_active_setup(
        data["5M"],
        setup,
        direction
    )

    # Add original spike size if missing
    structure[
        "spike_size_atr"
    ] = setup.get(
        "spike_size_atr",
        0
    )

    structure[
        "spike_body_atr"
    ] = setup.get(
        "spike_body_atr",
        0
    )

    phase = structure.get(
        "phase"
    )

    print(
        f"{target} STRUCTURE "
        f"phase={phase} "
        f"age={structure.get('age')} "
        f"spike="
        f"{setup.get('spike_size_atr', 0):.2f} "
        f"ATR "
        f"pullback="
        f"{structure.get('pullback_atr', 0):.2f} ATR"
    )

    # --------------------------------------------------------
    # INVALID SETUP
    # --------------------------------------------------------

    if not structure.get(
        "valid",
        False
    ):

        print(
            f"{target} SETUP RESET "
            f"reason={phase}"
        )

        clear_setup(
            target
        )

        save_state(
            STATE
        )

        # Search immediately for another recent spike.
        candidate = detect_recent_spike(
            data["5M"],
            direction
        )

        if candidate:

            new_setup = create_spike_state(
                data["5M"],
                candidate,
                direction
            )

            store_setup(
                target,
                new_setup
            )

            save_state(
                STATE
            )

            print(
                f"{target} REPLACED SPIKE "
                f"size="
                f"{candidate['move_atr']:.2f} ATR"
            )

        return

    # --------------------------------------------------------
    # 5M TRIGGER
    # --------------------------------------------------------

    five_trigger, five_rsi = (
        five_minute_trigger(
            data["5M"],
            direction
        )
    )

    print(
        f"{target} 5M "
        f"trigger={five_trigger} "
        f"RSI={five_rsi:.1f}"
    )

    # --------------------------------------------------------
    # ONLY ENTRY PHASES CAN BECOME SIGNALS
    # --------------------------------------------------------

    ready_phase = phase in [
        "PULLBACK_WEAKENING",
        "CONTROLLED_REVERSAL",
    ]

    if not ready_phase:

        if phase in [
            "WAITING_FOR_PULLBACK",
            "SPIKE_DETECTED",
            "WAITING_FOR_CONTROLLED_REVERSAL",
            "PULLBACK_STALE",
            "PULLBACK_BUT_CANDLE_TOO_LARGE",
        ]:

            print(
                f"{target} WAITING "
                f"phase={phase}"
            )

        return

    # --------------------------------------------------------
    # HARD HTF PROTECTION
    # --------------------------------------------------------

    if not alignment[
        "h12_ok"
    ]:

        print(
            f"{target} BLOCKED "
            f"- 12H regime does not agree "
            f"with {direction}"
        )

        return

    if not alignment[
        "lower_ok"
    ]:

        print(
            f"{target} BLOCKED "
            f"- only "
            f"{alignment['lower_agreement']}/3 "
            f"lower HTFs agree"
        )

        return

    # --------------------------------------------------------
    # 5M ENTRY
    # --------------------------------------------------------

    if REQUIRE_5M_TRIGGER and not five_trigger:

        print(
            f"{target} BLOCKED "
            f"- 5M trigger not ready"
        )

        return

    # --------------------------------------------------------
    # SCORE
    # --------------------------------------------------------

    score = calculate_score(
        structure,
        alignment,
        five_trigger
    )

    print(
        f"{target} TECHNICAL SCORE "
        f"{score}/10"
    )

    if score < SIGNAL_MIN:

        print(
            f"{target} BELOW SIGNAL SCORE"
        )

        return

    # --------------------------------------------------------
    # DUPLICATE CONTROL
    # --------------------------------------------------------

    candle_time = (
        data["5M"]
        .iloc[-1]["time"]
        .isoformat()
    )

    if was_sent(
        target,
        candle_time
    ):

        print(
            f"{target} DUPLICATE "
            f"already sent"
        )

        return

    # --------------------------------------------------------
    # GROQ ADVISORY
    # --------------------------------------------------------

    groq = groq_review(
        target,
        direction,
        score,
        structure,
        alignment,
        five_trigger,
    )

    print(
        f"{target} GROQ "
        f"{groq['decision']} "
        f"{groq['confidence']}%"
    )

    # IMPORTANT:
    # Groq does NOT veto a technically valid signal.
    # It is advisory only.

    message = build_signal_message(
        target,
        direction,
        score,
        structure,
        alignment,
        five_rsi,
        groq,
    )

    # --------------------------------------------------------
    # TELEGRAM
    # --------------------------------------------------------

    sent = send_telegram(
        message
    )

    if sent:

        mark_sent(
            target,
            candle_time
        )

        # Clear setup after signal so it doesn't fire again.
        clear_setup(
            target
        )

        save_state(
            STATE
        )

        print(
            f"{target} >>> SIGNAL SENT <<<"
        )

    else:

        print(
            f"{target} TELEGRAM FAILED"
        )


# ============================================================
# CLEAN OLD SETUPS
# ============================================================

def clean_old_setups():
    """
    Prevent permanently stuck states.
    """

    remove = []

    for target, setup in STATE[
        "setups"
    ].items():

        try:
            created = setup.get(
                "created_at"
            )

            if not created:
                remove.append(target)
                continue

            created_dt = datetime.fromisoformat(
                created
            )

            age_seconds = (
                now_utc()
                - created_dt
            ).total_seconds()

            # Maximum life of a setup:
            # roughly 20 minutes.
            if age_seconds > 20 * 60:
                remove.append(target)

        except Exception:
            remove.append(target)

    for target in remove:
        STATE["setups"].pop(
            target,
            None
        )

    if remove:
        save_state(
            STATE
        )


# ============================================================
# MAIN SCAN
# ============================================================

def scan_cycle(symbol_map):
    print("")
    print("=" * 65)
    print(
        f"STARTING SCAN CYCLE "
        f"{datetime.now(timezone.utc).isoformat()}"
    )
    print(
        f"VERSION: {VERSION}"
    )
    print("=" * 65)

    clean_old_setups()

    for target in TARGETS:

        symbol = symbol_map.get(
            target
        )

        if not symbol:

            print(
                f"{target} "
                f"NO SYMBOL MAPPING"
            )

            continue

        try:

            scan_one(
                target,
                symbol
            )

        except Exception as e:

            print(
                f"{target} SCAN ERROR:",
                e
            )

            traceback.print_exc()

    save_state(
        STATE
    )

    print("")
    print(
        "CYCLE FINISHED"
    )


# ============================================================
# STARTUP CHECK
# ============================================================

def startup_check():

    print("")
    print("=" * 65)
    print(
        "DERIV CRASH/BOOM PULLBACK SCANNER"
    )
    print(
        f"VERSION: {VERSION}"
    )
    print("=" * 65)

    print(
        f"Scan interval: "
        f"{SCAN_INTERVAL_SECONDS}s"
    )

    print(
        f"Targets: "
        f"{len(TARGETS)}"
    )

    print(
        "Automatic trading: DISABLED"
    )

    print(
        "Crash direction: SELL only"
    )

    print(
        "Boom direction: BUY only"
    )

    print(
        "12H filter: 2-of-3 broad regime"
    )

    print(
        "Lower HTFs: 2-of-3"
    )

    print(
        "5M: entry trigger"
    )

    print(
        f"Signal minimum: "
        f"{SIGNAL_MIN}/10"
    )

    if not TELEGRAM_BOT_TOKEN:
        print(
            "WARNING: TELEGRAM_BOT_TOKEN missing"
        )

    if not TELEGRAM_CHAT_ID:
        print(
            "WARNING: TELEGRAM_CHAT_ID missing"
        )

    if not GROQ_API_KEY:
        print(
            "INFO: GROQ_API_KEY not configured "
            "- AI review disabled"
        )

    print("=" * 65)
    print("")


# ============================================================
# PROGRAM
# ============================================================

def main():

    startup_check()

    symbol_map = get_symbol_map()

    if not symbol_map:

        print(
            "ERROR: Could not discover "
            "Crash/Boom symbols."
        )

        # Retry later instead of killing Railway.
        while True:

            time.sleep(
                60
            )

            symbol_map = get_symbol_map()

            if symbol_map:
                break

    while True:

        cycle_start = time.time()

        try:

            # Refresh mapping occasionally.
            if (
                not symbol_map
                or
                len(symbol_map)
                < len(TARGETS)
            ):
                new_map = discover_symbols()

                if new_map:
                    symbol_map = new_map
                    STATE[
                        "symbol_map"
                    ] = new_map

                    save_state(
                        STATE
                    )

            scan_cycle(
                symbol_map
            )

        except Exception as e:

            print(
                "MAIN LOOP ERROR:",
                e
            )

            traceback.print_exc()

        # ----------------------------------------------------
        # IMPORTANT:
        #
        # Don't sleep 300 seconds AFTER a 28-second cycle.
        # That makes the actual cycle ~328 seconds.
        #
        # Instead, keep the cycle interval approximately
        # equal to SCAN_INTERVAL_SECONDS.
        # ----------------------------------------------------

        elapsed = (
            time.time()
            - cycle_start
        )

        sleep_for = max(
            5,
            SCAN_INTERVAL_SECONDS
            - elapsed
        )

        print(
            f"Next scan in "
            f"{sleep_for:.1f} seconds"
        )

        time.sleep(
            sleep_for
        )


if __name__ == "__main__":
    main()
