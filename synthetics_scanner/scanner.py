# ============================================================
# DERIV SYNTHETIC PULLBACK SCANNER
# Version: 2026-09-BALANCED-PULLBACK-V3
#
# PURPOSE:
#   CRASH  -> SELL only
#   BOOM   -> BUY only
#
# Strategy:
#   1. Detect a genuine spike
#   2. Wait for price to pull back
#   3. Make sure the pullback is not another huge continuation
#   4. Wait for a controlled reversal on 5M
#   5. Confirm higher-timeframe direction
#   6. Send Telegram alert
#
# NO AUTO TRADING.
# ============================================================

import os
import json
import time
import math
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

VERSION = "2026-09-BALANCED-PULLBACK-V3"

print("=" * 60)
print(f"DERIV PULLBACK SCANNER {VERSION}")
print("NO AUTO TRADING")
print("=" * 60)


# ============================================================
# ENVIRONMENT
# ============================================================

# IMPORTANT:
# This is the current public Deriv market-data endpoint.
# No authentication is required for public market data.
PUBLIC_WS_URL = (
    "wss://api.derivws.com/trading/v1/options/ws/public"
)

# If DERIV_WS_URL exists in Railway, it will be used.
# We also protect against the old binaryws endpoint.
DERIV_WS_URL = os.getenv(
    "DERIV_WS_URL",
    PUBLIC_WS_URL
).strip()

TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    ""
).strip()

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    ""
).strip()

GROQ_API_KEY = os.getenv(
    "GROQ_API_KEY",
    ""
).strip()

GROQ_MODEL = os.getenv(
    "GROQ_MODEL",
    "openai/gpt-oss-120b"
).strip()

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

# Spike detection
SPIKE_ATR_MULT = 1.10
SPIKE_LOOKBACK = 12
SPIKE_MAX_AGE = 8

# Pullback
PULLBACK_MIN_ATR = 0.15
PULLBACK_MAX_ATR = 1.80

# Reversal candle
MAX_REVERSAL_ATR = 1.80

# Signal quality
MIN_LOWER_TF_AGREEMENT = 2

# 5M entry
ENTRY_MIN_BODY_ATR = 0.08

# Do not send another alert for same symbol/direction/candle
DUPLICATE_PROTECTION = True


# ============================================================
# UTILITY
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def normalize_name(value):
    if value is None:
        return ""

    return (
        str(value)
        .upper()
        .replace(" ", "")
        .replace("_", "")
        .replace("-", "")
        .replace("INDEX", "")
    )


def safe_float(value, default=np.nan):
    try:
        return float(value)
    except Exception:
        return default


def clamp(value, low, high):
    return max(low, min(high, value))


# ============================================================
# STATE
# ============================================================

def load_state():
    default = {
        "version": VERSION,
        "alerts": {},
        "setups": {},
    }

    try:
        if not os.path.exists(STATE_FILE):
            return default

        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)

        # Reset old state after strategy/API changes.
        if state.get("version") != VERSION:
            print("OLD STATE VERSION DETECTED - RESETTING STATE")
            return default

        state.setdefault("alerts", {})
        state.setdefault("setups", {})
        state["version"] = VERSION

        return state

    except Exception as e:
        print("STATE LOAD ERROR:", e)
        return default


def save_state(state):
    try:
        tmp = STATE_FILE + ".tmp"

        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)

        os.replace(tmp, STATE_FILE)

    except Exception as e:
        print("STATE SAVE ERROR:", e)


STATE = load_state()


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN:
        print("TELEGRAM_BOT_TOKEN missing")
        return False

    if not TELEGRAM_CHAT_ID:
        print("TELEGRAM_CHAT_ID missing")
        return False

    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": True,
    }

    try:
        r = requests.post(
            url,
            json=payload,
            timeout=20
        )

        if r.ok:
            return True

        print(
            "TELEGRAM ERROR:",
            r.status_code,
            r.text[:500]
        )

    except Exception as e:
        print("TELEGRAM REQUEST ERROR:", e)

    return False


# ============================================================
# DERIV WEBSOCKET
# ============================================================

def ws_connect():
    if websocket is None:
        raise RuntimeError(
            "websocket-client is not installed"
        )

    url = DERIV_WS_URL

    # Protect against the old endpoint causing:
    # 401 InvalidAppID
    if "ws.binaryws.com/websockets/v3" in url:
        print(
            "OLD DERIV ENDPOINT DETECTED - "
            "USING CURRENT PUBLIC ENDPOINT"
        )
        url = PUBLIC_WS_URL

    print("Connecting to Deriv public market-data WebSocket...")

    ws = websocket.create_connection(
        url,
        timeout=25
    )

    print("DERIV WEBSOCKET CONNECTED")

    return ws


def recv_json(ws, timeout=25):
    ws.settimeout(timeout)

    raw = ws.recv()

    if not raw:
        raise RuntimeError(
            "Empty WebSocket response"
        )

    try:
        return json.loads(raw)

    except Exception:
        raise RuntimeError(
            f"Invalid JSON from Deriv: {raw[:500]}"
        )


def request(ws, payload, expected_types=None, timeout=25):
    ws.send(json.dumps(payload))

    deadline = time.time() + timeout

    while time.time() < deadline:

        remaining = max(
            1,
            int(deadline - time.time())
        )

        ws.settimeout(remaining)

        try:
            data = recv_json(ws, remaining)

        except Exception:
            raise

        if "error" in data:
            raise RuntimeError(
                str(data["error"])
            )

        msg_type = data.get("msg_type")

        if expected_types is None:
            return data

        if msg_type in expected_types:
            return data

        # Ignore unrelated messages.
        continue

    raise TimeoutError(
        f"Timed out waiting for {expected_types}"
    )


# ============================================================
# SYMBOL DISCOVERY
# ============================================================

def discover_symbols():
    """
    Uses current Deriv active_symbols API.

    New API fields:
      underlying_symbol
      underlying_symbol_name

    Legacy fields are also supported as fallback.
    """

    ws = None

    try:
        ws = ws_connect()

        response = request(
            ws,
            {
                "active_symbols": "brief",
            },
            expected_types={"active_symbols"},
            timeout=25
        )

        items = response.get(
            "active_symbols",
            []
        )

        if not items:
            raise RuntimeError(
                "Deriv returned zero active symbols"
            )

        discovered = {}

        for item in items:

            symbol = (
                item.get("underlying_symbol")
                or item.get("symbol")
            )

            display = (
                item.get("underlying_symbol_name")
                or item.get("display_name")
                or item.get("name")
                or symbol
            )

            if not symbol:
                continue

            discovered[
                normalize_name(display)
            ] = symbol

            discovered[
                normalize_name(symbol)
            ] = symbol

        result = {}

        for target in TARGETS:

            ntarget = normalize_name(target)

            found = None

            # Exact normalized name match
            for key, symbol in discovered.items():

                if key == ntarget:
                    found = symbol
                    break

            # Partial match
            if found is None:

                for key, symbol in discovered.items():

                    if ntarget in key or key in ntarget:
                        found = symbol
                        break

            if found:
                result[target] = found
                print(
                    f"FOUND {target} -> {found}"
                )

            else:
                print(
                    f"NOT FOUND: {target}"
                )

        if not result:
            raise RuntimeError(
                "None of the requested synthetic indices "
                "were found."
            )

        return result

    finally:

        if ws is not None:

            try:
                ws.close()
            except Exception:
                pass


# ============================================================
# CANDLE DATA
# ============================================================

def fetch_candles(
    symbol,
    granularity,
    count=250
):
    """
    Fetch OHLC candles from Deriv ticks_history.
    """

    ws = None

    try:
        ws = ws_connect()

        payload = {
            "ticks_history": symbol,
            "end": "latest",
            "style": "candles",
            "granularity": int(granularity),
            "count": int(count),
            "subscribe": 0,
        }

        response = request(
            ws,
            payload,
            expected_types={"candles"},
            timeout=30
        )

        candles = response.get(
            "candles",
            []
        )

        if not candles:
            raise RuntimeError(
                f"No candles returned for {symbol}"
            )

        rows = []

        for candle in candles:

            epoch = candle.get("epoch")

            if epoch is None:
                continue

            rows.append({
                "time": pd.to_datetime(
                    int(epoch),
                    unit="s",
                    utc=True
                ),
                "open": safe_float(
                    candle.get("open")
                ),
                "high": safe_float(
                    candle.get("high")
                ),
                "low": safe_float(
                    candle.get("low")
                ),
                "close": safe_float(
                    candle.get("close")
                ),
            })

        df = pd.DataFrame(rows)

        if df.empty:
            raise RuntimeError(
                f"Empty candle dataframe: {symbol}"
            )

        for col in [
            "open",
            "high",
            "low",
            "close"
        ]:
            df[col] = pd.to_numeric(
                df[col],
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
            df.sort_values("time")
            .drop_duplicates("time")
            .reset_index(drop=True)
        )

        # Remove current/incomplete candle.
        now = pd.Timestamp.now(tz="UTC")

        candle_seconds = int(granularity)

        cutoff = (
            now.floor(
                f"{candle_seconds}s"
            )
        )

        df = df[
            df["time"] < cutoff
        ].copy()

        if len(df) < 60:
            raise RuntimeError(
                f"Not enough completed candles "
                f"for {symbol}: {len(df)}"
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


def atr(df, length=14):

    prev_close = df["close"].shift(1)

    tr1 = (
        df["high"] -
        df["low"]
    )

    tr2 = (
        df["high"] -
        prev_close
    ).abs()

    tr3 = (
        df["low"] -
        prev_close
    ).abs()

    tr = pd.concat(
        [tr1, tr2, tr3],
        axis=1
    ).max(axis=1)

    return tr.ewm(
        alpha=1 / length,
        adjust=False
    ).mean()


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

    df["rsi"] = rsi(
        df["close"],
        RSI_LENGTH
    )

    df["atr"] = atr(
        df,
        ATR_LENGTH
    )

    # ATR-normalized body
    df["body"] = (
        df["close"] -
        df["open"]
    )

    df["body_abs"] = df["body"].abs()

    df["range"] = (
        df["high"] -
        df["low"]
    )

    df["body_atr"] = (
        df["body_abs"] /
        df["atr"].replace(0, np.nan)
    )

    return df


# ============================================================
# 12H DATA
# ============================================================

def build_12h(df_4h):

    x = df_4h.copy()

    x = x.set_index("time")

    result = x.resample(
        "12h",
        origin="start_day"
    ).agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
    })

    result = result.dropna()

    result = result.reset_index()

    return add_indicators(result)


# ============================================================
# TIMEFRAME DIRECTION
# ============================================================

def direction_score(df):

    if len(df) < 55:
        return 0, {
            "price": False,
            "ema": False,
            "slope": False,
        }

    last = df.iloc[-1]

    price = float(last["close"])
    e20 = float(last["ema20"])
    e50 = float(last["ema50"])

    previous_ema = float(
        df["ema20"].iloc[-4]
    )

    slope_up = e20 > previous_ema
    slope_down = e20 < previous_ema

    bullish_points = 0
    bearish_points = 0

    if price > e20:
        bullish_points += 1
    elif price < e20:
        bearish_points += 1

    if e20 > e50:
        bullish_points += 1
    elif e20 < e50:
        bearish_points += 1

    if slope_up:
        bullish_points += 1
    elif slope_down:
        bearish_points += 1

    if bullish_points >= 2:
        direction = 1

    elif bearish_points >= 2:
        direction = -1

    else:
        direction = 0

    details = {
        "price": price > e20,
        "ema": e20 > e50,
        "slope": slope_up,
    }

    return direction, details


# ============================================================
# 12H REGIME
# ============================================================

def get_12h_regime(df):

    direction, details = direction_score(df)

    return direction


# ============================================================
# LOWER TIMEFRAME ALIGNMENT
# ============================================================

def get_alignment(
    df_12h,
    df_4h,
    df_1h,
    df_15m,
    df_5m,
    expected_direction
):

    d12, _ = direction_score(df_12h)
    d4, _ = direction_score(df_4h)
    d1, _ = direction_score(df_1h)
    d15, _ = direction_score(df_15m)
    d5, _ = direction_score(df_5m)

    lower = [
        d4,
        d1,
        d15,
    ]

    lower_agreement = sum(
        1
        for d in lower
        if d == expected_direction
    )

    # 12H must agree.
    h12_ok = (
        d12 == expected_direction
    )

    # At least 2 of 4H, 1H, 15M must agree.
    lower_ok = (
        lower_agreement >=
        MIN_LOWER_TF_AGREEMENT
    )

    # 5M should also point in expected direction.
    five_ok = (
        d5 == expected_direction
    )

    return {
        "12H": d12,
        "4H": d4,
        "1H": d1,
        "15M": d15,
        "5M": d5,
        "h12_ok": h12_ok,
        "lower_agreement": lower_agreement,
        "lower_ok": lower_ok,
        "five_ok": five_ok,
    }


# ============================================================
# SPIKE DETECTION
# ============================================================

def detect_spike(
    df_15m,
    expected_direction
):

    """
    CRASH SELL:
        Find recent large bearish candle.

    BOOM BUY:
        Find recent large bullish candle.

    Uses body and range so a spike is not restricted
    to exactly one candle shape.
    """

    df = df_15m.copy()

    start = max(
        2,
        len(df) - SPIKE_LOOKBACK
    )

    candidates = []

    for i in range(
        start,
        len(df) - 1
    ):

        row = df.iloc[i]

        atr_value = float(row["atr"])

        if (
            not np.isfinite(atr_value)
            or atr_value <= 0
        ):
            continue

        body = float(
            row["close"] -
            row["open"]
        )

        body_abs = abs(body)

        candle_range = float(
            row["high"] -
            row["low"]
        )

        body_atr = (
            body_abs /
            atr_value
        )

        range_atr = (
            candle_range /
            atr_value
        )

        bearish = body < 0
        bullish = body > 0

        # CRASH spike
        if expected_direction == -1:

            correct_direction = bearish

            close_near_low = (
                row["close"] <=
                row["low"] +
                candle_range * 0.40
            )

            valid = (
                correct_direction
                and
                (
                    body_atr >= SPIKE_ATR_MULT
                    or
                    (
                        range_atr >= 1.35
                        and close_near_low
                    )
                )
            )

        # BOOM spike
        else:

            correct_direction = bullish

            close_near_high = (
                row["close"] >=
                row["high"] -
                candle_range * 0.40
            )

            valid = (
                correct_direction
                and
                (
                    body_atr >= SPIKE_ATR_MULT
                    or
                    (
                        range_atr >= 1.35
                        and close_near_high
                    )
                )
            )

        if not valid:
            continue

        candidates.append({
            "index": i,
            "time": row["time"],
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "open": float(row["open"]),
            "atr": atr_value,
            "size_atr": max(
                body_atr,
                range_atr
            ),
        })

    if not candidates:
        return None

    # Most recent valid spike
    return candidates[-1]


# ============================================================
# PULLBACK ANALYSIS
# ============================================================

def analyze_pullback(
    df_15m,
    spike,
    expected_direction
):

    if spike is None:
        return None

    spike_index = spike["index"]

    current_index = len(df_15m) - 1

    age = (
        current_index -
        spike_index
    )

    if age <= 0:
        return {
            "phase": "SPIKE",
            "age": age,
            "pullback_atr": 0,
            "reversal_atr": 0,
        }

    if age > SPIKE_MAX_AGE:
        return {
            "phase": "EXPIRED",
            "age": age,
            "pullback_atr": 0,
            "reversal_atr": 0,
        }

    after = df_15m.iloc[
        spike_index + 1:
    ]

    if after.empty:
        return None

    current = df_15m.iloc[-1]

    atr_value = float(
        current["atr"]
    )

    if (
        not np.isfinite(atr_value)
        or atr_value <= 0
    ):
        return None

    if expected_direction == -1:

        # CRASH:
        # spike goes DOWN.
        # pullback must go UP.
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

        # How far current price has returned
        # toward the spike low.
        reversal_distance = (
            pullback_high -
            current_close
        )

    else:

        # BOOM:
        # spike goes UP.
        # pullback must go DOWN.
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

    # No meaningful pullback yet.
    if pullback_atr < PULLBACK_MIN_ATR:

        return {
            "phase": "WAITING_FOR_PULLBACK",
            "age": age,
            "pullback_atr": pullback_atr,
            "reversal_atr": 0,
        }

    # Pullback became too large.
    if pullback_atr > PULLBACK_MAX_ATR:

        return {
            "phase": "PULLBACK_TOO_DEEP",
            "age": age,
            "pullback_atr": pullback_atr,
            "reversal_atr": 0,
        }

    reversal_atr = (
        abs(reversal_distance) /
        atr_value
    )

    # If price has already travelled too far
    # in the original direction, it may be
    # another spike rather than an entry.
    if reversal_atr > MAX_REVERSAL_ATR:

        return {
            "phase": "CONTINUATION_TOO_STRONG",
            "age": age,
            "pullback_atr": pullback_atr,
            "reversal_atr": reversal_atr,
        }

    # Pullback is mature enough.
    return {
        "phase": "PULLBACK",
        "age": age,
        "pullback_atr": pullback_atr,
        "reversal_atr": reversal_atr,
    }


# ============================================================
# 5M ENTRY TRIGGER
# ============================================================

def five_minute_trigger(
    df_5m,
    expected_direction
):

    if len(df_5m) < 60:
        return False, "not enough 5M data"

    last = df_5m.iloc[-1]
    previous = df_5m.iloc[-2]

    atr_value = float(
        last["atr"]
    )

    if (
        not np.isfinite(atr_value)
        or atr_value <= 0
    ):
        return False, "invalid ATR"

    body = float(
        last["close"] -
        last["open"]
    )

    body_abs = abs(body)

    body_atr = (
        body_abs /
        atr_value
    )

    # We do NOT want a huge continuation candle.
    if body_atr > MAX_REVERSAL_ATR:
        return False, "reversal candle too large"

    # -------------------------
    # CRASH SELL
    # -------------------------

    if expected_direction == -1:

        bearish = (
            last["close"] <
            last["open"]
        )

        break_previous_low = (
            last["close"] <
            previous["low"]
        )

        below_ema = (
            last["close"] <
            last["ema20"]
        )

        rsi_ok = (
            float(last["rsi"]) < 55
        )

        controlled_body = (
            body_atr >=
            ENTRY_MIN_BODY_ATR
        )

        if (
            bearish
            and
            break_previous_low
            and
            below_ema
            and
            rsi_ok
            and
            controlled_body
        ):
            return True, (
                "5M bearish reversal "
                "broke previous low"
            )

        # Secondary trigger:
        # bearish candle below EMA20
        # after pullback.
        if (
            bearish
            and
            below_ema
            and
            rsi_ok
            and
            controlled_body
        ):
            return True, (
                "5M controlled bearish "
                "reversal below EMA20"
            )

        return False, "no bearish 5M trigger"

    # -------------------------
    # BOOM BUY
    # -------------------------

    bullish = (
        last["close"] >
        last["open"]
    )

    break_previous_high = (
        last["close"] >
        previous["high"]
    )

    above_ema = (
        last["close"] >
        last["ema20"]
    )

    rsi_ok = (
        float(last["rsi"]) > 45
    )

    controlled_body = (
        body_atr >=
        ENTRY_MIN_BODY_ATR
    )

    if (
        bullish
        and
        break_previous_high
        and
        above_ema
        and
        rsi_ok
        and
        controlled_body
    ):
        return True, (
            "5M bullish reversal "
            "broke previous high"
        )

    # Secondary trigger
    if (
        bullish
        and
        above_ema
        and
        rsi_ok
        and
        controlled_body
    ):
        return True, (
            "5M controlled bullish "
            "reversal above EMA20"
        )

    return False, "no bullish 5M trigger"


# ============================================================
# QUALITY SCORE
# ============================================================

def calculate_score(
    expected_direction,
    alignment,
    pullback,
    trigger
):

    score = 0

    # 12H
    if alignment["h12_ok"]:
        score += 3

    # Lower TF agreement
    score += alignment[
        "lower_agreement"
    ]

    # 5M
    if alignment["five_ok"]:
        score += 2

    # Pullback
    if pullback["phase"] == "PULLBACK":
        score += 2

    # Better pullback zone
    pb = pullback["pullback_atr"]

    if (
        PULLBACK_MIN_ATR <=
        pb <= 1.20
    ):
        score += 1

    # Trigger
    if trigger:
        score += 2

    return score


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
        setup_name = "CRASH PULLBACK"
    else:
        emoji = "🟢"
        action = "BUY"
        setup_name = "BOOM PULLBACK"

    def direction_text(value):

        if value == 1:
            return "BULLISH"

        if value == -1:
            return "BEARISH"

        return "NEUTRAL"

    message = (
        f"{emoji} {target} — {action} PULLBACK SIGNAL\n\n"

        f"Technical score: {score}/15\n"
        f"Symbol: {symbol}\n"
        f"Price: {price:.5f}\n\n"

        f"SETUP:\n"
        f"Spike → Pullback → Reversal\n"
        f"Type: {setup_name}\n\n"

        f"Spike age: "
        f"{pullback['age']} candles\n"

        f"Spike size: "
        f"{spike['size_atr']:.2f} ATR\n"

        f"Pullback: "
        f"{pullback['pullback_atr']:.2f} ATR\n\n"

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

        f"Scanner: {VERSION}\n"
        f"Time: "
        f"{utc_now().strftime('%Y-%m-%d %H:%M UTC')}"
    )

    return message


# ============================================================
# WATCH MESSAGE
# ============================================================

def build_watch_message(
    target,
    symbol,
    expected_direction,
    pullback,
    alignment,
    spike
):

    if expected_direction == -1:
        emoji = "🔎"
        action = "SELL"
    else:
        emoji = "🔎"
        action = "BUY"

    def d(value):

        if value == 1:
            return "BULL"

        if value == -1:
            return "BEAR"

        return "NEUTRAL"

    return (
        f"{emoji} {target} — WATCH {action}\n\n"

        f"Symbol: {symbol}\n"
        f"Phase: {pullback['phase']}\n"
        f"Spike age: {pullback['age']}\n"
        f"Spike size: "
        f"{spike['size_atr']:.2f} ATR\n"
        f"Pullback: "
        f"{pullback['pullback_atr']:.2f} ATR\n\n"

        f"12H: {d(alignment['12H'])}\n"
        f"4H: {d(alignment['4H'])}\n"
        f"1H: {d(alignment['1H'])}\n"
        f"15M: {d(alignment['15M'])}\n"
        f"5M: {d(alignment['5M'])}\n\n"

        f"Waiting for the 5M entry trigger."
    )


# ============================================================
# DUPLICATE PROTECTION
# ============================================================

def already_alerted(
    target,
    direction,
    candle_time
):

    key = (
        f"{target}|"
        f"{direction}"
    )

    old = STATE["alerts"].get(key)

    return old == str(candle_time)


def mark_alerted(
    target,
    direction,
    candle_time
):

    key = (
        f"{target}|"
        f"{direction}"
    )

    STATE["alerts"][key] = str(
        candle_time
    )

    save_state(STATE)


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

    STATE["setups"][key] = {
        "spike_time": str(
            spike["time"]
        ),
        "spike_size_atr": float(
            spike["size_atr"]
        ),
    }

    save_state(STATE)


def get_saved_setup(
    target,
    direction
):

    return STATE["setups"].get(
        setup_key(
            target,
            direction
        )
    )


def clear_setup(
    target,
    direction
):

    key = setup_key(
        target,
        direction
    )

    if key in STATE["setups"]:

        del STATE["setups"][key]

        save_state(STATE)


# ============================================================
# SCAN ONE SYMBOL
# ============================================================

def scan_one(
    target,
    symbol
):

    print("-" * 60)
    print(
        f"SCANNING {target} -> {symbol}"
    )

    try:

        # ----------------------------------------------------
        # Fetch data
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # Indicators
        # ----------------------------------------------------

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
                f"{target} - not enough 12H data"
            )

            return

        # ----------------------------------------------------
        # CRASH = SELL ONLY
        # BOOM = BUY ONLY
        # ----------------------------------------------------

        target_upper = target.upper()

        if "CRASH" in target_upper:

            expected_direction = -1

        elif "BOOM" in target_upper:

            expected_direction = 1

        else:

            print(
                f"{target} - unsupported target"
            )

            return

        # ----------------------------------------------------
        # Spike
        # ----------------------------------------------------

        spike = detect_spike(
            df_15m,
            expected_direction
        )

        if spike is None:

            print(
                f"{target} NO SPIKE"
            )

            return

        # ----------------------------------------------------
        # Pullback
        # ----------------------------------------------------

        pullback = analyze_pullback(
            df_15m,
            spike,
            expected_direction
        )

        if pullback is None:

            print(
                f"{target} NO PULLBACK DATA"
            )

            return

        # ----------------------------------------------------
        # Save setup
        # ----------------------------------------------------

        if pullback["phase"] in [
            "WAITING_FOR_PULLBACK",
            "PULLBACK",
        ]:

            save_setup(
                target,
                expected_direction,
                spike
            )

        # ----------------------------------------------------
        # Alignment
        # ----------------------------------------------------

        alignment = get_alignment(
            df_12h,
            df_4h,
            df_1h,
            df_15m,
            df_5m,
            expected_direction
        )

        print(
            f"{target} ALIGNMENT "
            f"12H={alignment['h12_ok']} "
            f"4H={alignment['4H']} "
            f"1H={alignment['1H']} "
            f"15M={alignment['15M']} "
            f"5M={alignment['5M']} "
            f"lower={alignment['lower_agreement']}/3"
        )

        # ----------------------------------------------------
        # If pullback is still developing
        # ----------------------------------------------------

        if pullback["phase"] == "WAITING_FOR_PULLBACK":

            print(
                f"{target} "
                f"PULLBACK WAITING "
                f"age={pullback['age']} "
                f"pullback="
                f"{pullback['pullback_atr']:.2f} ATR"
            )

            return

        # ----------------------------------------------------
        # Expired / invalid setup
        # ----------------------------------------------------

        if pullback["phase"] in [
            "EXPIRED",
            "PULLBACK_TOO_DEEP",
            "CONTINUATION_TOO_STRONG",
        ]:

            clear_setup(
                target,
                expected_direction
            )

            print(
                f"{target} "
                f"SETUP INVALID: "
                f"{pullback['phase']}"
            )

            return

        # ----------------------------------------------------
        # Need actual pullback
        # ----------------------------------------------------

        if pullback["phase"] != "PULLBACK":

            print(
                f"{target} "
                f"WAITING - "
                f"{pullback['phase']}"
            )

            return

        # ----------------------------------------------------
        # Direction protection
        # ----------------------------------------------------

        if not alignment["h12_ok"]:

            print(
                f"{target} BLOCKED - "
                f"12H does not agree"
            )

            return

        # ----------------------------------------------------
        # Lower timeframe agreement
        # ----------------------------------------------------

        if not alignment["lower_ok"]:

            print(
                f"{target} BLOCKED - "
                f"lower TF agreement "
                f"{alignment['lower_agreement']}/3"
            )

            return

        # ----------------------------------------------------
        # 5M trigger
        # ----------------------------------------------------

        trigger, trigger_reason = (
            five_minute_trigger(
                df_5m,
                expected_direction
            )
        )

        if not trigger:

            print(
                f"{target} PULLBACK READY - "
                f"WAITING FOR 5M: "
                f"{trigger_reason}"
            )

            return

        # ----------------------------------------------------
        # 5M direction must agree
        # ----------------------------------------------------

        if not alignment["five_ok"]:

            print(
                f"{target} "
                f"5M trigger rejected - "
                f"5M direction not aligned"
            )

            return

        # ----------------------------------------------------
        # Score
        # ----------------------------------------------------

        score = calculate_score(
            expected_direction,
            alignment,
            pullback,
            trigger
        )

        # ----------------------------------------------------
        # Current price
        # ----------------------------------------------------

        last_5m = df_5m.iloc[-1]

        price = float(
            last_5m["close"]
        )

        rsi_5m = float(
            last_5m["rsi"]
        )

        candle_time = last_5m["time"]

        # ----------------------------------------------------
        # Duplicate protection
        # ----------------------------------------------------

        if DUPLICATE_PROTECTION:

            if already_alerted(
                target,
                expected_direction,
                candle_time
            ):

                print(
                    f"{target} "
                    f"DUPLICATE SIGNAL BLOCKED"
                )

                return

        # ----------------------------------------------------
        # Final alert
        # ----------------------------------------------------

        message = build_signal_message(
            target=target,
            symbol=symbol,
            expected_direction=expected_direction,
            score=score,
            price=price,
            spike=spike,
            pullback=pullback,
            alignment=alignment,
            trigger_reason=trigger_reason,
            rsi_5m=rsi_5m,
        )

        print(
            f"{target} SIGNAL READY"
        )

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
                f"{target} TELEGRAM SENT"
            )

        else:

            print(
                f"{target} TELEGRAM FAILED"
            )

    except Exception as e:

        print(
            f"{target} ERROR: {e}"
        )

        traceback.print_exc()


# ============================================================
# FULL SCAN
# ============================================================

def run_scan(symbols):

    started = time.time()

    print()
    print("=" * 60)
    print(
        f"STARTING SCAN "
        f"{utc_now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    print(
        f"VERSION: {VERSION}"
    )
    print("=" * 60)

    for target in TARGETS:

        symbol = symbols.get(target)

        if not symbol:

            print(
                f"{target} SKIPPED - "
                f"symbol not discovered"
            )

            continue

        scan_one(
            target,
            symbol
        )

    elapsed = time.time() - started

    print("=" * 60)
    print(
        f"CYCLE FINISHED "
        f"in {elapsed:.1f} seconds"
    )
    print("=" * 60)


# ============================================================
# MAIN LOOP
# ============================================================

def main():

    print(
        f"SCAN INTERVAL: "
        f"{SCAN_INTERVAL_SECONDS} seconds"
    )

    print(
        f"DERIV ENDPOINT: "
        f"{PUBLIC_WS_URL}"
    )

    # --------------------------------------------------------
    # Telegram test
    # --------------------------------------------------------

    if not TELEGRAM_BOT_TOKEN:
        print(
            "WARNING: TELEGRAM_BOT_TOKEN missing"
        )

    if not TELEGRAM_CHAT_ID:
        print(
            "WARNING: TELEGRAM_CHAT_ID missing"
        )

    # --------------------------------------------------------
    # Discover symbols
    # --------------------------------------------------------

    while True:

        try:

            symbols = discover_symbols()

            break

        except Exception as e:

            print(
                "SYMBOL DISCOVERY ERROR:",
                e
            )

            print(
                "Retrying in 30 seconds..."
            )

            time.sleep(30)

    print()
    print("SYMBOL MAP:")

    for target in TARGETS:

        print(
            f"  {target}: "
            f"{symbols.get(target, 'NOT FOUND')}"
        )

    print()

    # --------------------------------------------------------
    # Continuous scanning
    # --------------------------------------------------------

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
            f"{sleep_time:.0f} seconds"
        )

        time.sleep(
            sleep_time
        )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    main()
