import os
import json
import time
import requests
import websocket
import pandas as pd
import numpy as np

# ============================================================
# DERIV CURRENT PUBLIC API
# ============================================================

DERIV_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"

# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv(
    "GROQ_MODEL",
    "llama-3.3-70b-versatile"
)

# ============================================================
# SETTINGS
# ============================================================

STATE_FILE = "state.json"

WATCH_MIN = 5
SIGNAL_MIN = 7

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
        with open(
            STATE_FILE,
            "r",
            encoding="utf-8"
        ) as f:
            return json.load(f)
    except Exception:
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
# DERIV CONNECTION
# ============================================================

def connect_deriv():

    log(
        "Connecting to current "
        "Deriv public API..."
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
    timeout=45
):

    ws.settimeout(timeout)

    ws.send(
        json.dumps(payload)
    )

    wanted_req_id = payload.get(
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

        if (
            data.get("error")
            or data.get("errors")
        ):
            raise RuntimeError(
                str(
                    data.get("error")
                    or data.get("errors")
                )
            )

        if (
            wanted_req_id is None
            or data.get("req_id")
            == wanted_req_id
        ):
            return data

        if data.get("msg_type") in {
            "active_symbols",
            "candles",
            "history"
        }:
            return data

    raise TimeoutError(
        "Deriv response timeout"
    )


# ============================================================
# ACTIVE SYMBOLS
# ============================================================

def get_active_symbols(ws):

    log(
        "Requesting Deriv active symbols..."
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
            "No active symbols returned: "
            f"{response}"
        )

    log(
        "Deriv returned "
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
        or item.get("symbol")
        or ""
    )


def get_symbol_name(item):

    return str(
        item.get(
            "underlying_symbol_name"
        )
        or item.get("display_name")
        or ""
    )


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
                name + " " + code
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
                f"{target} -> "
                "NOT FOUND"
            )

    return results


# ============================================================
# CANDLE DATA
# ============================================================

def get_candles(
    ws,
    symbol,
    minutes,
    count=250
):

    req_id = (
        int(time.time() * 1000)
        % 1000000000
    )

    response = deriv_request(
        ws,
        {
            "ticks_history": symbol,
            "end": "latest",
            "style": "candles",
            "granularity":
                minutes * 60,
            "count": count,
            "adjust_start_time": 1,
            "subscribe": 0,
            "req_id": req_id
        },
        timeout=60
    )

    candles = response.get(
        "candles",
        []
    )

    if not candles:

        raise RuntimeError(
            f"No candles for "
            f"{symbol} {minutes}m: "
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
        .drop_duplicates("time")
        .reset_index(drop=True)
    )

    now = pd.Timestamp.now(
        tz="UTC"
    )

    current_candle_start = (
        now.floor(
            f"{minutes}min"
        )
    )

    df = df[
        df["time"]
        < current_candle_start
    ].reset_index(
        drop=True
    )

    if len(df) < 60:

        raise RuntimeError(
            f"Only {len(df)} "
            f"completed candles for "
            f"{symbol} {minutes}m"
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

    return result.fillna(50)


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
    ).max(axis=1)

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

def analyze_timeframe(df):

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

    data["st"] = supertrend(
        data,
        10,
        3.0
    )

    last = data.iloc[-1]

    score = 0
    reasons = []

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
        "score": int(score),

        "close":
            float(last["close"]),

        "rsi":
            float(last["rsi"]),

        "candle_time":
            last["time"].isoformat(),

        "reasons":
            reasons
    }


# ============================================================
# 5 MINUTE CONFIRMATION
# ============================================================

def analyze_5m(df):

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

    if (
        last["close"]
        >
        last["ema20"]
    ):

        bull_count += 1

    if (
        last["close"]
        <
        last["ema20"]
    ):

        bear_count += 1

    if last["st"] == 1:

        bull_count += 1

    else:

        bear_count += 1

    if (
        last["close"]
        >
        previous["high"]
    ):

        bull_count += 1

    if (
        last["close"]
        <
        previous["low"]
    ):

        bear_count += 1

    return {

        "bullish":
            bull_count >= 2,

        "bearish":
            bear_count >= 2,

        "bull_count":
            bull_count,

        "bear_count":
            bear_count,

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

    # Status

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

If technical direction is BUY:
you may return BUY, WATCH or PASS.

If technical direction is SELL:
you may return SELL, WATCH or PASS.

If technical direction is NONE:
return PASS.

Do not be excessively strict.

WATCH is allowed when a setup
is developing.

PASS when the timeframes strongly
conflict.

Return JSON only:

{{
  "decision":
    "BUY|SELL|WATCH|PASS",

  "confidence":
    0,

  "reason":
    "short explanation"
}}
"""

    response = (
        client
        .chat
        .completions
        .create(
            model=GROQ_MODEL,
            temperature=0.1,
            max_tokens=200,
            messages=[
                {
                    "role":
                        "system",
                    "content":
                        "Return valid JSON only."
                },
                {
                    "role":
                        "user",
                    "content":
                        prompt
                }
            ]
        )
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

    result = json.loads(
        text
    )

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

        "decision":
            decision,

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
        f"bot{TELEGRAM_BOT_TOKEN}"
        "/sendMessage"
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

    technical = calculate_score(
        h12,
        h4,
        h1,
        m15,
        m5
    )

    log(
        f"{instrument} "
        f"direction="
        f"{technical['direction']} "
        f"status="
        f"{technical['status']} "
        f"buy="
        f"{technical['buy_score']} "
        f"sell="
        f"{technical['sell_score']}"
    )

    if (
        technical["status"]
        == "PASS"
    ):

        log(
            f"{instrument} "
            "NO SETUP"
        )

        return

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
        f"{instrument} GROQ "
        f"decision={ai['decision']} "
        f"confidence="
        f"{ai['confidence']}"
    )

    # Prevent AI from reversing
    # the technical direction.

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
                "AI direction rejected."
            )

            return

    if ai["decision"] == "PASS":

        log(
            f"{instrument} "
            "GROQ PASS"
        )

        return

    candle_time = m5[
        "candle_time"
    ]

    previous = state.get(
        instrument,
        {}
    )

    if (
        previous.get("decision")
        ==
        ai["decision"]
        and
        previous.get("candle_time")
        ==
        candle_time
    ):

        log(
            f"{instrument} "
            "DUPLICATE - not sending"
        )

        return

    if ai["decision"] == "BUY":

        icon = "🟢"

    elif ai["decision"] == "SELL":

        icon = "🔴"

    else:

        icon = "🟡"

    message = (
        f"{icon} "
        f"{ai['decision']} — "
        f"{instrument}\n\n"

        f"Score: "
        f"{technical['score']}\n"

        f"Buy score: "
        f"{technical['buy_score']}\n"

        f"Sell score: "
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

        f"AI confidence: "
        f"{ai['confidence']}\n\n"

        f"AI review: "
        f"{ai['reason']}\n\n"

        "Analysis only — "
        "no auto trading."
    )

    send_telegram(
        message
    )

    log(
        f"{instrument} "
        "TELEGRAM ALERT SENT: "
        f"{ai['decision']}"
    )

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
    log("=" * 60)

    log(
        "SYNTHETIC INDICES "
        "SCANNER"
    )

    log(
        "CURRENT DERIV PUBLIC API"
    )

    log("=" * 60)

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

        for item in missing:

            log(
                f"ERROR: "
                f"{item} missing"
            )

        return

    ws = None

    try:

        ws = connect_deriv()

        active_symbols = (
            get_active_symbols(ws)
        )

        symbols = (
            discover_symbols(
                active_symbols
            )
        )

        state = load_state()

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


if __name__ == "__main__":
    main()
