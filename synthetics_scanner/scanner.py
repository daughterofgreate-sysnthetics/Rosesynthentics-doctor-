import os
import json
import time
import math
import requests
import pandas as pd
import websocket

try:
    from groq import Groq
except ImportError:
    Groq = None


# =========================
# SETTINGS
# =========================

DERIV_WS_URL = os.getenv(
    "DERIV_WS_URL",
    "wss://ws.binaryws.com/websockets/v3"
)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv(
    "GROQ_MODEL",
    "llama-3.3-70b-versatile"
)

STATE_FILE = "state.json"

TARGETS = {
    "CRASH 1000": ["Crash 1000", "Crash 1000 Index"],
    "BOOM 1000": ["Boom 1000", "Boom 1000 Index"],
    "CRASH 500": ["Crash 500", "Crash 500 Index"],
}

TIMEFRAMES = {
    "12H": 43200,
    "4H": 14400,
    "1H": 3600,
    "15M": 900,
    "5M": 300,
}

EMA_FAST = 20
EMA_SLOW = 50
RSI_LEN = 14
ATR_LEN = 14

ST_FACTOR = 3.0
ST_ATR_LEN = 10

SIGNAL_MIN = 7
WATCH_MIN = 5


# =========================
# BASIC HELPERS
# =========================

def load_state():
    if not os.path.exists(STATE_FILE):
        return {}

    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing"
        )

    url = (
        f"https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    response = requests.post(
        url,
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
        },
        timeout=20,
    )

    response.raise_for_status()


# =========================
# DERIV CONNECTION
# =========================

def deriv_request(payload):
    ws = websocket.create_connection(
        DERIV_WS_URL,
        timeout=20
    )

    try:
        ws.send(json.dumps(payload))

        while True:
            raw = ws.recv()

            if not raw:
                continue

            data = json.loads(raw)

            if "error" in data:
                raise RuntimeError(
                    data["error"].get(
                        "message",
                        "Deriv API error"
                    )
                )

            return data

    finally:
        ws.close()


def get_active_symbols():
    data = deriv_request({
        "active_symbols": "full",
        "product_type": "basic",
    })

    return data.get("active_symbols", [])


def find_symbol(target_name):
    symbols = get_active_symbols()

    wanted = [
        x.lower()
        for x in TARGETS.get(target_name, [])
    ]

    for item in symbols:

        display = str(
            item.get("underlying_symbol_name")
            or item.get("display_name")
            or item.get("symbol")
            or ""
        ).strip()

        symbol = str(
            item.get("underlying_symbol")
            or item.get("symbol")
            or ""
        ).strip()

        values = [
            display.lower(),
            symbol.lower(),
        ]

        for want in wanted:
            for value in values:
                if value == want or want in value:
                    return symbol

    return None


def get_candles(symbol, granularity, count=250):
    data = deriv_request({
        "ticks_history": symbol,
        "style": "candles",
        "granularity": granularity,
        "count": count,
        "subscribe": 0,
    })

    candles = data.get("candles", [])

    if not candles:
        raise RuntimeError(
            f"No candles returned for {symbol}"
        )

    df = pd.DataFrame(candles)

    if "epoch" not in df.columns:
        raise RuntimeError(
            f"Invalid candle data for {symbol}"
        )

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        )

    df["epoch"] = pd.to_numeric(
        df["epoch"],
        errors="coerce"
    )

    df = df.dropna(
        subset=[
            "epoch",
            "open",
            "high",
            "low",
            "close",
        ]
    )

    df = df.sort_values("epoch")

    # Remove currently forming candle
    now = time.time()

    df = df[
        df["epoch"] + granularity <= now
    ]

    if len(df) < 60:
        raise RuntimeError(
            f"Not enough completed candles for {symbol}"
        )

    return df.reset_index(drop=True)


# =========================
# INDICATORS
# =========================

def ema(series, length):
    return series.ewm(
        span=length,
        adjust=False
    ).mean()


def rma(series, length):
    return series.ewm(
        alpha=1 / length,
        adjust=False
    ).mean()


def calculate_atr(df, length=14):
    previous_close = df["close"].shift(1)

    tr1 = df["high"] - df["low"]

    tr2 = (
        df["high"] -
        previous_close
    ).abs()

    tr3 = (
        df["low"] -
        previous_close
    ).abs()

    true_range = pd.concat(
        [tr1, tr2, tr3],
        axis=1
    ).max(axis=1)

    return rma(
        true_range,
        length
    )


def calculate_rsi(series, length=14):
    change = series.diff()

    gain = change.clip(lower=0)
    loss = -change.clip(upper=0)

    avg_gain = rma(
        gain,
        length
    )

    avg_loss = rma(
        loss,
        length
    )

    rs = avg_gain / avg_loss.replace(
        0,
        math.nan
    )

    rsi = 100 - (
        100 / (1 + rs)
    )

    return rsi.fillna(50)


def calculate_supertrend(
    df,
    atr_length=10,
    factor=3.0
):
    high = df["high"]
    low = df["low"]
    close = df["close"]

    atr = calculate_atr(
        df,
        atr_length
    )

    hl2 = (
        high + low
    ) / 2

    upper = hl2 + factor * atr
    lower = hl2 - factor * atr

    final_upper = upper.copy()
    final_lower = lower.copy()

    direction = pd.Series(
        1,
        index=df.index,
        dtype=int
    )

    for i in range(1, len(df)):

        if (
            upper.iloc[i] <
            final_upper.iloc[i - 1]
            or close.iloc[i - 1] >
            final_upper.iloc[i - 1]
        ):
            final_upper.iloc[i] = upper.iloc[i]
        else:
            final_upper.iloc[i] = (
                final_upper.iloc[i - 1]
            )

        if (
            lower.iloc[i] >
            final_lower.iloc[i - 1]
            or close.iloc[i - 1] <
            final_lower.iloc[i - 1]
        ):
            final_lower.iloc[i] = lower.iloc[i]
        else:
            final_lower.iloc[i] = (
                final_lower.iloc[i - 1]
            )

        if direction.iloc[i - 1] == -1:

            if close.iloc[i] > final_upper.iloc[i]:
                direction.iloc[i] = 1
            else:
                direction.iloc[i] = -1

        else:

            if close.iloc[i] < final_lower.iloc[i]:
                direction.iloc[i] = -1
            else:
                direction.iloc[i] = 1

    return direction


def get_snapshot(df):
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
        RSI_LEN
    )

    df["atr"] = calculate_atr(
        df,
        ATR_LEN
    )

    df["supertrend"] = calculate_supertrend(
        df,
        ST_ATR_LEN,
        ST_FACTOR
    )

    last = df.iloc[-1]
    previous = df.iloc[-2]

    close = float(last["close"])

    ema20 = float(last["ema20"])
    ema50 = float(last["ema50"])

    rsi = float(last["rsi"])
    atr = float(last["atr"])

    st = int(last["supertrend"])

    if close > ema20 and ema20 > ema50:
        ema_trend = 1
    elif close < ema20 and ema20 < ema50:
        ema_trend = -1
    else:
        ema_trend = 0

    if rsi >= 55:
        rsi_bias = 1
    elif rsi <= 45:
        rsi_bias = -1
    else:
        rsi_bias = 0

    break_up = (
        close >
        float(previous["high"])
    )

    break_down = (
        close <
        float(previous["low"])
    )

    atr_percent = (
        atr / close * 100
        if close
        else 0
    )

    return {
        "close": close,
        "ema20": ema20,
        "ema50": ema50,
        "rsi": rsi,
        "atr": atr,
        "atr_percent": atr_percent,
        "supertrend": st,
        "ema_trend": ema_trend,
        "rsi_bias": rsi_bias,
        "break_up": break_up,
        "break_down": break_down,
        "candle_time": int(last["epoch"]),
    }


# =========================
# SCORING
# =========================

def score_market(data):
    score = 0
    buy_points = []
    sell_points = []

    def add(direction, points, reason):

        nonlocal score

        score += points

        if direction == 1:
            buy_points.append(
                (points, reason)
            )

        elif direction == -1:
            sell_points.append(
                (points, reason)
            )

    # 12H
    s = data["12H"]

    if s["ema_trend"] == 1:
        add(1, 1, "12H bullish")

    elif s["ema_trend"] == -1:
        add(-1, 1, "12H bearish")

    # 4H
    s = data["4H"]

    if s["ema_trend"] == 1:
        add(1, 1, "4H bullish")

    elif s["ema_trend"] == -1:
        add(-1, 1, "4H bearish")

    # 1H
    s = data["1H"]

    if s["ema_trend"] == 1:
        add(1, 2, "1H bullish")

    elif s["ema_trend"] == -1:
        add(-1, 2, "1H bearish")

    # 15M
    s = data["15M"]

    if s["ema_trend"] == 1:
        add(1, 1, "15M EMA bullish")

    elif s["ema_trend"] == -1:
        add(-1, 1, "15M EMA bearish")

    if s["supertrend"] == 1:
        add(1, 1, "15M Supertrend bullish")

    elif s["supertrend"] == -1:
        add(-1, 1, "15M Supertrend bearish")

    if s["rsi_bias"] == 1:
        add(1, 1, "15M RSI bullish")

    elif s["rsi_bias"] == -1:
        add(-1, 1, "15M RSI bearish")

    # 5M
    s = data["5M"]

    if s["supertrend"] == 1:
        add(1, 1, "5M Supertrend bullish")

    elif s["supertrend"] == -1:
        add(-1, 1, "5M Supertrend bearish")

    if s["break_up"]:
        add(1, 1, "5M price break")

    elif s["break_down"]:
        add(-1, 1, "5M price break")

    buy_score = sum(
        x[0] for x in buy_points
    )

    sell_score = sum(
        x[0] for x in sell_points
    )

    if buy_score >= SIGNAL_MIN:
        proposal = "BUY"

    elif sell_score >= SIGNAL_MIN:
        proposal = "SELL"

    elif max(
        buy_score,
        sell_score
    ) >= WATCH_MIN:

        proposal = "WATCH"

    else:
        proposal = "PASS"

    return {
        "proposal": proposal,
        "buy_score": buy_score,
        "sell_score": sell_score,
        "buy_reasons": [
            x[1] for x in buy_points
        ],
        "sell_reasons": [
            x[1] for x in sell_points
        ],
    }


# =========================
# GROQ
# =========================

def groq_review(
    instrument,
    market_data,
    technical
):
    if not GROQ_API_KEY:
        raise RuntimeError(
            "GROQ_API_KEY is missing"
        )

    if Groq is None:
        raise RuntimeError(
            "Groq package is not installed"
        )

    client = Groq(
        api_key=GROQ_API_KEY
    )

    prompt = f"""
You are the final reviewer for a non-trading
synthetic-indices signal scanner.

Instrument:
{instrument}

The technical engine has proposed:
{technical["proposal"]}

Buy score:
{technical["buy_score"]}

Sell score:
{technical["sell_score"]}

Technical BUY reasons:
{technical["buy_reasons"]}

Technical SELL reasons:
{technical["sell_reasons"]}

Market snapshots:

12H:
{market_data["12H"]}

4H:
{market_data["4H"]}

1H:
{market_data["1H"]}

15M:
{market_data["15M"]}

5M:
{market_data["5M"]}

Review the technical proposal.

Do not invent market data.
Do not place trades.
Do not require every timeframe to agree.
Give greater importance to 15M and 5M,
while using 12H, 4H and 1H as context.

Return ONLY valid JSON:

{{
  "decision": "BUY|SELL|WATCH|PASS",
  "confidence": 0,
  "reason": "short explanation"
}}

Confidence must be an integer from 0 to 100.
"""

    response = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a disciplined market "
                    "analysis reviewer."
                ),
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        temperature=0.1,
        max_tokens=300,
    )

    content = (
        response.choices[0]
        .message
        .content
        .strip()
    )

    # Remove markdown fences if Groq adds them
    if content.startswith("```"):
        content = content.replace(
            "```json",
            ""
        ).replace(
            "```",
            ""
        ).strip()

    result = json.loads(content)

    decision = str(
        result.get("decision", "PASS")
    ).upper()

    confidence = int(
        result.get("confidence", 0)
    )

    reason = str(
        result.get("reason", "")
    )

    if decision not in {
        "BUY",
        "SELL",
        "WATCH",
        "PASS",
    }:
        decision = "PASS"

    confidence = max(
        0,
        min(100, confidence)
    )

    return {
        "decision": decision,
        "confidence": confidence,
        "reason": reason,
    }


# =========================
# ALERT MESSAGE
# =========================

def make_message(
    instrument,
    market,
    technical,
    ai
):
    price = market["5M"]["close"]

    decision = ai["decision"]

    if decision == "BUY":
        title = "🟢 BUY SIGNAL"
    elif decision == "SELL":
        title = "🔴 SELL SIGNAL"
    else:
        title = "🟡 WATCH"

    return f"""
{title}

Instrument: {instrument}
Price: {price}

AI Decision: {decision}
AI Confidence: {ai["confidence"]}%

Technical Buy Score:
{technical["buy_score"]}

Technical Sell Score:
{technical["sell_score"]}

12H: {market["12H"]["ema_trend"]}
4H: {market["4H"]["ema_trend"]}
1H: {market["1H"]["ema_trend"]}
15M: {market["15M"]["ema_trend"]}
5M Supertrend: {market["5M"]["supertrend"]}

5M RSI:
{market["5M"]["rsi"]:.1f}

Groq Review:
{ai["reason"]}

Non-auto-trading scanner.
""".strip()


# =========================
# SCAN ONE INSTRUMENT
# =========================

def scan_instrument(
    instrument,
    symbol,
    state
):
    print(
        f"Scanning {instrument} ({symbol})..."
    )

    market = {}

    for tf, granularity in TIMEFRAMES.items():

        df = get_candles(
            symbol,
            granularity
        )

        market[tf] = get_snapshot(df)

    technical = score_market(
        market
    )

    proposal = technical["proposal"]

    print(
        f"{instrument}: "
        f"proposal={proposal} "
        f"buy={technical['buy_score']} "
        f"sell={technical['sell_score']}"
    )

    if proposal == "PASS":
        print(
            f"{instrument}: NO SETUP"
        )
        return

    # Groq is REQUIRED
    try:
        ai = groq_review(
            instrument,
            market,
            technical
        )

    except Exception as e:
        print(
            f"{instrument}: GROQ ERROR: {e}"
        )
        return

    print(
        f"{instrument}: "
        f"Groq={ai['decision']} "
        f"confidence={ai['confidence']}"
    )

    decision = ai["decision"]

    # Groq must agree with BUY/SELL
    if decision in {"BUY", "SELL"}:

        if proposal != decision:
            print(
                f"{instrument}: "
                f"AI disagreement - no signal"
            )
            return

    elif decision == "WATCH":

        if proposal not in {
            "BUY",
            "SELL",
            "WATCH",
        }:
            return

    elif decision == "PASS":
        print(
            f"{instrument}: "
            f"Groq rejected setup"
        )
        return

    else:
        return

    candle_time = str(
        market["5M"]["candle_time"]
    )

    state_key = instrument

    if state.get(state_key) == (
        f"{decision}:{candle_time}"
    ):
        print(
            f"{instrument}: duplicate alert"
        )
        return

    message = make_message(
        instrument,
        market,
        technical,
        ai
    )

    try:
        send_telegram(message)

        state[state_key] = (
            f"{decision}:{candle_time}"
        )

        save_state(state)

        print(
            f"{instrument}: "
            f"Telegram alert sent"
        )

    except Exception as e:
        print(
            f"{instrument}: "
            f"TELEGRAM ERROR: {e}"
        )


# =========================
# MAIN
# =========================

def main():

    print(
        "Synthetic Indices Scanner Starting..."
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
            "ERROR: GROQ_API_KEY missing"
        )
        return

    state = load_state()

    for instrument in TARGETS:

        try:

            symbol = find_symbol(
                instrument
            )

            if not symbol:
                print(
                    f"{instrument}: "
                    f"symbol not found"
                )
                continue

            print(
                f"{instrument}: "
                f"Deriv symbol = {symbol}"
            )

            scan_instrument(
                instrument,
                symbol,
                state
            )

        except Exception as e:

            print(
                f"{instrument} ERROR: {e}"
            )

    print(
        "Scan completed."
    )


if __name__ == "__main__":
    main()
