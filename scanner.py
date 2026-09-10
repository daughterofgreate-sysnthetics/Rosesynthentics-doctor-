import os
import json
import time
import math
import logging
from datetime import datetime, timezone

import pandas as pd
import requests
from websocket import create_connection

try:
    from groq import Groq
except ImportError:
    Groq = None

# ---------------- CONFIG ----------------
DERIV_WS_URL = os.getenv("DERIV_WS_URL", "wss://ws.binaryws.com/websockets/v3")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

# The exact Deriv symbols are discovered automatically from active_symbols.
TARGET_NAMES = {
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

# Moderate, deliberately non-perfect alignment.
SIGNAL_MIN = 7
WATCH_MIN = 5

EMA_FAST = 20
EMA_SLOW = 50
RSI_LEN = 14
ATR_LEN = 14
ST_FACTOR = 3.0
ST_ATR_LEN = 10

STATE_FILE = "state.json"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("synthetic-scanner")


# ---------------- HELPERS ----------------
def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram variables are not configured.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(
        url,
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
        timeout=20,
    )
    r.raise_for_status()
    return True


def deriv_request(ws, payload):
    ws.send(json.dumps(payload))
    while True:
        raw = ws.recv()
        data = json.loads(raw)
        if data.get("error"):
            raise RuntimeError(data["error"].get("message", "Deriv API error"))
        return data


def get_active_symbols():
    ws = create_connection(DERIV_WS_URL, timeout=20)
    try:
        data = deriv_request(ws, {"active_symbols": "brief", "req_id": 1})
    finally:
        ws.close()

    symbols = data.get("active_symbols", [])
    found = {}

    for item in symbols:
        name = (
            item.get("underlying_symbol_name")
            or item.get("display_name")
            or item.get("symbol")
            or ""
        )
        symbol = item.get("underlying_symbol") or item.get("symbol")

        if not symbol:
            continue

        normalized = " ".join(str(name).upper().split())
        for target, aliases in TARGET_NAMES.items():
            if target in normalized or any(
                " ".join(a.upper().split()) in normalized for a in aliases
            ):
                found[target] = symbol

    return found


def get_candles(symbol, granularity, count=250):
    """Fetch completed OHLC candles directly from Deriv."""
    ws = create_connection(DERIV_WS_URL, timeout=30)
    try:
        payload = {
            "ticks_history": symbol,
            "end": "latest",
            "count": count,
            "style": "candles",
            "granularity": granularity,
            "subscribe": 0,
            "req_id": 2,
        }
        data = deriv_request(ws, payload)
    finally:
        ws.close()

    candles = data.get("candles", [])
    if not candles:
        raise RuntimeError(f"No candles returned for {symbol} / {granularity}")

    df = pd.DataFrame(candles)
    required = ["epoch", "open", "high", "low", "close"]
    for col in required:
        if col not in df.columns:
            raise RuntimeError(f"Missing {col} in Deriv candle response")

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["time"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
    df = (
        df.dropna(subset=["open", "high", "low", "close"])
        .drop_duplicates("epoch")
        .sort_values("epoch")
        .set_index("time")
    )

    # Exclude the currently forming candle.
    now = pd.Timestamp.now(tz="UTC")
    if len(df) and (now - df.index[-1]).total_seconds() < granularity:
        df = df.iloc[:-1]

    return df


# ---------------- INDICATORS ----------------
def rma(series, length):
    return series.ewm(alpha=1 / length, adjust=False).mean()


def atr(df, length=ATR_LEN):
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return rma(tr, length)


def rsi(series, length=RSI_LEN):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = rma(gain, length)
    avg_loss = rma(loss, length)
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    out = 100 - (100 / (1 + rs))
    return out.fillna(50)


def supertrend(df, atr_len=ST_ATR_LEN, factor=ST_FACTOR):
    a = atr(df, atr_len)
    hl2 = (df["high"] + df["low"]) / 2
    upper_basic = hl2 + factor * a
    lower_basic = hl2 - factor * a

    upper = upper_basic.copy()
    lower = lower_basic.copy()
    direction = pd.Series(index=df.index, dtype="int64")

    direction.iloc[0] = 1
    for i in range(1, len(df)):
        if upper_basic.iloc[i] < upper.iloc[i - 1] or df["close"].iloc[i - 1] > upper.iloc[i - 1]:
            upper.iloc[i] = upper_basic.iloc[i]
        else:
            upper.iloc[i] = upper.iloc[i - 1]

        if lower_basic.iloc[i] > lower.iloc[i - 1] or df["close"].iloc[i - 1] < lower.iloc[i - 1]:
            lower.iloc[i] = lower_basic.iloc[i]
        else:
            lower.iloc[i] = lower.iloc[i - 1]

        if direction.iloc[i - 1] == -1 and df["close"].iloc[i] > upper.iloc[i]:
            direction.iloc[i] = 1
        elif direction.iloc[i - 1] == 1 and df["close"].iloc[i] < lower.iloc[i]:
            direction.iloc[i] = -1
        else:
            direction.iloc[i] = direction.iloc[i - 1]

    return direction


def indicator_snapshot(df):
    if len(df) < 80:
        raise RuntimeError("Not enough candles for indicators")

    e20 = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    e50 = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    r = rsi(df["close"])
    a = atr(df)
    st = supertrend(df)

    last = df.iloc[-1]
    prev = df.iloc[-2]

    bullish = bool(e20.iloc[-1] > e50.iloc[-1])
    bearish = bool(e20.iloc[-1] < e50.iloc[-1])
    st_bull = bool(st.iloc[-1] == 1)
    st_bear = bool(st.iloc[-1] == -1)

    # RSI is intentionally broad; we do not demand 70/30 extremes.
    rsi_bull = bool(r.iloc[-1] >= 52)
    rsi_bear = bool(r.iloc[-1] <= 48)

    atr_pct = float(a.iloc[-1] / last["close"] * 100) if last["close"] else 0.0

    # Simple momentum / structure trigger.
    break_up = bool(last["close"] > prev["high"])
    break_down = bool(last["close"] < prev["low"])

    return {
        "close": float(last["close"]),
        "ema20": float(e20.iloc[-1]),
        "ema50": float(e50.iloc[-1]),
        "rsi": float(r.iloc[-1]),
        "atr": float(a.iloc[-1]),
        "atr_pct": atr_pct,
        "supertrend": "BULLISH" if st_bull else "BEARISH",
        "ema_trend": "BULLISH" if bullish else "BEARISH",
        "rsi_bias": "BULLISH" if rsi_bull else ("BEARISH" if rsi_bear else "NEUTRAL"),
        "break_up": break_up,
        "break_down": break_down,
        "candle_time": df.index[-1].isoformat(),
    }


def score_market(snaps):
    """
    Returns directional scores. Higher TFs provide context;
    15M and 5M carry more weight. Perfect alignment is NOT required.
    """
    buy = 0
    sell = 0

    # 12H: context only
    if snaps["12H"]["ema_trend"] == "BULLISH":
        buy += 1
    elif snaps["12H"]["ema_trend"] == "BEARISH":
        sell += 1

    # 4H: context only
    if snaps["4H"]["ema_trend"] == "BULLISH":
        buy += 1
    elif snaps["4H"]["ema_trend"] == "BEARISH":
        sell += 1

    # 1H: stronger bias
    if snaps["1H"]["ema_trend"] == "BULLISH":
        buy += 2
    elif snaps["1H"]["ema_trend"] == "BEARISH":
        sell += 2

    # 15M: primary setup
    m15 = snaps["15M"]
    if m15["ema_trend"] == "BULLISH":
        buy += 1
    elif m15["ema_trend"] == "BEARISH":
        sell += 1

    if m15["supertrend"] == "BULLISH":
        buy += 1
    elif m15["supertrend"] == "BEARISH":
        sell += 1

    if m15["rsi_bias"] == "BULLISH":
        buy += 1
    elif m15["rsi_bias"] == "BEARISH":
        sell += 1

    # 5M: trigger/confirmation
    m5 = snaps["5M"]
    if m5["supertrend"] == "BULLISH":
        buy += 1
    elif m5["supertrend"] == "BEARISH":
        sell += 1

    if m5["break_up"] and m5["rsi"] >= 50:
        buy += 1
    if m5["break_down"] and m5["rsi"] <= 50:
        sell += 1

    return buy, sell


def groq_review(name, snaps, buy, sell):
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY is required for every decision")
    if Groq is None:
        raise RuntimeError("groq package is not installed")

    try:
        client = Groq(api_key=GROQ_API_KEY)
        compact = {}
        for tf, s in snaps.items():
            compact[tf] = {
                "close": round(s["close"], 8),
                "ema20": round(s["ema20"], 8),
                "ema50": round(s["ema50"], 8),
                "rsi": round(s["rsi"], 2),
                "atr_pct": round(s["atr_pct"], 4),
                "supertrend": s["supertrend"],
                "ema_trend": s["ema_trend"],
                "rsi_bias": s["rsi_bias"],
                "break_up": s["break_up"],
                "break_down": s["break_down"],
            }

        prompt = f"""
You are a conservative market-analysis filter. This is a signal scanner, NOT an auto-trader.
Instrument: {name}
Rule scores: BUY={buy}, SELL={sell}
Multi-timeframe indicator data:
{json.dumps(compact, indent=2)}

Return ONLY JSON:
{{"decision":"BUY|SELL|WATCH|PASS","confidence":0-100,"reason":"short reason"}}

Do not invent price data. Do not give a trade instruction if the evidence is mixed.
A WATCH is acceptable when the setup is developing but not yet strong.
"""
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            temperature=0.1,
            messages=[
                {"role": "system", "content": "Return valid JSON only."},
                {"role": "user", "content": prompt},
            ],
        )
        text = response.choices[0].message.content.strip()
        # Strip accidental markdown fences.
        if text.startswith("```"):
            text = text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        log.warning("Groq review failed: %s", e)
        return {"decision": "PASS", "confidence": None, "reason": "Groq unavailable"}


def build_message(name, symbol, direction, score, groq_result, snaps):
    emoji = "🟢" if direction == "BUY" else "🔴"
    reason = groq_result.get("reason", "")
    confidence = groq_result.get("confidence")

    lines = [
        f"{emoji} {name} — {direction} SIGNAL",
        f"Score: {score}",
        f"Symbol: {symbol}",
        f"Price: {snaps['5M']['close']}",
        "",
        "Timeframes:",
        f"12H {snaps['12H']['ema_trend']} | 4H {snaps['4H']['ema_trend']}",
        f"1H {snaps['1H']['ema_trend']} | 15M {snaps['15M']['supertrend']}",
        f"5M {snaps['5M']['supertrend']} | RSI {snaps['5M']['rsi']:.1f}",
    ]

    if confidence is not None:
        lines.append(f"Groq confidence: {confidence}%")
    if reason:
        lines.append(f"AI note: {reason}")

    lines.append("")
    lines.append("Scanner only — no automatic trading.")
    return "\n".join(lines)


# ---------------- SCAN ----------------
def scan_one(name, symbol, state):
    log.info("Scanning %s (%s)", name, symbol)

    snaps = {}
    for tf, seconds in TIMEFRAMES.items():
        df = get_candles(symbol, seconds, count=250)
        snaps[tf] = indicator_snapshot(df)

    buy, sell = score_market(snaps)
    log.info("%s scores BUY=%d SELL=%d", name, buy, sell)

    # Technical engine proposes a direction; Groq MUST review every proposal.
    if buy >= SIGNAL_MIN and buy > sell:
        proposal, score = "BUY", buy
    elif sell >= SIGNAL_MIN and sell > buy:
        proposal, score = "SELL", sell
    elif max(buy, sell) >= WATCH_MIN and buy != sell:
        proposal, score = ("BUY" if buy > sell else "SELL"), max(buy, sell)
    else:
        log.info("%s NO SETUP BUY=%d SELL=%d", name, buy, sell)
        return

    try:
        groq_result = groq_review(name, snaps, buy, sell)
    except Exception as e:
        log.error("%s Groq unavailable; no alert sent: %s", name, e)
        return

    ai_decision = str(groq_result.get("decision", "PASS")).upper()

    candle_key = snaps["5M"]["candle_time"]
    state_key = f"{name}:{ai_decision}"
    if state.get(state_key) == candle_key:
        log.info("%s duplicate %s ignored", name, ai_decision)
        return

    if ai_decision == "WATCH":
        text = (
            f"👀 {name} — WATCH {proposal}\\n"
            f"Technical score: {score}\\n"
            f"Price: {snaps['5M']['close']}\\n"
            f"12H {snaps['12H']['ema_trend']} | 4H {snaps['4H']['ema_trend']} | "
            f"1H {snaps['1H']['ema_trend']}\\n"
            f"15M {snaps['15M']['supertrend']} | 5M {snaps['5M']['supertrend']}\\n"
            f"5M RSI: {snaps['5M']['rsi']:.1f}\\n"
            f"Groq confidence: {groq_result.get('confidence', 'N/A')}%\\n"
            f"AI note: {groq_result.get('reason', '')}\\n"
            "Developing setup — not a confirmed signal."
        )
        send_telegram(text)
        state[state_key] = candle_key
        return

    if ai_decision in ("BUY", "SELL"):
        if ai_decision != proposal:
            log.info(
                "%s Groq direction %s conflicts with technical proposal %s; no alert.",
                name, ai_decision, proposal
            )
            return

        text = build_message(name, symbol, ai_decision, score, groq_result, snaps)
        send_telegram(text)
        state[state_key] = candle_key
        return

    log.info("%s Groq returned PASS; no alert.", name)



def main():
    log.info("Starting synthetic scanner")
    log.info("Public Deriv market-data API; no trading/account permissions required.")

    state = load_state()
    symbols = get_active_symbols()

    log.info("Discovered symbols: %s", symbols)

    missing = [name for name in TARGET_NAMES if name not in symbols]
    if missing:
        log.warning("Could not discover: %s", ", ".join(missing))

    for name, symbol in symbols.items():
        try:
            scan_one(name, symbol, state)
        except Exception as e:
            log.exception("%s ERROR: %s", name, e)

    save_state(state)


if __name__ == "__main__":
    main()
