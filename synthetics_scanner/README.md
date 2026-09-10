# Synthetic Signal Scanner

Scanner for Deriv synthetic indices:
- Crash 1000
- Boom 1000
- Crash 500

## Timeframes
12H -> 4H -> 1H -> 15M -> 5M

12H/4H are context, 1H is stronger bias, 15M is the primary setup, and 5M is the trigger.

## Indicators
- EMA 20/50
- Supertrend
- RSI
- ATR
- 5M price-break confirmation
- Score-based signal engine (not all timeframes must agree)

## APIs
Deriv public market data is used for prices/candles. No Deriv trading token is required.
Groq is required for every WATCH/SIGNAL decision and acts as the final analysis/filter layer.
Telegram is used for alerts.

## Railway variables
Required:
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID

Optional:
- GROQ_API_KEY
- GROQ_MODEL (optional; defaults to llama-3.3-70b-versatile)

No DERIV_API_KEY is required for the public market-data connection.

## Important
This is a signal scanner, not an auto-trader. Test signals on demo/paper conditions before risking money.
