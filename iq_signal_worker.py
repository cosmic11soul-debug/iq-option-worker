"""
IQ Option Live Signal Worker
============================
Runs on your own VPS (Railway / Fly.io / Render / any Linux box).
Connects to IQ Option, subscribes to live candles, computes RSI/EMA/MACD,
and POSTs signals to your Lovable Cloud ingest endpoint.

Setup:
  pip install iqoptionapi pandas numpy requests
  export IQ_EMAIL="your@email.com"
  export IQ_PASSWORD="yourpassword"
  export INGEST_URL="https://asmdcvkhoqxtxsoffuyu.supabase.co/functions/v1/iq-signal-ingest"
  export WORKER_TOKEN="<paste token you inserted into iq_worker_tokens>"
  python iq_signal_worker.py

Notes:
- Use a demo account first. IQ Option may flag automated logins.
- Symbols supported here: EURUSD, GBPUSD, EURJPY (edit ACTIVES to add more).
"""
import os, sys, time, traceback, requests, numpy as np, pandas as pd

print("Starting Optic Trader IQ worker build 2026-07-08-public-sync", flush=True)

try:
    from iqoptionapi.stable_api import IQ_Option
except ModuleNotFoundError as e:
    print("IQ Option API import failed.", flush=True)
    print("This deployed copy is the UPDATED worker. If you still see only a raw traceback, Railway is running an old deployment.", flush=True)
    print("Expected package: git+https://github.com/iqoptionapi/iqoptionapi.git@master#egg=iqoptionapi", flush=True)
    print(f"Python executable: {sys.executable}", flush=True)
    print(f"Python path: {sys.path}", flush=True)
    traceback.print_exc()
    while True:
        time.sleep(60)

EMAIL   = os.environ["IQ_EMAIL"]
PASSWD  = os.environ["IQ_PASSWORD"]
INGEST  = os.environ["INGEST_URL"]
TOKEN   = os.environ["WORKER_TOKEN"]

ACTIVES = ["EURUSD", "GBPUSD", "EURJPY"]
TF_SECS = 60          # 1-minute candles
LOOKBACK = 100        # candles for indicators
POLL = 5              # seconds between analysis loops

def rsi(closes, period=14):
    d = np.diff(closes)
    up = np.where(d > 0, d, 0); dn = np.where(d < 0, -d, 0)
    ru = pd.Series(up).rolling(period).mean().iloc[-1]
    rd = pd.Series(dn).rolling(period).mean().iloc[-1]
    if rd == 0: return 100.0
    rs = ru / rd
    return 100 - (100 / (1 + rs))

def ema(series, period):
    return pd.Series(series).ewm(span=period, adjust=False).mean().iloc[-1]

def macd(closes):
    s = pd.Series(closes)
    ema12 = s.ewm(span=12, adjust=False).mean()
    ema26 = s.ewm(span=26, adjust=False).mean()
    m = ema12 - ema26
    sig = m.ewm(span=9, adjust=False).mean()
    return m.iloc[-1], sig.iloc[-1]

def decide(closes):
    r = rsi(closes); e50 = ema(closes, 50); e20 = ema(closes, 20)
    m, sig = macd(closes); price = closes[-1]
    score = 0; reasons = []
    if r < 30: score += 2; reasons.append(f"RSI oversold {r:.1f}")
    elif r > 70: score -= 2; reasons.append(f"RSI overbought {r:.1f}")
    if e20 > e50: score += 1; reasons.append("EMA20>EMA50 uptrend")
    else: score -= 1; reasons.append("EMA20<EMA50 downtrend")
    if m > sig: score += 1; reasons.append("MACD bullish")
    else: score -= 1; reasons.append("MACD bearish")
    if score >= 2:  d = "CALL"
    elif score <= -2: d = "PUT"
    else: d = "WAIT"
    conf = min(100, abs(score) * 25)
    return d, conf, price, " | ".join(reasons), {"rsi": float(r), "ema20": float(e20), "ema50": float(e50), "macd": float(m), "signal": float(sig)}

def post_signal(sym, direction, conf, price, reasoning, ind):
    try:
        r = requests.post(INGEST, json={
            "symbol": sym, "direction": direction, "confidence": conf,
            "timeframe": "1m", "price": price, "reasoning": reasoning, "indicators": ind,
        }, headers={"x-worker-token": TOKEN}, timeout=10)
        print(f"[{sym}] {direction} {conf}% -> {r.status_code}")
    except Exception as e:
        print(f"POST failed: {e}")

def main():
    api = IQ_Option(EMAIL, PASSWD)
    for attempt in range(1, 6):
        try:
            ok, reason = api.connect()
            print(f"connect attempt {attempt}: ok={ok} reason={reason}")
        except Exception as e:
            ok, reason = False, str(e)
            print(f"connect attempt {attempt} exception: {e}")
        if ok and api.check_connect():
            break
        time.sleep(10)
    else:
        print("IQ Option login failed after 5 attempts — sleeping to avoid crash loop")
        while True:
            time.sleep(60)
    print("Connected to IQ Option.")

    for sym in ACTIVES:
        api.start_candles_stream(sym, TF_SECS, LOOKBACK)

    last_ts = {sym: 0 for sym in ACTIVES}
    while True:
        try:
            for sym in ACTIVES:
                candles = api.get_realtime_candles(sym, TF_SECS)
                if not candles: continue
                closes = [c["close"] for c in list(candles.values())[-LOOKBACK:]]
                if len(closes) < 30: continue
                ts = list(candles.keys())[-1]
                if ts == last_ts[sym]: continue
                last_ts[sym] = ts
                d, conf, price, reason, ind = decide(closes)
                if d != "WAIT":
                    post_signal(sym, d, conf, price, reason, ind)
        except Exception as e:
            print(f"loop err: {e}")
        time.sleep(POLL)

if __name__ == "__main__":
    main()
