"""
IQ Option Live Signal Worker + Executor
========================================
Direct WebSocket build — no external IQ Option package import.

Two loops:
  1. Analysis loop: reads live candle snapshots, computes RSI/EMA/MACD,
     and posts market snapshots/signals to Lovable Cloud.
  2. Executor loop: polls Lovable Cloud for pending Gemini signals and sends
     best-effort binary-option buyV2 orders over the IQ Option WebSocket.

Env vars:
  IQ_EMAIL, IQ_PASSWORD
  INGEST_URL   -> https://<project>.supabase.co/functions/v1/iq-signal-ingest
  FETCH_URL    -> https://<project>.supabase.co/functions/v1/iq-signal-fetch
  MARK_URL     -> https://<project>.supabase.co/functions/v1/iq-signal-mark
  SNAP_URL     -> https://<project>.supabase.co/functions/v1/iq-snapshot-ingest
  WORKER_TOKEN -> matches a row in iq_worker_tokens
  IQ_ACCOUNT   -> PRACTICE (default) or REAL
"""
import json
import math
import os
import queue
import ssl
import threading
import time
import traceback
import uuid
from collections import defaultdict, deque

import numpy as np
import pandas as pd
import requests
import websocket

print("Starting Optic Trader IQ worker build 2026-07-08-direct-websocket", flush=True)

EMAIL = os.environ["IQ_EMAIL"]
PASSWD = os.environ["IQ_PASSWORD"]
INGEST = os.environ.get("INGEST_URL", "")
FETCH_URL = os.environ.get("FETCH_URL", "")
MARK_URL = os.environ.get("MARK_URL", "")
SNAP_URL = os.environ.get("SNAP_URL", "")
OUTCOME_URL = os.environ.get("OUTCOME_URL", "")
TOKEN = os.environ["WORKER_TOKEN"]
MIN_CONFIDENCE = int(os.environ.get("MIN_CONFIDENCE", "70"))
ACCOUNT = os.environ.get("IQ_ACCOUNT", "PRACTICE").upper()

ACTIVES = ["EURUSD", "GBPUSD", "EURJPY"]
TF_SECS = 60
LOOKBACK = 100
POLL = 5
EXEC_POLL = 3

# Core IQ Option active IDs used by this worker. Keep symbols exact.
ACTIVE_IDS = {
    "EURUSD": 1,
    "EURGBP": 2,
    "GBPJPY": 3,
    "EURJPY": 4,
    "GBPUSD": 5,
    "USDJPY": 6,
    # OTC weekend/off-hours variants
    "EURUSD-OTC": 76,
    "EURGBP-OTC": 77,
    "GBPUSD-OTC": 81,
    "USDJPY-OTC": 74,
    "EURJPY-OTC": 75,
    "GBPJPY-OTC": 84,
}

# Fallback chain when the primary asset is closed
OTC_FALLBACK = {
    "EURUSD": "EURUSD-OTC",
    "EURGBP": "EURGBP-OTC",
    "GBPUSD": "GBPUSD-OTC",
    "USDJPY": "USDJPY-OTC",
    "EURJPY": "EURJPY-OTC",
    "GBPJPY": "GBPJPY-OTC",
}


class IQWebSocketClient:
    """Small direct IQ Option WebSocket client for auth, candles, and buyV2."""

    def __init__(self, email, password, account_type="PRACTICE"):
        self.email = email
        self.password = password
        self.account_type = account_type
        self.session = requests.Session()
        self.ws = None
        self.ws_thread = None
        self.connected = False
        self.authorized = False
        self.stop_event = threading.Event()
        self.recv_queue = queue.Queue()
        self.lock = threading.Lock()
        self.candles = defaultdict(lambda: deque(maxlen=LOOKBACK))
        self.balance_id = None
        self.server_timestamp = int(time.time())

    def _safe_post(self, url, **kwargs):
        return self.session.post(url, timeout=20, **kwargs)

    def login(self):
        """HTTP login to obtain the ssid used by the WebSocket."""
        payload = {"identifier": self.email, "password": self.password}
        headers = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}
        endpoints = [
            "https://auth.iqoption.com/api/v1.0/login",
            "https://iqoption.com/api/login",
        ]
        last_error = None
        for url in endpoints:
            try:
                res = self._safe_post(url, json=payload, headers=headers)
                if res.status_code >= 400:
                    # Some legacy endpoints expect form data.
                    res = self._safe_post(url, data=payload, headers={"User-Agent": "Mozilla/5.0"})
                ssid = None
                try:
                    data = res.json()
                    ssid = data.get("ssid") or data.get("data", {}).get("ssid")
                except Exception:
                    data = {}
                ssid = ssid or res.cookies.get("ssid") or self.session.cookies.get("ssid")
                if ssid:
                    print(f"IQ HTTP login OK via {url.split('/api')[0]}", flush=True)
                    return ssid
                last_error = f"{url} -> {res.status_code}: {str(data)[:180] or res.text[:180]}"
            except Exception as exc:
                last_error = f"{url} -> {exc}"
        raise RuntimeError(f"IQ Option login failed: {last_error}")

    def connect(self):
        ssid = self.login()
        self.stop_event.clear()
        self.connected = False
        self.authorized = False
        self.ws = websocket.WebSocketApp(
            "wss://iqoption.com/echo/websocket",
            on_open=lambda ws: self._on_open(ws, ssid),
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self.ws_thread = threading.Thread(
            target=lambda: self.ws.run_forever(
                sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=20, ping_timeout=10
            ),
            daemon=True,
        )
        self.ws_thread.start()

        start = time.time()
        while time.time() - start < 25:
            if self.authorized:
                self.change_balance(self.account_type)
                print("IQ WebSocket authorized", flush=True)
                return True, None
            time.sleep(0.25)
        return False, "Timed out waiting for WebSocket authorization"

    def check_connect(self):
        return self.connected and self.authorized

    def _on_open(self, ws, ssid):
        self.connected = True
        self._send_raw({"name": "ssid", "msg": ssid})
        # Newer protocol also accepts authorization; harmless if ignored.
        self._send_raw({"name": "authorization", "msg": {"ssid": ssid}})

    def _on_close(self, ws, close_status_code, close_msg):
        self.connected = False
        self.authorized = False
        print(f"IQ WebSocket closed: {close_status_code} {close_msg}", flush=True)

    def _on_error(self, ws, error):
        print(f"IQ WebSocket error: {error}", flush=True)

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
        except Exception:
            return

        name = data.get("name")
        msg = data.get("msg", {})
        if name in ("profile", "profile-changed"):
            self.authorized = True
            self._pick_balance(msg)
        elif name == "timeSync":
            self.server_timestamp = int(float(msg) / 1000) if isinstance(msg, (int, float)) else int(time.time())
        elif name in ("candles", "get-candles"):
            self._handle_candles_response(data)
        elif name == "candle-generated":
            self._handle_generated_candle(msg)
        elif name in ("buyComplete", "buy-complete", "option"):
            self.recv_queue.put(data)

    def _pick_balance(self, profile):
        balances = profile.get("balances") or profile.get("balance") or []
        if isinstance(balances, dict):
            balances = list(balances.values())
        wanted = 4 if self.account_type == "PRACTICE" else 1
        for balance in balances:
            if not isinstance(balance, dict):
                continue
            if balance.get("type") == wanted or str(balance.get("type_name", "")).upper() == self.account_type:
                self.balance_id = balance.get("id")
                return
        if balances and isinstance(balances[0], dict):
            self.balance_id = balances[0].get("id")

    def _send_raw(self, payload):
        with self.lock:
            if self.ws:
                self.ws.send(json.dumps(payload))

    def send(self, name, msg=None, request_id=None):
        payload = {"name": name, "msg": msg or {}}
        if request_id:
            payload["request_id"] = request_id
        self._send_raw(payload)

    def send_message(self, inner_name, body=None, version="2.0", request_id=None):
        rid = request_id or str(uuid.uuid4())
        self.send(
            "sendMessage",
            {"name": inner_name, "version": version, "body": body or {}},
            request_id=rid,
        )
        return rid

    def subscribe(self, inner_name, routing_filters=None, version="2.0"):
        self.send(
            "subscribeMessage",
            {"name": inner_name, "version": version, "params": {"routingFilters": routing_filters or {}}},
        )

    def change_balance(self, account_type):
        # Prefer profile balance ID when present. If not present, the account is still usable for data.
        if not self.balance_id:
            print(f"Balance mode requested: {account_type}; waiting for profile balance id", flush=True)
            return
        try:
            self.session.post(
                "https://iqoption.com/api/profile/changebalance",
                data={"balance_id": self.balance_id},
                timeout=10,
            )
        except Exception as exc:
            print(f"change_balance HTTP err: {exc}", flush=True)
        print(f"Balance mode: {account_type}", flush=True)

    def start_candles_stream(self, symbol, size, maxdict):
        active_id = ACTIVE_IDS.get(symbol)
        if not active_id:
            raise ValueError(f"Unsupported IQ active: {symbol}")
        self.get_candles(symbol, size, maxdict)
        self.subscribe("candle-generated", {"active_id": active_id, "size": size})

    def get_candles(self, symbol, size, count):
        active_id = ACTIVE_IDS[symbol]
        self.send_message("get-candles", {"active_id": active_id, "size": size, "to": int(time.time()), "count": count})

    def get_realtime_candles(self, symbol, size):
        rows = list(self.candles[(symbol, size)])
        return {int(c["from"]): c for c in rows if "from" in c}

    def _handle_candles_response(self, data):
        msg = data.get("msg", {})
        candles = msg.get("candles") if isinstance(msg, dict) else None
        if not candles and isinstance(msg, list):
            candles = msg
        if not candles:
            return
        # Candle responses often omit symbol. Seed every empty symbol with history only if sizes match.
        for raw in candles:
            candle = self._normalize_candle(raw)
            if not candle:
                continue
            size = int(candle.get("size") or TF_SECS)
            active_id = candle.get("active_id")
            symbol = self._symbol_from_active(active_id) if active_id else None
            targets = [symbol] if symbol else ACTIVES
            for sym in targets:
                self._append_candle(sym, size, candle)

    def _handle_generated_candle(self, raw):
        candle = self._normalize_candle(raw)
        if not candle:
            return
        symbol = self._symbol_from_active(candle.get("active_id"))
        if symbol:
            self._append_candle(symbol, int(candle.get("size") or TF_SECS), candle)

    def _normalize_candle(self, raw):
        if not isinstance(raw, dict):
            return None
        close = raw.get("close") or raw.get("value")
        if close is None:
            return None
        start = raw.get("from") or raw.get("at") or raw.get("time") or int(time.time())
        if isinstance(start, (int, float)) and start > 10_000_000_000:
            start = int(start / 1_000_000_000)
        return {
            "from": int(start),
            "open": float(raw.get("open") or close),
            "close": float(close),
            "min": float(raw.get("min") or raw.get("low") or close),
            "max": float(raw.get("max") or raw.get("high") or close),
            "volume": float(raw.get("volume") or 0),
            "active_id": raw.get("active_id"),
            "size": int(raw.get("size") or TF_SECS),
        }

    def _append_candle(self, symbol, size, candle):
        key = (symbol, size)
        existing = self.candles[key]
        if existing and existing[-1].get("from") == candle.get("from"):
            existing[-1] = candle
        else:
            existing.append(candle)

    def _symbol_from_active(self, active_id):
        for symbol, aid in ACTIVE_IDS.items():
            if aid == active_id:
                return symbol
        return None

    def buy(self, amount, symbol, direction, duration):
        active_id = ACTIVE_IDS.get(symbol)
        if not active_id:
            return False, f"Unsupported IQ active: {symbol}"
        now = int(time.time())
        exp = now + int(duration * 60)
        exp = int(math.ceil(exp / 60.0) * 60)
        request_id = str(uuid.uuid4())
        payloads = [
            # Legacy buyV2 format used by the original Python API.
            ("buyV2", {"price": amount, "act": active_id, "exp": exp, "type": "turbo", "direction": direction, "time": now}),
            # Newer wrapped message format used by the current WebSocket protocol.
            (
                "sendMessage",
                {
                    "name": "binary-options.open-option",
                    "version": "1.0",
                    "body": {
                        "user_balance_id": self.balance_id,
                        "active_id": active_id,
                        "option_type_id": 3,
                        "direction": direction,
                        "expired": exp,
                        "refund_value": 0,
                        "price": amount,
                        "value": amount,
                    },
                },
            ),
        ]
        for name, msg in payloads:
            self.send(name, msg, request_id=request_id)
        deadline = time.time() + 8
        got_response = False
        while time.time() < deadline:
            try:
                event = self.recv_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            text = json.dumps(event)
            if request_id in text or "buy" in text.lower() or "option" in text.lower():
                got_response = True
                msg = event.get("msg", {})
                order_id = msg.get("id") or msg.get("option_id") or msg.get("order_id")
                err = msg.get("message") or msg.get("error")
                if msg.get("isSuccessful") is False or err:
                    return False, err or "IQ rejected order"
                if order_id:
                    return True, order_id
        # No confirmation received — asset likely closed
        if not got_response:
            return False, "no confirmation from IQ (asset closed?)"
        return False, "buy timed out without order id"


# ------------ indicators ------------
def rsi(closes, period=14):
    d = np.diff(closes)
    up = np.where(d > 0, d, 0)
    dn = np.where(d < 0, -d, 0)
    ru = pd.Series(up).rolling(period).mean().iloc[-1]
    rd = pd.Series(dn).rolling(period).mean().iloc[-1]
    if rd == 0:
        return 100.0
    return 100 - (100 / (1 + (ru / rd)))


def ema(series, period):
    return pd.Series(series).ewm(span=period, adjust=False).mean().iloc[-1]


def macd(closes):
    s = pd.Series(closes)
    m = s.ewm(span=12, adjust=False).mean() - s.ewm(span=26, adjust=False).mean()
    sig = m.ewm(span=9, adjust=False).mean()
    return m.iloc[-1], sig.iloc[-1]


def decide(closes):
    r = rsi(closes)
    e50 = ema(closes, 50)
    e20 = ema(closes, 20)
    e9 = ema(closes, 9)
    m, sig_line = macd(closes)
    price = closes[-1]
    prev = closes[-2] if len(closes) >= 2 else price
    # momentum: last 5 candles net change vs volatility
    recent = np.array(closes[-6:])
    momentum = (recent[-1] - recent[0]) / (np.std(recent) + 1e-9)

    score = 0
    reasons = []

    # RSI: strong signal at extremes only
    if r < 25:
        score += 3; reasons.append(f"RSI extreme oversold {r:.1f}")
    elif r < 35:
        score += 1; reasons.append(f"RSI oversold {r:.1f}")
    elif r > 75:
        score -= 3; reasons.append(f"RSI extreme overbought {r:.1f}")
    elif r > 65:
        score -= 1; reasons.append(f"RSI overbought {r:.1f}")

    # Trend alignment (both EMAs agreeing)
    if e9 > e20 > e50:
        score += 2; reasons.append("Trend up (9>20>50)")
    elif e9 < e20 < e50:
        score -= 2; reasons.append("Trend down (9<20<50)")

    # MACD confirmation
    if m > sig_line and m > 0:
        score += 2; reasons.append("MACD bullish + above 0")
    elif m > sig_line:
        score += 1; reasons.append("MACD bullish")
    elif m < sig_line and m < 0:
        score -= 2; reasons.append("MACD bearish + below 0")
    else:
        score -= 1; reasons.append("MACD bearish")

    # Momentum
    if momentum > 1.2:
        score += 1; reasons.append(f"Momentum + ({momentum:.2f})")
    elif momentum < -1.2:
        score -= 1; reasons.append(f"Momentum - ({momentum:.2f})")

    # Price crossing EMA9 (entry trigger)
    if prev < e9 <= price:
        score += 1; reasons.append("Cross above EMA9")
    elif prev > e9 >= price:
        score -= 1; reasons.append("Cross below EMA9")

    # Need score of at least 4 for a real signal (out of ~9 max)
    if score >= 4:
        d = "CALL"
    elif score <= -4:
        d = "PUT"
    else:
        d = "WAIT"
    # Map score → confidence: 4→65, 5→75, 6→82, 7→88, 8→93, 9+→97
    conf = int(min(97, 50 + abs(score) * 7)) if d != "WAIT" else int(abs(score) * 10)
    return d, conf, price, " | ".join(reasons), {
        "rsi": float(r),
        "ema9": float(e9),
        "ema20": float(e20),
        "ema50": float(e50),
        "macd": float(m),
        "signal": float(sig_line),
        "momentum": float(momentum),
        "score": int(score),
    }


# ------------ analysis loop ------------
def post_signal(sym, direction, conf, price, reasoning, ind):
    if not INGEST:
        return
    try:
        r = requests.post(
            INGEST,
            json={
                "symbol": sym,
                "direction": direction,
                "confidence": conf,
                "timeframe": "1m",
                "price": price,
                "reasoning": reasoning,
                "indicators": ind,
            },
            headers={"x-worker-token": TOKEN},
            timeout=10,
        )
        print(f"[analysis {sym}] {direction} {conf}% -> {r.status_code}", flush=True)
    except Exception as e:
        print(f"analysis POST failed: {e}", flush=True)


def post_snapshot(sym, price, ind, closes):
    if not SNAP_URL:
        return
    try:
        r = requests.post(
            SNAP_URL,
            json={
                "symbol": sym,
                "timeframe": "1m",
                "price": float(price),
                "rsi": ind["rsi"],
                "ema20": ind["ema20"],
                "ema50": ind["ema50"],
                "macd": ind["macd"],
                "macd_signal": ind["signal"],
                "recent_closes": [float(c) for c in closes[-30:]],
            },
            headers={"x-worker-token": TOKEN},
            timeout=10,
        )
        print(f"[snapshot {sym}] {r.status_code}", flush=True)
    except Exception as e:
        print(f"snapshot POST failed: {e}", flush=True)


def analysis_loop(api):
    for sym in ACTIVES:
        try:
            api.start_candles_stream(sym, TF_SECS, LOOKBACK)
            print(f"stream start {sym}: OK", flush=True)
        except Exception as e:
            print(f"stream start {sym}: {e}", flush=True)
    last_ts = {sym: 0 for sym in ACTIVES}
    refresh_counter = 0
    while True:
        try:
            refresh_counter += 1
            if refresh_counter % 12 == 0:
                for sym in ACTIVES:
                    api.get_candles(sym, TF_SECS, LOOKBACK)
            for sym in ACTIVES:
                candles = api.get_realtime_candles(sym, TF_SECS)
                if not candles:
                    continue
                closes = [c["close"] for c in list(candles.values())[-LOOKBACK:]]
                if len(closes) < 30:
                    continue
                ts = list(candles.keys())[-1]
                if ts == last_ts[sym]:
                    continue
                last_ts[sym] = ts
                d, conf, price, reason, ind = decide(closes)
                post_snapshot(sym, price, ind, closes)
                if d != "WAIT" and conf >= MIN_CONFIDENCE:
                    post_signal(sym, d, conf, price, reason, ind)
                elif d != "WAIT":
                    print(f"[skip {sym}] {d} conf={conf}% below MIN_CONFIDENCE={MIN_CONFIDENCE}", flush=True)
        except Exception as e:
            print(f"analysis loop err: {e}", flush=True)
            traceback.print_exc()
        time.sleep(POLL)


# ------------ executor loop ------------
def report(signal_id, success, result):
    if not MARK_URL:
        return
    try:
        requests.post(
            MARK_URL,
            json={"signal_id": signal_id, "success": success, "result": result},
            headers={"x-worker-token": TOKEN},
            timeout=10,
        )
    except Exception as e:
        print(f"mark POST failed: {e}", flush=True)


def post_outcome(signal_id, entry_price, close_price):
    if not OUTCOME_URL:
        return
    try:
        r = requests.post(
            OUTCOME_URL,
            json={"signal_id": signal_id, "entry_price": float(entry_price), "close_price": float(close_price)},
            headers={"x-worker-token": TOKEN},
            timeout=10,
        )
        print(f"[outcome {signal_id}] entry={entry_price} close={close_price} -> {r.status_code} {r.text[:120]}", flush=True)
    except Exception as e:
        print(f"outcome POST failed: {e}", flush=True)


def _current_price(api, sym):
    candles = api.get_realtime_candles(sym, TF_SECS)
    if not candles:
        return None
    return list(candles.values())[-1]["close"]


def _track_outcome(api, signal_id, sym, entry_price, duration_min):
    # Wait for expiration + small buffer, then read close and record outcome.
    wait_secs = duration_min * 60 + 5
    time.sleep(wait_secs)
    try:
        close = _current_price(api, sym)
        if close is None:
            print(f"[outcome {signal_id}] no close price available", flush=True)
            return
        post_outcome(signal_id, entry_price, close)
    except Exception as e:
        print(f"[outcome {signal_id}] err: {e}", flush=True)


def execute_signal(api, sig):
    sym = sig["symbol"]
    direction = sig["direction"].lower()
    amount = float(sig.get("amount") or 1)
    duration = int(sig.get("expiration_min") or 1)
    entry_price = _current_price(api, sym)
    print(f"[exec] {sym} {direction.upper()} ${amount} {duration}m entry={entry_price}", flush=True)

    # Try primary asset, then OTC fallback if it fails (asset closed)
    attempts = [sym]
    fallback = OTC_FALLBACK.get(sym)
    if fallback:
        attempts.append(fallback)

    last_err = None
    for try_sym in attempts:
        try:
            ok, result = api.buy(amount, try_sym, direction, duration)
            if ok:
                print(f"[exec] placed order {result} on {try_sym}", flush=True)
                report(sig["id"], True, {"order_id": result, "entry_price": entry_price, "asset": try_sym})
                if entry_price is not None:
                    threading.Thread(
                        target=_track_outcome,
                        args=(api, sig["id"], try_sym, entry_price, duration),
                        daemon=True,
                    ).start()
                return
            last_err = result
            print(f"[exec] {try_sym} rejected: {result}", flush=True)
        except Exception as e:
            last_err = str(e)
            print(f"[exec] {try_sym} exception: {e}", flush=True)

    report(sig["id"], False, {"error": str(last_err), "attempts": attempts})


def executor_loop(api):
    if not FETCH_URL:
        print("FETCH_URL not set; executor disabled", flush=True)
        return
    while True:
        try:
            if not api.check_connect():
                print("IQ WebSocket disconnected; reconnecting", flush=True)
                api.connect()
                for sym in ACTIVES:
                    api.start_candles_stream(sym, TF_SECS, LOOKBACK)
            r = requests.get(FETCH_URL, headers={"x-worker-token": TOKEN}, timeout=10)
            if r.status_code == 200:
                data = r.json()
                sig = data.get("signal")
                if sig:
                    execute_signal(api, sig)
        except Exception as e:
            print(f"executor loop err: {e}", flush=True)
        time.sleep(EXEC_POLL)


# ------------ main ------------
def main():
    api = IQWebSocketClient(EMAIL, PASSWD, ACCOUNT)
    for attempt in range(1, 6):
        try:
            ok, reason = api.connect()
            print(f"connect attempt {attempt}: ok={ok} reason={reason}", flush=True)
        except Exception as e:
            ok, reason = False, str(e)
            print(f"connect attempt {attempt} exception: {e}", flush=True)
            traceback.print_exc()
        if ok and api.check_connect():
            break
        time.sleep(10)
    else:
        print("IQ Option login/WebSocket failed after 5 attempts — sleeping.", flush=True)
        while True:
            time.sleep(60)

    print("Connected. Starting analysis + executor threads.", flush=True)
    threading.Thread(target=analysis_loop, args=(api,), daemon=True).start()
    executor_loop(api)


if __name__ == "__main__":
    main()
