"""
Duvenchy – TopstepX-only data ▶ (optional) ProjectX orders
Port of your Pine v6 strategy:
  - EMA(9/21) crossover with optional VWAP filter
  - 24 hourly enable toggles w/ timezone
  - ATR-based sizing + SL/TP (Long 1:3, Short 1:2)
  - Weekly loss kill switch
  - Test-fire & paper modes
  - Account balance fetch (startup + optional polling)

Run:

source .venv/bin/activate
python -u duvenchy_topstepx_projectx_bot.py


  python3 -m venv .venv && source .venv/bin/activate
  pip install requests python-dotenv websockets pandas
  # Create .env (see below), then:
  python -u duvenchy_topstepx_projectx_bot.py

.env example (TopstepX-direct):
  EXECUTION_MODE=tsx
  TSX_API_KEY=<your key>
  TSX_ACCOUNT=<your account id>
  TSX_ORDER_URL=https://<topstepx-host>/v1/orders
  TSX_ACCOUNT_INFO_URL=https://<topstepx-host>/v1/accounts/{id}
  TZ=America/New_York
  SYMBOL=MNQ
  PAPER=true
  TEST_FIRE=false
  SHOW_BALANCE_ON_START=true
  BALANCE_POLL_SECS=0

"""
from __future__ import annotations
import os, json, time, math, uuid, asyncio, dataclasses
from dataclasses import dataclass
from typing import Dict, Optional, List, Tuple
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import requests
from dotenv import load_dotenv

# =========================
# Load ENV
# =========================
load_dotenv()
# Execution mode: "tsx" (direct TopstepX) or "projectx"
EXECUTION_MODE = os.getenv("EXECUTION_MODE", "tsx").strip().lower()

# --- ProjectX vars (only used if EXECUTION_MODE=projectx) ---
PX_URL       = os.getenv("PX_URL", "").strip()
PX_TOKEN     = os.getenv("PX_TOKEN", "").strip()

# --- TopstepX vars (used if EXECUTION_MODE=tsx) ---
TSX_API_KEY           = os.getenv("TSX_API_KEY", "").strip()            # device-bound credential
TSX_ACCOUNT           = os.getenv("TSX_ACCOUNT", "").strip()            # Combine/Funded id
TSX_ORDER_URL         = os.getenv("TSX_ORDER_URL", "").strip()          # e.g., https://api.topstepx.example.com/v1/orders
TSX_ACCOUNT_INFO_URL  = os.getenv("TSX_ACCOUNT_INFO_URL", "").strip()   # e.g., https://<host>/v1/accounts/{id}

TZ_NAME  = os.getenv("TZ", "America/New_York").strip()
SYMBOL   = os.getenv("SYMBOL", "MNQ").strip()
PAPER    = os.getenv("PAPER", "false").lower() == "true"
TEST_FIRE = os.getenv("TEST_FIRE", "false").lower() == "true"
# SHOW_BALANCE_ON_START = os.getenv("SHOW_BALANCE_ON_START", "true").lower() == "true"
# BALANCE_POLL_SECS = int(os.getenv("BALANCE_POLL_SECS", "0"))  # 0 = disabled

# =========================
# Config – mirrors Pine inputs
# =========================
@dataclass
class Inputs:
    riskPerTrade: int = 500            # Max risk per trade ($)
    profitTarget: int = 3000           # (unused in Pine; kept)
    maxWeeklyLoss: int = 2000          # Weekly loss cap ($)
    emaShort: int = 9
    emaLong: int = 21
    vwapEnabled: bool = True
    contractSizeMax: int = 50          # Max micro contracts
    atrLength: int = 14
    tzChoice: str = TZ_NAME
    padTicks: int = 0                  # Not needed here; Pine alert buffer only

    # Trading hour toggles (00..23)
    hours: List[bool] = dataclasses.field(default_factory=lambda: [True]*24)

# Micro NASDAQ tick math
MIN_TICK = 0.25
DOLLARS_PER_POINT_PER_MICRO = 5.0

# =========================
# State persisted across runs
# =========================
STATE_FILE = "state.json"

def load_state() -> Dict:
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        pass
    return {
        "weekly_anchor": datetime.now(ZoneInfo(TZ_NAME)).isocalendar().week,
        "weekly_net": 0.0,
        "last_trade_time": 0.0,
        "open_position": None  # {"side":"LONG/SHORT","qty":int,"avg":float,"sl":float,"tp":float}
    }

def save_state(state: Dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

STATE = load_state()

# =========================
# Indicators
# =========================
class Ema:
    def __init__(self, length:int):
        self.k = 2 / (length + 1.0)
        self.value: Optional[float] = None
    def update(self, price: float) -> float:
        self.value = price if self.value is None else (price * self.k + self.value * (1 - self.k))
        return self.value

class Atr:
    def __init__(self, length:int):
        self.length = length
        self.trs: List[float] = []
        self.last_hlc: Optional[Tuple[float,float,float]] = None
        self.value: Optional[float] = None
    def update(self, high: float, low: float, close: float) -> float:
        prev_close = self.last_hlc[2] if self.last_hlc else close
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        self.trs.append(tr)
        if len(self.trs) > self.length:
            self.trs.pop(0)
        self.value = sum(self.trs) / len(self.trs)
        self.last_hlc = (high, low, close)
        return self.value

class VWAP:
    """Session VWAP using rolling sums. Requires volume; disable if volume unavailable."""
    def __init__(self):
        self.reset()
    def reset(self):
        self.pv_sum = 0.0
        self.v_sum = 0.0
        self.value: Optional[float] = None
    def update(self, price: float, volume: float) -> Optional[float]:
        self.pv_sum += price * volume
        self.v_sum += volume
        self.value = (self.pv_sum / self.v_sum) if self.v_sum > 0 else None
        return self.value

# =========================
# TopstepX data client (STUB)
# Replace with real TopstepX WS/SDK
# =========================
class TSXClient:
    def __init__(self, tz="America/New_York"):
        self.tz = ZoneInfo(tz)
    async def stream_bars(self, symbol: str, tf_seconds:int=60):
        """Yield fake bars for demo. Replace with TopstepX quotes/candles WS.
        Yields: {"ts": datetime, "open":, "high":, "low":, "close":, "volume":}
        """
        price = 18000.0
        while True:
            o = price
            h = o + math.fabs(math.sin(time.time()/5.0))*5
            l = o - math.fabs(math.cos(time.time()/6.0))*5
            c = o + math.sin(time.time()/7.0)*2
            v = 100 + int(abs(math.sin(time.time()/3.0))*50)
            price = c
            yield {
                "ts": datetime.now(self.tz),
                "open": round(o,2), "high": round(h,2), "low": round(l,2), "close": round(c,2), "volume": v
            }
            await asyncio.sleep(tf_seconds)

# =========================
# Execution adapters
# =========================
class ProjectX:
    def __init__(self, url:str, token:str|None):
        self.url = url
        self.token = token
    def send(self, payload:Dict) -> Tuple[bool,str]:
        headers = {"Content-Type":"application/json", "Idempotency-Key": f"px-{uuid.uuid4()}"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if PAPER:
            print("[PAPER] POST", self.url, json.dumps(payload))
            return True, "paper"
        try:
            r = requests.post(self.url, json=payload, headers=headers, timeout=7)
            ok = 200 <= r.status_code < 300
            return ok, (r.text[:400] if r.text else str(r.status_code))
        except Exception as e:
            return False, str(e)

class TSXOrderClient:
    """Minimal direct TopstepX REST execution. Auth: Bearer <TSX_API_KEY>."""
    def __init__(self, api_key:str, account_id:str, order_url:str):
        self.api_key = api_key
        self.account_id = account_id
        self.order_url = order_url
    def place_order(self, order:Dict) -> Tuple[bool,str]:
        if not self.order_url:
            return False, "Missing TSX_ORDER_URL"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
            "Idempotency-Key": f"tsx-{uuid.uuid4()}"
        }
        body = {
            "accountId": self.account_id,
            "symbol": order.get("symbol"),
            "side": order.get("side"),             # BUY/SELL
            "type": order.get("orderType", "market"),
            "qty": order.get("qty", 1),
            "bracket": order.get("bracket", {}),   # {stopLoss:{price}, takeProfit:{price}}
            "metadata": order.get("metadata", {})
        }
        if PAPER:
            print("[PAPER] TSX POST", self.order_url, json.dumps(body))
            return True, "paper"
        try:
            r = requests.post(self.order_url, headers=headers, json=body, timeout=7)
            ok = 200 <= r.status_code < 300
            return ok, (r.text[:400] if r.text else str(r.status_code))
        except Exception as e:
            return False, f"exception: {e}"

class TSXAccountClient:
    """GET account info (cash/equity/balance) from TopstepX.
    Set TSX_ACCOUNT_INFO_URL to something like https://<host>/v1/accounts/{id}
    """
    def __init__(self, api_key: str, account_id: str, info_url: str):
        self.api_key = api_key
        self.account_id = account_id
        self.info_url = info_url
    def get_info(self) -> Tuple[bool, Dict | str]:
        if not self.info_url:
            return False, "Missing TSX_ACCOUNT_INFO_URL"
        url = self.info_url.replace("{id}", self.account_id)
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }
        try:
            r = requests.get(url, headers=headers, timeout=7)
            if 200 <= r.status_code < 300:
                return True, r.json()
            return False, f"HTTP {r.status_code}: {r.text[:400]}"
        except Exception as e:
            return False, f"exception: {e}"

# =========================
# Strategy engine (Python port of Pine)
# =========================
@dataclass
class StrategyConfig:
    inputs: Inputs
    symbol: str = SYMBOL

class StrategyEngine:
    def __init__(self, cfg: StrategyConfig, tz: str):
        self.cfg = cfg
        self.tz = ZoneInfo(tz)
        i = cfg.inputs
        self.ema_fast = Ema(i.emaShort)
        self.ema_slow = Ema(i.emaLong)
        self.atr = Atr(i.atrLength)
        self.vwap = VWAP()
        self.vwap_enabled = i.vwapEnabled
        self.last_cross_state: Optional[str] = None  # "above"/"below"

    def in_hour_window(self, ts: datetime) -> bool:
        hour = ts.astimezone(self.tz).hour
        return self.cfg.inputs.hours[hour]

    def update_indicators(self, bar: Dict) -> Dict:
        f = self.ema_fast.update(bar["close"])
        s = self.ema_slow.update(bar["close"])
        a = self.atr.update(bar["high"], bar["low"], bar["close"])
        v = self.vwap.update(bar["close"], bar.get("volume",0) or 0)
        return {"ema_fast":f, "ema_slow":s, "atr":a, "vwap":v}

    def crossover(self, f: float, s: float) -> bool:
        prev = self.last_cross_state
        cur = "above" if f > s else "below" if f < s else prev
        crossed = prev == "below" and cur == "above"
        self.last_cross_state = cur
        return crossed

    def crossunder(self, f: float, s: float) -> bool:
        prev = self.last_cross_state
        cur = "above" if f > s else "below" if f < s else prev
        crossed = prev == "above" and cur == "below"
        self.last_cross_state = cur
        return crossed

    def weekly_kill_switch(self) -> bool:
        return STATE.get("weekly_net", 0.0) <= -abs(self.cfg.inputs.maxWeeklyLoss)

    def position_size(self, atr_value: float, side: str) -> int:
        i = self.cfg.inputs
        stop_pts = (atr_value * 2.0) if side == "LONG" else (atr_value * 1.5)
        if stop_pts <= 0:
            return 0
        contracts = math.floor(i.riskPerTrade / (stop_pts * DOLLARS_PER_POINT_PER_MICRO))
        if side == "SHORT":
            contracts = math.floor(min(contracts, math.floor(max(1, contracts) * 0.75)))
        return max(0, min(contracts, i.contractSizeMax))

    def build_bracket(self, ref_price: float, atr_value: float, side: str) -> Tuple[float,float]:
        if side == "LONG":
            sl = ref_price - atr_value * 2.0
            tp = ref_price + atr_value * 2.0 * 3.0  # 1:3
        else:
            sl = ref_price + atr_value * 1.5
            tp = ref_price - atr_value * 1.5 * 2.0  # 1:2
        def round_to_tick(px: float) -> float:
            return round(px / MIN_TICK) * MIN_TICK
        return round_to_tick(sl), round_to_tick(tp)

    def vwap_ok(self, price: float, vwap: Optional[float], side: str) -> bool:
        if not self.vwap_enabled:
            return True
        if vwap is None:
            return False
        return (price > vwap) if side == "LONG" else (price < vwap)

# =========================
# Orchestrator
# =========================
async def run_bot():
    tz = TZ_NAME
    inputs = Inputs()
    cfg = StrategyConfig(inputs=inputs)
    engine = StrategyEngine(cfg, tz)
    tsx = TSXClient(tz)

    # Execution wiring
    px = ProjectX(PX_URL, PX_TOKEN) if EXECUTION_MODE == "projectx" else None
    tsx_exec = TSXOrderClient(TSX_API_KEY, TSX_ACCOUNT, TSX_ORDER_URL) if EXECUTION_MODE == "tsx" else None
    tsx_account = TSXAccountClient(TSX_API_KEY, TSX_ACCOUNT, TSX_ACCOUNT_INFO_URL) if EXECUTION_MODE == "tsx" else None

    mode_label = "TopstepX-direct" if EXECUTION_MODE == "tsx" else "ProjectX"
    print(f"Duvenchy bot running… mode={mode_label} | symbol={cfg.symbol}")
    print(f"EXECUTION_MODE={EXECUTION_MODE} PAPER={PAPER} TSX_ORDER_URL={'set' if (EXECUTION_MODE=='tsx' and TSX_ORDER_URL) else 'missing'} TSX_ACCOUNT={TSX_ACCOUNT if EXECUTION_MODE=='tsx' else '-'}")

    # # Show account balance at start
    # if EXECUTION_MODE == 'tsx' and SHOW_BALANCE_ON_START:
    #     ok, data = tsx_account.get_info()
    #     if ok:
    #         bal  = data.get('balance') or data.get('cash') or data.get('equity') or 'unknown'
    #         upnl = data.get('unrealizedPnL') or data.get('upnl') or 0
    #         rpnl = data.get('realizedPnL') or data.get('rpnl') or 0
    #         print(f"[ACCOUNT] balance={bal} realizedPnL={rpnl} unrealizedPnL={upnl}")
    #     else:
    #         print(f"[ACCOUNT] failed: {data}")

    # Optional periodic balance polling
    # if EXECUTION_MODE == 'tsx' and BALANCE_POLL_SECS > 0:
    #     async def poll_balance():
    #         while True:
    #             await asyncio.sleep(BALANCE_POLL_SECS)
    #             ok, data = tsx_account.get_info()
    #             if ok:
    #                 bal  = data.get('balance') or data.get('cash') or data.get('equity') or 'unknown'
    #                 upnl = data.get('unrealizedPnL') or data.get('upnl') or 0
    #                 rpnl = data.get('realizedPnL') or data.get('rpnl') or 0
    #                 print(f"[ACCOUNT] balance={bal} realizedPnL={rpnl} unrealizedPnL={upnl}")
    #             else:
    #                 print(f"[ACCOUNT] failed: {data}")
    #     asyncio.create_task(poll_balance())

    # Fire one immediate test order (no need to wait for signals)
    if TEST_FIRE and EXECUTION_MODE == 'tsx' and tsx_exec:
        test_price = 18000.0
        sl = round(test_price - 20.0, 2)
        tp = round(test_price + 30.0, 2)
        payload = {
            "symbol": cfg.symbol,
            "side": "BUY",
            "orderType": "market",
            "qty": 1,
            "bracket": {"stopLoss": {"price": sl}, "takeProfit": {"price": tp}},
            "metadata": {"clientId": "duvenchy-bot", "strategy": "test-fire",
                         "sentAt": datetime.now(timezone.utc).isoformat().replace("+00:00","Z")}
        }
        ok, info = tsx_exec.place_order({**payload, "accountId": TSX_ACCOUNT})
        print(f"[TEST_FIRE] ok={ok} info={info}")

    # Main bar loop (replace with real TopstepX data stream)
    async for bar in tsx.stream_bars(SYMBOL, tf_seconds=60):
        ts = bar["ts"]
        # Weekly reset
        now_week = ts.isocalendar().week
        if now_week != STATE.get("weekly_anchor"):
            STATE["weekly_anchor"] = now_week
            STATE["weekly_net"] = 0.0
            save_state(STATE)
            print("[RESET] New ISO week detected – weekly PnL reset")

        ind = engine.update_indicators(bar)
        f, s, a, v = ind["ema_fast"], ind["ema_slow"], ind["atr"], ind["vwap"]
        if f is None or s is None or a is None:
            print("[WARMUP] building indicators…")
            continue

        if not engine.in_hour_window(ts):
            print("[IDLE] hour disabled")
            continue

        if engine.weekly_kill_switch():
            print(f"[LOCK] weekly loss cap reached (weekly_net={STATE.get('weekly_net')}) – ignoring")
            continue

        price = bar["close"]

        # EMA cross + VWAP filter
        long_signal  = engine.crossover(f, s)  and engine.vwap_ok(price, v, "LONG")
        short_signal = engine.crossunder(f, s) and engine.vwap_ok(price, v, "SHORT")

        side = "LONG" if long_signal else ("SHORT" if short_signal else None)
        if not side:
            print("[WAIT] no signal")
            continue

        qty = engine.position_size(a, side)
        if qty <= 0:
            print("[SKIP] qty=0 due to risk sizing / ATR")
            continue

        sl, tp = engine.build_bracket(price, a, side)
        payload = {
            "symbol": cfg.symbol,
            "side": "BUY" if side == "LONG" else "SELL",
            "orderType": "market",
            "qty": int(qty),
            "bracket": {
                "stopLoss": {"price": round(sl, 2)},
                "takeProfit": {"price": round(tp, 2)}
            },
            "metadata": {
                "clientId": "duvenchy-bot",
                "strategy": "Nasdaq100Micro-OptimizedRisk",
                "emaShort": inputs.emaShort,
                "emaLong": inputs.emaLong,
                "atrLen": inputs.atrLength,
                "sentAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            }
        }

        # Route the order
        if EXECUTION_MODE == "projectx" and px:
            ok, info = px.send(payload)
        else:  # tsx direct
            ok, info = tsx_exec.place_order({**payload, "accountId": TSX_ACCOUNT})

        if ok:
            STATE["last_trade_time"] = time.time()
            STATE["open_position"] = {"side": side, "qty": qty, "avg": price, "sl": sl, "tp": tp}
            save_state(STATE)
            print(f"[ORDER] {side} {qty} {cfg.symbol} @~{price:.2f} SL={sl:.2f} TP={tp:.2f} :: {info}")
        else:
            print(f"[FAIL] send: {info}")

# =========================
# Entrypoint
# =========================
if __name__ == "__main__":
    try:
        import sys
        sys.stdout.reconfigure(line_buffering=True)
        print("Starting Duvenchy bot…")
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        print("Graceful exit")
