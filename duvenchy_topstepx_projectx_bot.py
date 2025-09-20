import requests
import yaml
import asyncio
import logging
import urllib3
import datetime as dt
import time
import threading
from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np
from quart import Quart, request, jsonify
try:
    from signalrcore.hub_connection_builder import HubConnectionBuilder  # type: ignore
except Exception:
    HubConnectionBuilder = None  # type: ignore

# --- Suppress SSL warnings ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Load config ---
with open("config.yaml") as f:
    config = yaml.safe_load(f)

API_URL = config.get("api_base") or config.get("API_URL") or "https://api.topstepx.com"
AUTH = config.get("auth", {}) or {}
USERNAME = config.get("username") or AUTH.get("username") or AUTH.get("email") or ""
API_KEY = config.get("api_key") or AUTH.get("api_key") or ""
ACCOUNT_ID = int(config.get("account_id") or (config.get("account") or {}).get("id") or 0)
SYMBOL = (config.get("symbol") or "MNQ").upper()
MARKET_HUB = config.get("market_hub") or "https://rtc.topstepx.com/hubs/market"
LIVE_FLAG = bool((config.get("market") or {}).get("live", False))

app = Quart(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

oco_orders = {}  # entry_id: [tp_id, sl_id]
contract_map = {}  # "MNQ" → full contract metadata dict
bars_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
last_price: Dict[str, float] = {}
_bars_lock = threading.Lock()
indicator_state: Dict[str, Dict[str, Optional[float]]] = {}
DEBUG = bool(config.get("debug", False))
active_entries: Dict[Any, Dict[str, Any]] = {}
account_snapshot: Dict[str, Optional[float]] = {"balance": None, "equity": None}
streamers: Dict[str, Any] = {}

# --- Helpers ---
def snap_to_tick(price: float, tick_size: float, decimals: int) -> float:
    try:
        if tick_size and tick_size > 0:
            ticks = round(price / tick_size)
            return round(ticks * tick_size, int(decimals))
    except Exception:
        pass
    return float(round(price, 4))

def fmt_num(x: Optional[float]) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return "NA"

# Strategy/indicator params from config (Pine-like)
STRAT = (config.get("strategy") or {})
EMA_SHORT = int(STRAT.get("emaShort", 9))
# Default long EMA to 21 to match Pine 9/21 crossover
EMA_LONG = int(STRAT.get("emaLong", 21))
USE_VWAP = bool(STRAT.get("vwapEnabled", False))
ATR_LENGTH = int(STRAT.get("atrLength", 14))
RISK_PER_TRADE = float(STRAT.get("riskPerTrade", 500))
CONTRACT_SIZE_MAX = int(STRAT.get("contractSizeMax", 10))
INSTR = (config.get("instrument") or {})
RISK_PER_POINT_CONFIG = float(INSTR.get("riskPerPoint", 0) or 0)
TRAILING_STOP_ENABLED = bool(STRAT.get("trailingStopEnabled", False))
SHORT_SIZE_FACTOR = float(STRAT.get("shortSizeFactor", 0.75))
RISK_BUDGET_FRACTION = float(STRAT.get("riskBudgetFraction", 0.0))  # e.g., 0.24 of (balance - maxLoss)
TRADE_COOLDOWN_SEC = int((config.get("risk") or {}).get("trade_cooldown_sec", 10))
ORDER_SIZE = int((config.get("trade") or {}).get("order_size", 1))
PAD_TICKS = int(STRAT.get("padTicks", 0))
BREAK_EVEN_ENABLED = bool(STRAT.get("breakEvenEnabled", True))
BREAK_EVEN_LONG_FACTOR = float(STRAT.get("breakEvenLongFactor", 1.5))
BREAK_EVEN_SHORT_FACTOR = float(STRAT.get("breakEvenShortFactor", 1.0))
# Additional entry gates
REQUIRE_PRICE_ABOVE_EMAS = bool(STRAT.get("requirePriceAboveEMAs", False))
CONFIRM_BARS = int(STRAT.get("confirmBars", 0))
INTRABAR_CROSS = bool(STRAT.get("intrabarCross", False))
# Fixed targets option (points) for 1m chart
TRADE_CFG = (config.get("trade") or {})
FIXED_TP_POINTS = float(TRADE_CFG.get("tpPoints", 0) or 0)
FIXED_SL_POINTS = float(TRADE_CFG.get("slPoints", 0) or 0)
USE_FIXED_TARGETS = bool(TRADE_CFG.get("useFixedTargets", False))

# --- Auth ---
def get_token():
    try:
        res = requests.post(
            f"{API_URL}/api/Auth/loginKey",
            json={"userName": USERNAME, "apiKey": API_KEY},
            headers={"Content-Type": "application/json"},
            timeout=10,
            verify=False
        )
        res.raise_for_status()
        data = res.json()
        return data.get("token") if data.get("success") else None
    except Exception as e:
        logging.error(f"Auth error: {e}")
        return None

# --- API POST ---
def api_post(token, endpoint, payload):
    try:
        res = requests.post(
            f"{API_URL}{endpoint}",
            json=payload,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            verify=False
        )
        res.raise_for_status()
        return res.json()
    except Exception as e:
        logging.error(f"API error on {endpoint}: {e}")
        return {}

# --- Cancel Order ---
def cancel_order(token, account_id, order_id):
    try:
        res = requests.post(
            f"{API_URL}/api/Order/cancel",
            json={"accountId": account_id, "orderId": order_id},
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            verify=False
        )
        res.raise_for_status()
        return res.json().get("success", False)
    except Exception as e:
        logging.error(f"Cancel failed for {order_id}: {e}")
        return False

# --- Load Contracts ---
def _seed_contract_from_config():
    try:
        sym = (config.get("symbol") or SYMBOL or "").upper()
        cid = config.get("CONTRACT_ID") or (AUTH.get("CONTRACT_ID") if isinstance(AUTH, dict) else None)
        instr = (config.get("instrument") or {})
        tick_size = float(instr.get("tickSize") or 0.0)
        decimals = int(instr.get("decimalPlaces") or 2)
        if not sym or not cid:
            return False
        rec = {
            "contractId": cid,
            "tickValue": float(instr.get("tickValue") or 0.0),
            "tickSize": tick_size,
            "pointValue": float(instr.get("pointValue") or 0.0),
            "exchangeFee": 0.0,
            "regulatoryFee": 0.0,
            "totalFees": 0.0,
            "decimalPlaces": decimals,
            "priceScale": decimals,
        }
        contract_map[sym] = rec
        # If CONTRACT_ID follows pattern like CON.F.US.MNQ.U25, also add month key
        try:
            parts = str(cid).split(".")
            if len(parts) >= 4:
                base_sym = parts[-2].upper()
                mon = parts[-1].upper()
                contract_map[base_sym] = rec
                contract_map[f"{base_sym}.{mon}"] = rec
        except Exception:
            pass
        logging.info(f"Seeded contract from config for {sym}: {cid}")
        return True
    except Exception:
        return False

def load_contracts():
    token = get_token()
    if not token:
        logging.error("Contract preload failed: auth error")
        # Try to seed from config to allow startup
        _seed_contract_from_config()
        return

    try:
        res = requests.get(
            "https://userapi.topstepx.com/UserContract/active/nonprofesional",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0",
                "x-app-type": "px-desktop",
                "x-app-version": "1.21.1"
            },
            verify=False
        )
        res.raise_for_status()
        contracts = res.json()
        if not isinstance(contracts, list):
            logging.warning("Unexpected contract format; seeding from config if available.")
            _seed_contract_from_config()
            return

        for c in contracts:
            if c.get("disabled"):
                continue
            product_id = c.get("productId")
            if not product_id or not c.get("contractId"):
                continue
            parts = product_id.split(".")
            # Derive symbol and month from contractId (more reliable for month)
            cid = str(c.get("contractId"))
            cid_parts = cid.split(".")
            sym_from_cid = cid_parts[-2] if len(cid_parts) >= 2 else None
            mon_from_cid = cid_parts[-1] if len(cid_parts) >= 1 else None
            # Fallback symbol from productId last token (e.g., US.MNQ -> MNQ)
            sym_from_pid_last = parts[-1] if len(parts) >= 1 else None
            # Choose base symbol: prefer contractId-derived symbol
            base_sym = sym_from_cid or sym_from_pid_last
            mon = mon_from_cid if (mon_from_cid and mon_from_cid != base_sym) else None
            if not base_sym:
                continue

            rec = {
                "contractId": c["contractId"],
                "tickValue": c["tickValue"],
                "tickSize": c["tickSize"],
                "pointValue": c["pointValue"],
                "exchangeFee": c["exchangeFee"],
                "regulatoryFee": c["regulatoryFee"],
                "totalFees": c["totalFees"],
                "decimalPlaces": c["decimalPlaces"],
                "priceScale": c["priceScale"]
            }

            # Map by base symbol (e.g., MNQ)
            contract_map[base_sym.upper()] = rec
            # Also map by combined code (e.g., MNQ.U25) in case callers use it
            if base_sym and mon:
                contract_map[f"{base_sym.upper()}.{mon.upper()}"] = rec
            # Preserve original productId-based key (e.g., US.MNQ) for compatibility
            if len(parts) >= 2:
                contract_map[f"{parts[-2].upper()}.{parts[-1].upper()}"] = rec

        if not contract_map:
            _seed_contract_from_config()
        logging.info(f"Loaded {len(contract_map)} contracts")
        print("\n--- Contract Map (keys -> contractId) ---")
        for k, v in contract_map.items():
            try:
                print(f"{k}: {v['contractId']}")
            except Exception:
                pass

    except Exception as e:
        logging.error(f"UserContract load error: {e}")
        _seed_contract_from_config()


def risk_per_point(symbol: str, contract_id: Any) -> float:
    # 1) Prefer explicit config
    if RISK_PER_POINT_CONFIG and RISK_PER_POINT_CONFIG > 0:
        return float(RISK_PER_POINT_CONFIG)
    # 2) Try contract map by symbol
    c = contract_map.get(symbol)
    if not c:
        # 3) Try find by contract_id
        for v in contract_map.values():
            if v.get("contractId") == contract_id:
                c = v
                break
    try:
        if c and c.get("tickValue") and c.get("tickSize"):
            tick_value = float(c["tickValue"])      # $ per tick
            tick_size = float(c["tickSize"])        # points per tick
            if tick_size > 0:
                return tick_value / tick_size        # $ per point
    except Exception:
        pass
    # 4) Fallback: common micro futures default
    return 5.0


def get_risk_dollars() -> float:
    # If a risk budget fraction is configured, derive risk per trade from account buffer
    if RISK_BUDGET_FRACTION and RISK_BUDGET_FRACTION > 0:
        token = get_token()
        if not token:
            return RISK_PER_TRADE
        acct = get_account_info(token)
        try:
            balance = float(acct.get("balance"))
            maximum_loss = float(acct.get("maximumLoss"))
            buffer = max(0.0, balance - maximum_loss)
            return max(0.0, buffer * float(RISK_BUDGET_FRACTION))
        except Exception:
            return RISK_PER_TRADE
    return RISK_PER_TRADE


# --- Indicators ---
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # EMA of close (configurable spans; columns named ema{span})
    df[f'ema{EMA_SHORT}'] = df['close'].ewm(span=EMA_SHORT, adjust=False).mean()
    df[f'ema{EMA_LONG}'] = df['close'].ewm(span=EMA_LONG, adjust=False).mean()

    # VWAP: cumulative intraday
    df['cum_vol'] = df['volume'].cumsum()
    df['cum_pv'] = (df['close'] * df['volume']).cumsum()
    df['vwap'] = df['cum_pv'] / df['cum_vol']
    # RSI (14)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / (loss + 1e-10)
    df['rsi14'] = 100 - (100 / (1 + rs))
    # MACD (12,26,9)
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = ema12 - ema26
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = df['macd'] - df['macd_signal']
    # Momentum: pct change over last bar
    df['mom'] = df['close'].pct_change(periods=1)
    # Volatility: rolling std of returns (last 20 bars)
    df['volatility'] = df['close'].pct_change().rolling(window=20).std()
    # Time features
    df['minute_of_day'] = df.index.hour * 60 + df.index.minute
    # Drop temporary columns
    return df.drop(columns=['cum_vol','cum_pv'])

# (EMA and SessionVWAP classes removed; pandas-based indicators are used for EMAs/VWAP)

class ATR:
    def __init__(self, period: int):
        self.period = int(period)
        self.mult = 1.0 / float(self.period)
        self.value: Optional[float] = None
        self.prev_close: Optional[float] = None
    @staticmethod
    def _tr(h: float, l: float, pc: Optional[float]) -> float:
        if pc is None:
            return float(h - l)
        return max(h - l, abs(h - pc), abs(l - pc))
    def update(self, h: float, l: float, c: float) -> Optional[float]:
        tr = self._tr(h, l, self.prev_close)
        if self.value is None:
            self.value = float(tr)
        else:
            self.value = (tr - self.value) * self.mult + self.value
        self.prev_close = float(c)
        return self.value


# --- Market streaming and bar builder ---
class MarketStreamer:
    def __init__(self, symbol: str, contract_id: Any, unit: int = 2, unit_n: int = 1):
        self.symbol = symbol
        self.contract_id = contract_id
        self.unit = unit  # 2 = minute
        self.unit_n = unit_n
        self.cur_minute: Optional[dt.datetime] = None
        self.cur_bar: Optional[Dict[str, Any]] = None
        self.conn = None
        self._started: bool = False
        self._subscribed: bool = False
        # EMAs/VWAP are computed from pandas snapshot; keep ATR incremental
        self.atr = ATR(ATR_LENGTH)
        self._last_rel: Optional[float] = None
        self._last_signal_ts: float = 0.0
        # Pending confirmation state for N-bar confirmation entries
        self._pending_signal: Optional[dict] = None  # {"dir": "long"|"short", "bars_left": int}
        # Track per-minute intrabar entry to avoid duplicate fires
        self._last_intrabar_minute: Optional[dt.datetime] = None

    def debug_force_cross(self, direction: str) -> Dict[str, Any]:
        symbol = self.symbol
        st = indicator_state.get(symbol) or {}
        ef = st.get("emaFast")
        es = st.get("emaSlow")
        vwap_val = st.get("vwap")
        atr_val = st.get("atr")
        price = last_price.get(symbol)
        if any(v is None for v in (ef, es, atr_val)) or price is None:
            return {"ok": False, "error": "missing_state", "ef": ef, "es": es, "atr": atr_val, "price": price}
        try:
            ef = float(ef); es = float(es); price = float(price); atr_val = float(atr_val)
        except Exception:
            return {"ok": False, "error": "bad_state_types"}
        # Force prev_rel to opposite sign and tweak ef_local slightly to ensure sign flip
        eps = max(1e-6, abs(es) * 1e-6)
        if direction.lower() in ("up", "long", "buy"):
            self._last_rel = -eps
            ef_local = es + eps if (ef - es) <= 0 else ef
            side = 0
        else:
            self._last_rel = eps
            ef_local = es - eps if (ef - es) >= 0 else ef
            side = 1
        # Reuse gating via _maybe_trade
        self._maybe_trade(price, float(ef_local), float(es), vwap_val, atr_val)
        return {"ok": True, "dir": "long" if side == 0 else "short", "ef": ef_local, "es": es, "price": price, "atr": atr_val}

    def _pd_indicator_snapshot(self) -> Optional[Dict[str, Optional[float]]]:
        try:
            with _bars_lock:
                bars = list(bars_by_symbol.get(self.symbol, []))
            if len(bars) < 2:
                return None
            # Build DataFrame with close/volume and DateTimeIndex
            df = pd.DataFrame(bars)
            # Ensure expected columns exist; map to compute_indicators schema
            if 'c' not in df or 'v' not in df or 't' not in df:
                return None
            df = df.rename(columns={'c': 'close', 'v': 'volume', 't': 'time'})
            df['time'] = pd.to_datetime(df['time'], utc=True)
            df = df.set_index('time')
            ind = compute_indicators(df)
            if ind.shape[0] < 2:
                return None
            last = ind.iloc[-1]
            prev = ind.iloc[-2]
            ef_key = f'ema{EMA_SHORT}'
            es_key = f'ema{EMA_LONG}'
            ef = float(last.get(ef_key)) if pd.notna(last.get(ef_key)) else None
            es = float(last.get(es_key)) if pd.notna(last.get(es_key)) else None
            vw = float(last.get('vwap')) if pd.notna(last.get('vwap')) else None
            prev_ef = float(prev.get(ef_key)) if pd.notna(prev.get(ef_key)) else None
            prev_es = float(prev.get(es_key)) if pd.notna(prev.get(es_key)) else None
            prev_rel = None
            if (prev_ef is not None) and (prev_es is not None):
                prev_rel = prev_ef - prev_es
            return {"emaFast": ef, "emaSlow": es, "vwap": vw, "prev_rel": prev_rel}
        except Exception:
            return None

    def _finalize_bar(self):
        if not self.cur_bar:
            return
        with _bars_lock:
            lst = bars_by_symbol.setdefault(self.symbol, [])
            lst.append(self.cur_bar)
            bars_by_symbol[self.symbol] = lst[-3000:]
        # Update indicators on bar close (pandas-based EMA/VWAP; ATR incremental)
        try:
            c = float(self.cur_bar.get("c"))
            h = float(self.cur_bar.get("h"))
            l = float(self.cur_bar.get("l"))
            # Keep ATR incremental for stops sizing
            av = self.atr.update(h, l, c)
            # Compute EMA9/EMA20 + VWAP from pandas
            snap = self._pd_indicator_snapshot() or {}
            ef = snap.get("emaFast")
            es = snap.get("emaSlow")
            vw = snap.get("vwap")
            prev_rel = snap.get("prev_rel")
            # Optional bar-close debug to trace cross timing
            if DEBUG and (ef is not None) and (es is not None):
                try:
                    rel = float(ef) - float(es)
                    cross_up = (prev_rel is not None) and (float(prev_rel) <= 0) and (rel > 0)
                    cross_dn = (prev_rel is not None) and (float(prev_rel) >= 0) and (rel < 0)
                    logging.info(
                        f"{self.symbol} bar_close {self.cur_bar.get('t')} | prev_rel={prev_rel} rel={rel} cross_up={cross_up} cross_dn={cross_dn}"
                    )
                except Exception:
                    pass
            # Seed previous relation for crossover calc
            self._last_rel = prev_rel
            if (ef is not None) and (es is not None):
                indicator_state[self.symbol] = {"emaFast": ef, "emaSlow": es, "vwap": vw, "atr": av}
                logging.info(
                    f"{self.symbol} close={c:.2f} | EMA{EMA_SHORT}={ef:.2f} EMA{EMA_LONG}={es:.2f} VWAP={(vw if vw is not None else float('nan')):.2f}"
                )
                self._maybe_trade(c, float(ef), float(es), vw, av)
        except Exception:
            pass
        self.cur_bar = None

    def _maybe_trade(self, close_px: float, ef: float, es: float, vwap_val: Optional[float], atr_val: Optional[float]):
        now = time.time()
        # Require ATR and EMA ready
        if atr_val is None or ef is None or es is None:
            return
        # Crossover detection (EMA/VWAP only)
        rel = ef - es
        prev_rel = self._last_rel
        cross_up = (prev_rel is not None) and (prev_rel <= 0) and (rel > 0)
        cross_dn = (prev_rel is not None) and (prev_rel >= 0) and (rel < 0)
        # VWAP gating
        vwap_long_ok = True
        vwap_short_ok = True
        if USE_VWAP and vwap_val is not None:
            vwap_long_ok = close_px > vwap_val
            vwap_short_ok = close_px < vwap_val
        # Price vs EMAs gating
        ema_long_ok = (not REQUIRE_PRICE_ABOVE_EMAS) or (close_px >= ef and close_px >= es)
        ema_short_ok = (not REQUIRE_PRICE_ABOVE_EMAS) or (close_px <= ef and close_px <= es)
        # Gate by orders
        if get_open_orders_count() > 0:
            self._last_rel = rel
            return
        # Debug logging on cross detection
        if DEBUG and (cross_up or cross_dn):
            logging.info(
                "Cross detected dir=%s | cooldown_ok=%s openOrders=%d vwap_ok=%s/%s ema_ok=%s/%s confirmBars=%d",
                "UP" if cross_up else "DOWN",
                str((now - self._last_signal_ts) >= TRADE_COOLDOWN_SEC),
                get_open_orders_count(),
                str(vwap_long_ok), str(vwap_short_ok),
                str(close_px >= ef and close_px >= es),
                str(close_px <= ef and close_px <= es),
                int(CONFIRM_BARS)
            )

        # Confirmation handling
        if CONFIRM_BARS and CONFIRM_BARS > 0:
            # Start or update pending signal
            if cross_up:
                self._pending_signal = {"dir": "long", "bars_left": int(CONFIRM_BARS)}
                logging.info("Cross detected LONG; confirming over %d bars", int(CONFIRM_BARS))
            elif cross_dn:
                self._pending_signal = {"dir": "short", "bars_left": int(CONFIRM_BARS)}
                logging.info("Cross detected SHORT; confirming over %d bars", int(CONFIRM_BARS))

            # Process existing pending
            if self._pending_signal:
                dir_ = self._pending_signal.get("dir")
                # Invalidate if relation flips back
                if (dir_ == "long" and rel <= 0) or (dir_ == "short" and rel >= 0):
                    logging.info("Cross confirmation cancelled: relation flipped back")
                    self._pending_signal = None
                else:
                    self._pending_signal["bars_left"] = int(self._pending_signal["bars_left"]) - 1
                    if self._pending_signal["bars_left"] <= 0:
                        # Final gates
                        if (dir_ == "long" and vwap_long_ok and ema_long_ok) or (dir_ == "short" and vwap_short_ok and ema_short_ok):
                            if now - self._last_signal_ts >= TRADE_COOLDOWN_SEC:
                                self._last_signal_ts = now
                                if dir_ == "long":
                                    logging.info("Entering LONG after %d-bar confirmation", int(CONFIRM_BARS))
                                    self._place_market_with_brackets(side=0, op=close_px, atr_val=atr_val)
                                else:
                                    logging.info("Entering SHORT after %d-bar confirmation", int(CONFIRM_BARS))
                                    self._place_market_with_brackets(side=1, op=close_px, atr_val=atr_val)
                            else:
                                logging.info("Skip: cooldown active during confirmation entry")
                        else:
                            if DEBUG:
                                logging.info(
                                    "Confirmation gating failed | vwap_ok=%s/%s ema_ok=%s/%s",
                                    str(vwap_long_ok), str(vwap_short_ok), str(ema_long_ok), str(ema_short_ok)
                                )
                        self._pending_signal = None
        else:
            # Immediate entry on cross
            if cross_up and vwap_long_ok and ema_long_ok and (now - self._last_signal_ts >= TRADE_COOLDOWN_SEC):
                self._last_signal_ts = now
                logging.info(
                    "Placing LONG on cross | close=%.4f ef=%.4f es=%.4f prev_rel=%.5f rel=%.5f",
                    close_px, ef, es, float(prev_rel) if prev_rel is not None else float('nan'), rel
                )
                self._place_market_with_brackets(side=0, op=close_px, atr_val=atr_val)
            elif cross_dn and vwap_short_ok and ema_short_ok and (now - self._last_signal_ts >= TRADE_COOLDOWN_SEC):
                self._last_signal_ts = now
                logging.info(
                    "Placing SHORT on cross | close=%.4f ef=%.4f es=%.4f prev_rel=%.5f rel=%.5f",
                    close_px, ef, es, float(prev_rel) if prev_rel is not None else float('nan'), rel
                )
                self._place_market_with_brackets(side=1, op=close_px, atr_val=atr_val)
            else:
                if DEBUG and (cross_up or cross_dn):
                    reasons = []
                    if cross_up:
                        if not vwap_long_ok: reasons.append("vwap_long")
                        if not ema_long_ok: reasons.append("ema_long")
                    if cross_dn:
                        if not vwap_short_ok: reasons.append("vwap_short")
                        if not ema_short_ok: reasons.append("ema_short")
                    if (now - self._last_signal_ts) < TRADE_COOLDOWN_SEC:
                        reasons.append("cooldown")
                    oc = get_open_orders_count()
                    if oc > 0:
                        reasons.append(f"openOrders={oc}")
                    logging.info("Skipped entry on cross due to: %s", ", ".join(reasons) or "unknown")
        self._last_rel = rel

    def _place_market_with_brackets(self, side: int, op: float, atr_val: float):
        token = get_token()
        if not token:
            logging.error("Auto-trade auth failed")
            return
        # Contract details for tick snapping
        cm = contract_map.get(self.symbol) or {}
        tick_size = float(cm.get("tickSize") or 0.0)
        decimals = int(cm.get("decimalPlaces") or 2)
        # Compute SL/TP: either fixed points or ATR-based
        if USE_FIXED_TARGETS and FIXED_TP_POINTS > 0 and FIXED_SL_POINTS > 0:
            if side == 0:
                stop_points = float(FIXED_SL_POINTS)
                tgt_points = float(FIXED_TP_POINTS)
                sl = op - stop_points
                tp = op + tgt_points
            else:
                stop_points = float(FIXED_SL_POINTS)
                tgt_points = float(FIXED_TP_POINTS)
                sl = op + stop_points
                tp = op - tgt_points
        else:
            if side == 0:
                stop_points = float(atr_val) * 2.0
                tgt_points = stop_points * 3.0
                sl = op - stop_points
                tp = op + tgt_points
            else:
                stop_points = float(atr_val) * 1.5
                tgt_points = stop_points * 2.0
                sl = op + stop_points
                tp = op - tgt_points
        # Optional pad ticks outwards (Pine-like)
        if PAD_TICKS and tick_size and tick_size > 0:
            pad_dist = PAD_TICKS * tick_size
            if side == 0:
                sl -= pad_dist
                tp += pad_dist
            else:
                sl += pad_dist
                tp -= pad_dist
        # Snap to tick
        sl = snap_to_tick(sl, tick_size, decimals)
        tp = snap_to_tick(tp, tick_size, decimals)
        # Position sizing per Pine: size = floor(riskDollars / (stop_points * $/point))
        rp = risk_per_point(self.symbol, self.contract_id)
        risk_dollars = get_risk_dollars()
        try:
            base_contracts = int(max(0, int(risk_dollars / max(1e-6, stop_points * rp))))
        except Exception:
            base_contracts = 0
        if side == 0:
            size = min(CONTRACT_SIZE_MAX, base_contracts)
        else:
            size_long = min(CONTRACT_SIZE_MAX, base_contracts)
            size = min(int(max(0, int(size_long * SHORT_SIZE_FACTOR))), CONTRACT_SIZE_MAX)
        if size < 1:
            logging.info(
                "Skip auto-entry: computed size < 1 (atr=%.4f stop_pts=%.4f $/pt=%.4f risk=%.2f)",
                atr_val, stop_points, rp, risk_dollars
            )
            return
        logging.info(
            "Auto-entry signal side=%s op=%.2f size=%d SL=%.2f TP=%.2f (stop_pts=%.4f $/pt=%.4f)",
            "BUY" if side == 0 else "SELL", op, size, sl, tp, stop_points, rp
        )
        # Entry market
        entry = api_post(token, "/api/Order/place", {
            "accountId": ACCOUNT_ID,
            "contractId": self.contract_id,
            "type": 2,
            "side": side,
            "size": size,
            "customTag": f"ema_cross_auto_{int(time.time())}"
        })
        entry_id = entry.get("orderId")
        if not entry.get("success") or not entry_id:
            logging.error("Auto entry order failed: %s", entry)
            return
        use_trail = bool(TRAILING_STOP_ENABLED)
        # For trailing, use ATR-based trail distance similar to Pine
        trail_points = (atr_val * (1.5 if side == 0 else 1.0)) if use_trail else None
        if trail_points and tick_size and tick_size > 0:
            # distance must respect tick increment as well
            trail_points = snap_to_tick(trail_points, tick_size, decimals)
        # Place brackets from a background thread (this code runs in a non-async thread)
        threading.Thread(
            target=self._place_brackets_sync,
            args=(token, entry_id, side, size, tp, sl, use_trail, trail_points, op, stop_points),
            daemon=True,
        ).start()

    def _place_brackets_sync(self, token: str, entry_id: Any, side: int, size: int, tp: float, sl: float,
                              use_trail: bool, trail_points: Optional[float], entry_price: float, stop_points: float):
        try:
            time.sleep(0.25)
            # Try to coerce linkedOrderId to int if possible (some tenants require int)
            try:
                linked_id = int(str(entry_id))
            except Exception:
                linked_id = entry_id

            logging.info("Placing TP limit @ %.4f linked to %s", tp, entry_id)
            tp_payload = {
                "accountId": ACCOUNT_ID,
                "contractId": self.contract_id,
                "type": 1,
                "side": 1 - side,
                "size": size,
                "limitPrice": tp,
                "linkedOrderId": linked_id,
            }
            tp_order = api_post(token, "/api/Order/place", tp_payload)
            if not tp_order.get("success") or not tp_order.get("orderId"):
                logging.warning(f"TP place failed: payload={tp_payload} resp={tp_order}")
                return
            time.sleep(0.25)
            sl_order = None
            if use_trail and trail_points is not None and trail_points > 0:
                logging.info("Placing TRAIL distance=%.4f linked to %s", trail_points, entry_id)
                trail_payload = {
                    "accountId": ACCOUNT_ID,
                    "contractId": self.contract_id,
                    "type": 5,
                    "side": 1 - side,
                    "size": size,
                    "trailPrice": trail_points,
                    "linkedOrderId": linked_id,
                }
                sl_order = api_post(token, "/api/Order/place", trail_payload)
                if not sl_order.get("success") or not sl_order.get("orderId"):
                    logging.warning(f"TRAIL place failed, falling back to STOP: payload={trail_payload} resp={sl_order}")
                    sl_order = None

            if sl_order is None:
                logging.info("Placing SL stop @ %.4f linked to %s", sl, entry_id)
                sl_payload = {
                    "accountId": ACCOUNT_ID,
                    "contractId": self.contract_id,
                    "type": 4,
                    "side": 1 - side,
                    "size": size,
                    "stopPrice": sl,
                    "linkedOrderId": linked_id,
                }
                sl_order = api_post(token, "/api/Order/place", sl_payload)
                if not sl_order.get("success") or not sl_order.get("orderId"):
                    logging.warning(f"SL place failed: payload={sl_payload} resp={sl_order}")
                    # Try to cleanup TP to avoid orphan
                    try:
                        cancel_order(token, ACCOUNT_ID, tp_order.get("orderId"))
                    except Exception:
                        pass
                    return

            oco_orders[entry_id] = [tp_order.get("orderId"), sl_order.get("orderId")]
            # Track active entry for break-even monitoring
            cm = contract_map.get(self.symbol) or {}
            active_entries[entry_id] = {
                "symbol": self.symbol,
                "contractId": self.contract_id,
                "side": side,
                "size": size,
                "entry": entry_price,
                "stop_points": float(stop_points),
                "tp_id": tp_order.get("orderId"),
                "sl_id": sl_order.get("orderId"),
                "be_done": False,
                "tickSize": float(cm.get("tickSize") or 0.0),
                "decimals": int(cm.get("decimalPlaces") or 2),
            }
            logging.info(
                "Auto OCO placed: TP_id=%s SL/TRAIL_id=%s", tp_order.get("orderId"), sl_order.get("orderId")
            )
            if not tp_order.get("success") or not sl_order.get("success"):
                logging.warning("Bracket responses: TP=%s SL/TRAIL=%s", tp_order, sl_order)
        except Exception as e:
            logging.warning("Auto bracket placement failed: %s", e)

    def _ingest(self, t: dt.datetime, price: float, vol: float):
        last_price[self.symbol] = price
        try:
            bal = account_snapshot.get("balance")
            eq = account_snapshot.get("equity")
            logging.info(f"{self.symbol} price: {price:.2f} | bal={fmt_num(bal)} eq={fmt_num(eq)}")
        except Exception:
            logging.info(f"{self.symbol} price: {price:.2f}")
        minute = t.replace(second=0, microsecond=0, tzinfo=dt.timezone.utc)
        if self.cur_minute is None:
            self.cur_minute = minute
            self.cur_bar = {"t": minute.isoformat().replace("+00:00", "Z"), "o": price, "h": price, "l": price, "c": price, "v": max(0.0, vol)}
        elif minute == self.cur_minute:
            b = self.cur_bar
            if b:
                b["h"] = max(b["h"], price)
                b["l"] = min(b["l"], price)
                b["c"] = price
                b["v"] = float(b.get("v", 0.0)) + max(0.0, vol)
            # Intrabar cross detection (optional)
            if INTRABAR_CROSS:
                try:
                    # Need previous bar EMA snapshot and ATR
                    st = indicator_state.get(self.symbol) or {}
                    ef_prev = st.get("emaFast")
                    es_prev = st.get("emaSlow")
                    av = self.atr.value
                    if (ef_prev is None) or (es_prev is None) or (av is None):
                        return
                    # Previous relation from last closed bar
                    prev_rel = self._last_rel
                    if prev_rel is None:
                        return
                    # Provisional intrabar EMA update using EMA recurrence
                    alpha_f = 2.0 / float(EMA_SHORT + 1)
                    alpha_s = 2.0 / float(EMA_LONG + 1)
                    ef_tick = alpha_f * float(price) + (1.0 - alpha_f) * float(ef_prev)
                    es_tick = alpha_s * float(price) + (1.0 - alpha_s) * float(es_prev)
                    rel_tick = ef_tick - es_tick
                    cross_up = (prev_rel <= 0) and (rel_tick > 0)
                    cross_dn = (prev_rel >= 0) and (rel_tick < 0)
                    if not (cross_up or cross_dn):
                        return
                    # Avoid multiple intrabar entries in same minute
                    if self._last_intrabar_minute == self.cur_minute:
                        return
                    # VWAP gate based on last known vwap
                    vwap_val = st.get("vwap")
                    vwap_long_ok = True
                    vwap_short_ok = True
                    if USE_VWAP and vwap_val is not None:
                        vwap_long_ok = price > float(vwap_val)
                        vwap_short_ok = price < float(vwap_val)
                    # EMA price gating
                    ema_long_ok = (not REQUIRE_PRICE_ABOVE_EMAS) or (price >= ef_tick and price >= es_tick)
                    ema_short_ok = (not REQUIRE_PRICE_ABOVE_EMAS) or (price <= ef_tick and price <= es_tick)
                    # Cooldown/open orders
                    now = time.time()
                    if (now - self._last_signal_ts) < TRADE_COOLDOWN_SEC:
                        return
                    if get_open_orders_count() > 0:
                        return
                    # Fire live entry
                    if cross_up and vwap_long_ok and ema_long_ok:
                        self._last_signal_ts = now
                        self._last_intrabar_minute = self.cur_minute
                        logging.info(
                            "Placing LONG on INTRABAR cross | price=%.4f ef=%.4f es=%.4f prev_rel=%.5f rel_tick=%.5f",
                            price, ef_tick, es_tick, float(prev_rel), rel_tick
                        )
                        self._place_market_with_brackets(side=0, op=float(price), atr_val=float(av))
                    elif cross_dn and vwap_short_ok and ema_short_ok:
                        self._last_signal_ts = now
                        self._last_intrabar_minute = self.cur_minute
                        logging.info(
                            "Placing SHORT on INTRABAR cross | price=%.4f ef=%.4f es=%.4f prev_rel=%.5f rel_tick=%.5f",
                            price, ef_tick, es_tick, float(prev_rel), rel_tick
                        )
                        self._place_market_with_brackets(side=1, op=float(price), atr_val=float(av))
                except Exception:
                    pass
        else:
            self._finalize_bar()
            self.cur_minute = minute
            self.cur_bar = {"t": minute.isoformat().replace("+00:00", "Z"), "o": price, "h": price, "l": price, "c": price, "v": max(0.0, vol)}

    def _on_quote(self, args):
        try:
            if not isinstance(args, list) or len(args) < 2:
                return
            _cid, data = str(args[0]), args[1]
            price = float(data.get("lastPrice") or data.get("bestBid") or data.get("bestAsk") or 0)
            ts = data.get("timestamp") or data.get("lastUpdated")
            if not price or not ts:
                return
            t = dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(dt.timezone.utc)
            self._ingest(t, price, 0.0)
        except Exception:
            pass

    def _on_trade(self, args):
        try:
            if not isinstance(args, list) or len(args) < 2:
                return
            _cid, data = str(args[0]), args[1]
            price = float(data.get("price") or 0)
            vol = float(data.get("volume") or 0)
            ts = data.get("timestamp")
            if not price or not ts:
                return
            t = dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(dt.timezone.utc)
            self._ingest(t, price, vol)
        except Exception:
            pass

    def start(self, token: str):
        if self._started:
            logging.info("Market stream already started; skipping")
            return
        if HubConnectionBuilder is None:
            logging.error("signalrcore not installed. pip install signalrcore")
            return
        base = MARKET_HUB
        qurl = f"{base}{'&' if '?' in base else '?'}access_token={token}"
        def build(url, headers):
            return (HubConnectionBuilder()
                    .with_url(url, options={"verify_ssl": True, "headers": headers})
                    .with_automatic_reconnect({"type": "raw", "keep_alive_interval": 10, "reconnect_interval": 5, "max_attempts": 0})
                    .build())
        def attach_lifecycle(c):
            try:
                c.on_open(lambda: self._on_open())
                c.on_close(lambda: logging.info("Market hub closed"))
                c.on_error(lambda data: logging.error(f"Market hub error: {data}"))
                # Optional reconnection hooks if available in this signalrcore version
                if hasattr(c, "on_reconnecting"):
                    c.on_reconnecting(lambda: logging.info("Market hub reconnecting"))
                if hasattr(c, "on_reconnected"):
                    c.on_reconnected(lambda: self._on_open(reconnect=True))
            except Exception:
                pass

        try:
            self.conn = build(qurl, {})
            attach_lifecycle(self.conn)
            self.conn.on("GatewayQuote", self._on_quote)
            self.conn.on("GatewayTrade", self._on_trade)
            self.conn.start()
            self._started = True
        except Exception as e:
            logging.warning(f"Market hub start failed ({e}); retrying with header token")
            self.conn = build(base, {"Authorization": f"Bearer {token}"})
            attach_lifecycle(self.conn)
            self.conn.on("GatewayQuote", self._on_quote)
            self.conn.on("GatewayTrade", self._on_trade)
            self.conn.start()
            self._started = True

    def _on_open(self, reconnect: bool = False):
        logging.info("Market hub connected%s", " (reconnected)" if reconnect else "")
        if self.conn and not self._subscribed:
            try:
                self.conn.send("SubscribeContractQuotes", [self.contract_id])
                self.conn.send("SubscribeContractTrades", [self.contract_id])
                self._subscribed = True
                logging.info(f"Subscribed market stream for {self.symbol} / {self.contract_id}")
            except Exception as e:
                logging.error(f"Subscribe failed: {e}")


def iso_utc_z(ts: dt.datetime) -> str:
    ts_utc = ts.astimezone(dt.timezone.utc).replace(microsecond=0, tzinfo=None)
    return ts_utc.isoformat() + "Z"

def seed_streamer_from_warmup(ms: 'MarketStreamer') -> None:
    try:
        sym = ms.symbol
        with _bars_lock:
            last_bar = (bars_by_symbol.get(sym) or [])[-1] if bars_by_symbol.get(sym) else None
        # Seed ATR prev_close from last bar; EMAs/VWAP computed from pandas during finalize
        state = indicator_state.get(sym) or {}
        av = state.get("atr")
        if av is not None:
            ms.atr.value = float(av)
        if last_bar and last_bar.get("c") is not None:
            try:
                ms.atr.prev_close = float(last_bar.get("c"))
            except Exception:
                pass
        # Optionally compute previous relation from pandas snapshot for initial crossover state
        try:
            snap = ms._pd_indicator_snapshot() or {}
            ms._last_rel = snap.get("prev_rel")
        except Exception:
            pass
        logging.info(
            "Seeded indicators for %s | atr=%s",
            sym,
            str(ms.atr.value)
        )
    except Exception as e:
        logging.warning(f"Warmup seeding skipped due to error: {e}")

def warmup_bars(symbol: str, contract_id: Any, days: int = 1, unit: int = 2, unit_n: int = 1, live: bool = False):
    token = get_token()
    if not token:
        logging.warning("Warmup failed: no token")
        return
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(days=max(1, days))
    payload = {
        "contractId": contract_id,
        "live": bool(live),
        "startTime": iso_utc_z(start),
        "endTime": iso_utc_z(end),
        "unit": unit,
        "unitNumber": unit_n,
        "limit": 20000,
        "includePartialBar": True,
    }
    j = api_post(token, "/api/History/retrieveBars", payload)
    items = j.get("bars", j.get("candles", []))
    loaded = 0
    atr = ATR(ATR_LENGTH)
    with _bars_lock:
        lst = bars_by_symbol.setdefault(symbol, [])
        for b in items or []:
            try:
                tval = b.get("t") or b.get("time") or b.get("timestamp")
                if isinstance(tval, (int, float)):
                    ts = float(tval)
                    if ts > 1e12:
                        ts = ts / 1000.0
                    t = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
                else:
                    t = dt.datetime.fromisoformat(str(tval).replace("Z", "+00:00")).astimezone(dt.timezone.utc)
                o = float(b.get("o") or b.get("open"))
                h = float(b.get("h") or b.get("high"))
                l = float(b.get("l") or b.get("low"))
                c = float(b.get("c") or b.get("close"))
                v = float(b.get("v") or b.get("volume") or 0)
                lst.append({"t": t.isoformat().replace("+00:00", "Z"), "o": o, "h": h, "l": l, "c": c, "v": v})
                # Seed ATR from history
                atr.update(h, l, c)
                loaded += 1
            except Exception:
                continue
        bars_by_symbol[symbol] = lst[-3000:]
    # Compute EMA/VWAP from pandas on the warmed up bars
    try:
        df = pd.DataFrame(bars_by_symbol.get(symbol, []))
        if not df.empty:
            df = df.rename(columns={'c': 'close', 'v': 'volume', 't': 'time'})
            df['time'] = pd.to_datetime(df['time'], utc=True)
            df = df.set_index('time')
            ind = compute_indicators(df)
            last = ind.iloc[-1] if not ind.empty else None
            ef_key = f'ema{EMA_SHORT}'
            es_key = f'ema{EMA_LONG}'
            ef = float(last[ef_key]) if last is not None and pd.notna(last.get(ef_key)) else None
            es = float(last[es_key]) if last is not None and pd.notna(last.get(es_key)) else None
            vw = float(last['vwap']) if last is not None and pd.notna(last.get('vwap')) else None
        else:
            ef = es = vw = None
    except Exception:
        ef = es = vw = None
    indicator_state[symbol] = {"emaFast": ef, "emaSlow": es, "vwap": vw, "atr": atr.value}
    logging.info(f"Warmup loaded {loaded} bars for {symbol} | EMA{EMA_SHORT}={ef} EMA{EMA_LONG}={es} VWAP={vw}")

# --- Monitor OCO Orders ---
async def monitor_oco_orders():
    while True:
        if not oco_orders:
            await asyncio.sleep(0.3)
            continue

        token = get_token()
        if not token:
            await asyncio.sleep(0.3)
            continue

        response = api_post(token, "/api/Order/searchOpen", {"accountId": ACCOUNT_ID})
        orders = response.get("orders", [])
        # Some tenants return "orderId" instead of "id" — include both
        active_ids = set()
        if isinstance(orders, list):
            for o in orders:
                oid = o.get("id") or o.get("orderId")
                if oid is not None:
                    active_ids.add(oid)

        for entry_id, linked_ids in list(oco_orders.items()):
            if not entry_id or not all(linked_ids):
                continue

            tp_id, sl_id = linked_ids
            tp_missing = tp_id not in active_ids
            sl_missing = sl_id not in active_ids

            # If either SL or TP is triggered, cancel the other
            if tp_missing or sl_missing:
                remaining_id = sl_id if tp_missing else tp_id
                if remaining_id in active_ids:
                    success = cancel_order(token, ACCOUNT_ID, remaining_id)
                    if success:
                        logging.info(f"Canceled remaining OCO leg: {remaining_id}")
                    else:
                        logging.warning(f"Failed to cancel remaining leg: {remaining_id}")
                else:
                    logging.info(f"Remaining leg already inactive: {remaining_id}")

                # Remove the OCO group from tracking
                del oco_orders[entry_id]
                # Also drop from active_entries if present
                if entry_id in active_entries:
                    del active_entries[entry_id]

        await asyncio.sleep(0.3)

async def monitor_account_snapshot():
    while True:
        try:
            token = get_token()
            if not token:
                await asyncio.sleep(2.0)
                continue
            acct = get_account_info(token) or {}
            # Balance
            bal = None
            for k in ("balance", "cash", "cashBalance", "balanceValue"):
                v = acct.get(k)
                if v is not None:
                    try:
                        bal = float(v)
                        break
                    except Exception:
                        pass
            # Equity
            eq = None
            for k in ("equity", "netLiq", "netLiquidation", "accountEquity", "balance"):
                v = acct.get(k)
                if v is not None:
                    try:
                        eq = float(v)
                        break
                    except Exception:
                        pass
            account_snapshot["balance"] = bal
            account_snapshot["equity"] = eq
        except Exception:
            pass
        await asyncio.sleep(5.0)

def get_account_info(token):
    try:
        res = requests.get(
            "https://userapi.topstepx.com/TradingAccount",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0",
                "x-app-type": "px-desktop",
                "x-app-version": "1.21.1"
            },
            verify=False
        )
        res.raise_for_status()
        accounts = res.json()
        if not isinstance(accounts, list) or not accounts:
            logging.warning("No account data found.")
            return None

        return accounts[0]  # Return the first account
    except Exception as e:
        logging.error(f"Account info fetch error: {e}")
        return None

# --- Place OCO ---
async def place_oco_generic(data, entry_type):
    quantity = int(data.get("quantity", 1))
    op = data.get("op")
    tp = data.get("tp")
    sl = data.get("sl")
    symbol = data.get("symbol", "").upper()
    custom_tag = data.get("customTag")

    contract = contract_map.get(symbol)
    if not contract:
        return jsonify({"error": f"Unknown symbol: {symbol}"}), 400

    tick_size = contract["tickSize"]
    tick_value = contract["tickValue"]
    contract_id = contract["contractId"]
    decimals = int(contract.get("decimalPlaces", 2))

    token = get_token()
    if not token:
        return jsonify({"error": "Authentication failed"}), 500

    account_info = get_account_info(token)
    if not account_info:
        return jsonify({"error": "Failed to fetch account data"}), 500

    balance = account_info.get("balance")
    maximum_loss = account_info.get("maximumLoss")
    if balance is None or maximum_loss is None:
        return jsonify({"error": "Missing account data"}), 500

    # Snap input prices to tick grid
    try:
        op = snap_to_tick(float(op), float(tick_size), decimals)
        tp = snap_to_tick(float(tp), float(tick_size), decimals)
        sl = snap_to_tick(float(sl), float(tick_size), decimals)
    except Exception:
        pass

    sl_ticks = abs(op - sl) / tick_size
    if sl_ticks == 0:
        return jsonify({"error": "SL too close to OP"}), 400

    risk_budget = (balance - maximum_loss) * 0.24
    quantity = int(risk_budget / (sl_ticks * tick_value))
    print(risk_budget)
    if quantity <= 0:
        return jsonify({"error": "Calculated quantity is zero"}), 400

    # Micro to standard override
    micro_to_standard = {
        "MNQ": "NQ",
        "MYM": "YM",
        "MGC": "GC",
        "MES": "ES"
    }

    if quantity >= 10 and symbol in micro_to_standard:
        symbol = micro_to_standard[symbol]
        contract = contract_map.get(symbol)
        if not contract:
            return jsonify({"error": f"Standard symbol not found: {symbol}"}), 400
        contract_id = contract["contractId"]
        tick_size = contract["tickSize"]
        tick_value = contract["tickValue"]
        quantity = int(risk_budget / (sl_ticks * tick_value))

    if quantity > 3: quantity = 3 #Maxium 3 contracts

    side = 0 if op < tp else 1
    size = abs(quantity)
    # return jsonify({
    #     "contract": contract_id,
    #     "side": side,
    #     "size": size,
    #     "balance": balance,
    #     "maximum_loss": maximum_loss,
    #     "risk_budget": risk_budget,
    #     "message": "OCO placed"
    # })
    entry = api_post(token, "/api/Order/place", {
        "accountId": ACCOUNT_ID,
        "contractId": contract_id,
        "type": entry_type,
        "side": side,
        "size": size,
        "limitPrice": op if entry_type == 1 else None,
        "stopPrice": op if entry_type == 4 else None
    })
    entry_id = entry.get("orderId")
    if not entry.get("success") or not entry_id:
        return jsonify({"error": "Entry order failed"}), 500
    await asyncio.sleep(0.3)
    tp_order = api_post(token, "/api/Order/place", {
        "accountId": ACCOUNT_ID,
        "contractId": contract_id,
        "type": 1,
        "side": 1 - side,
        "size": size,
        "limitPrice": tp,
        "linkedOrderId": entry_id
    })
    await asyncio.sleep(0.3)
    sl_order = api_post(token, "/api/Order/place", {
        "accountId": ACCOUNT_ID,
        "contractId": contract_id,
        "type": 4,
        "side": 1 - side,
        "size": size,
        "stopPrice": sl,
        "linkedOrderId": entry_id
    })
    print(sl_order)

    oco_orders[entry_id] = [tp_order.get("orderId"), sl_order.get("orderId")]

    return jsonify({
        "entryOrderId": entry_id,
        "takeProfitOrderId": tp_order.get("orderId"),
        "stopLossOrderId": sl_order.get("orderId"),
        "contractId": contract_id,
        "tickSize": contract["tickSize"],
        "tickValue": contract["tickValue"],
        "balance": balance,
        "maximum_loss": maximum_loss,
        "risk_budget": risk_budget,
        "message": "OCO placed"
    })

@app.route("/place-oco", methods=["POST"])
async def place_oco():
    data = await request.get_json()
    return await place_oco_generic(data, entry_type=1)

@app.route("/place-oco-stop", methods=["POST"])
async def place_oco_stop():
    data = await request.get_json()
    return await place_oco_generic(data, entry_type=4)

@app.route("/balance", methods=["GET"])
async def balance():
    token = get_token()
    if not token:
        return jsonify({"error": "Authentication failed"}), 500

    account_info = get_account_info(token)
    if not account_info:
        return jsonify({"error": "Failed to fetch account data"}), 500

    balance = account_info.get("balance")
    maximum_loss = account_info.get("maximumLoss")

    return jsonify({
        "balance": balance,
        "maximumLoss": maximum_loss
    })

@app.get("/bars")
async def get_bars():
    symbol = (request.args.get("symbol") or SYMBOL).upper()
    limit = int(request.args.get("limit", 200))
    with _bars_lock:
        out = list(bars_by_symbol.get(symbol, []))[-limit:]
    return jsonify({"symbol": symbol, "bars": out})

@app.get("/price")
async def get_price():
    symbol = (request.args.get("symbol") or SYMBOL).upper()
    px = last_price.get(symbol)
    return jsonify({"symbol": symbol, "price": px})

@app.get("/indicators")
async def get_indicators():
    symbol = (request.args.get("symbol") or SYMBOL).upper()
    state = indicator_state.get(symbol) or {}
    return jsonify({"symbol": symbol, **state})

def _resolve_contract_for_symbol(sym: str) -> Optional[Dict[str, Any]]:
    s = sym.upper()
    return (contract_map.get(s)
            or contract_map.get(f"US.{s}")
            or next((v for k, v in contract_map.items() if k.upper().startswith(f"{s}.") or k.upper().endswith(f".{s}")), None))

@app.post("/test/force-entry")
async def test_force_entry():
    try:
        data = await request.get_json()
    except Exception:
        data = {}
    symbol = (data.get("symbol") or SYMBOL).upper()
    side_s = (data.get("side") or data.get("direction") or "").lower()
    side = 0 if side_s in ("long", "buy", "up") else 1 if side_s in ("short", "sell", "down") else None
    if side is None:
        return jsonify({"error": "Provide side as 'long' or 'short'"}), 400
    contract = _resolve_contract_for_symbol(symbol)
    if not contract:
        return jsonify({"error": f"No contract for {symbol}"}), 400
    contract_id = contract.get("contractId")
    tick_size = float(contract.get("tickSize") or 0.0)
    decimals = int(contract.get("decimalPlaces") or 2)
    # Price
    op = last_price.get(symbol)
    if op is None:
        # fallback to last bar close
        with _bars_lock:
            bars = list(bars_by_symbol.get(symbol, []))
        if bars:
            try:
                op = float(bars[-1].get("c"))
            except Exception:
                op = None
    if op is None:
        return jsonify({"error": "No last price available"}), 400
    # ATR value
    atr_val = None
    st = indicator_state.get(symbol) or {}
    av = st.get("atr")
    if av is not None:
        try:
            atr_val = float(av)
        except Exception:
            atr_val = None
    if atr_val is None:
        # derive a quick ATR from last ~20 bars
        with _bars_lock:
            bars = list(bars_by_symbol.get(symbol, []))[-50:]
        if bars:
            a = ATR(ATR_LENGTH)
            for b in bars:
                try:
                    a.update(float(b["h"]), float(b["l"]), float(b["c"]))
                except Exception:
                    continue
            atr_val = a.value or 1.0
        else:
            atr_val = 1.0
    # Compute SL/TP (reuse strategy settings)
    if USE_FIXED_TARGETS and FIXED_TP_POINTS > 0 and FIXED_SL_POINTS > 0:
        if side == 0:
            stop_points = float(FIXED_SL_POINTS)
            tgt_points = float(FIXED_TP_POINTS)
            sl = op - stop_points
            tp = op + tgt_points
        else:
            stop_points = float(FIXED_SL_POINTS)
            tgt_points = float(FIXED_TP_POINTS)
            sl = op + stop_points
            tp = op - tgt_points
    else:
        if side == 0:
            stop_points = float(atr_val) * 2.0
            tgt_points = stop_points * 3.0
            sl = op - stop_points
            tp = op + tgt_points
        else:
            stop_points = float(atr_val) * 1.5
            tgt_points = stop_points * 2.0
            sl = op + stop_points
            tp = op - tgt_points
    # Snap
    if tick_size and tick_size > 0:
        pad_dist = PAD_TICKS * tick_size if PAD_TICKS else 0.0
        if side == 0:
            sl = sl - pad_dist
            tp = tp + pad_dist
        else:
            sl = sl + pad_dist
            tp = tp - pad_dist
    sl = snap_to_tick(sl, tick_size, decimals)
    tp = snap_to_tick(tp, tick_size, decimals)
    # Sizing
    rp = risk_per_point(symbol, contract_id)
    risk_dollars = get_risk_dollars()
    try:
        base_contracts = int(max(0, int(risk_dollars / max(1e-6, stop_points * rp))))
    except Exception:
        base_contracts = 0
    if side == 0:
        size = min(CONTRACT_SIZE_MAX, base_contracts)
    else:
        size_long = min(CONTRACT_SIZE_MAX, base_contracts)
        factored = int(size_long * SHORT_SIZE_FACTOR)
        if size_long >= 1 and factored < 1 and SHORT_SIZE_FACTOR > 0:
            factored = 1
        size = min(max(0, factored), CONTRACT_SIZE_MAX)
    if size < 1:
        return jsonify({
            "error": "size_lt_1",
            "detail": {
                "atr": atr_val,
                "stop_points": stop_points,
                "risk_per_point": rp,
                "risk_dollars": risk_dollars
            }
        }), 400
    # Place orders
    token = get_token()
    if not token:
        return jsonify({"error": "auth_failed"}), 500
    entry = api_post(token, "/api/Order/place", {
        "accountId": ACCOUNT_ID,
        "contractId": contract_id,
        "type": 2,
        "side": side,
        "size": size,
        "customTag": f"force_test_{int(time.time())}"
    })
    entry_id = entry.get("orderId")
    if not entry.get("success") or not entry_id:
        return jsonify({"error": "entry_failed", "resp": entry}), 500
    await asyncio.sleep(0.25)
    tp_order = api_post(token, "/api/Order/place", {
        "accountId": ACCOUNT_ID,
        "contractId": contract_id,
        "type": 1,
        "side": 1 - side,
        "size": size,
        "limitPrice": tp,
        "linkedOrderId": entry_id,
    })
    await asyncio.sleep(0.25)
    sl_order = api_post(token, "/api/Order/place", {
        "accountId": ACCOUNT_ID,
        "contractId": contract_id,
        "type": 4,
        "side": 1 - side,
        "size": size,
        "stopPrice": sl,
        "linkedOrderId": entry_id,
    })
    oco_orders[entry_id] = [tp_order.get("orderId"), sl_order.get("orderId")]
    return jsonify({
        "symbol": symbol,
        "entryOrderId": entry_id,
        "tpOrderId": tp_order.get("orderId"),
        "slOrderId": sl_order.get("orderId"),
        "side": "BUY" if side == 0 else "SELL",
        "size": size,
        "entry": op,
        "tp": tp,
        "sl": sl,
        "atr": atr_val,
        "stop_points": stop_points
    })

@app.post("/test/force-cross")
async def test_force_cross():
    try:
        data = await request.get_json()
    except Exception:
        data = {}
    symbol = (data.get("symbol") or SYMBOL).upper()
    direction = (data.get("direction") or data.get("side") or "").lower()
    if direction not in ("up", "down", "long", "short", "buy", "sell"):
        return jsonify({"error": "Provide direction as 'up'/'down' or side as 'long'/'short'"}), 400
    ms = streamers.get(symbol)
    if not ms:
        return jsonify({"error": f"No streamer for {symbol}"}), 400
    res = ms.debug_force_cross(direction)
    return jsonify({"symbol": symbol, **res})

@app.get("/signals")
async def get_signals():
    symbol = (request.args.get("symbol") or SYMBOL).upper()
    limit = int(request.args.get("limit", 200))
    with _bars_lock:
        bars = list(bars_by_symbol.get(symbol, []))
    if not bars:
        return jsonify({"symbol": symbol, "signals": []})
    try:
        df = pd.DataFrame(bars)
        df = df.rename(columns={'c': 'close', 'v': 'volume', 't': 'time'})
        df['time'] = pd.to_datetime(df['time'], utc=True)
        df = df.set_index('time')
        ind = compute_indicators(df)
        ef_key = f'ema{EMA_SHORT}'
        es_key = f'ema{EMA_LONG}'
        out = []
        prev_rel = None
        for idx, row in ind.iterrows():
            try:
                ef = float(row.get(ef_key)) if pd.notna(row.get(ef_key)) else None
                es = float(row.get(es_key)) if pd.notna(row.get(es_key)) else None
                cl = float(row.get('close')) if pd.notna(row.get('close')) else None
                if ef is None or es is None or cl is None:
                    out.append({"time": idx.isoformat(), "close": cl, "emaShort": ef, "emaLong": es})
                    continue
                rel = ef - es
                cross_up = (prev_rel is not None) and (prev_rel <= 0) and (rel > 0)
                cross_dn = (prev_rel is not None) and (prev_rel >= 0) and (rel < 0)
                out.append({
                    "time": idx.isoformat(),
                    "close": cl,
                    "emaShort": ef,
                    "emaLong": es,
                    "rel": rel,
                    "crossUp": bool(cross_up),
                    "crossDown": bool(cross_dn),
                })
                prev_rel = rel
            except Exception:
                continue
        return jsonify({"symbol": symbol, "signals": out[-limit:]})
    except Exception as e:
        return jsonify({"symbol": symbol, "signals": [], "error": str(e)})


# --- Break-even monitor ---
async def monitor_break_even():
    while True:
        try:
            if not BREAK_EVEN_ENABLED or not active_entries:
                await asyncio.sleep(0.3)
                continue
            token = get_token()
            if not token:
                await asyncio.sleep(0.3)
                continue
            # iterate over a copy to allow modification
            for entry_id, info in list(active_entries.items()):
                if info.get("be_done"):
                    continue
                symbol = info.get("symbol")
                side = int(info.get("side", 0))
                entry_px = float(info.get("entry"))
                stop_pts = float(info.get("stop_points") or 0.0)
                sl_id = info.get("sl_id")
                if sl_id is None:
                    continue
                lp = last_price.get(symbol)
                if lp is None:
                    continue
                # Compute BE trigger
                if side == 0:
                    trigger = entry_px + stop_pts * float(BREAK_EVEN_LONG_FACTOR)
                    triggered = lp >= trigger
                    new_sl = entry_px
                else:
                    trigger = entry_px - stop_pts * float(BREAK_EVEN_SHORT_FACTOR)
                    triggered = lp <= trigger
                    new_sl = entry_px
                if not triggered:
                    continue
                # Snap new SL to tick
                tick_size = float(info.get("tickSize") or 0.0)
                decimals = int(info.get("decimals") or 2)
                new_sl = snap_to_tick(new_sl, tick_size, decimals)
                # Cancel old SL and place new SL at entry
                ok = cancel_order(token, ACCOUNT_ID, sl_id)
                if not ok:
                    logging.warning(f"Break-even: failed to cancel SL {sl_id} for {entry_id}")
                    continue
                # Place replacement SL linked to parent
                resp = api_post(token, "/api/Order/place", {
                    "accountId": ACCOUNT_ID,
                    "contractId": info.get("contractId"),
                    "type": 4,
                    "side": 1 - side,
                    "size": int(info.get("size", 1)),
                    "stopPrice": new_sl,
                    "linkedOrderId": entry_id
                })
                if not resp.get("success"):
                    logging.warning(f"Break-even: replace SL failed for {entry_id}: {resp}")
                    continue
                new_sl_id = resp.get("orderId")
                info["sl_id"] = new_sl_id
                info["be_done"] = True
                active_entries[entry_id] = info
                # Update OCO map SL id to keep monitors in sync
                if entry_id in oco_orders and isinstance(oco_orders[entry_id], list) and len(oco_orders[entry_id]) == 2:
                    oco_orders[entry_id][1] = new_sl_id
                logging.info(f"Break-even: moved SL to entry for {entry_id} at {new_sl}")
        except Exception:
            pass
        await asyncio.sleep(0.3)

def get_open_orders_count() -> int:
    token = get_token()
    if not token:
        return 0
    resp = api_post(token, "/api/Order/searchOpen", {"accountId": ACCOUNT_ID})
    orders = resp.get("orders", [])
    return len(orders) if isinstance(orders, list) else 0

@app.get("/trading/status")
async def trading_status():
    cnt = get_open_orders_count()
    return jsonify({
        "accountId": ACCOUNT_ID,
        "symbol": SYMBOL,
        "openOrders": cnt,
        "active": cnt > 0
    })

@app.before_serving
async def startup():
    load_contracts()
    asyncio.create_task(monitor_oco_orders())
    asyncio.create_task(monitor_break_even())
    asyncio.create_task(monitor_account_snapshot())
    # Start market stream + warmup
    # Resolve contract robustly: try plain symbol, month-coded, and productId-like keys
    contract = (contract_map.get(SYMBOL)
                or contract_map.get(f"US.{SYMBOL}")
                or next((v for k, v in contract_map.items() if k.upper().startswith(f"{SYMBOL}.") or k.upper().endswith(f".{SYMBOL}")), None))
    if not contract:
        logging.warning(f"No contract info for {SYMBOL}; verify symbol or load_contracts() output")
        return
    contract_id = contract["contractId"]
    warmup_bars(SYMBOL, contract_id, days=1, unit=2, unit_n=1, live=LIVE_FLAG)

    def run_stream():
        tok = get_token()
        if not tok:
            logging.error("Market stream auth failed")
            return
        ms = MarketStreamer(SYMBOL, contract_id, unit=2, unit_n=1)
        seed_streamer_from_warmup(ms)
        ms.start(tok)
        streamers[SYMBOL] = ms
        while True:
            time.sleep(1)

    threading.Thread(target=run_stream, daemon=True).start()

    def run_server():
        app.run(port=5000)

if __name__ == "__main__":
    run_server()
