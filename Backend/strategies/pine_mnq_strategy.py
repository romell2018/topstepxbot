import json
import os
import sys
import logging
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional, List

# Make Backend importable when running this file directly
BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

from topstepx_bot.utils import snap_to_tick, fmt_num
from topstepx_bot.indicators import ATR as _ATR, compute_indicators as _compute_indicators
from topstepx_bot.api import (
    get_token as _api_get_token,
    api_post as _api_post,
    cancel_order as _api_cancel_order,
    get_account_info as _api_get_account_info,
)
from topstepx_bot.market import (
    load_contracts as market_load_contracts,
    warmup_bars as market_warmup_bars,
    seed_streamer_from_warmup as market_seed_streamer_from_warmup,
)
from topstepx_bot.monitors import (
    make_monitor_oco_orders,
    make_monitor_account_snapshot,
    make_monitor_synth_trailing,
    make_get_open_orders_count,
)
from topstepx_bot.risk import (
    risk_per_point as risk_per_point_module,
    get_risk_dollars as get_risk_dollars_module,
)
from topstepx_bot.streamer import MarketStreamer


def _try_parse_config(blob: str) -> Optional[Dict[str, Any]]:
    if not blob:
        return None
    if yaml is not None:
        try:
            cfg = yaml.safe_load(blob)
            if isinstance(cfg, dict):
                return cfg
        except Exception:
            logging.exception("Failed to parse config with yaml; will try JSON")
    try:
        cfg = json.loads(blob)
        if isinstance(cfg, dict):
            return cfg
    except Exception:
        logging.exception("Failed to parse config JSON override")
    return None


def load_config() -> Dict[str, Any]:
    inline_json = os.environ.get("TOPSTEPX_CONFIG_JSON")
    cfg = _try_parse_config(inline_json) if inline_json else None
    if cfg:
        return cfg
    inline = os.environ.get("TOPSTEPX_CONFIG")
    cfg = _try_parse_config(inline) if inline else None
    if cfg:
        return cfg
    cfg_path_env = os.environ.get("TOPSTEPX_CONFIG_PATH")
    cfg_path = Path(cfg_path_env).expanduser() if cfg_path_env else (BACKEND_ROOT / "config.yaml")
    if yaml is None:
        raise RuntimeError(
            "PyYAML is required to load config.yaml from disk. Install pyyaml or provide TOPSTEPX_CONFIG_JSON."
        )
    with cfg_path.open() as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"Configuration at {cfg_path} is not a mapping")
    return data


class PineMarketStreamer(MarketStreamer):
    """
    Pine-style strategy mapping for MNQ:
    - Entry: EMA(9) cross EMA(21) with optional VWAP filter
    - Sizing: contracts = floor(riskPerTrade / (stopPoints * riskPerPoint)), capped by contractSizeMax
      where stopPoints = ATR * 2.0 for LONG, ATR * 1.5 for SHORT
    - Targets: LONG TP = entry + 3x stopPoints; SHORT TP = entry - 2x stopPoints
    - Stops: native trailing preferred
        LONG: trailPoints = ATR * 1.5, offset = ATR * 0.5
        SHORT: trailPoints = ATR * 1.0, offset = ATR * 0.5
    - Weekly loss guard: optional gate to stop new entries
    """

    def _maybe_trade(self, close_px: float, ef: float, es: float, vwap_val: Optional[float], atr_val: Optional[float]):
        # Reuse parent cross + gating logic except we insert optional hour gating and weekly-loss kill gate
        now = time.time()
        if atr_val is None or ef is None or es is None:
            return

        # Global kill-switch or weekly kill gate
        if self.ctx.get('TRADING_DISABLED'):
            return
        if bool(self.ctx.get('WEEKLY_KILL_ACTIVE')):
            return

        # Optional hour gating via config.hours.enable["HH"] booleans
        try:
            hours_cfg = (self.ctx.get('HOURS_CFG') or {}).get('enable') or {}
            if isinstance(hours_cfg, dict) and hours_cfg:
                # Determine hour in configured tz (default America/New_York)
                import datetime as dt
                from zoneinfo import ZoneInfo
                tzname = self.ctx.get('TZ') or 'America/New_York'
                now_dt = dt.datetime.now(ZoneInfo(tzname))
                hh = f"{now_dt.hour:02d}"
                ok = bool(hours_cfg.get(hh))
                if not ok:
                    self._last_rel = (ef - es)  # keep relation updated for next cross calc
                    return
        except Exception:
            pass

        rel = ef - es
        prev_rel = self._last_rel
        cross_up, cross_dn = self._calc_cross(prev_rel, rel, bool(self.ctx.get('STRICT_CROSS_ONLY')))

        # VWAP and EMA price gating (match parent behavior)
        vwap_long_ok = True
        vwap_short_ok = True
        if self.ctx['USE_VWAP'] and vwap_val is not None:
            vwap_long_ok = close_px > vwap_val
            vwap_short_ok = close_px < vwap_val
        ema_long_ok = (not self.ctx['REQUIRE_PRICE_ABOVE_EMAS']) or (close_px >= ef and close_px >= es)
        ema_short_ok = (not self.ctx['REQUIRE_PRICE_ABOVE_EMAS']) or (close_px <= ef and close_px <= es)

        # Cooldown guard
        oc = self.ctx['get_open_orders_count'](self.contract_id)
        if (time.time() - getattr(self, '_last_signal_ts', 0.0)) < self.ctx['TRADE_COOLDOWN_SEC']:
            self._last_rel = rel
            return

        # Optional N-bar confirmation (unchanged semantics from parent)
        confirm_n = int(self.ctx.get('CONFIRM_BARS', 0) or 0)
        if confirm_n > 0:
            if cross_up:
                self._pending_signal = {"dir": "long", "bars_left": confirm_n}
            elif (not self.ctx['LONG_ONLY']) and cross_dn:
                self._pending_signal = {"dir": "short", "bars_left": confirm_n}
            if self._pending_signal:
                # store last relation and return; parent will decrement at bar close
                self._last_rel = rel
                return

        did = False
        if cross_up and vwap_long_ok and ema_long_ok:
            # If reversing is enabled, flatten/cancel existing orders/position before new entry
            if bool(self.ctx.get('REVERSE_ON_CROSS', True)) and (oc > 0 or self._has_open_position()):
                self._flatten_and_cancel()
                oc = self.ctx['get_open_orders_count'](self.contract_id)
            self._last_signal_ts = now
            self._place_pine_entry_with_brackets(side=0, op=close_px, atr_val=float(atr_val))
            did = True
        elif (not self.ctx['LONG_ONLY']) and cross_dn and vwap_short_ok and ema_short_ok:
            if bool(self.ctx.get('REVERSE_ON_CROSS', True)) and (oc > 0 or self._has_open_position()):
                self._flatten_and_cancel()
                oc = self.ctx['get_open_orders_count'](self.contract_id)
            self._last_signal_ts = now
            self._place_pine_entry_with_brackets(side=1, op=close_px, atr_val=float(atr_val))
            did = True
        self._last_rel = rel
        if did:
            return

    def _has_open_position(self) -> bool:
        try:
            token = self.ctx['get_token']()
            if not token:
                return False
            resp = self.ctx['api_post'](token, "/api/Position/searchOpen", {"accountId": self.ctx['ACCOUNT_ID']})
            pos_list = resp.get("positions") if isinstance(resp, dict) else (resp if isinstance(resp, list) else [])
            for p in pos_list or []:
                try:
                    cid = p.get("contractId") or (p.get("contract") or {}).get("id")
                    if cid != self.contract_id:
                        continue
                    qty = p.get("quantity") or p.get("qty") or p.get("size")
                    if qty is None:
                        continue
                    if float(qty) != 0.0:
                        return True
                except Exception:
                    continue
        except Exception:
            return False
        return False

    def _flatten_and_cancel(self) -> None:
        try:
            token = self.ctx['get_token']()
            if not token:
                return
            # Cancel open orders for this contract
            try:
                resp = self.ctx['api_post'](token, "/api/Order/searchOpen", {"accountId": self.ctx['ACCOUNT_ID']})
                orders = resp.get("orders", []) if isinstance(resp, dict) else []
                for o in orders:
                    try:
                        cid = o.get("contractId") or (o.get("contract") or {}).get("id")
                        oid = o.get("id") or o.get("orderId")
                        if cid == self.contract_id and oid is not None:
                            self.ctx['cancel_order'](token, self.ctx['ACCOUNT_ID'], oid)
                    except Exception:
                        continue
            except Exception:
                pass
            # Close open position on this contract, if any
            try:
                presp = self.ctx['api_post'](token, "/api/Position/searchOpen", {"accountId": self.ctx['ACCOUNT_ID']})
                pos_list = presp.get("positions") if isinstance(presp, dict) else (presp if isinstance(presp, list) else [])
                for p in pos_list or []:
                    try:
                        cid = p.get("contractId") or (p.get("contract") or {}).get("id")
                        if cid != self.contract_id:
                            continue
                        qty = p.get("quantity") or p.get("qty") or p.get("size")
                        if qty is None:
                            continue
                        q = int(abs(int(float(qty))))
                        if q <= 0:
                            continue
                        # Determine side: positive = long -> SELL to flatten; negative = short -> BUY to flatten
                        side = None
                        try:
                            if float(qty) > 0:
                                side = 1  # SELL
                            elif float(qty) < 0:
                                side = 0  # BUY
                        except Exception:
                            pass
                        if side is None:
                            s = p.get("side")
                            if s in (0, 1):
                                side = 1 - int(s)
                        if side is None:
                            continue
                        self.ctx['api_post'](token, "/api/Order/place", {
                            "accountId": self.ctx['ACCOUNT_ID'],
                            "contractId": self.contract_id,
                            "type": 2,
                            "side": int(side),
                            "size": int(q),
                        })
                    except Exception:
                        continue
            except Exception:
                pass
            # Clear local tracking for this contract to avoid stale state
            try:
                for eid, info in list(self.ctx['active_entries'].items()):
                    if (info or {}).get('contractId') == self.contract_id:
                        del self.ctx['active_entries'][eid]
                for eid in list(self.ctx['oco_orders'].keys()):
                    try:
                        del self.ctx['oco_orders'][eid]
                    except Exception:
                        pass
            except Exception:
                pass
        except Exception:
            pass

    def _place_pine_entry_with_brackets(self, side: int, op: float, atr_val: float):
        token = self.ctx['get_token']()
        if not token:
            logging.error("Auth failed for placing entry")
            return
        cm = self.ctx['contract_map'].get(self.symbol) or {}
        tick_size = float(cm.get("tickSize") or 0.0)
        decimals = int(cm.get("decimalPlaces") or 2)

        # Pine stop/targets in price points
        stop_long_pts = float(atr_val) * 2.0
        stop_short_pts = float(atr_val) * 1.5
        trail_long_pts = float(atr_val) * float(self.ctx.get('ATR_TRAIL_K_LONG', 0.5) or 0.5)
        trail_short_pts = float(atr_val) * float(self.ctx.get('ATR_TRAIL_K_SHORT', 0.5) or 0.5)
        trail_offset_pts = float(atr_val) * 0.3

        # Risk-based sizing per Pine
        rp = self.ctx['risk_per_point'](self.symbol, self.contract_id)
        risk_dollars = float(self.ctx.get('RISK_PER_TRADE') or 0.0)
        if side == 0:
            stop_pts = stop_long_pts
            base_ct = int(risk_dollars // max(1e-6, stop_pts * rp))
            size = min(self.ctx['CONTRACT_SIZE_MAX'], max(0, base_ct))
        else:
            stop_pts = stop_short_pts
            base_ct_long = int(risk_dollars // max(1e-6, (float(atr_val) * 2.0) * rp))
            size_long = min(self.ctx['CONTRACT_SIZE_MAX'], max(0, base_ct_long))
            size = min(int(size_long * float(self.ctx.get('SHORT_SIZE_FACTOR', 0.75) or 0.75)), self.ctx['CONTRACT_SIZE_MAX'])
        if size < 1:
            logging.info("Skip entry: size < 1 (risk=$%.2f, rp=%.2f, stop_pts=%.4f)", risk_dollars, rp, stop_pts)
            return

        # Entry market order
        entry_payload = {
            "accountId": self.ctx['ACCOUNT_ID'],
            "contractId": self.contract_id,
            "type": 2,
            "side": side,
            "size": size,
            "customTag": f"pine_mnq_{'long' if side==0 else 'short'}_{int(time.time())}",
        }
        entry = self.ctx['api_post'](token, "/api/Order/place", entry_payload)
        if not entry.get("success") or not entry.get("orderId"):
            logging.error("Entry failed: %s", entry)
            return
        entry_id = entry.get("orderId")
        logging.info("Pine entry placed id=%s side=%s size=%d", str(entry_id), "BUY" if side==0 else "SELL", int(size))

        # Compute TP and SL prices (snap to tick)
        if side == 0:
            sl_px_fixed = snap_to_tick(op - stop_long_pts, tick_size, decimals)
            tp_px = snap_to_tick(op + stop_long_pts * 3.0, tick_size, decimals)
            t_points = trail_long_pts
        else:
            sl_px_fixed = snap_to_tick(op + stop_short_pts, tick_size, decimals)
            tp_px = snap_to_tick(op - stop_short_pts * 2.0, tick_size, decimals)
            t_points = trail_short_pts

        # Place TP first (limit)
        tp_order = self.ctx['api_post'](token, "/api/Order/place", {
            "accountId": self.ctx['ACCOUNT_ID'],
            "contractId": self.contract_id,
            "type": 1,
            "side": 1 - side,
            "size": size,
            "limitPrice": tp_px,
            "linkedOrderId": entry_id,
        })
        tp_id = tp_order.get("orderId") if tp_order.get("success") else None

        # Place an initial fixed STOP at Pine SL; the synthetic monitor will convert it
        # to a native trailing stop (type 5) after activation (offset_k * ATR) is reached.
        sl_id = None
        r = self.ctx['api_post'](token, "/api/Order/place", {
                "accountId": self.ctx['ACCOUNT_ID'],
                "contractId": self.contract_id,
                "type": 4,
                "side": 1 - side,
                "size": int(size),
                "stopPrice": sl_px_fixed,
                "linkedOrderId": entry_id,
            })
        if r.get("success"):
            sl_id = r.get("orderId")

        # Track OCO pair and active entry for trailing monitor support
        if entry_id and (tp_id or sl_id):
            try:
                self.ctx['oco_orders'][entry_id] = [tp_id, sl_id]
            except Exception:
                pass
        cm = self.ctx['contract_map'].get(self.symbol) or {}
        try:
            self.ctx['active_entries'][entry_id] = {
                "symbol": self.symbol,
                "contractId": self.contract_id,
                "side": int(side),
                "size": int(size),
                "entry": float(op),
                "stop_points": float(stop_pts),
                "tp_id": tp_id,
                "sl_id": sl_id,
                "native_trail": False,
                "tickSize": float(cm.get("tickSize") or 0.0),
                "decimals": int(cm.get("decimalPlaces") or 2),
            }
        except Exception:
            pass


def run_strategy_server():
    # Load config
    config = load_config()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    # Resolve API + account
    API_URL = config.get("api_base") or config.get("API_URL") or "https://api.topstepx.com"
    AUTH = config.get("auth", {}) or {}
    USERNAME = config.get("username") or AUTH.get("username") or AUTH.get("email") or ""
    API_KEY = config.get("api_key") or AUTH.get("api_key") or ""
    ACCOUNT_ID = int(config.get("account_id") or (config.get("account") or {}).get("id") or 0)
    SYMBOL = (config.get("symbol") or "MNQ").upper()
    MARKET_HUB = config.get("market_hub") or "https://rtc.topstepx.com/hubs/market"
    LIVE_FLAG = bool((config.get("market") or {}).get("live", True))
    INCLUDE_PARTIAL = bool((config.get("market") or {}).get("includePartialBar", True))
    BOOTSTRAP_HISTORY_HOURS = int(config.get("bootstrap_history_hours", 3) or 3)

    # Strategy params (mirror Pine defaults)
    STRAT = (config.get("strategy") or {})
    EMA_SHORT = int(STRAT.get("emaShort", 9))
    EMA_LONG = int(STRAT.get("emaLong", 21))
    EMA_SOURCE = str(STRAT.get("emaSource", "close")).strip().lower()
    USE_VWAP = bool(STRAT.get("vwapEnabled", True))
    ATR_LENGTH = int(STRAT.get("atrLength", 14))
    RISK_PER_TRADE = float(STRAT.get("riskPerTrade", 500))
    CONTRACT_SIZE_MAX = int(STRAT.get("contractSizeMax", 50))
    SHORT_SIZE_FACTOR = float(STRAT.get("shortSizeFactor", 0.75))
    TRADE_COOLDOWN_SEC = int((config.get("risk") or {}).get("trade_cooldown_sec", 10))
    LONG_ONLY = bool(STRAT.get("longOnly", False))
    RTH_ONLY = bool(STRAT.get("rthOnly", False))
    # Weekly kill switch
    WEEKLY_KILL_ENABLED = bool(STRAT.get("weeklyKillEnabled", True))
    MAX_WEEKLY_LOSS = float(STRAT.get("maxWeeklyLoss", 2000))

    # Shared state
    oco_orders: Dict[Any, List[Any]] = {}
    contract_map: Dict[str, Dict[str, Any]] = {}
    bars_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    indicator_state: Dict[str, Dict[str, Optional[float]]] = {}
    last_price: Dict[str, float] = {}
    _bars_lock = threading.Lock()
    active_entries: Dict[Any, Dict[str, Any]] = {}
    account_snapshot: Dict[str, Optional[float]] = {"balance": None, "equity": None}

    # API wrappers
    def get_token():
        return _api_get_token(API_URL, USERNAME, API_KEY)

    def api_post(token, endpoint, payload):
        return _api_post(API_URL, token, endpoint, payload)

    def cancel_order(token, account_id, order_id):
        return _api_cancel_order(API_URL, token, account_id, order_id)

    def get_account_info(token):
        return _api_get_account_info(token)

    # Market helpers
    def load_contracts():
        return market_load_contracts(get_token, contract_map, config, SYMBOL)

    def seed_streamer_from_warmup(ms: Any) -> None:
        return market_seed_streamer_from_warmup(ms, bars_by_symbol, _bars_lock, indicator_state)

    def warmup_bars(symbol: str, contract_id: Any, days: int = 1, unit: int = 2, unit_n: int = 1, live: bool = False):
        token = get_token()
        if not token:
            logging.warning("Warmup failed: no token")
            return
        tzname = config.get("tz") or "America/New_York"
        hours = BOOTSTRAP_HISTORY_HOURS if BOOTSTRAP_HISTORY_HOURS and BOOTSTRAP_HISTORY_HOURS > 0 else None
        return market_warmup_bars(
            lambda endpoint, payload: api_post(token, endpoint, payload),
            token,
            symbol,
            contract_id,
            days,
            unit,
            unit_n,
            live,
            bars_by_symbol,
            _bars_lock,
            indicator_state,
            ATR_LENGTH,
            EMA_SHORT,
            EMA_LONG,
            EMA_SOURCE,
            RTH_ONLY,
            tzname,
            hours,
            INCLUDE_PARTIAL,
        )

    # Risk helpers
    def risk_per_point(symbol: str, contract_id: Any) -> float:
        instr = config.get("instrument") or {}
        cfg_rpp = float(instr.get("riskPerPoint") or 0)
        return risk_per_point_module(contract_map, cfg_rpp, symbol, contract_id)

    def get_risk_dollars() -> float:
        # Use Pine's riskPerTrade (no budget fraction)
        return float(RISK_PER_TRADE)

    # Compose context for streamer + monitors
    ctx: Dict[str, Any] = {
        'get_token': get_token,
        'api_post': api_post,
        'get_account_info': get_account_info,
        'cancel_order': cancel_order,
        'contract_map': contract_map,
        'bars_by_symbol': bars_by_symbol,
        'bars_lock': _bars_lock,
        'indicator_state': indicator_state,
        'last_price': last_price,
        'oco_orders': oco_orders,
        'active_entries': active_entries,
        'account_snapshot': account_snapshot,
        'SYMBOL': SYMBOL,
        'ACCOUNT_ID': ACCOUNT_ID,
        'LIVE_FLAG': LIVE_FLAG,
        'BOOTSTRAP_HISTORY_HOURS': BOOTSTRAP_HISTORY_HOURS,
        'DEBUG': bool(config.get('debug', False)),
        'snap_to_tick': snap_to_tick,
        'fmt_num': fmt_num,
        'load_contracts': load_contracts,
        'warmup_bars': warmup_bars,
        'seed_streamer_from_warmup': seed_streamer_from_warmup,
        'EMA_SHORT': EMA_SHORT,
        'EMA_LONG': EMA_LONG,
        'USE_VWAP': USE_VWAP,
        'REQUIRE_PRICE_ABOVE_EMAS': bool(STRAT.get("requirePriceAboveEMAS", False)),
        'CONFIRM_BARS': int(STRAT.get("confirmBars", 0)),
        'STRICT_CROSS_ONLY': bool(STRAT.get("strictCrossOnly", False)),
        'INTRABAR_CROSS': bool(STRAT.get("intrabarCross", False)),
        'TRADE_COOLDOWN_SEC': TRADE_COOLDOWN_SEC,
        # Pine: use synthetic trailing activation to convert fixed SL into a native trailing (type 5)
        # Enable native trailing conversion after activation/BE (so venue shows type=5)
        'TRAILING_STOP_ENABLED': True,
        'SYNTH_TRAILING_ENABLED': True,
        'FORCE_NATIVE_TRAIL': bool(STRAT.get("forceNativeTrailing", False)),
        'FORCE_FIXED_TRAIL_TICKS': bool(STRAT.get('forceFixedTrailTicks', False)),
        # No default 5-tick override for Pine; leave None unless explicitly configured
        'TRAIL_TICKS_FIXED': (int(STRAT.get("trailDistanceTicks")) if (STRAT.get("trailDistanceTicks") not in (None, '', False)) else None),
        'PAD_TICKS': int(STRAT.get("padTicks", 0)),
        'SHORT_SIZE_FACTOR': SHORT_SIZE_FACTOR,
        'CONTRACT_SIZE_MAX': CONTRACT_SIZE_MAX,
        'LONG_ONLY': LONG_ONLY,
        'ATR_TRAIL_K_LONG': float(STRAT.get("atrTrailKLong", 1.5)),
        'ATR_TRAIL_K_SHORT': float(STRAT.get("atrTrailKShort", 1.0)),
        # Activation offsets (Pine trail_offset = 0.5 * ATR)
        'TRAIL_OFFSET_K_LONG': float(STRAT.get("atrTrailOffsetKLong", STRAT.get("atrTrailOffsetK", 0.5))),
        'TRAIL_OFFSET_K_SHORT': float(STRAT.get("atrTrailOffsetKShort", STRAT.get("atrTrailOffsetK", 0.5))),
        # Prefer a specific native trailing variant if provided (e.g., trailingdistance, distanceTicks, trailticks, distance)
        'TRAIL_VARIANT': str(STRAT.get('trailVariant') or '').strip().lower(),
        'ATR_LENGTH': ATR_LENGTH,
        'MARKET_HUB': MARKET_HUB,
        'risk_per_point': risk_per_point,
        'get_risk_dollars': get_risk_dollars,
        'compute_indicators': lambda df: _compute_indicators(df, EMA_SHORT, EMA_LONG, STRAT.get("emaSource", "close"), bool(STRAT.get("rthOnly", False)), config.get("tz") or "America/New_York"),
        # time/hour gating
        'HOURS_CFG': config.get('hours') or {},
        'TZ': config.get('tz') or 'America/New_York',
        # weekly kill gate state
        'WEEKLY_KILL_ENABLED': WEEKLY_KILL_ENABLED,
        'MAX_WEEKLY_LOSS': MAX_WEEKLY_LOSS,
        'WEEKLY_KILL_ACTIVE': False,
        'RISK_PER_TRADE': RISK_PER_TRADE,
        # Reverse behavior: on a new opposite cross, flatten/cancel and take new signal
        'REVERSE_ON_CROSS': bool(STRAT.get('reverseOnCross', True)),
        # Delay new entries briefly after a reverse/flatten to let venue settle
        'REVERSE_ENTRY_DELAY_SEC': float((config.get('runtime') or {}).get('reverse_entry_delay_sec', 1.0) or 0.0),
        # Switch to native trailing (type=5) as soon as activation is met
        'REQUIRE_BEFORE_NATIVE_TRAIL': False,
        # Optional fixed activation in ticks; if set, overrides ATR-based offset
        'TRAIL_OFFSET_TICKS_LONG': (int(STRAT.get('trailOffsetTicksLong')) if (STRAT.get('trailOffsetTicksLong') not in (None, '', False)) else (int(STRAT.get('trailOffsetTicks')) if (STRAT.get('trailOffsetTicks') not in (None, '', False)) else None)),
        'TRAIL_OFFSET_TICKS_SHORT': (int(STRAT.get('trailOffsetTicksShort')) if (STRAT.get('trailOffsetTicksShort') not in (None, '', False)) else (int(STRAT.get('trailOffsetTicks')) if (STRAT.get('trailOffsetTicks') not in (None, '', False)) else None)),
    }

    # Monitors
    ctx['monitor_oco_orders'] = make_monitor_oco_orders(ctx)
    ctx['monitor_account_snapshot'] = make_monitor_account_snapshot(ctx)
    ctx['monitor_synth_trailing'] = make_monitor_synth_trailing(ctx)
    ctx['get_open_orders_count'] = make_get_open_orders_count(ctx)

    # Weekly loss watcher (lightweight): if equity drop since start > limit, gate new entries
    def weekly_kill_watchdog():
        start_equity = None
        while True:
            try:
                tok = get_token()
                if not tok:
                    time.sleep(3.0)
                    continue
                acct = get_account_info(tok) or {}
                eq = acct.get('equity') or acct.get('netLiq') or acct.get('netLiquidation') or acct.get('balance')
                if eq is None:
                    time.sleep(5.0)
                    continue
                try:
                    eq = float(eq)
                except Exception:
                    time.sleep(5.0); continue
                if start_equity is None:
                    start_equity = eq
                dd = float(eq) - float(start_equity)
                if ctx.get('WEEKLY_KILL_ENABLED') and (dd <= -abs(ctx.get('MAX_WEEKLY_LOSS', 0.0))):
                    ctx['WEEKLY_KILL_ACTIVE'] = True
                    logging.error("Weekly loss gate engaged: %.2f <= -%.2f", dd, float(ctx.get('MAX_WEEKLY_LOSS')))
                time.sleep(10.0)
            except Exception:
                time.sleep(10.0)

    # Boot + stream
    def run_stream():
        ctx['load_contracts']()
        # Resolve contract
        contract = (contract_map.get(SYMBOL)
                    or contract_map.get(f"US.{SYMBOL}")
                    or next((v for k, v in contract_map.items() if k.upper().startswith(f"{SYMBOL}.") or k.upper().endswith(f".{SYMBOL}")), None))
        if not contract:
            logging.error("No contract for symbol %s", SYMBOL)
            return
        contract_id = contract["contractId"]
        # Warmup indicators
        warmup_bars(SYMBOL, contract_id, days=1, unit=2, unit_n=1, live=LIVE_FLAG)

        tok = get_token()
        if not tok:
            logging.error("Stream auth failed")
            return

        ms = PineMarketStreamer(ctx, SYMBOL, contract_id, unit=2, unit_n=1)
        market_seed_streamer_from_warmup(ms, bars_by_symbol, _bars_lock, indicator_state)
        ms.start(tok)
        logging.info("Pine strategy streamer running for %s (%s)", SYMBOL, str(contract_id))
        while True:
            time.sleep(1)

    # Start monitors
    load_contracts()
    threading.Thread(target=lambda: asyncio_run(ctx['monitor_oco_orders']()), daemon=True).start()
    threading.Thread(target=lambda: asyncio_run(ctx['monitor_account_snapshot']()), daemon=True).start()
    threading.Thread(target=lambda: asyncio_run(ctx['monitor_synth_trailing']()), daemon=True).start()
    threading.Thread(target=weekly_kill_watchdog, daemon=True).start()

    # Start streaming in foreground thread
    run_stream()


def asyncio_run(coro):
    import asyncio
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(coro)
    except Exception:
        try:
            asyncio.get_event_loop().run_until_complete(coro)
        except Exception:
            pass


if __name__ == "__main__":
    run_strategy_server()
