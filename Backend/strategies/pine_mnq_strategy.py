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


def _resolve_account_id(cfg: Dict[str, Any]) -> int:
    """Resolve account_id from several common locations in config.yaml.
    Preference order:
      1) top-level account_id
      2) account.id (object)
      3) auth.account_id
      4) topstepx.account_id
    Returns 0 if none found or not coercible to int.
    """
    candidates = [
        cfg.get("account_id"),
        (cfg.get("account") or {}).get("id"),
        (cfg.get("auth") or {}).get("account_id"),
        (cfg.get("topstepx") or {}).get("account_id"),
    ]
    for c in candidates:
        try:
            if c is not None:
                return int(c)
        except Exception:
            continue
    return 0


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
        # Pine logic with robust reverse handling: ensure flat/cleared before new entry and respect a brief guard delay.
        now = time.time()
        if atr_val is None or ef is None or es is None:
            return

        # Global gates
        if self.ctx.get('TRADING_DISABLED'):
            return
        if bool(self.ctx.get('WEEKLY_KILL_ACTIVE')):
            return
        try:
            # Hour gating via config.hours.enable["HH"] (optional)
            hours_cfg = (self.ctx.get('HOURS_CFG') or {}).get('enable') or {}
            if isinstance(hours_cfg, dict) and hours_cfg:
                import datetime as dt
                from zoneinfo import ZoneInfo
                tzname = self.ctx.get('TZ') or 'America/New_York'
                now_dt = dt.datetime.now(ZoneInfo(tzname))
                hh = f"{now_dt.hour:02d}"
                if not bool(hours_cfg.get(hh)):
                    self._last_rel = (ef - es)
                    return
        except Exception:
            pass

        # Reverse/cooldown guard (avoid stacking around cancels)
        try:
            if now < float(getattr(self, '_reverse_guard_until', 0.0)):
                self._last_rel = (ef - es)
                return
        except Exception:
            pass

        rel = ef - es
        prev_rel = self._last_rel
        cross_up, cross_dn = self._calc_cross(prev_rel, rel, bool(self.ctx.get('STRICT_CROSS_ONLY')))

        vwap_long_ok = True
        vwap_short_ok = True
        if self.ctx['USE_VWAP'] and vwap_val is not None:
            vwap_long_ok = close_px > vwap_val
            vwap_short_ok = close_px < vwap_val
        ema_long_ok = (not self.ctx['REQUIRE_PRICE_ABOVE_EMAS']) or (close_px >= ef and close_px >= es)
        ema_short_ok = (not self.ctx['REQUIRE_PRICE_ABOVE_EMAS']) or (close_px <= ef and close_px <= es)

        oc = self.ctx['get_open_orders_count'](self.contract_id)
        if (now - float(getattr(self, '_last_signal_ts', 0.0))) < float(self.ctx.get('TRADE_COOLDOWN_SEC', 0)):
            self._last_rel = rel
            return

        # Optional N-bar confirmation: store and let subsequent bars complete it
        confirm_n = int(self.ctx.get('CONFIRM_BARS', 0) or 0)
        if confirm_n > 0:
            if cross_up:
                self._pending_signal = {"dir": "long", "bars_left": confirm_n}
            elif (not self.ctx['LONG_ONLY']) and cross_dn:
                self._pending_signal = {"dir": "short", "bars_left": confirm_n}
            if self._pending_signal:
                self._last_rel = rel
                return

        # Entry decisions with no-stacking and reverse-on-cross safety
        pos_s = self._position_qty_sign()
        if cross_up and vwap_long_ok and ema_long_ok:
            # Do not add to an existing long
            if pos_s > 0:
                logging.info("Skip LONG: already long position present")
                self._last_rel = rel
                return
            # If currently short or have open orders, flatten/cancel first when reversing is enabled
            if bool(self.ctx.get('REVERSE_ON_CROSS', True)) and ((oc > 0) or (pos_s < 0)):
                if not self._ensure_flat_and_cleared():
                    logging.info("Reverse: not flat/clear yet; skipping LONG entry on cross")
                    self._last_rel = rel
                    return
                oc = self.ctx['get_open_orders_count'](self.contract_id)
                # Optional post-flatten delay to avoid overlapping entry with cancels
                try:
                    delay = float(self.ctx.get('REVERSE_ENTRY_DELAY_SEC', 0.0) or 0.0)
                except Exception:
                    delay = 0.0
                if delay > 0:
                    try:
                        self._reverse_guard_until = time.time() + delay
                    except Exception:
                        pass
                    logging.info("Reverse: delaying new LONG entry by %.2fs after flatten", delay)
                    self._last_rel = rel
                    return
            self._last_signal_ts = now
            self._place_pine_entry_with_brackets(side=0, op=close_px, atr_val=float(atr_val))
            self._last_rel = rel
            return

        if (not self.ctx['LONG_ONLY']) and cross_dn and vwap_short_ok and ema_short_ok:
            # Do not add to an existing short
            if pos_s < 0:
                logging.info("Skip SHORT: already short position present")
                self._last_rel = rel
                return
            # If currently long or have open orders, flatten/cancel first when reversing is enabled
            if bool(self.ctx.get('REVERSE_ON_CROSS', True)) and ((oc > 0) or (pos_s > 0)):
                if not self._ensure_flat_and_cleared():
                    logging.info("Reverse: not flat/clear yet; skipping SHORT entry on cross")
                    self._last_rel = rel
                    return
                oc = self.ctx['get_open_orders_count'](self.contract_id)
                # Optional post-flatten delay to avoid overlapping entry with cancels
                try:
                    delay = float(self.ctx.get('REVERSE_ENTRY_DELAY_SEC', 0.0) or 0.0)
                except Exception:
                    delay = 0.0
                if delay > 0:
                    try:
                        self._reverse_guard_until = time.time() + delay
                    except Exception:
                        pass
                    logging.info("Reverse: delaying new SHORT entry by %.2fs after flatten", delay)
                    self._last_rel = rel
                    return
            self._last_signal_ts = now
            self._place_pine_entry_with_brackets(side=1, op=close_px, atr_val=float(atr_val))
            self._last_rel = rel
            return

        self._last_rel = rel

    def _position_qty_sign(self) -> int:
        """Return +1 if net long, -1 if net short, 0 if flat/unavailable for this contract."""
        try:
            token = self.ctx['get_token']()
            if not token:
                return 0
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
                    q = float(qty)
                    if q > 0:
                        return 1
                    if q < 0:
                        return -1
                except Exception:
                    continue
        except Exception:
            return 0
        return 0

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
        # Optional: directly place native trailing stop on entry (skips initial fixed SL/TP)
        try:
            if bool(self.ctx.get('FORCE_NATIVE_TRAIL_ON_ENTRY')):
                self._place_market_with_brackets(side=side, op=op, atr_val=atr_val)
                return
        except Exception:
            pass
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
        # Daily contracts limit gate (if configured)
        try:
            max_day = int(self.ctx.get('MAX_CONTRACTS_PER_DAY') or 0)
        except Exception:
            max_day = 0
        if max_day and max_day > 0:
            try:
                import datetime as dt
                from zoneinfo import ZoneInfo
                tzname = self.ctx.get('TZ') or 'UTC'
                day_key = dt.datetime.now(ZoneInfo(tzname)).date().isoformat()
            except Exception:
                day_key = time.strftime('%Y-%m-%d')
            cur_key = self.ctx.get('contracts_day_key')
            if cur_key != day_key:
                self.ctx['contracts_day_key'] = day_key
                self.ctx['contracts_traded_today'] = 0
            used = int(self.ctx.get('contracts_traded_today') or 0)
            if (used + int(size)) > int(max_day):
                logging.info("Daily contracts limit reached (%d/%d); skipping entry", used, max_day)
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
            try:
                logging.error(
                    "Entry failed (acct=%s cid=%s side=%s size=%s): %s",
                    str(self.ctx.get('ACCOUNT_ID')),
                    str(self.contract_id),
                    str(side),
                    str(size),
                    entry,
                )
            except Exception:
                logging.error("Entry failed: %s", entry)
            return
        entry_id = entry.get("orderId")
        logging.info("Pine entry placed id=%s side=%s size=%d", str(entry_id), "BUY" if side==0 else "SELL", int(size))
        # increment daily contracts counter on success
        try:
            self.ctx['contracts_traded_today'] = int(self.ctx.get('contracts_traded_today') or 0) + int(size)
        except Exception:
            pass

        # Compute TP and SL prices (snap to tick)
        if side == 0:
            sl_px_fixed = snap_to_tick(op - stop_long_pts, tick_size, decimals)
            tp_px = snap_to_tick(op + stop_long_pts * 3.0, tick_size, decimals)
            t_points = trail_long_pts
        else:
            sl_px_fixed = snap_to_tick(op + stop_short_pts, tick_size, decimals)
            tp_px = snap_to_tick(op - stop_short_pts * 2.0, tick_size, decimals)
            t_points = trail_short_pts

        # Place STOP first (safer if side limits are tight); retry once on failure
        sl_id = None
        sl_resp = self.ctx['api_post'](token, "/api/Order/place", {
            "accountId": self.ctx['ACCOUNT_ID'],
            "contractId": self.contract_id,
            "type": 4,
            "side": 1 - side,
            "size": int(size),
            "stopPrice": sl_px_fixed,
            "linkedOrderId": entry_id,
        })
        if sl_resp.get("success") and sl_resp.get("orderId"):
            sl_id = sl_resp.get("orderId")
            logging.info("Pine SL placed id=%s price=%.4f", str(sl_id), float(sl_px_fixed))
        else:
            logging.warning("Pine SL placement failed for entry %s: %s", str(entry_id), sl_resp)
            # brief retry
            try:
                time.sleep(0.2)
            except Exception:
                pass
            sl_resp2 = self.ctx['api_post'](token, "/api/Order/place", {
                "accountId": self.ctx['ACCOUNT_ID'],
                "contractId": self.contract_id,
                "type": 4,
                "side": 1 - side,
                "size": int(size),
                "stopPrice": sl_px_fixed,
                "linkedOrderId": entry_id,
            })
            if sl_resp2.get("success") and sl_resp2.get("orderId"):
                sl_id = sl_resp2.get("orderId")
                logging.info("Pine SL placed (retry) id=%s price=%.4f", str(sl_id), float(sl_px_fixed))
            else:
                logging.error("Pine SL retry failed for entry %s: %s", str(entry_id), sl_resp2)

        # If fixed SL could not be placed, optionally fall back to native trailing stop immediately
        if (sl_id is None) and bool(self.ctx.get('TRAILING_STOP_ENABLED', True)):
            try:
                # Derive ticks: prefer fixed config; else ATR-based approximation
                t_ticks = None
                try:
                    t_ticks = int(self.ctx.get('TRAIL_TICKS_FIXED')) if (self.ctx.get('TRAIL_TICKS_FIXED') is not None) else None
                except Exception:
                    t_ticks = None
                if (t_ticks is None) and (tick_size and tick_size > 0):
                    t_pts = trail_long_pts if side == 0 else trail_short_pts
                    try:
                        t_ticks = int(max(1, round(abs(t_pts) / float(tick_size))))
                    except Exception:
                        t_ticks = None
                if t_ticks is not None:
                    tr = self._place_trailing_order(token, entry_id, side, size, int(t_ticks),
                                                    entry_price=float(op), tick_size=tick_size, decimals=decimals)
                    if tr and tr.get("success") and tr.get("orderId"):
                        sl_id = tr.get("orderId")
                        logging.info("Pine native TRAIL placed id=%s ticks=%s", str(sl_id), str(t_ticks))
                        # mark native
                        try:
                            self.ctx['active_entries'][entry_id] = {
                                "symbol": self.symbol,
                                "contractId": self.contract_id,
                                "side": int(side),
                                "size": int(size),
                                "entry": float(op),
                                "stop_points": float(trail_long_pts if side == 0 else trail_short_pts),
                                "tp_id": None,
                                "sl_id": sl_id,
                                "native_trail": True,
                                "tickSize": float(tick_size),
                                "decimals": int(decimals),
                            }
                        except Exception:
                            pass
                    else:
                        logging.error("Pine native TRAIL failed for entry %s: %s", str(entry_id), tr)
            except Exception:
                logging.exception("Error while placing native trailing fallback")

        # Place TP second (limit). Even if SL failed, TP can still be placed; OCO tracking only if both exist
        tp_id = None
        tp_order = self.ctx['api_post'](token, "/api/Order/place", {
            "accountId": self.ctx['ACCOUNT_ID'],
            "contractId": self.contract_id,
            "type": 1,
            "side": 1 - side,
            "size": size,
            "limitPrice": tp_px,
            "linkedOrderId": entry_id,
        })
        if tp_order.get("success") and tp_order.get("orderId"):
            tp_id = tp_order.get("orderId")
            logging.info("Pine TP placed id=%s price=%.4f", str(tp_id), float(tp_px))
        else:
            logging.warning("Pine TP placement failed for entry %s: %s", str(entry_id), tp_order)

        # Track OCO pair and active entry for trailing monitor support
        if entry_id and (tp_id is not None) and (sl_id is not None):
            try:
                self.ctx['oco_orders'][entry_id] = [tp_id, sl_id]
            except Exception:
                pass
        # Only set/overwrite active_entries if we did not already mark native_trail in fallback
        if entry_id not in self.ctx['active_entries']:
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
    ACCOUNT_ID = _resolve_account_id(config)
    SYMBOL = (config.get("symbol") or "MNQ").upper()
    MARKET_HUB = config.get("market_hub") or "https://rtc.topstepx.com/hubs/market"
    LIVE_FLAG = bool((config.get("market") or {}).get("live", True))
    INCLUDE_PARTIAL = bool((config.get("market") or {}).get("includePartialBar", True))
    BOOTSTRAP_HISTORY_HOURS = int(config.get("bootstrap_history_hours", 3) or 3)
    logging.info("Config resolved: account_id=%s symbol=%s", str(ACCOUNT_ID), SYMBOL)

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
    bars_by_symbol: Dict[
        str, List[Dict[str, Any]]] = {}
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
        # If set true, place native trailing stop immediately on entry instead of fixed SL -> then convert
        'FORCE_NATIVE_TRAIL_ON_ENTRY': bool(STRAT.get('forceNativeTrailing', False) or STRAT.get('forceNativeTrailOnEntry', False)),
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
        # Cap total contracts entered per day (sum of entry sizes). 0/None disables.
        'MAX_CONTRACTS_PER_DAY': int(((config.get('risk') or {}).get('daily_contracts_limit')) or ((config.get('risk') or {}).get('max_contracts_per_day')) or 0),
        # Reverse behavior: on a new opposite cross, flatten/cancel and take new signal
        'REVERSE_ON_CROSS': bool(STRAT.get('reverseOnCross', True)),
        # Always preflight cancel all open orders (account-wide) and flatten any open
        # position before placing a new entry (safer with venues that enforce strict
        # side limits and slow cancel propagation)
        'ALWAYS_FLATTEN_BEFORE_ENTRY': bool((config.get('runtime') or {}).get('always_flatten_before_entry', True)),
        # Delay new entries briefly after a reverse/flatten to let venue settle
        'REVERSE_ENTRY_DELAY_SEC': float((config.get('runtime') or {}).get('reverse_entry_delay_sec', 1.0) or 0.0),
        # Switch to native trailing (type=5) as soon as activation is met
        'REQUIRE_BEFORE_NATIVE_TRAIL': False,
        # Optional fixed activation in ticks; if set, overrides ATR-based offset
        'TRAIL_OFFSET_TICKS_LONG': (int(STRAT.get('trailOffsetTicksLong')) if (STRAT.get('trailOffsetTicksLong') not in (None, '', False)) else (int(STRAT.get('trailOffsetTicks')) if (STRAT.get('trailOffsetTicks') not in (None, '', False)) else None)),
        'TRAIL_OFFSET_TICKS_SHORT': (int(STRAT.get('trailOffsetTicksShort')) if (STRAT.get('trailOffsetTicksShort') not in (None, '', False)) else (int(STRAT.get('trailOffsetTicks')) if (STRAT.get('trailOffsetTicks') not in (None, '', False)) else None)),
    }

    # Stream health defaults
    ctx['STREAM_CONNECTED'] = False
    ctx['last_stream_event_ts'] = 0.0
    ctx['START_TS'] = time.time()

    # Monitors
    ctx['monitor_oco_orders'] = make_monitor_oco_orders(ctx)
    ctx['monitor_account_snapshot'] = make_monitor_account_snapshot(ctx)
    ctx['monitor_synth_trailing'] = make_monitor_synth_trailing(ctx)
    ctx['get_open_orders_count'] = make_get_open_orders_count(ctx)

    # On boot or after a restart, rebuild local tracking from venue state
    def rebuild_open_state() -> None:
        try:
            tok = get_token()
            if not tok:
                logging.warning("State rebuild skipped: no token")
                return
            # Resolve current contract id for the configured symbol
            load_contracts()
            contract = (contract_map.get(SYMBOL)
                        or contract_map.get(f"US.{SYMBOL}")
                        or next((v for k, v in contract_map.items() if k.upper().startswith(f"{SYMBOL}.") or k.upper().endswith(f".{SYMBOL}")), None))
            if not contract:
                return
            cid = contract.get("contractId")
            tick_size = float(contract.get("tickSize") or 0.0)
            decimals = int(contract.get("decimalPlaces") or 2)
            # Fetch open orders
            ores = api_post(tok, "/api/Order/searchOpen", {"accountId": ACCOUNT_ID})
            orders = ores.get("orders", []) if isinstance(ores, dict) else []
            # Group children by linked parent (entry) id
            groups: Dict[Any, Dict[str, Any]] = {}
            for o in orders:
                try:
                    ocid = o.get("contractId") or (o.get("contract") or {}).get("id")
                    if ocid != cid:
                        continue
                    typ = o.get("type")
                    if typ not in (1, 4, 5):
                        continue
                    pid = o.get("linkedOrderId")
                    oid = o.get("id") or o.get("orderId")
                    if not pid or oid is None:
                        continue
                    g = groups.setdefault(pid, {"tp": None, "sl": None, "sl_type": None, "size": None, "entry_side": None})
                    if g.get("size") is None:
                        try:
                            g["size"] = int(abs(int(float(o.get("size") or 1))))
                        except Exception:
                            g["size"] = 1
                    try:
                        # child side is opposite of entry side
                        cs = o.get("side")
                        if cs in (0, 1):
                            g["entry_side"] = 1 - int(cs)
                    except Exception:
                        pass
                    if typ == 1:
                        g["tp"] = {"id": oid, "price": o.get("limitPrice")}
                    elif typ in (4, 5):
                        g["sl"] = {"id": oid, "price": o.get("stopPrice")}
                        g["sl_type"] = int(typ)
                except Exception:
                    continue
            # Attempt to infer current open position avg price (if provided by API)
            avg_px: Optional[float] = None
            pos_side_sign = 0
            try:
                pres = api_post(tok, "/api/Position/searchOpen", {"accountId": ACCOUNT_ID})
                plist = pres.get("positions") if isinstance(pres, dict) else (pres if isinstance(pres, list) else [])
                for p in plist or []:
                    try:
                        pcid = p.get("contractId") or (p.get("contract") or {}).get("id")
                        if pcid != cid:
                            continue
                        q = p.get("quantity") or p.get("qty") or p.get("size")
                        if q is None:
                            continue
                        qf = float(q)
                        if qf == 0.0:
                            continue
                        pos_side_sign = 1 if qf > 0 else -1
                        # Try common avg price fields
                        for k in ("avgPrice", "averagePrice", "avgEntry", "price", "entryPrice"):
                            v = p.get(k)
                            if v is not None:
                                try:
                                    avg_px = float(v)
                                    break
                                except Exception:
                                    pass
                        break
                    except Exception:
                        continue
            except Exception:
                pass
            # Rebuild local maps
            for entry_id, g in groups.items():
                tp = g.get("tp")
                sl = g.get("sl")
                if not tp or not sl:
                    continue
                tp_id = tp.get("id"); sl_id = sl.get("id")
                if tp_id is None or sl_id is None:
                    continue
                try:
                    ctx['oco_orders'][entry_id] = [tp_id, sl_id]
                except Exception:
                    pass
                native = (g.get("sl_type") == 5)
                side = int(g.get("entry_side") if g.get("entry_side") in (0, 1) else (0 if pos_side_sign > 0 else 1))
                size = int(g.get("size") or 1)
                # Attempt to infer entry price and stop distance when not trailing
                entry_px: Optional[float] = None
                stop_pts: Optional[float] = None
                try:
                    tp_px = float(tp.get("price")) if tp.get("price") is not None else None
                except Exception:
                    tp_px = None
                try:
                    sl_px = float(sl.get("price")) if sl.get("price") is not None else None
                except Exception:
                    sl_px = None
                if not native and (tp_px is not None) and (sl_px is not None):
                    try:
                        if side == 0:
                            # LONG: tp - op = 3s, op - sl = s => s = (tp - sl)/4, op = sl + s
                            s = (tp_px - sl_px) / 4.0
                            entry_px = sl_px + s
                            stop_pts = s
                        else:
                            # SHORT: op - tp = 2s, sl - op = s => s = (sl - tp)/3, op = sl - s
                            s = (sl_px - tp_px) / 3.0
                            entry_px = sl_px - s
                            stop_pts = s
                    except Exception:
                        entry_px = None
                        stop_pts = None
                if entry_px is None and avg_px is not None:
                    entry_px = avg_px
                info = {
                    "symbol": SYMBOL,
                    "contractId": cid,
                    "side": side,
                    "size": size,
                    "entry": entry_px if entry_px is not None else 0.0,
                    "stop_points": stop_pts,
                    "tp_id": tp_id,
                    "sl_id": sl_id,
                    "native_trail": bool(native),
                    "tickSize": float(tick_size),
                    "decimals": int(decimals),
                }
                try:
                    ctx['active_entries'][entry_id] = info
                except Exception:
                    pass
            if ctx['oco_orders']:
                logging.info("Rebuilt %d OCO groups from venue state", len(ctx['oco_orders']))
            if ctx['active_entries']:
                logging.info("Rebuilt %d active entries from venue state", len(ctx['active_entries']))
        except Exception:
            logging.exception("Failed to rebuild open state")

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
    # Rebuild tracking before monitors begin to avoid orphan logic surprises
    rebuild_open_state()
    threading.Thread(target=lambda: asyncio_run(ctx['monitor_oco_orders']()), daemon=True).start()
    threading.Thread(target=lambda: asyncio_run(ctx['monitor_account_snapshot']()), daemon=True).start()
    threading.Thread(target=lambda: asyncio_run(ctx['monitor_synth_trailing']()), daemon=True).start()
    threading.Thread(target=weekly_kill_watchdog, daemon=True).start()

    # Flatten on hour boundaries when the new hour is disabled by hours.enable
    def hour_boundary_watchdog():
        runtime = config.get('runtime') or {}
        flatten_on_disabled = bool(runtime.get('flatten_on_disabled_hour', True))
        flatten_on_any = bool(runtime.get('flatten_on_any_hour_boundary', False))
        hours_cfg = (config.get('hours') or {}).get('enable') or {}
        if not flatten_on_disabled and not flatten_on_any:
            return
        try:
            from zoneinfo import ZoneInfo
            import datetime as dt
        except Exception:
            logging.warning("Hour boundary watchdog disabled: zoneinfo not available")
            return
        last_hour = None
        while True:
            try:
                tzname = config.get('tz') or 'America/New_York'
                now_dt = dt.datetime.now(ZoneInfo(tzname))
                hh = now_dt.hour
                if last_hour is None:
                    last_hour = hh
                if hh != last_hour:
                    # Entered a new hour
                    do_flatten = False
                    if flatten_on_any:
                        do_flatten = True
                    elif isinstance(hours_cfg, dict) and hours_cfg:
                        hkey = f"{hh:02d}"
                        enabled = bool(hours_cfg.get(hkey))
                        if not enabled:
                            do_flatten = True
                    if do_flatten:
                        try:
                            # Resolve current contract id
                            contract = (contract_map.get(SYMBOL)
                                        or contract_map.get(f"US.{SYMBOL}")
                                        or next((v for k, v in contract_map.items() if k.upper().startswith(f"{SYMBOL}.") or k.upper().endswith(f".{SYMBOL}")), None))
                            contract_id = contract.get('contractId') if isinstance(contract, dict) else (config.get('CONTRACT_ID') or None)
                        except Exception:
                            contract_id = (config.get('CONTRACT_ID') or None)
                        if contract_id is None:
                            logging.warning("Hour boundary flatten: contract id unavailable")
                        else:
                            try:
                                tok = get_token()
                                if tok:
                                    # Cancel all open orders for this contract
                                    try:
                                        resp = api_post(tok, "/api/Order/searchOpen", {"accountId": ACCOUNT_ID})
                                        orders = resp.get("orders", []) if isinstance(resp, dict) else []
                                        for o in orders:
                                            try:
                                                cid = o.get("contractId") or (o.get("contract") or {}).get("id")
                                                oid = o.get("id") or o.get("orderId")
                                                if cid == contract_id and oid is not None:
                                                    cancel_order(tok, ACCOUNT_ID, oid)
                                            except Exception:
                                                continue
                                    except Exception:
                                        pass
                                    # Flatten any open position on this contract
                                    try:
                                        presp = api_post(tok, "/api/Position/searchOpen", {"accountId": ACCOUNT_ID})
                                        pos_list = presp.get("positions") if isinstance(presp, dict) else (presp if isinstance(presp, list) else [])
                                        for p in pos_list or []:
                                            try:
                                                cid = p.get("contractId") or (p.get("contract") or {}).get("id")
                                                if cid != contract_id:
                                                    continue
                                                qty = p.get("quantity") or p.get("qty") or p.get("size")
                                                if qty is None:
                                                    continue
                                                q = int(abs(int(float(qty))))
                                                if q <= 0:
                                                    continue
                                                side = None
                                                try:
                                                    if float(qty) > 0:
                                                        side = 1  # SELL to flatten long
                                                    elif float(qty) < 0:
                                                        side = 0  # BUY to flatten short
                                                except Exception:
                                                    pass
                                                if side is None:
                                                    s = p.get("side")
                                                    if s in (0, 1):
                                                        side = 1 - int(s)
                                                if side is None:
                                                    continue
                                                api_post(tok, "/api/Order/place", {
                                                    "accountId": ACCOUNT_ID,
                                                    "contractId": contract_id,
                                                    "type": 2,
                                                    "side": int(side),
                                                    "size": int(q),
                                                })
                                            except Exception:
                                                continue
                                    except Exception:
                                        pass
                                    logging.info("Flattened orders/positions at hour %02d boundary (%s)", hh, tzname)
                                else:
                                    logging.warning("Hour boundary flatten: no token available")
                            except Exception:
                                logging.exception("Hour boundary flatten failed")
                    last_hour = hh
            except Exception:
                pass
            time.sleep(5.0)

    threading.Thread(target=hour_boundary_watchdog, daemon=True).start()

    # Connection watchdog: auto-reboot on prolonged disconnect/idle
    def connection_watchdog():
        runtime = config.get('runtime') or {}
        auto_reboot = bool(runtime.get('auto_reboot_on_disconnect', True))
        if not auto_reboot:
            return
        try:
            startup_grace = float(runtime.get('connection_startup_grace_sec', 120) or 120)
        except Exception:
            startup_grace = 120.0
        try:
            idle_limit = float(runtime.get('connection_idle_reboot_sec', 180) or 180)
        except Exception:
            idle_limit = 180.0
        try:
            disconnect_limit = float(runtime.get('connection_disconnected_reboot_sec', 60) or 60)
        except Exception:
            disconnect_limit = 60.0
        reboot_only_when_flat = bool(runtime.get('reboot_only_when_flat', False))
        while True:
            try:
                now = time.time()
                start_ts = float(ctx.get('START_TS') or now)
                last_evt = float(ctx.get('last_stream_event_ts') or 0.0)
                connected = bool(ctx.get('STREAM_CONNECTED'))
                # Respect startup grace window
                if (now - start_ts) < startup_grace:
                    time.sleep(5.0)
                    continue
                # If explicitly disconnected and idle beyond disconnect_limit, reboot
                if (not connected) and (now - max(last_evt, start_ts) >= disconnect_limit):
                    if reboot_only_when_flat:
                        try:
                            tok = get_token()
                            if tok:
                                pres = api_post(tok, "/api/Position/searchOpen", {"accountId": ACCOUNT_ID})
                                plist = pres.get("positions") if isinstance(pres, dict) else (pres if isinstance(pres, list) else [])
                                busy = False
                                for p in plist or []:
                                    try:
                                        q = p.get("quantity") or p.get("qty") or p.get("size")
                                        if q is not None and float(q) != 0.0:
                                            busy = True; break
                                    except Exception:
                                        continue
                                if busy:
                                    logging.warning("Disconnect watchdog: in a trade; defer reboot (only_when_flat)")
                                    time.sleep(5.0)
                                    continue
                        except Exception:
                            pass
                    logging.error("Market stream disconnected for %.0fs; rebooting strategy process", disconnect_limit)
                    os.execv(sys.executable, [sys.executable] + sys.argv)
                # If connected but no events beyond idle_limit, reboot (covers stalled stream/token expiry)
                if (now - max(last_evt, start_ts)) >= idle_limit:
                    if reboot_only_when_flat:
                        try:
                            tok = get_token()
                            if tok:
                                pres = api_post(tok, "/api/Position/searchOpen", {"accountId": ACCOUNT_ID})
                                plist = pres.get("positions") if isinstance(pres, dict) else (pres if isinstance(pres, list) else [])
                                busy = False
                                for p in plist or []:
                                    try:
                                        q = p.get("quantity") or p.get("qty") or p.get("size")
                                        if q is not None and float(q) != 0.0:
                                            busy = True; break
                                    except Exception:
                                        continue
                                if busy:
                                    logging.warning("Idle watchdog: in a trade; defer reboot (only_when_flat)")
                                    time.sleep(5.0)
                                    continue
                        except Exception:
                            pass
                    logging.error("No market stream events for %.0fs; rebooting strategy process", idle_limit)
                    os.execv(sys.executable, [sys.executable] + sys.argv)
            except Exception:
                # Avoid watchdog death; keep checking
                pass
            time.sleep(5.0)

    threading.Thread(target=connection_watchdog, daemon=True).start()

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
