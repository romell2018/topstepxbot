import logging
import time
import threading
import datetime as dt
from typing import Any, Dict, Optional
import pandas as pd

from .indicators import ATR


class MarketStreamer:
    def __init__(self, ctx: Dict[str, Any], symbol: str, contract_id: Any, unit: int = 2, unit_n: int = 1):
        self.ctx = ctx
        self.symbol = symbol
        self.contract_id = contract_id
        self.unit = unit
        self.unit_n = unit_n
        self.cur_minute: Optional[dt.datetime] = None
        self.cur_bar: Optional[Dict[str, Any]] = None
        self.conn = None
        self._started: bool = False
        self._subscribed: bool = False
        self.atr = ATR(self.ctx['ATR_LENGTH'])
        self._last_rel: Optional[float] = None
        self._last_signal_ts: float = 0.0
        self._pending_signal: Optional[dict] = None
        self._last_intrabar_minute: Optional[dt.datetime] = None
        self._real_bars: int = 0
        self._last_price_log_ts: float = 0.0
        self._tag_counter: int = 0
        self._reverse_guard_until: float = 0.0
        # Stream health markers
        try:
            if 'STREAM_CONNECTED' not in self.ctx:
                self.ctx['STREAM_CONNECTED'] = False
            if 'last_stream_event_ts' not in self.ctx:
                self.ctx['last_stream_event_ts'] = 0.0
        except Exception:
            pass

    def _mark_stream_event(self) -> None:
        try:
            self.ctx['last_stream_event_ts'] = time.time()
        except Exception:
            pass

    def _unique_tag(self, prefix: str = "ema_cross_auto") -> str:
        try:
            self._tag_counter = (self._tag_counter + 1) % 1000000
        except Exception:
            self._tag_counter = 1
        return f"{prefix}_{self.symbol}_{int(time.time()*1000)}_{self._tag_counter}"

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

    def _position_qty_sign(self) -> int:
        """Return +1 if net long, -1 if net short, 0 if flat or unavailable for this contract."""
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
            # Secondary safety: if anything lingers, attempt account-wide cancellation
            try:
                resp2 = self.ctx['api_post'](token, "/api/Order/searchOpen", {"accountId": self.ctx['ACCOUNT_ID']})
                orders2 = resp2.get("orders", []) if isinstance(resp2, dict) else []
                for o in orders2:
                    try:
                        oid = o.get("id") or o.get("orderId")
                        if oid is not None:
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

    def _ensure_flat_and_cleared(self, timeout_sec: float = 3.0) -> bool:
        """Ensure there are no open orders and no open position for this contract.
        Attempt cancel/flatten, then poll until both are cleared or timeout.
        """
        end_by = time.time() + float(max(0.5, timeout_sec))
        try:
            self._reverse_guard_until = end_by
        except Exception:
            pass
        # initial attempt
        self._flatten_and_cancel()
        while time.time() < end_by:
            try:
                oc = int(self.ctx['get_open_orders_count'](self.contract_id))
            except Exception:
                oc = None
            has_pos = self._has_open_position()
            if (oc in (0, None)) and (not has_pos):
                return True
            time.sleep(0.2)
        # final snapshot
        try:
            oc = int(self.ctx['get_open_orders_count'](self.contract_id))
        except Exception:
            oc = None
        has_pos = self._has_open_position()
        if oc and oc > 0:
            logging.warning("Reverse: still have %d open orders after timeout", oc)
        if has_pos:
            logging.warning("Reverse: still have open position after timeout")
        return (oc in (0, None)) and (not has_pos)

    def _calc_cross(self, prev_rel: Optional[float], rel: Optional[float], strict: bool) -> (bool, bool):
        try:
            if prev_rel is None or rel is None:
                return (False, False)
            eps = 1e-9
            if strict:
                cross_up = (float(prev_rel) < -eps) and (float(rel) > eps)
                cross_dn = (float(prev_rel) > eps) and (float(rel) < -eps)
            else:
                cross_up = (float(prev_rel) <= eps) and (float(rel) > eps)
                cross_dn = (float(prev_rel) >= -eps) and (float(rel) < -eps)
            return (cross_up, cross_dn)
        except Exception:
            return (False, False)

    def _pd_indicator_snapshot(self) -> Optional[Dict[str, Optional[float]]]:
        try:
            with self.ctx['bars_lock']:
                bars = list(self.ctx['bars_by_symbol'].get(self.symbol, []))
            if len(bars) < 2:
                return None
            df = pd.DataFrame(bars)
            if 'c' not in df or 'v' not in df or 't' not in df:
                return None
            df = df.rename(columns={'c': 'close', 'v': 'volume', 't': 'time', 'o': 'open', 'h': 'high', 'l': 'low'})
            df['time'] = pd.to_datetime(df['time'], utc=True)
            df = df.set_index('time')
            ind = self.ctx['compute_indicators'](df)
            if ind.shape[0] < 2:
                return None
            last = ind.iloc[-1]
            prev = ind.iloc[-2]
            ef_key = f"ema{self.ctx['EMA_SHORT']}"
            es_key = f"ema{self.ctx['EMA_LONG']}"
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
        try:
            # Mark finalized bar as real (non-synthetic)
            self.cur_bar["syn"] = False
        except Exception:
            pass
        with self.ctx['bars_lock']:
            lst = self.ctx['bars_by_symbol'].setdefault(self.symbol, [])
            lst.append(self.cur_bar)
            self.ctx['bars_by_symbol'][self.symbol] = lst[-300:]
        try:
            c = float(self.cur_bar.get("c"))
            h = float(self.cur_bar.get("h"))
            l = float(self.cur_bar.get("l"))
            av = self.atr.update(h, l, c)
            snap = self._pd_indicator_snapshot() or {}
            ef = snap.get("emaFast")
            es = snap.get("emaSlow")
            vw = snap.get("vwap")
            prev_rel = snap.get("prev_rel")
            if self.ctx['DEBUG'] and (ef is not None) and (es is not None):
                try:
                    rel = float(ef) - float(es)
                    cross_up, cross_dn = self._calc_cross(prev_rel, rel, bool(self.ctx.get('STRICT_CROSS_ONLY')))
                    logging.info(
                        f"{self.symbol} bar_close {self.cur_bar.get('t')} | prev_rel={prev_rel} rel={rel} cross_up={cross_up} cross_dn={cross_dn}"
                    )
                except Exception:
                    pass
            self._last_rel = prev_rel
            if (ef is not None) and (es is not None):
                # Compute real-only EMAs (exclude synthetic bars) using Pine-style math via compute_indicators
                ef_r = es_r = None
                try:
                    with self.ctx['bars_lock']:
                        bars = list(self.ctx['bars_by_symbol'].get(self.symbol, []))
                    if bars:
                        df = pd.DataFrame(bars)
                        if 'syn' in df.columns:
                            df = df[df.get('syn') != True]
                        if not df.empty:
                            df = df.rename(columns={'c': 'close', 'v': 'volume', 't': 'time', 'o': 'open', 'h': 'high', 'l': 'low'})
                            df['time'] = pd.to_datetime(df['time'], utc=True)
                            df = df.set_index('time')
                            ind_real = self.ctx['compute_indicators'](df)
                            if ind_real.shape[0] >= 1:
                                last_r = ind_real.iloc[-1]
                                ef_key = f"ema{self.ctx['EMA_SHORT']}"
                                es_key = f"ema{self.ctx['EMA_LONG']}"
                                ef_r = float(last_r.get(ef_key)) if pd.notna(last_r.get(ef_key)) else None
                                es_r = float(last_r.get(es_key)) if pd.notna(last_r.get(es_key)) else None
                except Exception:
                    pass
                # Prefer real EMAs in primary fields when available; also expose both variants
                st = {
                    "emaFast": (ef_r if ef_r is not None else ef),
                    "emaSlow": (es_r if es_r is not None else es),
                    "emaFastReal": ef_r,
                    "emaSlowReal": es_r,
                    "emaFastSeeded": ef,
                    "emaSlowSeeded": es,
                    "vwap": vw,
                    "atr": av,
                }
                self.ctx['indicator_state'][self.symbol] = st
                try:
                    log_ef = st.get("emaFast"); log_es = st.get("emaSlow")
                    logging.info(
                        f"{self.symbol} close={c:.2f} | EMA{self.ctx['EMA_SHORT']}={float(log_ef):.2f} EMA{self.ctx['EMA_LONG']}={float(log_es):.2f} VWAP={(vw if vw is not None else float('nan')):.2f}"
                    )
                except Exception:
                    logging.info(
                        f"{self.symbol} close={c:.2f} | EMA{self.ctx['EMA_SHORT']}={st.get('emaFast')} EMA{self.ctx['EMA_LONG']}={st.get('emaSlow')} VWAP={(vw if vw is not None else float('nan')):.2f}"
                    )
                self._maybe_trade(c, float(ef), float(es), vw, av)
        except Exception:
            pass
        try:
            self._real_bars += 1
        except Exception:
            pass
        self.cur_bar = None

    def _maybe_trade(self, close_px: float, ef: float, es: float, vwap_val: Optional[float], atr_val: Optional[float]):
        now = time.time()
        if atr_val is None or ef is None or es is None:
            return
        # Global kill-switch if venue/account disallows trading
        if self.ctx.get('TRADING_DISABLED'):
            if self.ctx['DEBUG']:
                logging.info("Trading disabled: %s", str(self.ctx.get('TRADING_DISABLED_REASON') or 'unknown'))
            return
        try:
            min_rb = int(self.ctx.get('MIN_REAL_BARS_BEFORE_TRADING', 0) or 0)
            if self._real_bars < min_rb:
                if self.ctx['DEBUG']:
                    logging.info("Gating trade: waiting for real bars (%d/%d)", int(self._real_bars), int(min_rb))
                return
        except Exception:
            pass
        # Guard: skip entries while a reverse/flatten is in progress or cooling down
        try:
            if now < float(self._reverse_guard_until):
                if self.ctx['DEBUG']:
                    logging.info("Reverse guard active; delaying new entries")
                return
        except Exception:
            pass
        rel = ef - es
        prev_rel = self._last_rel
        cross_up, cross_dn = self._calc_cross(prev_rel, rel, bool(self.ctx.get('STRICT_CROSS_ONLY')))
        # Optional override to force LONG cross so we don't wait for a natural crossover
        if self.ctx.get('FORCE_CROSS_UP'):
            cross_up, cross_dn = True, False
        vwap_long_ok = True
        vwap_short_ok = True
        if self.ctx['USE_VWAP'] and vwap_val is not None:
            vwap_long_ok = close_px > vwap_val
            vwap_short_ok = close_px < vwap_val
        ema_long_ok = (not self.ctx['REQUIRE_PRICE_ABOVE_EMAS']) or (close_px >= ef and close_px >= es)
        ema_short_ok = (not self.ctx['REQUIRE_PRICE_ABOVE_EMAS']) or (close_px <= ef and close_px <= es)
        oc = self.ctx['get_open_orders_count'](self.contract_id)
        if self.ctx['DEBUG'] and (cross_up or cross_dn):
            logging.info(
                "Cross detected dir=%s | cooldown_ok=%s openOrders=%d vwap_ok=%s/%s ema_ok=%s/%s confirmBars=%d",
                "UP" if cross_up else "DOWN",
                str((now - self._last_signal_ts) >= self.ctx['TRADE_COOLDOWN_SEC']),
                oc,
                str(vwap_long_ok), str(vwap_short_ok),
                str(close_px >= ef and close_px >= es),
                str(close_px <= ef and close_px <= es),
                int(self.ctx['CONFIRM_BARS'])
            )
        if self.ctx['CONFIRM_BARS'] and self.ctx['CONFIRM_BARS'] > 0:
            # If forcing cross-up, bypass confirmation waits and enter immediately (still respecting gating)
            if self.ctx.get('FORCE_CROSS_UP'):
                if vwap_long_ok and ema_long_ok and (now - self._last_signal_ts >= self.ctx['TRADE_COOLDOWN_SEC']):
                    self._last_signal_ts = now
                    pos_s = self._position_qty_sign()
                    # If already long, skip to avoid stacking; if short/orders exist, reverse then enter
                    if pos_s > 0:
                        logging.info("Skip LONG (force): already long position present")
                        self._last_rel = rel
                        return
                    if bool(self.ctx.get('REVERSE_ON_CROSS', True)) and ((oc > 0) or (pos_s < 0)):
                        if not self._ensure_flat_and_cleared():
                            logging.info("Reverse: not flat/clear yet; skipping new entry")
                            self._last_rel = rel
                            return
                        oc = self.ctx['get_open_orders_count'](self.contract_id)
                    logging.info("Forced LONG entry (forceCrossUp enabled); skipping confirmation")
                    self._place_market_with_brackets(side=0, op=close_px, atr_val=atr_val)
                else:
                    if self.ctx['DEBUG']:
                        reasons = []
                        if not vwap_long_ok: reasons.append("vwap_long")
                        if not ema_long_ok: reasons.append("ema_long")
                        if (now - self._last_signal_ts) < self.ctx['TRADE_COOLDOWN_SEC']: reasons.append("cooldown")
                        oc = self.ctx['get_open_orders_count'](self.contract_id)
                        if oc > 0: reasons.append(f"openOrders={oc}")
                        logging.info("Forced LONG skipped due to: %s", ", ".join(reasons) or "unknown")
                self._last_rel = rel
                return
            if cross_up:
                self._pending_signal = {"dir": "long", "bars_left": int(self.ctx['CONFIRM_BARS'])}
                logging.info("Cross detected LONG; confirming over %d bars", int(self.ctx['CONFIRM_BARS']))
            elif cross_dn:
                self._pending_signal = {"dir": "short", "bars_left": int(self.ctx['CONFIRM_BARS'])}
                logging.info("Cross detected SHORT; confirming over %d bars", int(self.ctx['CONFIRM_BARS']))
            if self._pending_signal:
                dir_ = self._pending_signal.get("dir")
                if (dir_ == "long" and rel <= 0) or (dir_ == "short" and rel >= 0):
                    logging.info("Cross confirmation cancelled: relation flipped back")
                    self._pending_signal = None
                else:
                    self._pending_signal["bars_left"] = int(self._pending_signal["bars_left"]) - 1
                    if self._pending_signal["bars_left"] <= 0:
                        if (dir_ == "long" and vwap_long_ok and ema_long_ok) or (dir_ == "short" and vwap_short_ok and ema_short_ok):
                            if now - self._last_signal_ts >= self.ctx['TRADE_COOLDOWN_SEC']:
                                self._last_signal_ts = now
                                if dir_ == "long":
                                    pos_s = self._position_qty_sign()
                                    if pos_s > 0:
                                        logging.info("Skip LONG: already long position present")
                                        self._last_rel = rel
                                        return
                                    if bool(self.ctx.get('REVERSE_ON_CROSS', True)) and ((oc > 0) or (pos_s < 0)):
                                        if not self._ensure_flat_and_cleared():
                                            logging.info("Reverse: not flat/clear yet; skipping new LONG entry")
                                            self._last_rel = rel
                                            return
                                        oc = self.ctx['get_open_orders_count'](self.contract_id)
                                    logging.info("Entering LONG after %d-bar confirmation", int(self.ctx['CONFIRM_BARS']))
                                    self._place_market_with_brackets(side=0, op=close_px, atr_val=atr_val)
                                elif (not self.ctx['LONG_ONLY']) and dir_ == "short":
                                    pos_s = self._position_qty_sign()
                                    if pos_s < 0:
                                        logging.info("Skip SHORT: already short position present")
                                        self._last_rel = rel
                                        return
                                    if bool(self.ctx.get('REVERSE_ON_CROSS', True)) and ((oc > 0) or (pos_s > 0)):
                                        if not self._ensure_flat_and_cleared():
                                            logging.info("Reverse: not flat/clear yet; skipping new SHORT entry")
                                            self._last_rel = rel
                                            return
                                        oc = self.ctx['get_open_orders_count'](self.contract_id)
                                    logging.info("Entering SHORT after %d-bar confirmation", int(self.ctx['CONFIRM_BARS']))
                                    self._place_market_with_brackets(side=1, op=close_px, atr_val=atr_val)
                            else:
                                logging.info("Skip: cooldown active during confirmation entry")
                        else:
                            if self.ctx['DEBUG']:
                                logging.info(
                                    "Confirmation gating failed | vwap_ok=%s/%s ema_ok=%s/%s",
                                    str(vwap_long_ok), str(vwap_short_ok), str(ema_long_ok), str(ema_short_ok)
                                )
                        self._pending_signal = None
        else:
            if cross_up and vwap_long_ok and ema_long_ok and (now - self._last_signal_ts >= self.ctx['TRADE_COOLDOWN_SEC']):
                self._last_signal_ts = now
                pos_s = self._position_qty_sign()
                if pos_s > 0:
                    logging.info("Skip LONG: already long position present")
                    self._last_rel = rel
                    return
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
                logging.info(
                    "Placing LONG on cross | close=%.4f ef=%.4f es=%.4f prev_rel=%.5f rel=%.5f",
                    close_px, ef, es, float(prev_rel) if prev_rel is not None else float('nan'), rel
                )
                self._place_market_with_brackets(side=0, op=close_px, atr_val=atr_val)
            elif (not self.ctx['LONG_ONLY']) and cross_dn and vwap_short_ok and ema_short_ok and (now - self._last_signal_ts >= self.ctx['TRADE_COOLDOWN_SEC']):
                self._last_signal_ts = now
                pos_s = self._position_qty_sign()
                if pos_s < 0:
                    logging.info("Skip SHORT: already short position present")
                    self._last_rel = rel
                    return
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
                logging.info(
                    "Placing SHORT on cross | close=%.4f ef=%.4f es=%.4f prev_rel=%.5f rel=%.5f",
                    close_px, ef, es, float(prev_rel) if prev_rel is not None else float('nan'), rel
                )
                self._place_market_with_brackets(side=1, op=close_px, atr_val=atr_val)
            else:
                if self.ctx['DEBUG'] and (cross_up or cross_dn):
                    reasons = []
                    if cross_up:
                        if not vwap_long_ok: reasons.append("vwap_long")
                        if not ema_long_ok: reasons.append("ema_long")
                    if cross_dn:
                        if not vwap_short_ok: reasons.append("vwap_short")
                        if not ema_short_ok: reasons.append("ema_short")
                        if self.ctx.get('LONG_ONLY'):
                            reasons.append("long_only")
                    if self.ctx.get('TRADING_DISABLED'):
                        reasons.append("trading_disabled")
                    if (now - self._last_signal_ts) < self.ctx['TRADE_COOLDOWN_SEC']:
                        reasons.append("cooldown")
                    if oc > 0:
                        reasons.append(f"openOrders={oc}")
                    logging.info("Skipped entry on cross due to: %s", ", ".join(reasons) or "unknown")
        self._last_rel = rel

    def _place_market_with_brackets(self, side: int, op: float, atr_val: float):
        """Place a market entry and immediately attach a native trailing stop."""
        # Safety: always attempt to cancel open orders and flatten any position
        # before placing a fresh entry when enabled in context. This guards
        # against sticky orders/positions on the venue.
        try:
            if bool(self.ctx.get('ALWAYS_FLATTEN_BEFORE_ENTRY', False)):
                ok = self._ensure_flat_and_cleared(timeout_sec=5.0)
                if not ok:
                    logging.info("Pre-entry flatten/cancel did not complete; skip new entry")
                    return
        except Exception:
            pass
        token = self.ctx['get_token']()
        if not token:
            logging.error("Auto-trade auth failed")
            return

        if not bool(self.ctx.get('TRAILING_STOP_ENABLED', True)):
            # If synthetic trailing is enabled, place entry + fixed TP/SL and let the monitor trail the stop.
            if bool(self.ctx.get('SYNTH_TRAILING_ENABLED', False)):
                cm = self.ctx['contract_map'].get(self.symbol) or {}
                tick_size = float(cm.get("tickSize") or 0.0)
                decimals = int(cm.get("decimalPlaces") or 2)

                # Pine-like stop distances for initial SL
                stop_points = float(abs(self.ctx.get('ATR_TRAIL_K_LONG', 1.5) * atr_val)) if side == 0 else float(abs(self.ctx.get('ATR_TRAIL_K_SHORT', 1.0) * atr_val))
                rp = self.ctx['risk_per_point'](self.symbol, self.contract_id)
                risk_dollars = self.ctx['get_risk_dollars']()
                try:
                    base_contracts = int(max(0, int(risk_dollars / max(1e-6, stop_points * rp))))
                except Exception:
                    base_contracts = 0
                if side == 0:
                    size = min(self.ctx['CONTRACT_SIZE_MAX'], base_contracts)
                else:
                    size_long = min(self.ctx['CONTRACT_SIZE_MAX'], base_contracts)
                    size = min(int(max(0, int(size_long * self.ctx['SHORT_SIZE_FACTOR']))), self.ctx['CONTRACT_SIZE_MAX'])
                if size < 1:
                    logging.info("Skip auto-entry (synth): size < 1 | stop_pts=%.4f $/pt=%.4f risk=%.2f", stop_points, rp, risk_dollars)
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
                entry_payload = {
                    "accountId": self.ctx['ACCOUNT_ID'],
                    "contractId": self.contract_id,
                    "type": 2,
                    "side": side,
                    "size": size,
                    "customTag": self._unique_tag("ema_cross_synth"),
                }
                entry = self.ctx['api_post'](token, "/api/Order/place", entry_payload)
                if not entry.get("success") or not entry.get("orderId"):
                    logging.error("Auto entry (synth) failed: %s", entry)
                    return
                # increment daily contracts counter on success
                try:
                    self.ctx['contracts_traded_today'] = int(self.ctx.get('contracts_traded_today') or 0) + int(size)
                except Exception:
                    pass
                entry_id = entry.get("orderId")
                # Compute Pine-matching TP distances
                if side == 0:
                    tp_points = float(stop_points) * 3.0
                    sl_px = float(op) - float(stop_points)
                    tp_px = float(op) + float(tp_points)
                else:
                    tp_points = float(stop_points) * 2.0
                    sl_px = float(op) + float(stop_points)
                    tp_px = float(op) - float(tp_points)
                if tick_size and tick_size > 0:
                    sl_px = self.ctx['snap_to_tick'](sl_px, tick_size, decimals)
                    tp_px = self.ctx['snap_to_tick'](tp_px, tick_size, decimals)
                # Place TP then SL as OCO
                tp_order = self.ctx['api_post'](token, "/api/Order/place", {
                    "accountId": self.ctx['ACCOUNT_ID'],
                    "contractId": self.contract_id,
                    "type": 1,
                    "side": 1 - side,
                    "size": size,
                    "limitPrice": tp_px,
                    "linkedOrderId": entry_id,
                })
                sl_order = self.ctx['api_post'](token, "/api/Order/place", {
                    "accountId": self.ctx['ACCOUNT_ID'],
                    "contractId": self.contract_id,
                    "type": 4,
                    "side": 1 - side,
                    "size": size,
                    "stopPrice": sl_px,
                    "linkedOrderId": entry_id,
                })
                tp_id = tp_order.get("orderId") if tp_order.get("success") else None
                sl_id = sl_order.get("orderId") if sl_order.get("success") else None
                if entry_id and (tp_id or sl_id):
                    try:
                        self.ctx['oco_orders'][entry_id] = [tp_id, sl_id]
                    except Exception:
                        pass
                # Active entry so trailing monitor can take over
                entry_info = {
                    "symbol": self.symbol,
                    "contractId": self.contract_id,
                    "side": side,
                    "size": size,
                    "entry": float(op),
                    "stop_points": float(stop_points),
                    "tp_id": tp_id,
                    "sl_id": sl_id,
                    "native_trail": False,
                    "tickSize": float(tick_size),
                    "decimals": int(decimals),
                }
                self.ctx['active_entries'][entry_id] = entry_info
                logging.info("Auto-entry (synth) placed: id=%s size=%d side=%s SL=%.4f TP=%.4f", str(entry_id), int(size), ("BUY" if side==0 else "SELL"), sl_px, tp_px)
                return
            else:
                logging.warning("Trailing stop disabled and synthetic trailing off; skipping auto-entry")
                return

        cm = self.ctx['contract_map'].get(self.symbol) or {}
        tick_size = float(cm.get("tickSize") or 0.0)
        decimals = int(cm.get("decimalPlaces") or 2)

        try:
            trail_ticks_cfg = int(self.ctx.get('TRAIL_TICKS_FIXED', 5) or 5)
        except Exception:
            trail_ticks_cfg = 5
        trail_ticks_cfg = int(max(1, trail_ticks_cfg))

        # Convert ticks to price distance for sizing and logging
        if tick_size and tick_size > 0:
            stop_points = float(trail_ticks_cfg) * float(tick_size)
        else:
            stop_points = float(abs(atr_val)) if atr_val else 1.0

        rp = self.ctx['risk_per_point'](self.symbol, self.contract_id)
        risk_dollars = self.ctx['get_risk_dollars']()
        try:
            base_contracts = int(max(0, int(risk_dollars / max(1e-6, stop_points * rp))))
        except Exception:
            base_contracts = 0
        if side == 0:
            size = min(self.ctx['CONTRACT_SIZE_MAX'], base_contracts)
        else:
            size_long = min(self.ctx['CONTRACT_SIZE_MAX'], base_contracts)
            size = min(int(max(0, int(size_long * self.ctx['SHORT_SIZE_FACTOR']))), self.ctx['CONTRACT_SIZE_MAX'])
        if size < 1:
            logging.info(
                "Skip auto-entry: computed size < 1 (trail_ticks=%d stop_pts=%.4f $/pt=%.4f risk=%.2f)",
                int(trail_ticks_cfg), stop_points, rp, risk_dollars
            )
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
        entry_payload = {
            "accountId": self.ctx['ACCOUNT_ID'],
            "contractId": self.contract_id,
            "type": 2,
            "side": side,
            "size": size,
            "customTag": self._unique_tag("ema_cross_native"),
        }
        entry = self.ctx['api_post'](token, "/api/Order/place", entry_payload)
        if not entry.get("success") or not entry.get("orderId"):
            logging.error("Auto entry order failed: %s", entry)
            try:
                self.ctx['LAST_ORDER_FAIL'] = {
                    'when': int(time.time()),
                    'reason': entry.get('errorMessage') or 'unknown',
                    'accountId': self.ctx.get('ACCOUNT_ID'),
                    'contractId': self.contract_id,
                    'side': side,
                    'size': size,
                    'op': op,
                    'trailTicks': trail_ticks_cfg,
                    'response': entry,
                }
            except Exception:
                pass
            return

        entry_id = entry.get("orderId")
        logging.info("Market entry placed: id=%s side=%s size=%d trail_ticks=%d", str(entry_id), "BUY" if side == 0 else "SELL", int(size), int(trail_ticks_cfg))
        # increment daily contracts counter on success
        try:
            self.ctx['contracts_traded_today'] = int(self.ctx.get('contracts_traded_today') or 0) + int(size)
        except Exception:
            pass

        # Place the native trailing stop (type 5)
        sl_order = None
        try:
            sl_order = self._place_trailing_order(token, entry_id, side, size, int(trail_ticks_cfg),
                                                 entry_price=float(op),
                                                 tick_size=tick_size if tick_size else None,
                                                 decimals=decimals if decimals is not None else None)
        except Exception as exc:
            logging.warning("Failed to place native trailing stop for %s: %s", str(entry_id), exc)
            sl_order = None

        sl_id = None
        if sl_order and sl_order.get("success") and sl_order.get("orderId"):
            sl_id = sl_order.get("orderId")
            logging.info("Native trailing stop placed: entry=%s trail_order=%s", str(entry_id), str(sl_id))
        else:
            logging.warning("Native trailing stop placement failed for %s: %s", str(entry_id), sl_order)

        # Record active position
        cm = self.ctx['contract_map'].get(self.symbol) or {}
        entry_info = {
            "symbol": self.symbol,
            "contractId": self.contract_id,
            "side": side,
            "size": size,
            "entry": op,
            "stop_points": float(stop_points),
            "tp_id": None,
            "sl_id": sl_id,
            "native_trail": bool(sl_id),
            "tickSize": float(cm.get("tickSize") or 0.0),
            "decimals": int(cm.get("decimalPlaces") or 2),
        }

        # Ensure there's no OCO tracking when we don't create TP legs
        if entry_id in self.ctx['oco_orders']:
            try:
                del self.ctx['oco_orders'][entry_id]
            except Exception:
                pass

        self.ctx['active_entries'][entry_id] = entry_info

    def _place_trailing_order(self, token: str, linked_id: int, side: int, size: int, t_ticks: int,
                              entry_price: float = None, tick_size: float = None, decimals: int = None):
        """Try native trailing stop (type 5) with several field variants.
        Returns the response dict on success; otherwise None.
        """
        try:
            base = {
                "accountId": self.ctx['ACCOUNT_ID'],
                "contractId": self.contract_id,
                "type": 5,
                "side": 1 - side,
                "size": int(size),
                "linkedOrderId": linked_id,
            }
            # Compute price-based distance and an initial stop level if we have entry/tick
            offset_points = None
            trail_points = None
            stop_init = None
            try:
                if tick_size and tick_size > 0 and entry_price is not None:
                    offset_points = float(abs(int(t_ticks))) * float(tick_size)
                    stop_init = float(entry_price - offset_points) if side == 0 else float(entry_price + offset_points)
                    if decimals is not None:
                        stop_init = round(stop_init, int(decimals))
                    try:
                        trail_points = float(abs(int(t_ticks))) * float(tick_size)
                        if decimals is not None:
                            trail_points = round(trail_points, int(decimals))
                    except Exception:
                        trail_points = None
            except Exception:
                offset_points = None
                stop_init = None
            # Build attempt list, honoring a preferred variant if one was recorded
            preferred = str(self.ctx.get('TRAIL_VARIANT') or '').strip().lower()
            # Diagnostic buffer of trail attempts
            trail_log = self.ctx.setdefault('LAST_TRAIL_ATTEMPTS', [])
            attempts = []
            # 1) trailTicks (signed)
            try:
                t_signed = -int(t_ticks) if side == 0 else int(t_ticks)
            except Exception:
                t_signed = int(t_ticks)
            # Prepare attempt builders
            if True:
                attempts.append(("trailticks", lambda: (dict(base, trailTicks=int(t_signed)), None)))
            if stop_init is not None:
                # Prefer absolute start price. Try distancePrice then trailPrice.
                attempts.append(("distanceprice", lambda: (dict(base, distancePrice=float(stop_init)), None)))
                attempts.append(("trailprice", lambda: (dict(base, trailPrice=float(stop_init)), None)))
            if trail_points is not None:
                attempts.append(("traildistance", lambda: (dict(base, trailDistance=float(trail_points)), None)))
                attempts.append(("distance", lambda: (dict(base, distance=float(trail_points)), None)))
                attempts.append(("distance+traildistance", lambda: (dict(base, distance=float(trail_points), trailDistance=float(trail_points)), None)))
            else:
                attempts.append(("traildistance", lambda: (dict(base, trailDistance=int(max(1, int(t_ticks)))), None)))
                attempts.append(("distance", lambda: (dict(base, distance=int(max(1, int(t_ticks)))), None)))
                attempts.append(("distance+traildistance", lambda: (dict(base, distance=int(max(1, int(t_ticks))), trailDistance=int(max(1, int(t_ticks)))), None)))
            attempts.append(("distanceticks", lambda: (dict(base, distanceTicks=int(max(1, int(t_ticks)))), None)))
            attempts.append(("trailingdistance", lambda: (dict(base, trailingDistance=int(max(1, int(t_ticks)))), None)))
            # Reorder to prefer the known-good variant
            if preferred:
                attempts.sort(key=lambda x: (x[0] != preferred,))
            for name, build in attempts:
                try:
                    payload, _ = build()
                    r = self.ctx['api_post'](token, "/api/Order/place", payload)
                    # record attempt (trim to last 50)
                    try:
                        rec = {
                            'ts': int(time.time()),
                            'variant': name,
                            'payloadKeys': sorted(list(payload.keys())),
                            'trailFields': {k: payload.get(k) for k in ('trailTicks','trailDistance','distance','distanceTicks','trailingDistance','trailPrice','distancePrice','stopPrice') if k in payload},
                            'resp': {'success': r.get('success'), 'orderId': r.get('orderId'), 'errorCode': r.get('errorCode'), 'errorMessage': r.get('errorMessage')},
                            'linkedId': linked_id,
                        }
                        trail_log.append(rec)
                        if len(trail_log) > 50:
                            del trail_log[:len(trail_log)-50]
                        self.ctx['LAST_TRAIL_ATTEMPTS'] = trail_log
                    except Exception:
                        pass
                    if r.get("success") and r.get("orderId"):
                        self.ctx['TRAIL_VARIANT'] = name
                        logging.info("Native TRAIL accepted using variant=%s", name)
                        return r
                except Exception:
                    continue
        except Exception:
            pass
        return None

    def _ingest(self, t: dt.datetime, price: float, vol: float):
        self.ctx['last_price'][self.symbol] = price
        # Optional tick log throttle to reduce console spam
        try:
            throttle = int(self.ctx.get('PRICE_LOG_THROTTLE_SEC', 0) or 0)
        except Exception:
            throttle = 0
        do_log = False
        if throttle and throttle > 0:
            now_ts = time.time()
            if (now_ts - self._last_price_log_ts) >= throttle:
                self._last_price_log_ts = now_ts
                do_log = True
        # Only log if throttled; otherwise suppress per-tick price spam
        if do_log:
            try:
                bal = self.ctx['account_snapshot'].get("balance")
                eq = self.ctx['account_snapshot'].get("equity")
                st = self.ctx['indicator_state'].get(self.symbol) or {}
                ef = st.get("emaFastReal") or st.get("emaFast")
                es = st.get("emaSlowReal") or st.get("emaSlow")
                if ef is not None and es is not None:
                    logging.info(
                        f"{self.symbol} price: {price:.2f} | EMA{self.ctx['EMA_SHORT']}={self.ctx['fmt_num'](ef)} EMA{self.ctx['EMA_LONG']}={self.ctx['fmt_num'](es)} | bal={self.ctx['fmt_num'](bal)} eq={self.ctx['fmt_num'](eq)}"
                    )
                else:
                    logging.info(f"{self.symbol} price: {price:.2f} | bal={self.ctx['fmt_num'](bal)} eq={self.ctx['fmt_num'](eq)}")
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
            if self.ctx['INTRABAR_CROSS']:
                try:
                    st = self.ctx['indicator_state'].get(self.symbol) or {}
                    ef_prev = st.get("emaFast")
                    es_prev = st.get("emaSlow")
                    av = self.atr.value
                    if (ef_prev is None) or (es_prev is None) or (av is None):
                        return
                    prev_rel = self._last_rel
                    if prev_rel is None:
                        return
                    alpha_f = 2.0 / float(self.ctx['EMA_SHORT'] + 1)
                    alpha_s = 2.0 / float(self.ctx['EMA_LONG'] + 1)
                    ef_tick = alpha_f * float(price) + (1.0 - alpha_f) * float(ef_prev)
                    es_tick = alpha_s * float(price) + (1.0 - alpha_s) * float(es_prev)
                    rel_tick = ef_tick - es_tick
                    cross_up, cross_dn = self._calc_cross(prev_rel, rel_tick, bool(self.ctx.get('STRICT_CROSS_ONLY')))
                    if not (cross_up or cross_dn):
                        return
                    if getattr(self, "_last_intrabar_minute", None) == self.cur_minute:
                        return
                    vwap_val = st.get("vwap")
                    vwap_long_ok = True
                    vwap_short_ok = True
                    if self.ctx['USE_VWAP'] and vwap_val is not None:
                        vwap_long_ok = price > float(vwap_val)
                        vwap_short_ok = price < float(vwap_val)
                    ema_long_ok = (not self.ctx['REQUIRE_PRICE_ABOVE_EMAS']) or (price >= ef_tick and price >= es_tick)
                    ema_short_ok = (not self.ctx['REQUIRE_PRICE_ABOVE_EMAS']) or (price <= ef_tick and price <= es_tick)
                    now = time.time()
                    if (now - self._last_signal_ts) < self.ctx['TRADE_COOLDOWN_SEC']:
                        return
                    try:
                        min_rb = int(self.ctx.get('MIN_REAL_BARS_BEFORE_TRADING', 0) or 0)
                        if self._real_bars < min_rb:
                            return
                    except Exception:
                        pass
                    oc = self.ctx['get_open_orders_count'](self.contract_id)
                    pos_s = self._position_qty_sign()
                    # No stacking: block adds in same direction
                    if cross_up and pos_s > 0:
                        return
                    if cross_dn and pos_s < 0:
                        return
                    # Reverse handling: if opposite pos or open orders exist, flatten/cancel when enabled
                    if cross_up and bool(self.ctx.get('REVERSE_ON_CROSS', True)) and ((oc > 0) or (pos_s < 0)):
                        if not self._ensure_flat_and_cleared():
                            return
                        oc = self.ctx['get_open_orders_count'](self.contract_id)
                        try:
                            delay = float(self.ctx.get('REVERSE_ENTRY_DELAY_SEC', 0.0) or 0.0)
                        except Exception:
                            delay = 0.0
                        if delay > 0:
                            try:
                                self._reverse_guard_until = time.time() + delay
                            except Exception:
                                pass
                            return
                    if cross_dn and (not self.ctx['LONG_ONLY']) and bool(self.ctx.get('REVERSE_ON_CROSS', True)) and ((oc > 0) or (pos_s > 0)):
                        if not self._ensure_flat_and_cleared():
                            return
                        oc = self.ctx['get_open_orders_count'](self.contract_id)
                        try:
                            delay = float(self.ctx.get('REVERSE_ENTRY_DELAY_SEC', 0.0) or 0.0)
                        except Exception:
                            delay = 0.0
                        if delay > 0:
                            try:
                                self._reverse_guard_until = time.time() + delay
                            except Exception:
                                pass
                            return
                    # Block if any open orders remain after checks
                    if oc > 0:
                        return
                    self._last_intrabar_minute = self.cur_minute
                    if cross_up and vwap_long_ok and ema_long_ok:
                        self._last_signal_ts = now
                        self._place_market_with_brackets(side=0, op=float(price), atr_val=float(av))
                    elif (not self.ctx['LONG_ONLY']) and cross_dn and vwap_short_ok and ema_short_ok:
                        self._last_signal_ts = now
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
            # Synthetic warmup: if no history and allowed, backfill flat bars to seed EMAs
            try:
                if self.ctx.get('ALLOW_SYNTH_WARMUP'):
                    with self.ctx['bars_lock']:
                        existing = list(self.ctx['bars_by_symbol'].get(self.symbol, []))
                    if not existing:
                        minutes = int(self.ctx.get('SYNTH_WARMUP_MINUTES') or 0)
                        if minutes > 0:
                            base_minute = t.replace(second=0, microsecond=0, tzinfo=dt.timezone.utc)
                            synth = []
                            for i in range(minutes, 0, -1):
                                m = base_minute - dt.timedelta(minutes=i)
                                synth.append({
                                    "t": m.isoformat().replace("+00:00", "Z"),
                                    "o": price, "h": price, "l": price, "c": price, "v": 0.0, "syn": True
                                })
                            with self.ctx['bars_lock']:
                                self.ctx['bars_by_symbol'][self.symbol] = synth[-300:]
                            # Compute and store indicators from synthetic bars (ATR left as is)
                            try:
                                snap = self._pd_indicator_snapshot() or {}
                                ef = snap.get("emaFast"); es = snap.get("emaSlow"); vw = snap.get("vwap")
                                if (ef is not None) and (es is not None):
                                    self.ctx['indicator_state'][self.symbol] = {"emaFast": ef, "emaSlow": es, "vwap": vw, "atr": self.atr.value}
                                    logging.info("Synthetic warmup seeded %d flat bars for %s to initialize EMAs", len(synth[-300:]), self.symbol)
                            except Exception:
                                pass
            except Exception:
                pass
            self._ingest(t, price, 0.0)
            self._mark_stream_event()
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
            self._mark_stream_event()
        except Exception:
            pass

    def start(self, token: str):
        if self._started:
            logging.info("Market stream already started; skipping")
            return
        try:
            from signalrcore.hub_connection_builder import HubConnectionBuilder  # type: ignore
        except Exception:
            logging.error("signalrcore not installed. pip install signalrcore")
            return
        base = self.ctx['MARKET_HUB']
        qurl = f"{base}{'&' if '?' in base else '?'}access_token={token}"
        def build(url, headers):
            return (HubConnectionBuilder()
                    .with_url(url, options={"verify_ssl": True, "headers": headers})
                    .with_automatic_reconnect({"type": "raw", "keep_alive_interval": 10, "reconnect_interval": 5, "max_attempts": 0})
                    .build())
        def attach_lifecycle(c):
            try:
                c.on_open(lambda: self._on_open())
                c.on_close(self._on_close)
                c.on_error(lambda data: logging.error(f"Market hub error: {data}"))
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
        try:
            self.ctx['STREAM_CONNECTED'] = True
        except Exception:
            pass
        self._mark_stream_event()
        if self.conn and not self._subscribed:
            try:
                self.conn.send("SubscribeContractQuotes", [self.contract_id])
                self.conn.send("SubscribeContractTrades", [self.contract_id])
                self._subscribed = True
                logging.info(f"Subscribed market stream for {self.symbol} / {self.contract_id}")
            except Exception as e:
                logging.error(f"Subscribe failed: {e}")

    def _on_close(self):
        try:
            self.ctx['STREAM_CONNECTED'] = False
        except Exception:
            pass
        logging.info("Market hub closed")
