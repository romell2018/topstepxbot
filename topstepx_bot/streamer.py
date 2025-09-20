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
                    cross_up = (prev_rel is not None) and (float(prev_rel) <= 0) and (rel > 0)
                    cross_dn = (prev_rel is not None) and (float(prev_rel) >= 0) and (rel < 0)
                    logging.info(
                        f"{self.symbol} bar_close {self.cur_bar.get('t')} | prev_rel={prev_rel} rel={rel} cross_up={cross_up} cross_dn={cross_dn}"
                    )
                except Exception:
                    pass
            self._last_rel = prev_rel
            if (ef is not None) and (es is not None):
                self.ctx['indicator_state'][self.symbol] = {"emaFast": ef, "emaSlow": es, "vwap": vw, "atr": av}
                logging.info(
                    f"{self.symbol} close={c:.2f} | EMA{self.ctx['EMA_SHORT']}={ef:.2f} EMA{self.ctx['EMA_LONG']}={es:.2f} VWAP={(vw if vw is not None else float('nan')):.2f}"
                )
                self._maybe_trade(c, float(ef), float(es), vw, av)
        except Exception:
            pass
        self.cur_bar = None

    def _maybe_trade(self, close_px: float, ef: float, es: float, vwap_val: Optional[float], atr_val: Optional[float]):
        now = time.time()
        if atr_val is None or ef is None or es is None:
            return
        rel = ef - es
        prev_rel = self._last_rel
        cross_up = (prev_rel is not None) and (prev_rel <= 0) and (rel > 0)
        cross_dn = (prev_rel is not None) and (prev_rel >= 0) and (rel < 0)
        vwap_long_ok = True
        vwap_short_ok = True
        if self.ctx['USE_VWAP'] and vwap_val is not None:
            vwap_long_ok = close_px > vwap_val
            vwap_short_ok = close_px < vwap_val
        ema_long_ok = (not self.ctx['REQUIRE_PRICE_ABOVE_EMAS']) or (close_px >= ef and close_px >= es)
        ema_short_ok = (not self.ctx['REQUIRE_PRICE_ABOVE_EMAS']) or (close_px <= ef and close_px <= es)
        oc = self.ctx['get_open_orders_count'](self.contract_id)
        if oc > 0:
            self._last_rel = rel
            if self.ctx['DEBUG']:
                logging.info("Skip entry: open orders for this contract=%d", oc)
            return
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
                                    logging.info("Entering LONG after %d-bar confirmation", int(self.ctx['CONFIRM_BARS']))
                                    self._place_market_with_brackets(side=0, op=close_px, atr_val=atr_val)
                                elif (not self.ctx['LONG_ONLY']) and dir_ == "short":
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
                logging.info(
                    "Placing LONG on cross | close=%.4f ef=%.4f es=%.4f prev_rel=%.5f rel=%.5f",
                    close_px, ef, es, float(prev_rel) if prev_rel is not None else float('nan'), rel
                )
                self._place_market_with_brackets(side=0, op=close_px, atr_val=atr_val)
            elif (not self.ctx['LONG_ONLY']) and cross_dn and vwap_short_ok and ema_short_ok and (now - self._last_signal_ts >= self.ctx['TRADE_COOLDOWN_SEC']):
                self._last_signal_ts = now
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
                    if (now - self._last_signal_ts) < self.ctx['TRADE_COOLDOWN_SEC']:
                        reasons.append("cooldown")
                    if oc > 0:
                        reasons.append(f"openOrders={oc}")
                    logging.info("Skipped entry on cross due to: %s", ", ".join(reasons) or "unknown")
        self._last_rel = rel

    def _place_market_with_brackets(self, side: int, op: float, atr_val: float):
        token = self.ctx['get_token']()
        if not token:
            logging.error("Auto-trade auth failed")
            return
        cm = self.ctx['contract_map'].get(self.symbol) or {}
        tick_size = float(cm.get("tickSize") or 0.0)
        decimals = int(cm.get("decimalPlaces") or 2)
        if self.ctx['USE_FIXED_TARGETS'] and self.ctx['FIXED_TP_POINTS'] > 0 and self.ctx['FIXED_SL_POINTS'] > 0:
            if side == 0:
                stop_points = float(self.ctx['FIXED_SL_POINTS'])
                tgt_points = float(self.ctx['FIXED_TP_POINTS'])
                sl = op - stop_points
                tp = op + tgt_points
            else:
                stop_points = float(self.ctx['FIXED_SL_POINTS'])
                tgt_points = float(self.ctx['FIXED_TP_POINTS'])
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
        if self.ctx['PAD_TICKS'] and tick_size and tick_size > 0:
            pad_dist = self.ctx['PAD_TICKS'] * tick_size
            if side == 0:
                sl -= pad_dist
                tp += pad_dist
            else:
                sl += pad_dist
                tp -= pad_dist
        sl = self.ctx['snap_to_tick'](sl, tick_size, decimals)
        tp = self.ctx['snap_to_tick'](tp, tick_size, decimals)
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
                "Skip auto-entry: computed size < 1 (atr=%.4f stop_pts=%.4f $/pt=%.4f risk=%.2f)",
                atr_val, stop_points, rp, risk_dollars
            )
            return
        logging.info(
            "Auto-entry signal side=%s op=%.2f size=%d SL=%.2f TP=%.2f (stop_pts=%.4f $/pt=%.4f)",
            "BUY" if side == 0 else "SELL", op, size, sl, tp, stop_points, rp
        )
        # Try bracket-style payload first if enabled
        entry_payload = {
            "accountId": self.ctx['ACCOUNT_ID'],
            "contractId": self.contract_id,
            "type": 2,
            "side": side,
            "size": size,
            "customTag": f"ema_cross_auto_{int(time.time())}",
        }
        used_brackets_payload = False
        if self.ctx.get('USE_BRACKETS_PAYLOAD') and tick_size and tick_size > 0:
            try:
                sl_ticks = int(max(1, round(abs(op - sl) / tick_size)))
                tp_ticks = int(max(1, round(abs(tp - op) / tick_size)))
                entry_payload["stopLossBracket"] = {"ticks": sl_ticks, "type": 0}
                entry_payload["takeProfitBracket"] = {"ticks": tp_ticks, "type": 0}
                if use_trail and trail_points is not None and trail_points > 0:
                    entry_payload["trailPrice"] = trail_points
                resp_try = self.ctx['api_post'](token, "/api/Order/place", entry_payload)
                if resp_try.get("success") and resp_try.get("orderId"):
                    entry = resp_try
                    used_brackets_payload = True
                    logging.info(
                        "Bracket entry accepted: id=%s side=%s size=%d sl_ticks=%d tp_ticks=%d trail=%s",
                        str(entry.get("orderId")),
                        ("BUY" if side == 0 else "SELL"),
                        int(size),
                        int(sl_ticks),
                        int(tp_ticks),
                        (str(trail_points) if (use_trail and trail_points is not None) else "None"),
                    )
                else:
                    logging.info("Bracket entry not accepted, falling back to linked TP/SL: resp=%s", str(resp_try))
                    entry = resp_try
            except Exception:
                entry = self.ctx['api_post'](token, "/api/Order/place", entry_payload)
        else:
            entry = self.ctx['api_post'](token, "/api/Order/place", entry_payload)
        entry_id = entry.get("orderId")
        if not entry.get("success") or not entry_id:
            logging.error("Auto entry order failed: %s", entry)
            return
        use_trail = bool(self.ctx['TRAILING_STOP_ENABLED'])
        trail_points = (atr_val * (self.ctx['ATR_TRAIL_K_LONG'] if side == 0 else self.ctx['ATR_TRAIL_K_SHORT'])) if use_trail else None
        if trail_points and tick_size and tick_size > 0:
            trail_points = self.ctx['snap_to_tick'](trail_points, tick_size, decimals)
        if used_brackets_payload:
            # Venue manages OCO; record active entry with no explicit child IDs
            cm = self.ctx['contract_map'].get(self.symbol) or {}
            self.ctx['active_entries'][entry_id] = {
                "symbol": self.symbol,
                "contractId": self.contract_id,
                "side": side,
                "size": size,
                "entry": op,
                "stop_points": float(stop_points),
                "tp_id": None,
                "sl_id": None,
                "be_done": True,  # skip break-even adjustments when venue manages bracket
                "tickSize": float(cm.get("tickSize") or 0.0),
                "decimals": int(cm.get("decimalPlaces") or 2),
            }
            logging.info("Recorded venue-managed bracket for entry=%s (no child IDs)", str(entry_id))
        else:
            threading.Thread(
                target=self._place_brackets_sync,
                args=(token, entry_id, side, size, tp, sl, use_trail, trail_points, op, stop_points),
                daemon=True,
            ).start()

    def _place_brackets_sync(self, token: str, entry_id: Any, side: int, size: int, tp: float, sl: float,
                              use_trail: bool, trail_points: Optional[float], entry_price: float, stop_points: float):
        try:
            time.sleep(0.25)
            try:
                linked_id = int(str(entry_id))
            except Exception:
                linked_id = entry_id
            logging.info("Placing TP limit @ %.4f linked to %s", tp, entry_id)
            tp_payload = {
                "accountId": self.ctx['ACCOUNT_ID'],
                "contractId": self.contract_id,
                "type": 1,
                "side": 1 - side,
                "size": size,
                "limitPrice": tp,
                "linkedOrderId": linked_id,
            }
            tp_order = self.ctx['api_post'](token, "/api/Order/place", tp_payload)
            if not tp_order.get("success") or not tp_order.get("orderId"):
                logging.warning(f"TP place failed: payload={tp_payload} resp={tp_order}")
                return
            time.sleep(0.25)
            sl_order = None
            if use_trail and trail_points is not None and trail_points > 0:
                logging.info("Placing TRAIL distance=%.4f linked to %s", trail_points, entry_id)
                trail_payload = {
                    "accountId": self.ctx['ACCOUNT_ID'],
                    "contractId": self.contract_id,
                    "type": 5,
                    "side": 1 - side,
                    "size": size,
                    "trailPrice": trail_points,
                    "linkedOrderId": linked_id,
                }
                sl_order = self.ctx['api_post'](token, "/api/Order/place", trail_payload)
                if not sl_order.get("success") or not sl_order.get("orderId"):
                    logging.warning(f"TRAIL place failed, falling back to STOP: payload={trail_payload} resp={sl_order}")
                    sl_order = None
            if sl_order is None:
                logging.info("Placing SL stop @ %.4f linked to %s", sl, entry_id)
                sl_payload = {
                    "accountId": self.ctx['ACCOUNT_ID'],
                    "contractId": self.contract_id,
                    "type": 4,
                    "side": 1 - side,
                    "size": size,
                    "stopPrice": sl,
                    "linkedOrderId": linked_id,
                }
                sl_order = self.ctx['api_post'](token, "/api/Order/place", sl_payload)
                if not sl_order.get("success") or not sl_order.get("orderId"):
                    logging.warning(f"SL place failed: payload={sl_payload} resp={sl_order}")
                    try:
                        self.ctx['cancel_order'](token, self.ctx['ACCOUNT_ID'], tp_order.get("orderId"))
                    except Exception:
                        pass
                    return
            self.ctx['oco_orders'][entry_id] = [tp_order.get("orderId"), sl_order.get("orderId")]
            cm = self.ctx['contract_map'].get(self.symbol) or {}
            self.ctx['active_entries'][entry_id] = {
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
            logging.info("Auto OCO placed: TP_id=%s SL/TRAIL_id=%s", tp_order.get("orderId"), sl_order.get("orderId"))
            if not tp_order.get("success") or not sl_order.get("success"):
                logging.warning("Bracket responses: TP=%s SL/TRAIL=%s", tp_order, sl_order)
        except Exception as e:
            logging.warning("Auto bracket placement failed: %s", e)

    def _ingest(self, t: dt.datetime, price: float, vol: float):
        self.ctx['last_price'][self.symbol] = price
        try:
            bal = self.ctx['account_snapshot'].get("balance")
            eq = self.ctx['account_snapshot'].get("equity")
            st = self.ctx['indicator_state'].get(self.symbol) or {}
            ef = st.get("emaFast")
            es = st.get("emaSlow")
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
                    cross_up = (prev_rel <= 0) and (rel_tick > 0)
                    cross_dn = (prev_rel >= 0) and (rel_tick < 0)
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
                    oc = self.ctx['get_open_orders_count'](self.contract_id)
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
                c.on_close(lambda: logging.info("Market hub closed"))
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
        if self.conn and not self._subscribed:
            try:
                self.conn.send("SubscribeContractQuotes", [self.contract_id])
                self.conn.send("SubscribeContractTrades", [self.contract_id])
                self._subscribed = True
                logging.info(f"Subscribed market stream for {self.symbol} / {self.contract_id}")
            except Exception as e:
                logging.error(f"Subscribe failed: {e}")
