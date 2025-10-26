"""
Copyright 2025, MetaQuotes Ltd.
https://www.mql5.com

MetaTrader 5 Python strategy script implementing the Duvenchy MNQ EMA cross
logic (translation of the Pine-style strategy used in this repo):

- Entry: EMA(9) cross EMA(21), optional VWAP and price-vs-EMA gating
- Sizing: floor(RiskPerTradeUSD / (stopPoints * RiskPerPoint)), capped by ContractSizeMax
          stopPoints = 2*ATR (long) or 1.5*ATR (short) from last closed bar
- Exits: initial SL/TP (3R long, 2R short); optional breakeven; optional ATR trailing
- Safety: cooldown; reverse-flatten; single position per symbol; hour window

Adjust SYMBOL and broker symbol mapping to your account (e.g., 'MNQZ5', 'NQ', etc.).
"""

from datetime import datetime, timezone
import time
import math
from dataclasses import dataclass
from typing import Optional, Tuple

import MetaTrader5 as mt5
import tensorflow as tf  # template import (unused)
import numpy as np
import matplotlib.pyplot as plt  # template import (unused)


# =============== Inputs ===============
SYMBOL = "MNQZ5"             # Adjust to your broker's symbol
TIMEFRAME = mt5.TIMEFRAME_M1  # Strategy designed for 1m; can change

EMA_SHORT = 9
EMA_LONG = 21
ATR_LENGTH = 14
USE_VWAP = False
REQUIRE_PRICE_ABOVE_EMAS = False
LONG_ONLY = False

RISK_PER_TRADE_USD = 500.0
RISK_PER_POINT = 2.0          # MNQ ~ $2/point; derive from tick if needed
CONTRACT_SIZE_MAX = 15
SHORT_SIZE_FACTOR = 0.75

TRADE_COOLDOWN_SEC = 30
CONFIRM_BARS = 0
ENABLE_BREAKEVEN = True
BE_PAD_TICKS = 0
ENABLE_TRAILING = True
ATR_TRAIL_K_LONG = 1.5
ATR_TRAIL_K_SHORT = 1.0

START_HOUR = 0               # inclusive
END_HOUR = 24                # exclusive; supports wrap if start > end
DEBUG = True


# =============== Helpers ===============
def symbol_select(sym: str) -> bool:
    info = mt5.symbol_info(sym)
    if info is None:
        return False
    if not info.visible:
        return mt5.symbol_select(sym, True)
    return True


def tick_size(sym: str) -> float:
    info = mt5.symbol_info(sym)
    if not info:
        return 0.0
    return float(info.trade_tick_size or info.point or 0.0)


def digits(sym: str) -> int:
    info = mt5.symbol_info(sym)
    if not info:
        return 2
    return int(info.digits)


def snap_to_tick(sym: str, price: float) -> float:
    step = tick_size(sym)
    d = digits(sym)
    if step and step > 0:
        ticks = round(price / step)
        return round(ticks * step, d)
    return round(price, d)


def now_hour_local() -> int:
    return datetime.now().hour


def within_hours(h_start: int, h_end: int) -> bool:
    h = now_hour_local()
    if h_start <= h_end:
        return h_start <= h < h_end
    return h >= h_start or h < h_end


def ema_pine(x: np.ndarray, n: int) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    out = np.full_like(x, np.nan, dtype=float)
    if n <= 0 or len(x) == 0:
        return out
    if len(x) >= n:
        init = np.nanmean(x[:n])
        out[n - 1] = init
        alpha = 2.0 / float(n + 1)
        for i in range(n, len(x)):
            prev = out[i - 1]
            xi = x[i]
            out[i] = prev if (np.isnan(prev) or np.isnan(xi)) else (alpha * xi + (1.0 - alpha) * prev)
    return out


def atr_wilder(h: np.ndarray, l: np.ndarray, c: np.ndarray, n: int) -> np.ndarray:
    h = np.asarray(h, dtype=float)
    l = np.asarray(l, dtype=float)
    c = np.asarray(c, dtype=float)
    tr = np.maximum(h - l, np.maximum(np.abs(h - np.roll(c, 1)), np.abs(l - np.roll(c, 1))))
    tr[0] = h[0] - l[0]
    out = np.full_like(tr, np.nan, dtype=float)
    if n <= 0 or len(tr) == 0:
        return out
    out[0] = tr[0]
    mult = 1.0 / float(n)
    for i in range(1, len(tr)):
        out[i] = (tr[i] - out[i - 1]) * mult + out[i - 1]
    return out


def vwap_intraday(close: np.ndarray, vol: np.ndarray, times: np.ndarray) -> np.ndarray:
    # Simple per-day cumulative VWAP; resets when date changes (UTC)
    vwap = np.full_like(close, np.nan, dtype=float)
    if len(close) == 0:
        return vwap
    day = None
    pv = 0.0
    vv = 0.0
    for i in range(len(close)):
        t = datetime.fromtimestamp(times[i], tz=timezone.utc).date()
        if (day is None) or (t != day):
            day = t
            pv = 0.0
            vv = 0.0
        c = float(close[i])
        v = float(vol[i])
        pv += c * v
        vv += v
        vwap[i] = (pv / vv) if vv > 0 else np.nan
    return vwap


def calc_cross(prev_rel: float, rel: float, strict: bool = False) -> Tuple[bool, bool]:
    eps = 1e-9
    if strict:
        return (prev_rel < -eps and rel > eps, prev_rel > eps and rel < -eps)
    return (prev_rel <= eps and rel > eps, prev_rel >= -eps and rel < -eps)


def derive_risk_per_point(sym: str) -> float:
    info = mt5.symbol_info(sym)
    if not info:
        return RISK_PER_POINT
    try:
        tv = float(info.trade_tick_value)
        ts = float(info.trade_tick_size or info.point)
        if ts and ts > 0:
            return tv / ts
    except Exception:
        pass
    return RISK_PER_POINT


def round_volume(sym: str, lots: float) -> float:
    info = mt5.symbol_info(sym)
    if not info:
        return 0.0
    step = float(info.volume_step or 0.01)
    vmin = float(info.volume_min or step)
    vmax = float(info.volume_max or max(CONTRACT_SIZE_MAX, 1))
    lots = max(vmin, min(vmax, lots))
    steps = round(lots / step)
    return steps * step


@dataclass
class State:
    last_bar_time: Optional[int] = None
    last_signal_ts: float = 0.0
    be_applied: bool = False
    init_stop_points: float = 0.0
    pending_dir: int = 0  # +1 long, -1 short, 0 none
    pending_bars: int = 0


def log(msg: str):
    if DEBUG:
        print(msg, flush=True)


def close_position(sym: str) -> bool:
    if not mt5.positions_total():
        return True
    if not mt5.positions_get(symbol=sym):
        return True
    pos = mt5.positions_get(symbol=sym)[0]
    volume = pos.volume
    if volume <= 0:
        return True
    action = mt5.ORDER_TYPE_SELL if pos.type == mt5.POSITION_TYPE_BUY else mt5.ORDER_TYPE_BUY
    price = mt5.symbol_info_tick(sym).bid if action == mt5.ORDER_TYPE_BUY else mt5.symbol_info_tick(sym).ask
    req = {
        'action': mt5.TRADE_ACTION_DEAL,
        'symbol': sym,
        'type': action,
        'volume': volume,
        'price': price,
        'deviation': 20,
        'type_filling': mt5.ORDER_FILLING_FOK,
        'comment': 'duvenchy_flatten',
    }
    r = mt5.order_send(req)
    ok = r is not None and r.retcode in (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_PLACED, mt5.TRADE_RETCODE_DONE_PARTIAL)
    if ok:
        log(f"Flattened position {sym} vol={volume}")
    else:
        log(f"Flatten failed: retcode={getattr(r,'retcode',None)}")
    return ok


def modify_sl_tp(sym: str, position_ticket: int, new_sl: Optional[float], new_tp: Optional[float]) -> bool:
    req = {
        'action': mt5.TRADE_ACTION_SLTP,
        'symbol': sym,
        'position': position_ticket,
    }
    if new_sl is not None:
        req['sl'] = new_sl
    if new_tp is not None:
        req['tp'] = new_tp
    r = mt5.order_send(req)
    return r is not None and r.retcode in (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_PLACED)


def apply_be_and_trail(sym: str, st: State):
    positions = mt5.positions_get(symbol=sym)
    if not positions:
        return
    p = positions[0]
    pos_type = p.type
    open_px = p.price_open
    sl = p.sl if p.sl else 0.0
    tp = p.tp if p.tp else 0.0
    digits_v = digits(sym)

    # Init stop points from SL if unknown
    if st.init_stop_points <= 0.0 and sl > 0.0:
        st.init_stop_points = abs(open_px - sl)

    # Breakeven at 50% of target
    if ENABLE_BREAKEVEN and (not st.be_applied) and st.init_stop_points > 0.0:
        target_pts = st.init_stop_points * (3.0 if pos_type == mt5.POSITION_TYPE_BUY else 2.0)
        half = 0.5 * target_pts
        tick = mt5.symbol_info_tick(sym)
        cur = tick.bid if pos_type == mt5.POSITION_TYPE_BUY else tick.ask
        fav_move = (cur - open_px) if pos_type == mt5.POSITION_TYPE_BUY else (open_px - cur)
        if fav_move >= half:
            step = tick_size(sym)
            be_px = open_px
            if BE_PAD_TICKS != 0 and step:
                pad = abs(BE_PAD_TICKS) * step
                be_px = open_px + pad if pos_type == mt5.POSITION_TYPE_BUY else open_px - pad
            be_px = snap_to_tick(sym, be_px)
            improve = (sl <= 0.0) or (be_px > sl if pos_type == mt5.POSITION_TYPE_BUY else be_px < sl)
            if improve:
                if modify_sl_tp(sym, p.ticket, be_px, tp if tp > 0.0 else None):
                    st.be_applied = True
                    log(f"Applied BE SL={be_px:.{digits_v}f}")
                    sl = be_px

    # ATR trailing
    if ENABLE_TRAILING:
        rates = mt5.copy_rates_from_pos(sym, TIMEFRAME, 0, max(ATR_LENGTH + 3, 100))
        if rates is not None and len(rates) >= ATR_LENGTH + 2:
            atr = atr_wilder(rates['high'], rates['low'], rates['close'], ATR_LENGTH)[-2]
            tick = mt5.symbol_info_tick(sym)
            cur = tick.bid if pos_type == mt5.POSITION_TYPE_BUY else tick.ask
            dist = atr * (ATR_TRAIL_K_LONG if pos_type == mt5.POSITION_TYPE_BUY else ATR_TRAIL_K_SHORT)
            target_sl = snap_to_tick(sym, cur - dist if pos_type == mt5.POSITION_TYPE_BUY else cur + dist)
            improve = (sl <= 0.0) or (target_sl > sl if pos_type == mt5.POSITION_TYPE_BUY else target_sl < sl)
            if improve:
                if modify_sl_tp(sym, p.ticket, target_sl, tp if tp > 0.0 else None):
                    log(f"Trail SL -> {target_sl:.{digits_v}f}")


def contracts_from_risk(sym: str, stop_points: float, is_short: bool) -> float:
    rpp = derive_risk_per_point(sym)
    if stop_points <= 0.0 or rpp <= 0.0:
        return 0.0
    base = math.floor(RISK_PER_TRADE_USD / (stop_points * rpp))
    size = base
    if is_short:
        size = math.floor(base * SHORT_SIZE_FACTOR)
    size = min(size, CONTRACT_SIZE_MAX)
    return round_volume(sym, max(0.0, float(size)))


def place_entry(sym: str, is_long: bool, st: State) -> bool:
    # Compute ATR-based stop distance from last closed bar
    rates = mt5.copy_rates_from_pos(sym, TIMEFRAME, 0, max(ATR_LENGTH + 3, 100))
    if rates is None or len(rates) < ATR_LENGTH + 2:
        log("Not enough bars for ATR")
        return False
    atr = atr_wilder(rates['high'], rates['low'], rates['close'], ATR_LENGTH)[-2]
    if not atr or atr <= 0:
        log("ATR not ready; skip entry")
        return False
    stop_pts = (2.0 * atr) if is_long else (1.5 * atr)
    volume = contracts_from_risk(sym, stop_pts, not is_long)
    if volume < (mt5.symbol_info(sym).volume_min or 0.01):
        log("Size < min; skip entry")
        return False
    tick = mt5.symbol_info_tick(sym)
    ask = tick.ask
    bid = tick.bid
    op = ask if is_long else bid
    sl = snap_to_tick(sym, op - stop_pts if is_long else op + stop_pts)
    tp = snap_to_tick(sym, op + stop_pts * 3.0 if is_long else op - stop_pts * 2.0)

    req = {
        'action': mt5.TRADE_ACTION_DEAL,
        'symbol': sym,
        'type': mt5.ORDER_TYPE_BUY if is_long else mt5.ORDER_TYPE_SELL,
        'volume': volume,
        'price': ask if is_long else bid,
        'sl': sl,
        'tp': tp,
        'deviation': 20,
        'type_filling': mt5.ORDER_FILLING_FOK,
        'comment': f'duvenchy_{"long" if is_long else "short"}',
    }
    r = mt5.order_send(req)
    ok = r is not None and r.retcode in (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_PLACED)
    if ok:
        st.last_signal_ts = time.time()
        st.be_applied = False
        st.init_stop_points = abs(op - sl)
        log(f"Entry {('LONG' if is_long else 'SHORT')} vol={volume} SL={sl} TP={tp}")
    else:
        log(f"Entry failed: retcode={getattr(r,'retcode',None)}")
    return ok


def main_loop():
    st = State()
    if not symbol_select(SYMBOL):
        print(f"Symbol not available/visible: {SYMBOL}")
        return
    log(f"Running Duvenchy strategy on {SYMBOL} {TIMEFRAME}")

    while True:
        try:
            # Always manage SL on every tick/loop
            apply_be_and_trail(SYMBOL, st)

            # Get fresh bars
            rates = mt5.copy_rates_from_pos(SYMBOL, TIMEFRAME, 0, 1000)
            if rates is None or len(rates) < max(EMA_LONG + 3, ATR_LENGTH + 3):
                time.sleep(1)
                continue

            last_t = int(rates['time'][-1])
            if st.last_bar_time is None:
                st.last_bar_time = last_t
                time.sleep(1)
                continue
            # Process only on new bar (avoid multi-triggers per bar)
            if last_t == st.last_bar_time:
                time.sleep(0.5)
                continue
            st.last_bar_time = last_t

            if not within_hours(START_HOUR, END_HOUR):
                continue

            close = rates['close']
            high = rates['high']
            low = rates['low']
            vol = rates['tick_volume']
            tarr = rates['time']

            ef = ema_pine(close, EMA_SHORT)
            es = ema_pine(close, EMA_LONG)
            vwap = vwap_intraday(close, vol, tarr) if USE_VWAP else np.full_like(close, np.nan)

            # Use last closed bar (index -2)
            if np.isnan(ef[-2]) or np.isnan(es[-2]) or np.isnan(ef[-3]) or np.isnan(es[-3]):
                continue
            rel = float(ef[-2] - es[-2])
            prev_rel = float(ef[-3] - es[-3])
            cross_up, cross_dn = calc_cross(prev_rel, rel, strict=False)

            close1 = float(close[-2])
            vwap_val = float(vwap[-2]) if USE_VWAP and not math.isnan(vwap[-2]) else None
            vwap_long_ok = True if (not USE_VWAP or vwap_val is None) else (close1 > vwap_val)
            vwap_short_ok = True if (not USE_VWAP or vwap_val is None) else (close1 < vwap_val)
            ema_long_ok = (not REQUIRE_PRICE_ABOVE_EMAS) or (close1 >= float(ef[-2]) and close1 >= float(es[-2]))
            ema_short_ok = (not REQUIRE_PRICE_ABOVE_EMAS) or (close1 <= float(ef[-2]) and close1 <= float(es[-2]))

            # Confirmation handling
            if CONFIRM_BARS > 0:
                if st.pending_bars > 0:
                    st.pending_bars -= 1
                    if st.pending_bars == 0 and st.pending_dir != 0 and (time.time() - st.last_signal_ts) >= TRADE_COOLDOWN_SEC:
                        if st.pending_dir > 0 and vwap_long_ok and ema_long_ok:
                            if not mt5.positions_get(symbol=SYMBOL):
                                place_entry(SYMBOL, True, st)
                        elif st.pending_dir < 0 and (not LONG_ONLY) and vwap_short_ok and ema_short_ok:
                            if not mt5.positions_get(symbol=SYMBOL):
                                place_entry(SYMBOL, False, st)
                        st.pending_dir = 0
                else:
                    if cross_up:
                        st.pending_dir = +1; st.pending_bars = CONFIRM_BARS
                    elif cross_dn and (not LONG_ONLY):
                        st.pending_dir = -1; st.pending_bars = CONFIRM_BARS
                continue

            # Immediate entries
            cooldown_ok = (time.time() - st.last_signal_ts) >= TRADE_COOLDOWN_SEC
            pos = mt5.positions_get(symbol=SYMBOL)
            pos_type = pos[0].type if pos else None

            if cross_up and vwap_long_ok and ema_long_ok and cooldown_ok:
                if pos_type == mt5.POSITION_TYPE_SELL:
                    close_position(SYMBOL)
                if not mt5.positions_get(symbol=SYMBOL):
                    place_entry(SYMBOL, True, st)
                continue

            if (not LONG_ONLY) and cross_dn and vwap_short_ok and ema_short_ok and cooldown_ok:
                if pos_type == mt5.POSITION_TYPE_BUY:
                    close_position(SYMBOL)
                if not mt5.positions_get(symbol=SYMBOL):
                    place_entry(SYMBOL, False, st)
                continue

        except KeyboardInterrupt:
            break
        except Exception as e:
            log(f"Loop error: {e}")
            time.sleep(1)


# =============== Boot/shutdown ===============
if mt5.initialize():
    try:
        main_loop()
    finally:
        mt5.shutdown()
else:
    print("Failed to initialize MetaTrader5 API")
