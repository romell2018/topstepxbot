import argparse
import datetime as dt
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from topstepx_bot.indicators import ATR
from topstepx_bot.utils import snap_to_tick
from topstepx_bot.api import get_token as _api_get_token, api_post as _api_post


def _load_config() -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
    except Exception as e:
        raise RuntimeError("PyYAML is required to load config.yaml. Please install pyyaml.") from e
    backend_dir = Path(__file__).resolve().parent
    cfg_path = backend_dir / "config.yaml"
    with cfg_path.open() as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise RuntimeError("Configuration is not a mapping")
    return data


def _get_api_context(cfg: Dict[str, Any]) -> Tuple[str, str, str]:
    api_base = cfg.get("api_base") or cfg.get("API_URL") or "https://api.topstepx.com"
    auth = (cfg.get("auth") or cfg.get("topstepx") or {})
    username = cfg.get("username") or auth.get("username") or auth.get("email") or ""
    api_key = cfg.get("api_key") or auth.get("api_key") or ""
    return api_base, username, api_key


def _history_fetch(api_url: str, token: str, contract_id: str, start: dt.datetime, end: dt.datetime,
                   unit: int = 2, unit_n: int = 1, live: bool = True,
                   include_partial: bool = True, limit: int = 20000) -> List[Dict[str, Any]]:
    payload = {
        "contractId": contract_id,
        "live": bool(live),
        "startTime": start.astimezone(dt.timezone.utc).replace(microsecond=0, tzinfo=None).isoformat() + "Z",
        "endTime": end.astimezone(dt.timezone.utc).replace(microsecond=0, tzinfo=None).isoformat() + "Z",
        "unit": int(unit),
        "unitNumber": int(unit_n),
        "limit": int(limit),
        "includePartialBar": bool(include_partial),
    }
    j = _api_post(api_url, token, "/api/History/retrieveBars", payload) or {}
    items = j.get("bars", j.get("candles", [])) or []
    if (not items) and bool(live):
        payload2 = dict(payload)
        payload2["live"] = False
        j = _api_post(api_url, token, "/api/History/retrieveBars", payload2) or {}
        items = j.get("bars", j.get("candles", [])) or []
    if not items:
        payload3 = dict(payload)
        payload3.update({
            "startTime": (end - dt.timedelta(hours=24)).astimezone(dt.timezone.utc).replace(microsecond=0, tzinfo=None).isoformat() + "Z",
            "live": False,
        })
        j = _api_post(api_url, token, "/api/History/retrieveBars", payload3) or {}
        items = j.get("bars", j.get("candles", [])) or []
    return items


def _bars_to_df(items: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
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
            v = float(b.get("v") or b.get("volume") or 0.0)
            rows.append({"time": t, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except Exception:
            continue
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values("time")
    df = df.set_index(pd.to_datetime(df["time"], utc=True))
    df = df.drop(columns=["time"])  # keep DatetimeIndex
    return df


@dataclass
class Trade:
    time: pd.Timestamp
    side: str  # 'long' or 'short'
    entry: float
    exit: Optional[float]
    size: int
    reason: Optional[str] = None
    tp: Optional[float] = None
    sl: Optional[float] = None
    pnl: Optional[float] = None
    entry_comm: float = 0.0
    exit_comm: float = 0.0


def _summarize(trades: List[Trade], initial_cash: float) -> Dict[str, Any]:
    pnl_list = [t.pnl for t in trades if t.pnl is not None]
    gross = float(np.nansum(pnl_list)) if pnl_list else 0.0
    wins = [p for p in pnl_list if p is not None and p > 0]
    losses = [p for p in pnl_list if p is not None and p <= 0]
    win_rate = (len(wins) / len(pnl_list)) * 100.0 if pnl_list else 0.0
    avg_win = float(np.nanmean(wins)) if wins else 0.0
    avg_loss = float(np.nanmean(losses)) if losses else 0.0
    rr = (abs(avg_win / avg_loss) if (avg_loss != 0) else np.nan) if (avg_win != 0 or avg_loss != 0) else np.nan
    return {
        "trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": win_rate,
        "net_pnl": gross,
        "ending_cash": initial_cash + gross,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "reward_risk": rr,
    }


def _local_parts(ts: pd.Timestamp, tzname: str) -> Tuple[int, int]:
    try:
        from zoneinfo import ZoneInfo
        tloc = ts.astimezone(ZoneInfo(tzname))
    except Exception:
        tloc = ts
    return int(tloc.hour), int(tloc.minute)


def backtest_tjr_asia_sweep(
    df: pd.DataFrame,
    tzname: str,
    tick_size: float,
    price_decimals: int,
    risk_per_point: float,
    commission_per_contract_side: float,
    # Session params
    asian_start_hour: int = 22,
    asian_end_hour: int = 3,
    london_start_hour: int = 3,
    london_end_hour: int = 8,
    cutoff_minutes: int = 60,
    force_london_close: bool = True,
    # Risk + sizing
    use_atr_sl: bool = False,
    atr_length: int = 14,
    atr_multiplier: float = 2.0,
    sizing_method: str = "USD Risk",  # or "Fixed Contracts"
    usd_risk_per_trade: float = 100.0,
    fixed_contracts: float = 1.0,
    contract_size_max: int = 50,
) -> Tuple[List[Trade], pd.Series]:
    atr = ATR(int(atr_length))
    trades: List[Trade] = []
    equity = 0.0
    eq_curve: List[Tuple[pd.Timestamp, float]] = []

    # Session state
    asian_active_prev = False
    london_active_prev = False

    asian_session_high: Optional[float] = None
    asian_session_low: Optional[float] = None
    asian_absolute_high: Optional[float] = None
    asian_absolute_low: Optional[float] = None
    high_broken = False
    low_broken = False

    london_session_high: Optional[float] = None
    london_session_low: Optional[float] = None

    breakout_direction: Optional[str] = None  # 'bullish' or 'bearish'
    last_hh_level: Optional[float] = None
    last_hl_level: Optional[float] = None
    last_ll_level: Optional[float] = None
    last_lh_level: Optional[float] = None
    last_swing_high: Optional[float] = None
    last_swing_low: Optional[float] = None
    last_high_bar_idx: Optional[int] = None
    last_low_bar_idx: Optional[int] = None
    structure_count = 0
    last_structure_type: Optional[str] = None  # 'HH','HL','LL','LH'
    most_recent_hl: Optional[float] = None
    most_recent_lh: Optional[float] = None
    most_recent_hl_idx: Optional[int] = None
    most_recent_lh_idx: Optional[int] = None
    bos_detected = False
    trade_taken = False

    pos: Optional[Trade] = None

    for i in range(len(df)):
        ts = df.index[i]
        row = df.iloc[i]
        o = float(row.get('open'))
        h = float(row.get('high'))
        l = float(row.get('low'))
        c = float(row.get('close'))

        av = atr.update(h, l, c)

        hour, minute = _local_parts(ts, tzname)
        prev_hour, _ = _local_parts(df.index[i-1], tzname) if i > 0 else (hour, minute)

        asian_active = (hour >= asian_start_hour) or (hour < asian_end_hour)
        london_active = (hour >= london_start_hour) and (hour < london_end_hour)

        asian_start = asian_active and (not asian_active_prev)
        asian_end = (not asian_active) and asian_active_prev
        london_start = london_active and (not london_active_prev)
        london_end = (not london_active) and london_active_prev

        # Update session states
        if asian_active:
            asian_session_high = h if asian_session_high is None else max(asian_session_high, h)
            asian_session_low = l if asian_session_low is None else min(asian_session_low, l)
            asian_absolute_high = h if asian_absolute_high is None else max(asian_absolute_high, h)
            asian_absolute_low = l if asian_absolute_low is None else min(asian_absolute_low, l)

        if london_active:
            london_session_high = h if london_session_high is None else max(london_session_high, h)
            london_session_low = l if london_session_low is None else min(london_session_low, l)

        # On Asian end, reset break flags for new phase
        if asian_end:
            high_broken = False
            low_broken = False

        # Detect first break of Asian range during London
        if london_active and (asian_session_high is not None) and (asian_session_low is not None) and (not high_broken) and (not low_broken):
            if h > float(asian_session_high):
                high_broken = True
                breakout_direction = 'bullish'
                last_swing_high = float(asian_session_high)
                last_swing_low = float(asian_session_low)
                last_high_bar_idx = i
                structure_count = 0
            elif l < float(asian_session_low):
                low_broken = True
                breakout_direction = 'bearish'
                last_swing_high = float(asian_session_high)
                last_swing_low = float(asian_session_low)
                last_low_bar_idx = i
                structure_count = 0

        # Market structure tracking after breakout (no BoS yet)
        if london_active and breakout_direction and (not bos_detected) and i > 0:
            prev = df.iloc[i-1]
            prev_o = float(prev.get('open')); prev_c = float(prev.get('close'))
            prev_h = float(prev.get('high')); prev_l = float(prev.get('low'))

            if breakout_direction == 'bullish':
                # HH detection
                pattern_high = max(prev_h, h)
                prev_hh = last_hh_level if last_hh_level is not None else (last_swing_high or pattern_high)
                cond_seq = (last_structure_type is None) or (last_structure_type == 'HL')
                if cond_seq and (prev_c > prev_o) and (c < o) and (pattern_high > float(prev_hh)) and (prev_c > float(prev_hh)):
                    is_too_close = (last_high_bar_idx is not None) and ((i - int(last_high_bar_idx)) <= 4)
                    should_create = True
                    if is_too_close and (structure_count > 0) and (last_hh_level is not None) and (pattern_high <= float(last_hh_level)):
                        should_create = False
                    if should_create:
                        structure_count += 1
                        last_hh_level = float(pattern_high)
                        last_swing_high = float(pattern_high)
                        last_high_bar_idx = i
                        last_structure_type = 'HH'

                # HL detection (only if last was HH)
                pattern_low = min(prev_l, l)
                prev_hl = last_hl_level if last_hl_level is not None else (last_swing_low or pattern_low)
                if (last_structure_type == 'HH') and (prev_c < prev_o) and (c > o) and (pattern_low > float(prev_hl)) and (prev_c > float(prev_hl)):
                    is_too_close = (last_low_bar_idx is not None) and ((i - int(last_low_bar_idx)) <= 4)
                    should_create = True
                    if is_too_close and (last_hl_level is not None) and (pattern_low <= float(last_hl_level)):
                        should_create = False
                    if should_create:
                        structure_count += 1
                        last_hl_level = float(pattern_low)
                        most_recent_hl = float(pattern_low)
                        most_recent_hl_idx = i - 1
                        last_low_bar_idx = i
                        last_structure_type = 'HL'

            elif breakout_direction == 'bearish':
                # LL detection
                pattern_low = min(prev_l, l)
                prev_ll = last_ll_level if last_ll_level is not None else (last_swing_low or pattern_low)
                cond_seq = (last_structure_type is None) or (last_structure_type == 'LH')
                if cond_seq and (prev_c < prev_o) and (c > o) and (pattern_low < float(prev_ll)) and (prev_c < float(prev_ll)):
                    is_too_close = (last_low_bar_idx is not None) and ((i - int(last_low_bar_idx)) <= 4)
                    should_create = True
                    if is_too_close and (structure_count > 0) and (last_ll_level is not None) and (pattern_low >= float(last_ll_level)):
                        should_create = False
                    if should_create:
                        structure_count += 1
                        last_ll_level = float(pattern_low)
                        last_swing_low = float(pattern_low)
                        last_low_bar_idx = i
                        last_structure_type = 'LL'

                # LH detection (only if last was LL)
                pattern_high = max(prev_h, h)
                prev_lh = last_lh_level if last_lh_level is not None else (last_swing_high or pattern_high)
                if (last_structure_type == 'LL') and (prev_c > prev_o) and (c < o) and (pattern_high < float(prev_lh)) and (prev_c < float(prev_lh)):
                    is_too_close = (last_high_bar_idx is not None) and ((i - int(last_high_bar_idx)) <= 4)
                    should_create = True
                    if is_too_close and (last_lh_level is not None) and (pattern_high >= float(last_lh_level)):
                        should_create = False
                    if should_create:
                        structure_count += 1
                        last_lh_level = float(pattern_high)
                        most_recent_lh = float(pattern_high)
                        most_recent_lh_idx = i - 1
                        last_high_bar_idx = i
                        last_structure_type = 'LH'

        # Minutes remaining in London session to gate new entries
        london_end_minutes = int(london_end_hour) * 60
        current_minutes = int(hour) * 60 + int(minute)
        london_remaining = london_end_minutes - current_minutes
        if london_remaining < 0:
            london_remaining += 24 * 60
        allow_new_trades = london_remaining > int(cutoff_minutes)

        # BoS detection and entry (only once per London session)
        if (not bos_detected) and (not trade_taken) and london_active and allow_new_trades and breakout_direction:
            if (breakout_direction == 'bullish') and (most_recent_hl is not None) and (most_recent_hl_idx is not None):
                if (c < float(most_recent_hl)) and ((i - int(most_recent_hl_idx)) >= 4):
                    # SELL entry
                    if london_session_high is not None and asian_absolute_low is not None:
                        stop_loss = (c + float(av) * float(atr_multiplier)) if use_atr_sl and (av is not None) else float(london_session_high)
                        take_profit = float(asian_absolute_low)
                        entry_px = c
                        # position size
                        if sizing_method.lower().startswith('usd'):
                            stop_dist = abs(entry_px - stop_loss)
                            size = int(max(0, int(usd_risk_per_trade // max(1e-9, stop_dist * float(risk_per_point)))))
                        else:
                            size = int(max(1, int(fixed_contracts)))
                        size = max(0, min(size, int(contract_size_max)))
                        if size >= 1:
                            pos = Trade(time=ts, side='short', entry=snap_to_tick(entry_px, tick_size, price_decimals), exit=None,
                                        size=size, reason='BoS Sell', tp=snap_to_tick(take_profit, tick_size, price_decimals),
                                        sl=snap_to_tick(stop_loss, tick_size, price_decimals), entry_comm=commission_per_contract_side*size)
                            trade_taken = True
                            bos_detected = True

            elif (breakout_direction == 'bearish') and (most_recent_lh is not None) and (most_recent_lh_idx is not None):
                if (c > float(most_recent_lh)) and ((i - int(most_recent_lh_idx)) >= 4):
                    # BUY entry
                    if london_session_low is not None and asian_absolute_high is not None:
                        stop_loss = (c - float(av) * float(atr_multiplier)) if use_atr_sl and (av is not None) else float(london_session_low)
                        take_profit = float(asian_absolute_high)
                        entry_px = c
                        if sizing_method.lower().startswith('usd'):
                            stop_dist = abs(entry_px - stop_loss)
                            size = int(max(0, int(usd_risk_per_trade // max(1e-9, stop_dist * float(risk_per_point)))))
                        else:
                            size = int(max(1, int(fixed_contracts)))
                        size = max(0, min(size, int(contract_size_max)))
                        if size >= 1:
                            pos = Trade(time=ts, side='long', entry=snap_to_tick(entry_px, tick_size, price_decimals), exit=None,
                                        size=size, reason='BoS Buy', tp=snap_to_tick(take_profit, tick_size, price_decimals),
                                        sl=snap_to_tick(stop_loss, tick_size, price_decimals), entry_comm=commission_per_contract_side*size)
                            trade_taken = True
                            bos_detected = True

        # Manage open position by bar extremes
        if pos is not None:
            exit_price = None
            exit_reason = None
            if pos.side == 'long':
                if (pos.tp is not None) and (h >= float(pos.tp)):
                    exit_price = float(pos.tp); exit_reason = 'TP'
                elif (pos.sl is not None) and (l <= float(pos.sl)):
                    exit_price = float(pos.sl); exit_reason = 'SL'
            else:
                if (pos.tp is not None) and (l <= float(pos.tp)):
                    exit_price = float(pos.tp); exit_reason = 'TP'
                elif (pos.sl is not None) and (h >= float(pos.sl)):
                    exit_price = float(pos.sl); exit_reason = 'SL'

            # Force close at London end
            if (exit_price is None) and force_london_close and london_end:
                exit_price = c; exit_reason = 'LondonClose'

            if exit_price is not None:
                pos.exit = snap_to_tick(float(exit_price), tick_size, price_decimals)
                pos.exit_comm = float(commission_per_contract_side) * float(pos.size)
                pts = (pos.exit - pos.entry) * (1.0 if pos.side == 'long' else -1.0)
                gross = float(pts) * float(risk_per_point) * float(pos.size)
                comm = float(pos.entry_comm + pos.exit_comm)
                pos.pnl = gross - comm
                trades.append(pos)
                equity += float(pos.pnl)
                pos = None

        # Reset state at new Asian session start
        if asian_start:
            asian_session_high = None
            asian_session_low = None
            asian_absolute_high = None
            asian_absolute_low = None
            high_broken = False
            low_broken = False
            london_session_high = None
            london_session_low = None
            breakout_direction = None
            last_hh_level = None
            last_hl_level = None
            last_ll_level = None
            last_lh_level = None
            last_swing_high = None
            last_swing_low = None
            last_high_bar_idx = None
            last_low_bar_idx = None
            structure_count = 0
            last_structure_type = None
            most_recent_hl = None
            most_recent_lh = None
            most_recent_hl_idx = None
            most_recent_lh_idx = None
            bos_detected = False
            trade_taken = False

        eq_curve.append((ts, equity))
        asian_active_prev = asian_active
        london_active_prev = london_active

    # Close any remaining open position at last close
    if pos is not None:
        last_ts = df.index[-1]
        last_close = float(df.iloc[-1]['close'])
        pos.exit = snap_to_tick(last_close, tick_size, price_decimals)
        pos.exit_comm = float(commission_per_contract_side) * float(pos.size)
        pts = (pos.exit - pos.entry) * (1.0 if pos.side == 'long' else -1.0)
        gross = float(pts) * float(risk_per_point) * float(pos.size)
        comm = float(pos.entry_comm + pos.exit_comm)
        pos.pnl = gross - comm
        trades.append(pos)
        equity += float(pos.pnl)
        eq_curve.append((last_ts, equity))

    eq_series = pd.Series({t: v for t, v in eq_curve}).sort_index()
    return trades, eq_series


def main():
    ap = argparse.ArgumentParser(description="Backtest TJR Asia Session Sweep strategy using TopstepX history API")
    ap.add_argument("--symbol", default="MNQ", help="Symbol to backtest (default MNQ)")
    ap.add_argument("--days", type=int, default=7, help="Number of days to fetch (default 7)")
    ap.add_argument("--cash", type=float, default=50000.0, help="Initial cash for reporting (default 50000)")
    ap.add_argument("--commission", type=float, default=0.74, help="Commission per contract per side (default 0.74)")
    ap.add_argument("--tz", type=str, default=None, help="Timezone for sessions (default config.tz)")
    # Session params
    ap.add_argument("--asian-start", type=int, default=22, help="Asian session start hour (0-23)")
    ap.add_argument("--asian-end", type=int, default=3, help="Asian session end hour (0-23)")
    ap.add_argument("--london-start", type=int, default=3, help="London session start hour (0-23)")
    ap.add_argument("--london-end", type=int, default=8, help="London session end hour (0-23)")
    ap.add_argument("--cutoff-minutes", type=int, default=60, help="Minutes before London end to stop new trades")
    ap.add_argument("--force-london-close", action="store_true", help="Force close any open trade at London session end")
    # Risk + sizing
    ap.add_argument("--use-atr-sl", action="store_true", help="Use ATR*multiplier for stop-loss instead of session high/low")
    ap.add_argument("--atr-length", type=int, default=14, help="ATR length")
    ap.add_argument("--atr-multiplier", type=float, default=2.0, help="ATR multiplier for stop-loss")
    ap.add_argument("--sizing", type=str, default="USD Risk", choices=["USD Risk", "Fixed Contracts"], help="Position sizing method")
    ap.add_argument("--usd-risk", type=float, default=100.0, help="USD risk per trade (for USD Risk sizing)")
    ap.add_argument("--fixed-contracts", type=float, default=1.0, help="Fixed number of contracts (for Fixed sizing)")
    ap.add_argument("--max-contracts", type=int, default=50, help="Contract size cap")
    ap.add_argument("--print-count", type=int, default=50, help="Number of trades to print")
    ap.add_argument("--export-csv", type=str, default=None, help="Write trade ledger to CSV at this path")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    cfg = _load_config()
    tzname = args.tz or cfg.get("tz") or "America/New_York"

    instr = cfg.get("instrument", {})
    tick_size = float(instr.get("tickSize", 0.25))
    price_decimals = int(instr.get("decimalPlaces", 2)) if instr.get("decimalPlaces") is not None else (2 if tick_size >= 0.01 else 4)
    risk_per_point = float(instr.get("riskPerPoint", 5.0))

    symbol = (args.symbol or cfg.get("symbol") or "MNQ").upper()
    contract_id = str(cfg.get("CONTRACT_ID") or (cfg.get("auth", {}).get("CONTRACT_ID")))
    if not contract_id:
        raise SystemExit("CONTRACT_ID is required in Backend/config.yaml for history fetch.")

    api_url, username, api_key = _get_api_context(cfg)
    logging.info("Fetching %d days of 1m bars for %s (contract %s)", int(args.days), symbol, contract_id)
    token = _api_get_token(api_url, username, api_key)
    if not token:
        raise SystemExit("Failed to authenticate to TopstepX API. Check credentials in config.yaml")

    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(days=max(1, int(args.days)))
    items = _history_fetch(api_url, token, contract_id, start, end, unit=2, unit_n=1, live=True, include_partial=True)
    if not items:
        raise SystemExit("No history returned from API. Try again later or adjust config.")
    df = _bars_to_df(items)
    if df is None or df.empty:
        raise SystemExit("No valid bars parsed for backtest.")

    trades, eq = backtest_tjr_asia_sweep(
        df=df,
        tzname=tzname,
        tick_size=tick_size,
        price_decimals=price_decimals,
        risk_per_point=risk_per_point,
        commission_per_contract_side=float(args.commission),
        asian_start_hour=int(args.asian_start),
        asian_end_hour=int(args.asian_end),
        london_start_hour=int(args.london_start),
        london_end_hour=int(args.london_end),
        cutoff_minutes=int(args.cutoff_minutes),
        force_london_close=bool(args.force_london_close),
        use_atr_sl=bool(args.use_atr_sl),
        atr_length=int(args.atr_length),
        atr_multiplier=float(args.atr_multiplier),
        sizing_method=str(args.sizing),
        usd_risk_per_trade=float(args.usd_risk),
        fixed_contracts=float(args.fixed_contracts),
        contract_size_max=int(args.max_contracts),
    )

    summary = _summarize(trades, initial_cash=float(args.cash))
    print("\nTJR Asia Sweep – Backtest Summary")
    print("---------------------------------")
    print(f"Symbol: {symbol} | Days: {int(args.days)} | Bars: {len(df)}")
    print(f"Trades: {summary['trades']} | Wins: {summary['wins']} | Losses: {summary['losses']} | Win%: {summary['win_rate_pct']:.1f}")
    print(f"Net PnL: ${summary['net_pnl']:.2f} | End Cash: ${summary['ending_cash']:.2f}")
    print(f"Avg Win: ${summary['avg_win']:.2f} | Avg Loss: ${summary['avg_loss']:.2f} | R/R: {summary['reward_risk']}")

    # Daily PnL breakdown
    if (eq is not None) and (not eq.empty):
        try:
            eq_daily = eq.copy()
            try:
                eq_daily.index = eq_daily.index.tz_convert(tzname)
            except Exception:
                pass
            daily_end = eq_daily.resample('D').last()
            if daily_end is not None and len(daily_end) > 0:
                daily_pnl = daily_end.diff()
                if pd.isna(daily_pnl.iloc[0]):
                    daily_pnl.iloc[0] = float(daily_end.iloc[0]) - 0.0
                print("\nDaily PnL")
                print("---------")
                for ts_i, val in daily_pnl.items():
                    day = ts_i.date() if hasattr(ts_i, 'date') else str(ts_i)
                    print(f"{day}: ${float(val):,.2f}")
        except Exception as e:
            print(f"Failed to compute daily PnL: {e}")

    # Export trades CSV
    if args.export_csv and trades:
        try:
            out_path = Path(args.export_csv).expanduser()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            rows = []
            for t in trades:
                try:
                    ts_local = t.time.tz_convert(tzname) if t.time.tzinfo is not None else t.time
                except Exception:
                    ts_local = t.time
                rows.append({
                    'time': str(ts_local),
                    'side': t.side,
                    'size': t.size,
                    'entry': t.entry,
                    'exit': t.exit,
                    'reason': t.reason,
                    'tp': t.tp,
                    'sl': t.sl,
                    'pnl': t.pnl,
                    'entry_comm': t.entry_comm,
                    'exit_comm': t.exit_comm,
                })
            pd.DataFrame(rows).to_csv(out_path, index=False)
            print(f"\nWrote trades CSV: {out_path}")
        except Exception as e:
            print(f"\nFailed to export trades CSV: {e}")


if __name__ == "__main__":
    main()

