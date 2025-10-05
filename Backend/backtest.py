import argparse
import datetime as dt
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Local imports
from topstepx_bot.api import get_token as _api_get_token, api_post as _api_post
from topstepx_bot.indicators import ATR, compute_indicators
from topstepx_bot.utils import snap_to_tick

#Backend/.venv/bin/python Backend/backtest.py --symbol MNQ --days 2 --commission 0.74 --native-trailing --trail-ticks 4 --trail-offset-ticks 8 --slippage-ticks 1 --ticks data/MNQ_ticks.jsonl --tz America/New_York

# Backend/.venv/bin/python Backend/backtest.py --symbol MNQ --days 2 --commission 0.74 --print-count 50

# Backend/.venv/bin/python Backend/backtest.py --symbol MNQ --days 2 --commission 0.74 --print-count 50 --export-csv Backend/trades_export.csv


def _load_config() -> Dict[str, Any]:
    """Load config.yaml from Backend directory without adding external deps here."""
    try:
        import yaml  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "PyYAML is required to load config.yaml. Please install pyyaml."
        ) from e
    # Find Backend/config.yaml relative to this file
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
    # Fallback: if live=True returned none, retry with live=False
    if (not items) and bool(live):
        payload2 = dict(payload)
        payload2["live"] = False
        j = _api_post(api_url, token, "/api/History/retrieveBars", payload2) or {}
        items = j.get("bars", j.get("candles", [])) or []
    # Fallback 2: widen to 24h if still empty
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


def _load_ticks_jsonl(path: Path) -> pd.DataFrame:
    """Load ticks from a JSONL file produced by the recorder.
    Expected fields per line: {"ts": iso8601, "price": float, "volume": float}
    Returns a DataFrame sorted by time with columns: time (DatetimeIndex, UTC), price, volume
    """
    import json
    rows: List[Dict[str, Any]] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                ts = obj.get("ts") or obj.get("time") or obj.get("timestamp")
                vol = obj.get("volume") or obj.get("lastSize") or 0.0
                if ts is None:
                    continue
                t = dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(dt.timezone.utc)
                # Accept both trade and quote styles
                rec: Dict[str, Any] = {"time": t, "volume": float(vol)}
                if obj.get("type") == "trade" and obj.get("last") is not None:
                    rec["last"] = float(obj.get("last"))
                else:
                    if obj.get("last") is not None:
                        rec["last"] = float(obj.get("last"))
                if obj.get("bid") is not None:
                    rec["bid"] = float(obj.get("bid"))
                if obj.get("ask") is not None:
                    rec["ask"] = float(obj.get("ask"))
                rows.append(rec)
            except Exception:
                continue
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values("time")
    df = df.set_index(pd.to_datetime(df["time"], utc=True))
    df = df.drop(columns=["time"])  # keep DatetimeIndex
    return df


def _resample_ticks_to_bars(ticks: pd.DataFrame, seconds: int = 1) -> pd.DataFrame:
    """Resample tick stream (last/bid/ask/volume) to OHLCV bars with given second granularity.
    - Price source prefers 'last'; if missing, uses mid(bid,ask); else best available side.
    """
    if ticks.empty:
        return ticks
    rule = f"{int(max(1, seconds))}S"
    # Build a mark price series
    px = None
    if 'last' in ticks:
        px = ticks['last'].copy()
    if px is None or px.isna().all():
        if ('bid' in ticks) and ('ask' in ticks):
            px = ((ticks['bid'].astype(float) + ticks['ask'].astype(float)) / 2.0)
        elif 'bid' in ticks:
            px = ticks['bid'].astype(float)
        elif 'ask' in ticks:
            px = ticks['ask'].astype(float)
        else:
            raise RuntimeError("Ticks do not contain price fields 'last', 'bid', or 'ask'")
    ohlc = px.resample(rule).ohlc()
    vol = (ticks['volume'] if 'volume' in ticks else pd.Series(0.0, index=ticks.index)).resample(rule).sum()
    df = ohlc.join(vol.rename('volume'))
    df = df.dropna(subset=['close'])
    df = df.rename(columns={'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close'})
    return df


def _load_csv_autodetect(path: Path, force_kind: str = "auto", csv_tz: Optional[str] = None) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], str]:
    """Load a CSV as bars or ticks.
    - Bars: columns include open,high,low,close[,volume]
    - Ticks: columns include last or price, and/or bid/ask[,volume]
    - time column: one of time,timestamp,datetime,date_time,date; if absent, try index
    Returns (bars_df, ticks_df, kind)
    """
    import pandas as pd
    df = pd.read_csv(path)
    cols_lower = {c: str(c).strip().lower() for c in df.columns}
    # Identify time column
    time_col = None
    for cand in ("time", "timestamp", "datetime", "date_time", "date"):
        if cand in cols_lower.values():
            # get original name
            for orig, low in cols_lower.items():
                if low == cand:
                    time_col = orig
                    break
            if time_col:
                break
    if time_col is None and not isinstance(df.index, pd.DatetimeIndex):
        raise RuntimeError("CSV must have a time/timestamp column or a datetime index")
    # Normalize time index to UTC
    if time_col is not None:
        ts = pd.to_datetime(df[time_col], errors='coerce', utc=False)
    else:
        ts = pd.to_datetime(df.index, errors='coerce', utc=False)
    if ts.dt.tz is None:
        if csv_tz:
            ts = ts.dt.tz_localize(csv_tz).dt.tz_convert('UTC')
        else:
            ts = ts.dt.tz_localize('UTC')
    else:
        ts = ts.dt.tz_convert('UTC')
    if time_col is not None:
        df = df.drop(columns=[time_col])
    df.index = ts
    df = df.sort_index()
    # Lower-name mapping for detection
    lmap = {str(c).strip().lower(): c for c in df.columns}
    has_ohlc = all(k in lmap for k in ("open", "high", "low", "close"))
    has_ticks = ("last" in lmap) or ("price" in lmap) or ("bid" in lmap) or ("ask" in lmap)
    kind = force_kind
    if force_kind == "auto":
        if has_ohlc:
            kind = "bars"
        elif has_ticks:
            kind = "ticks"
        else:
            raise RuntimeError("Could not detect CSV format; need OHLCV or tick columns")
    if kind == "bars":
        # Build bars dataframe
        bcols = {}
        for k in ("open", "high", "low", "close", "volume"):
            if k in lmap:
                bcols[k] = df[lmap[k]].astype(float)
        bars = pd.DataFrame(bcols)
        return bars, None, "bars"
    else:
        # Build ticks dataframe with last/bid/ask/volume
        tcols = {}
        if "last" in lmap:
            tcols["last"] = df[lmap["last"]].astype(float)
        elif "price" in lmap:
            tcols["last"] = df[lmap["price"]].astype(float)
        if "bid" in lmap:
            tcols["bid"] = df[lmap["bid"]].astype(float)
        if "ask" in lmap:
            tcols["ask"] = df[lmap["ask"]].astype(float)
        if "volume" in lmap:
            tcols["volume"] = df[lmap["volume"]].astype(float)
        ticks = pd.DataFrame(tcols)
        return None, ticks, "ticks"


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
    # Native trailing simulation state
    trailing_active: bool = False
    trailing_stop: Optional[float] = None
    trail_ticks: Optional[int] = None
    breakeven_applied: bool = False


def _calc_cross(prev_rel: Optional[float], rel: Optional[float], strict: bool = False) -> Tuple[bool, bool]:
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


def backtest_week(
    df: pd.DataFrame,
    ema_short: int,
    ema_long: int,
    ema_source: str,
    vwap_enabled: bool,
    atr_length: int,
    risk_per_trade: float,
    risk_per_point: float,
    contract_size_max: int,
    short_size_factor: float,
    tick_size: float,
    price_decimals: int,
    long_only: bool = False,
    strict_cross_only: bool = False,
    reverse_on_cross: bool = True,
    commission_per_contract_side: float = 0.0,
    # Native trailing options
    use_native_trailing: bool = False,
    force_fixed_trail_ticks: bool = True,
    trail_ticks_fixed: Optional[int] = None,
    atr_trail_k_long: float = 1.5,
    atr_trail_k_short: float = 1.0,
    trail_offset_ticks: Optional[int] = None,
    atr_trail_offset_k: float = 0.0,
    pad_ticks: int = 0,
    tzname: str = "America/New_York",
    # Tick-level simulation
    ticks: Optional[pd.DataFrame] = None,
    slippage_ticks: int = 0,
) -> Tuple[List[Trade], pd.Series]:
    # Compute indicators
    ind = compute_indicators(df, ema_short, ema_long, ema_source, rth_only=False, tzname=tzname)
    ef_key = f"ema{ema_short}"
    es_key = f"ema{ema_long}"
    ind["rel"] = ind[ef_key] - ind[es_key]
    ind["prev_rel"] = ind["rel"].shift(1)
    # ATR series
    atr = ATR(atr_length)
    atr_vals: List[float] = []
    prev_c: Optional[float] = None
    for i, row in ind.iterrows():
        h = float(row["high"]) if not pd.isna(row.get("high")) else np.nan
        l = float(row["low"]) if not pd.isna(row.get("low")) else np.nan
        c = float(row["close"]) if not pd.isna(row.get("close")) else np.nan
        if any(map(lambda x: np.isnan(x), (h, l, c))):
            atr_vals.append(np.nan)
            prev_c = c
            continue
        av = atr.update(h, l, c)
        atr_vals.append(np.nan if av is None else float(av))
        prev_c = c
    ind["atr"] = atr_vals

    # Trading state
    trades: List[Trade] = []
    pos: Optional[Trade] = None
    equity = 0.0  # track PnL only; initial cash not needed for sizing in this Pine emulation
    eq_curve: List[Tuple[pd.Timestamp, float]] = []
    pending_entry: Optional[Dict[str, Any]] = None  # defer entries to next bar open

    # Precompute ticks grouped by bar bucket (minute or second) for fast lookup
    ticks_by_bucket: Dict[pd.Timestamp, List[Dict[str, Any]]] = {}
    if ticks is not None and not ticks.empty:
        # Determine bar duration from df
        try:
            if len(df.index) >= 2:
                step = (df.index[1] - df.index[0]).total_seconds()
                bar_seconds = int(max(1, round(step)))
            else:
                bar_seconds = 60
        except Exception:
            bar_seconds = 60
        def _to_bucket(ts: pd.Timestamp) -> pd.Timestamp:
            tsu = ts.tz_convert("UTC") if ts.tzinfo is not None else ts.replace(tzinfo=dt.timezone.utc)
            sec = (tsu.second // bar_seconds) * bar_seconds
            return tsu.replace(second=sec, microsecond=0)
        g = ticks.groupby(ticks.index.map(_to_bucket))
        for bucket, sub in g:
            lst: List[Dict[str, Any]] = []
            for ts_i, row in sub.sort_index().iterrows():
                ev = {"ts": ts_i}
                for k in ("last","bid","ask","volume"):
                    if k in row and not pd.isna(row[k]):
                        ev[k] = float(row[k])
                lst.append(ev)
            ticks_by_bucket[pd.Timestamp(bucket, tz="UTC")] = lst

    def _apply_slippage(px: float, side: str, kind: str) -> float:
        """Apply adverse slippage in ticks depending on fill kind.
        kind in {"entry","stop","target"}
        """
        if not (slippage_ticks and tick_size and tick_size > 0):
            return px
        slip = abs(int(slippage_ticks)) * float(tick_size)
        if side == "long":
            if kind == "entry":
                return px + slip  # pay up
            elif kind == "stop":
                return px - slip  # worse stop
            else:  # target
                return px - slip  # worse target
        else:  # short
            if kind == "entry":
                return px - slip  # sell lower
            elif kind == "stop":
                return px + slip  # worse stop
            else:  # target
                return px + slip  # worse target

    for idx in range(1, len(ind)):
        ts = ind.index[idx]
        bar = ind.iloc[idx]
        prev = ind.iloc[idx - 1]
        open_px = float(bar["open"]) if not pd.isna(bar.get("open")) else None
        close_px = float(bar["close"]) if not pd.isna(bar.get("close")) else None
        high_px = float(bar["high"]) if not pd.isna(bar.get("high")) else None
        low_px = float(bar["low"]) if not pd.isna(bar.get("low")) else None
        ef = float(bar[ef_key]) if not pd.isna(bar.get(ef_key)) else None
        es = float(bar[es_key]) if not pd.isna(bar.get(es_key)) else None
        vwap_val = float(bar["vwap"]) if not pd.isna(bar.get("vwap")) else None
        atr_val = float(bar["atr"]) if not pd.isna(bar.get("atr")) else None
        if None in (close_px, ef, es, atr_val):
            eq_curve.append((ts, equity))
            continue

        # Fill any pending entry at the OPEN of this bar (enter after prior candle close)
        if (pending_entry is not None) and (pos is None):
            try:
                side = str(pending_entry["side"]).lower()
                size = int(pending_entry["size"])
                stop_pts = float(pending_entry["stop_pts"]) if pending_entry.get("stop_pts") is not None else None
                if size >= 1 and stop_pts is not None and stop_pts > 0:
                    # Fill at first event of this minute; prefer quote side (ask for long, bid for short)
                    entry_px_src = None
                    if ticks_by_bucket.get(ts) and len(ticks_by_bucket[ts]) > 0:
                        ev0 = ticks_by_bucket[ts][0]
                        bid0 = ev0.get("bid")
                        ask0 = ev0.get("ask")
                        last0 = ev0.get("last")
                        if side == "long":
                            if ask0 is not None:
                                entry_px_src = float(ask0)
                            elif last0 is not None:
                                entry_px_src = float(last0)
                        else:
                            if bid0 is not None:
                                entry_px_src = float(bid0)
                            elif last0 is not None:
                                entry_px_src = float(last0)
                    if entry_px_src is None and open_px is not None:
                        entry_px_src = float(open_px)
                    if entry_px_src is None:
                        raise RuntimeError("No tick/open price available for entry fill")
                    entry_px = snap_to_tick(entry_px_src, tick_size, price_decimals)
                    entry_px = _apply_slippage(entry_px, side, kind="entry")
                    if side == "long":
                        sl_px = snap_to_tick(entry_px - stop_pts, tick_size, price_decimals)
                        tp_px = snap_to_tick(entry_px + stop_pts * 3.0, tick_size, price_decimals)
                    else:
                        sl_px = snap_to_tick(entry_px + stop_pts, tick_size, price_decimals)
                        tp_px = snap_to_tick(entry_px - stop_pts * 2.0, tick_size, price_decimals)
                    pos = Trade(
                        time=ts,
                        side=side,
                        entry=entry_px,
                        exit=None,
                        size=size,
                        reason="cross",
                        sl=sl_px,
                        tp=tp_px,
                        pnl=None,
                        entry_comm=float(commission_per_contract_side) * float(size),
                    )
            finally:
                pending_entry = None

        # Evaluate existing position brackets using current bar's range (entry was at prev close)
        if pos is not None:
            exit_price = None
            exit_reason = None
            # 1) Optional native trailing: update trailing stop using bar extremes
            if use_native_trailing and (tick_size and tick_size > 0) and (high_px is not None) and (low_px is not None):
                # Activation offset based on config
                # Prefer fixed ticks; fallback to ATR multiple
                activated = pos.trailing_active
                if not activated:
                    offset_pts = None
                    if trail_offset_ticks is not None and trail_offset_ticks > 0:
                        offset_pts = float(abs(int(trail_offset_ticks))) * float(tick_size)
                    elif atr_trail_offset_k and atr_trail_offset_k > 0 and atr_val is not None:
                        offset_pts = float(atr_val) * float(atr_trail_offset_k)
                    if offset_pts is None or offset_pts <= 0:
                        activated = True
                    else:
                        fav_move = (high_px - pos.entry) if pos.side == "long" else (pos.entry - low_px)
                        if fav_move >= offset_pts:
                            activated = True
                # Optional break-even before native trailing takes over
                if (not pos.breakeven_applied) and (pos.tp is not None):
                    init_stop_pts = abs(pos.entry - pos.sl) if pos.sl is not None else None
                    try:
                        if init_stop_pts is not None and init_stop_pts > 0:
                            target_pts = float(init_stop_pts) * (3.0 if pos.side == "long" else 2.0)
                            half_tp = 0.5 * target_pts
                            fav_move = (high_px - pos.entry) if pos.side == "long" else (pos.entry - low_px)
                            if fav_move >= half_tp:
                                be_px = pos.entry
                                if pad_ticks and tick_size and tick_size > 0:
                                    if pos.side == "long":
                                        be_px = pos.entry + abs(int(pad_ticks)) * float(tick_size)
                                    else:
                                        be_px = pos.entry - abs(int(pad_ticks)) * float(tick_size)
                                be_px = snap_to_tick(be_px, tick_size, price_decimals)
                                pos.sl = be_px
                                pos.breakeven_applied = True
                                # Reset trailing anchor to BE level if we later activate
                                if pos.trailing_stop is None:
                                    pos.trailing_stop = be_px
                    except Exception:
                        pass
                # Activate trailing if allowed now
                if activated:
                    pos.trailing_active = True
                    # Determine trailing distance
                    if (force_fixed_trail_ticks and (trail_ticks_fixed is not None)):
                        t_ticks = int(max(1, int(trail_ticks_fixed)))
                        trail_points = float(t_ticks) * float(tick_size)
                    else:
                        k = float(atr_trail_k_long) if pos.side == "long" else float(atr_trail_k_short)
                        trail_points = float(atr_val) * float(k) if atr_val is not None else None
                        if (trail_points is None or trail_points <= 0) and (trail_ticks_fixed is not None):
                            t_ticks = int(max(1, int(trail_ticks_fixed)))
                            trail_points = float(t_ticks) * float(tick_size)
                    if trail_points is not None and trail_points > 0:
                        # Update trailing stop anchored to bar extremes
                        if pos.side == "long":
                            new_stop = high_px - trail_points
                            new_stop = snap_to_tick(new_stop, tick_size, price_decimals)
                            if (pos.trailing_stop is None) or (new_stop > pos.trailing_stop):
                                pos.trailing_stop = new_stop
                                pos.sl = new_stop
                        else:
                            new_stop = low_px + trail_points
                            new_stop = snap_to_tick(new_stop, tick_size, price_decimals)
                            if (pos.trailing_stop is None) or (new_stop < pos.trailing_stop):
                                pos.trailing_stop = new_stop
                                pos.sl = new_stop

            # 2) Check exits using ticks if available; otherwise conservative bar check
            if ticks_by_bucket.get(ts):
                # Iterate ticks chronologically within this minute
                cur_bid = None
                cur_ask = None
                for ev in ticks_by_bucket[ts]:
                    ts_i = ev.get("ts")
                    px_i = ev.get("last")
                    if ev.get("bid") is not None:
                        cur_bid = float(ev.get("bid"))
                    if ev.get("ask") is not None:
                        cur_ask = float(ev.get("ask"))
                    # Tick-level BE + activation + trailing
                    if use_native_trailing and (tick_size and tick_size > 0):
                        # Break-even: once 50% to TP is reached, set SL to entry (+/- pad)
                        if not pos.breakeven_applied and (pos.tp is not None) and (pos.sl is not None):
                            init_stop_pts = abs(pos.entry - pos.sl)
                            if init_stop_pts > 0:
                                target_pts = init_stop_pts * (3.0 if pos.side == "long" else 2.0)
                                half_tp = 0.5 * target_pts
                                # Use mark: last if available, else mid of bid/ask
                                mark = None
                                if px_i is not None:
                                    mark = float(px_i)
                                elif (cur_bid is not None) and (cur_ask is not None):
                                    mark = (cur_bid + cur_ask) / 2.0
                                else:
                                    mark = cur_ask if pos.side == "long" else cur_bid
                                fav_move_tick = (mark - pos.entry) if pos.side == "long" else (pos.entry - mark)
                                if fav_move_tick >= half_tp:
                                    be_px = pos.entry
                                    if pad_ticks and tick_size and tick_size > 0:
                                        be_px = pos.entry + (abs(int(pad_ticks)) * tick_size) if pos.side == "long" else pos.entry - (abs(int(pad_ticks)) * tick_size)
                                    be_px = snap_to_tick(be_px, tick_size, price_decimals)
                                    pos.sl = be_px
                                    pos.breakeven_applied = True
                                    if pos.trailing_stop is None:
                                        pos.trailing_stop = be_px
                        # Activation offset
                        activated = pos.trailing_active
                        if not activated:
                            offset_pts = None
                            if trail_offset_ticks is not None and trail_offset_ticks > 0:
                                offset_pts = float(abs(int(trail_offset_ticks))) * float(tick_size)
                            elif atr_trail_offset_k and atr_trail_offset_k > 0 and atr_val is not None:
                                offset_pts = float(atr_val) * float(atr_trail_offset_k)
                            if (offset_pts is None) or (offset_pts <= 0):
                                activated = True
                            else:
                                mark = None
                                if px_i is not None:
                                    mark = float(px_i)
                                elif (cur_bid is not None) and (cur_ask is not None):
                                    mark = (cur_bid + cur_ask) / 2.0
                                else:
                                    mark = cur_ask if pos.side == "long" else cur_bid
                                fav_move_tick = (mark - pos.entry) if pos.side == "long" else (pos.entry - mark)
                                if fav_move_tick >= offset_pts:
                                    activated = True
                            if activated:
                                pos.trailing_active = True
                        # Update trailing stop only when activated
                        if pos.trailing_active:
                            if (force_fixed_trail_ticks and (trail_ticks_fixed is not None)):
                                t_ticks = int(max(1, int(trail_ticks_fixed)))
                                trail_points = float(t_ticks) * float(tick_size)
                            else:
                                k = float(atr_trail_k_long) if pos.side == "long" else float(atr_trail_k_short)
                                trail_points = float(atr_val) * float(k) if atr_val is not None else None
                                if (trail_points is None or trail_points <= 0) and (trail_ticks_fixed is not None):
                                    t_ticks = int(max(1, int(trail_ticks_fixed)))
                                    trail_points = float(t_ticks) * float(tick_size)
                            if trail_points is not None and trail_points > 0:
                                if pos.side == "long":
                                    ref = px_i if px_i is not None else (cur_bid if cur_bid is not None else cur_ask)
                                    if ref is None:
                                        continue
                                    new_stop = snap_to_tick(float(ref) - trail_points, tick_size, price_decimals)
                                    if (pos.trailing_stop is None) or (new_stop > pos.trailing_stop):
                                        pos.trailing_stop = new_stop
                                        pos.sl = new_stop
                                else:
                                    ref = px_i if px_i is not None else (cur_ask if cur_ask is not None else cur_bid)
                                    if ref is None:
                                        continue
                                    new_stop = snap_to_tick(float(ref) + trail_points, tick_size, price_decimals)
                                    if (pos.trailing_stop is None) or (new_stop < pos.trailing_stop):
                                        pos.trailing_stop = new_stop
                                        pos.sl = new_stop
                    # Check SL/TP crossing at this tick
                    if pos.side == "long":
                        # For stop market sells, use bid if available
                        if (pos.sl is not None) and (((px_i is not None) and (px_i <= pos.sl)) or (cur_bid is not None and cur_bid <= pos.sl)):
                            base_px = cur_bid if cur_bid is not None else (px_i if px_i is not None else pos.sl)
                            exit_price = _apply_slippage(float(base_px), pos.side, kind="stop")
                            exit_reason = "SL"
                            break
                        if (pos.tp is not None) and (((px_i is not None) and (px_i >= pos.tp)) or (cur_bid is not None and cur_bid >= pos.tp)):
                            # For sell limit TP, fill at limit price conservatively
                            exit_price = float(pos.tp)
                            exit_reason = "TP"
                            break
                    else:
                        if (pos.sl is not None) and (((px_i is not None) and (px_i >= pos.sl)) or (cur_ask is not None and cur_ask >= pos.sl)):
                            base_px = cur_ask if cur_ask is not None else (px_i if px_i is not None else pos.sl)
                            exit_price = _apply_slippage(float(base_px), pos.side, kind="stop")
                            exit_reason = "SL"
                            break
                        if (pos.tp is not None) and (((px_i is not None) and (px_i <= pos.tp)) or (cur_ask is not None and cur_ask <= pos.tp)):
                            # For buy limit TP, fill at limit price
                            exit_price = float(pos.tp)
                            exit_reason = "TP"
                            break
            else:
                # Conservative bar-level ordering
                if pos.side == "long":
                    if (low_px is not None) and (pos.sl is not None) and (low_px <= pos.sl):
                        exit_price = float(pos.sl)
                        exit_reason = "SL"
                    elif (high_px is not None) and (pos.tp is not None) and (high_px >= pos.tp):
                        exit_price = float(pos.tp)
                        exit_reason = "TP"
                else:  # short
                    if (high_px is not None) and (pos.sl is not None) and (high_px >= pos.sl):
                        exit_price = float(pos.sl)
                        exit_reason = "SL"
                    elif (low_px is not None) and (pos.tp is not None) and (low_px <= pos.tp):
                        exit_price = float(pos.tp)
                        exit_reason = "TP"

            # If neither bracket hit and reverse-on-cross, flatten at close when opposite signal fires
            will_reverse = False
            prev_rel = float(bar["prev_rel"]) if not pd.isna(bar.get("prev_rel")) else None
            rel = float(bar["rel"]) if not pd.isna(bar.get("rel")) else None
            cu, cd = _calc_cross(prev_rel, rel, strict=strict_cross_only)
            # VWAP and EMA gating need to be met to count as a valid reverse signal
            vwap_long_ok = True if (not vwap_enabled or vwap_val is None) else (close_px > vwap_val)
            vwap_short_ok = True if (not vwap_enabled or vwap_val is None) else (close_px < vwap_val)
            ema_long_ok = (close_px >= ef and close_px >= es)
            ema_short_ok = (close_px <= ef and close_px <= es)
            if reverse_on_cross:
                if pos.side == "long" and (not long_only) and cd and vwap_short_ok and ema_short_ok:
                    will_reverse = True
                elif pos.side == "short" and cu and vwap_long_ok and ema_long_ok:
                    will_reverse = True

            if (exit_price is None) and will_reverse:
                exit_price = float(close_px)
                exit_reason = "REVERSE"

            if exit_price is not None:
                pos.exit = snap_to_tick(float(exit_price), tick_size, price_decimals)
                pos.exit_comm = float(commission_per_contract_side) * float(pos.size)
                # PnL in $: (exit-entry)*riskPerPoint*size minus commissions
                points = (pos.exit - pos.entry) * (1.0 if pos.side == "long" else -1.0)
                gross = float(points) * float(risk_per_point) * float(pos.size)
                comm = float(pos.entry_comm + pos.exit_comm)
                pos.pnl = gross - comm
                equity += float(pos.pnl)
                trades.append(pos)
                # If reversing, immediately open the opposite side after closing, using same bar close as entry
                if will_reverse:
                    # Defer opposite entry to next bar open (enter after close candle)
                    new_side = "short" if pos.side == "long" else "long"
                    stop_pts_long = float(atr_val) * 2.0
                    stop_pts_short = float(atr_val) * 1.5
                    if new_side == "long":
                        stop_pts = stop_pts_long
                        base_ct = int(float(risk_per_trade) // max(1e-6, stop_pts * float(risk_per_point)))
                        size = min(int(contract_size_max), max(0, int(base_ct)))
                    else:
                        stop_pts = stop_pts_short
                        base_ct_long = int(float(risk_per_trade) // max(1e-6, (stop_pts_long) * float(risk_per_point)))
                        size_long = min(int(contract_size_max), max(0, int(base_ct_long)))
                        size = min(int(size_long * float(short_size_factor)), int(contract_size_max))
                    if size >= 1:
                        if pending_entry is None:
                            pending_entry = {"side": new_side, "size": int(size), "stop_pts": float(stop_pts)}
                    pos = None
                else:
                    pos = None

        # If no open position, consider entries on cross + gating
        if pos is None:
            prev_rel = float(bar["prev_rel"]) if not pd.isna(bar.get("prev_rel")) else None
            rel = float(bar["rel"]) if not pd.isna(bar.get("rel")) else None
            cu, cd = _calc_cross(prev_rel, rel, strict=strict_cross_only)
            vwap_long_ok = True if (not vwap_enabled or vwap_val is None) else (close_px > vwap_val)
            vwap_short_ok = True if (not vwap_enabled or vwap_val is None) else (close_px < vwap_val)
            ema_long_ok = (close_px >= ef and close_px >= es)
            ema_short_ok = (close_px <= ef and close_px <= es)
            if cu and vwap_long_ok and ema_long_ok:
                # size by Pine risk
                stop_pts = float(atr_val) * 2.0
                base_ct = int(float(risk_per_trade) // max(1e-6, stop_pts * float(risk_per_point)))
                size = min(int(contract_size_max), max(0, int(base_ct)))
                if size >= 1 and (pending_entry is None):
                    pending_entry = {"side": "long", "size": int(size), "stop_pts": float(stop_pts)}
            elif (not long_only) and cd and vwap_short_ok and ema_short_ok:
                stop_pts_long = float(atr_val) * 2.0
                stop_pts_short = float(atr_val) * 1.5
                base_ct_long = int(float(risk_per_trade) // max(1e-6, stop_pts_long * float(risk_per_point)))
                size_long = min(int(contract_size_max), max(0, int(base_ct_long)))
                size = min(int(size_long * float(short_size_factor)), int(contract_size_max))
                if size >= 1 and (pending_entry is None):
                    pending_entry = {"side": "short", "size": int(size), "stop_pts": float(stop_pts_short)}
        eq_curve.append((ts, equity))

    # If an open position remains at the end, close at last close
    if pos is not None:
        last_ts = ind.index[-1]
        last_close = float(ind.iloc[-1]["close"]) if not pd.isna(ind.iloc[-1].get("close")) else None
        if last_close is not None:
            pos.exit = snap_to_tick(float(last_close), tick_size, price_decimals)
            pos.exit_comm = float(commission_per_contract_side) * float(pos.size)
            points = (pos.exit - pos.entry) * (1.0 if pos.side == "long" else -1.0)
            gross = float(points) * float(risk_per_point) * float(pos.size)
            comm = float(pos.entry_comm + pos.exit_comm)
            pos.pnl = gross - comm
            trades.append(pos)
            equity += float(pos.pnl)
            eq_curve.append((last_ts, equity))

    eq_series = pd.Series({t: v for t, v in eq_curve})
    eq_series = eq_series.sort_index()
    return trades, eq_series


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


def main():
    ap = argparse.ArgumentParser(description="Backtest Pine-style MNQ EMA cross strategy using TopstepX history API")
    ap.add_argument("--symbol", default="MNQ", help="Symbol to backtest (default MNQ)")
    ap.add_argument("--days", type=int, default=7, help="Number of days to fetch (default 7)")
    ap.add_argument("--cash", type=float, default=50000.0, help="Initial cash for reporting (default 50000)")
    ap.add_argument("--commission", type=float, default=0.74, help="Commission per contract per side (default 0.74)")
    ap.add_argument("--tz", type=str, default=None, help="Timezone for calculations/prints (default config.tz or America/New_York)")
    ap.add_argument("--ticks", type=str, default=None, help="Path to JSONL ticks file recorded via tools/record_ticks.py")
    ap.add_argument("--slippage-ticks", type=int, default=0, help="Adverse slippage in ticks applied to fills")
    ap.add_argument("--sec-bars", action="store_true", help="Use 1-second bars for indicators (from ticks if provided, else API)")
    ap.add_argument("--csv", type=str, default=None, help="Path to CSV file with either OHLCV bars or tick data (auto-detected)")
    ap.add_argument("--csv-type", type=str, choices=["auto", "bars", "ticks"], default="auto", help="Interpret CSV as bars or ticks (default auto)")
    ap.add_argument("--csv-tz", type=str, default=None, help="Timezone for naive CSV timestamps (if no tz info present)")
    ap.add_argument("--export-csv", type=str, default=None, help="Write trade ledger to CSV at this path")
    ap.add_argument("--ema-short", type=int, default=None, help="EMA fast length (default from config)")
    ap.add_argument("--ema-long", type=int, default=None, help="EMA slow length (default from config)")
    ap.add_argument("--atr-length", type=int, default=None, help="ATR length (default from config)")
    ap.add_argument("--strict", action="store_true", help="Use strict cross (prev<0 -> >0) and (prev>0 -> <0)")
    ap.add_argument("--no-reverse", action="store_true", help="Disable reverse-on-cross behavior")
    ap.add_argument("--no-vwap", action="store_true", help="Disable VWAP gating regardless of config")
    ap.add_argument("--long-only", action="store_true", help="Only take long entries")
    ap.add_argument("--print-count", type=int, default=50, help="Number of trades to print (default 50)")
    # Native trailing options
    ap.add_argument("--native-trailing", action="store_true", help="Enable native trailing simulation (1m chart)")
    ap.add_argument("--trail-ticks", type=int, default=None, help="Fixed trailing distance in ticks (overrides config)")
    ap.add_argument("--force-fixed-trail-ticks", action="store_true", help="Force fixed tick trailing distance (ignore ATR)")
    ap.add_argument("--trail-offset-ticks", type=int, default=None, help="Activation offset in ticks before trailing activates")
    ap.add_argument("--atr-trail-k-long", type=float, default=None, help="ATR multiplier for long trailing distance")
    ap.add_argument("--atr-trail-k-short", type=float, default=None, help="ATR multiplier for short trailing distance")
    ap.add_argument("--atr-trail-offset-k", type=float, default=None, help="ATR multiplier for trailing activation offset")
    ap.add_argument("--pad-ticks", type=int, default=None, help="Pad ticks for break-even move after activation")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    cfg = _load_config()
    strat = cfg.get("strategy", {})

    ema_short = int(args.ema_short or strat.get("emaShort", 9))
    ema_long = int(args.ema_long or strat.get("emaLong", 21))
    atr_length = int(args.atr_length or strat.get("atrLength", 14))
    ema_source = str(strat.get("emaSource", "close")).strip().lower()
    vwap_enabled = bool(strat.get("vwapEnabled", True)) and (not args.no_vwap)
    long_only = bool(args.long_only or strat.get("longOnly", False))
    reverse_on_cross = (not args.no_reverse) and bool(strat.get("reverseOnCross", True))
    risk_per_trade = float(strat.get("riskPerTrade", 500.0))
    contract_size_max = int(strat.get("contractSizeMax", 5))
    short_size_factor = float(strat.get("shortSizeFactor", 0.75))

    instr = cfg.get("instrument", {})
    tick_size = float(instr.get("tickSize", 0.25))
    price_decimals = int(instr.get("decimalPlaces", 2)) if instr.get("decimalPlaces") is not None else (2 if tick_size >= 0.01 else 4)
    risk_per_point = float(instr.get("riskPerPoint", 5.0))

    # Timezone resolution (defaults to config.tz -> America/New_York)
    tzname = args.tz or cfg.get("tz") or "America/New_York"

    symbol = (args.symbol or cfg.get("symbol") or "MNQ").upper()
    contract_id = str(cfg.get("CONTRACT_ID") or (cfg.get("auth", {}).get("CONTRACT_ID")))
    if not contract_id:
        raise SystemExit("CONTRACT_ID is required in Backend/config.yaml for history fetch.")

    api_url, username, api_key = _get_api_context(cfg)
    logging.info("Fetching %d days of %s bars for %s (contract %s)", int(args.days), "1s" if args.sec_bars else "1m", symbol, contract_id)
    token = _api_get_token(api_url, username, api_key)
    if not token:
        raise SystemExit("Failed to authenticate to TopstepX API. Check credentials in config.yaml")

    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(days=max(1, int(args.days)))
    df: Optional[pd.DataFrame] = None
    # Optional tick data (JSONL or CSV) and optional CSV bars
    ticks_df: Optional[pd.DataFrame] = None
    if args.ticks:
        tick_path = Path(args.ticks)
        if not tick_path.exists():
            raise SystemExit(f"Ticks file not found: {tick_path}")
        # Autodetect JSONL vs CSV by extension
        if tick_path.suffix.lower() == '.csv':
            _bars_csv, ticks_df, _ = _load_csv_autodetect(tick_path, force_kind='ticks', csv_tz=args.csv_tz)
        else:
            ticks_df = _load_ticks_jsonl(tick_path)
        if ticks_df is None or ticks_df.empty:
            logging.warning("Ticks file loaded but empty; falling back to API or CSV bars")
    if args.csv:
        csv_path = Path(args.csv)
        if not csv_path.exists():
            raise SystemExit(f"CSV file not found: {csv_path}")
        b_csv, t_csv, kind = _load_csv_autodetect(csv_path, force_kind=args.csv_type, csv_tz=args.csv_tz)
        if b_csv is not None and (df is None):
            df = b_csv
        # Merge/add ticks
        if t_csv is not None:
            if ticks_df is None:
                ticks_df = t_csv
            else:
                try:
                    ticks_df = pd.concat([ticks_df, t_csv]).sort_index()
                except Exception:
                    pass
    # Prefer 1s bars from ticks if requested
    if args.sec_bars and (ticks_df is not None) and (not ticks_df.empty):
        df = _resample_ticks_to_bars(ticks_df, seconds=1)
    if df is None:
        # Fetch from API
        unit = 1 if args.sec_bars else 2
        unit_n = 1
        items = _history_fetch(api_url, token, contract_id, start, end, unit=unit, unit_n=unit_n, live=True, include_partial=True)
        if not items:
            raise SystemExit("No history returned from API. Try again later or adjust config.")
        df = _bars_to_df(items)
    if df is None or df.empty:
        raise SystemExit("No valid bars parsed for backtest.")

    # Trailing options resolved from config + CLI
    use_native_trailing = bool(args.native_trailing or strat.get("trailingStopEnabled", False))
    force_fixed = bool(args.force_fixed_trail_ticks or strat.get("forceFixedTrailTicks", False))
    trail_ticks_fixed = int(args.trail_ticks) if args.trail_ticks is not None else (
        int(strat.get("trailDistanceTicks")) if (strat.get("trailDistanceTicks") not in (None, '', False)) else None
    )
    trail_offset_ticks = int(args.trail_offset_ticks) if args.trail_offset_ticks is not None else (
        int(strat.get("trailOffsetTicks")) if (strat.get("trailOffsetTicks") not in (None, '', False)) else None
    )
    atr_trail_k_long = float(args.atr_trail_k_long) if args.atr_trail_k_long is not None else float(strat.get("atrTrailKLong", 1.5))
    atr_trail_k_short = float(args.atr_trail_k_short) if args.atr_trail_k_short is not None else float(strat.get("atrTrailKShort", 1.0))
    atr_trail_offset_k = float(args.atr_trail_offset_k) if args.atr_trail_offset_k is not None else float(strat.get("atrTrailOffsetK", 0.0))
    pad_ticks = int(args.pad_ticks) if args.pad_ticks is not None else int(strat.get("padTicks", 0))

    # ticks_df is already loaded above if provided

    trades, eq = backtest_week(
        df=df,
        ema_short=ema_short,
        ema_long=ema_long,
        ema_source=ema_source,
        vwap_enabled=vwap_enabled,
        atr_length=atr_length,
        risk_per_trade=risk_per_trade,
        risk_per_point=risk_per_point,
        contract_size_max=contract_size_max,
        short_size_factor=short_size_factor,
        tick_size=tick_size,
        price_decimals=price_decimals,
        long_only=long_only,
        strict_cross_only=bool(args.strict),
        reverse_on_cross=reverse_on_cross,
        commission_per_contract_side=float(args.commission),
        use_native_trailing=use_native_trailing,
        force_fixed_trail_ticks=force_fixed,
        trail_ticks_fixed=trail_ticks_fixed,
        atr_trail_k_long=atr_trail_k_long,
        atr_trail_k_short=atr_trail_k_short,
        trail_offset_ticks=trail_offset_ticks,
        atr_trail_offset_k=atr_trail_offset_k,
        pad_ticks=pad_ticks,
        tzname=tzname,
        ticks=ticks_df,
        slippage_ticks=int(args.slippage_ticks or 0),
    )

    summary = _summarize(trades, initial_cash=float(args.cash))
    print("\nBacktest Summary")
    print("----------------")
    print(f"Symbol: {symbol} | Days: {int(args.days)} | Bars: {len(df)}")
    print(f"Trades: {summary['trades']} | Wins: {summary['wins']} | Losses: {summary['losses']} | Win%: {summary['win_rate_pct']:.1f}")
    print(f"Net PnL: ${summary['net_pnl']:.2f} | End Cash: ${summary['ending_cash']:.2f}")
    print(f"Avg Win: ${summary['avg_win']:.2f} | Avg Loss: ${summary['avg_loss']:.2f} | R/R: {summary['reward_risk']}")

    # Optional: export trades to CSV
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
                    'trailing_active': t.trailing_active,
                    'trailing_stop': t.trailing_stop,
                    'trail_ticks': t.trail_ticks,
                })
            pd.DataFrame(rows).to_csv(out_path, index=False)
            print(f"\nWrote trades CSV: {out_path}")
        except Exception as e:
            print(f"\nFailed to export trades CSV: {e}")

    # Print a concise trade ledger
    if trades:
        count = max(1, int(args.print_count))
        shown = min(count, len(trades))
        print(f"\nTrades (first {shown}):")
        for t in trades[:shown]:
            try:
                ts_local = t.time.tz_convert(tzname) if t.time.tzinfo is not None else t.time
            except Exception:
                ts_local = t.time
            ts_str = str(ts_local)
            print(f"{ts_str} | {t.side.upper()} size={t.size} entry={t.entry:.2f} exit={t.exit if t.exit is None else f'{t.exit:.2f}'} reason={t.reason} pnl={'NA' if t.pnl is None else f'${t.pnl:.2f}'}")


if __name__ == "__main__":
    main()
