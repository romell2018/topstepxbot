import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# Make Backend modules importable (so we can reuse backtest.week logic)
BACKEND_DIR = Path(__file__).resolve().parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

# Reuse the core backtest engine from Backend/backtest.py
import backtest as bt  # type: ignore


def _load_ticks_jsonl(path: Path) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    with path.open("r") as f:
        for line in f:
            try:
                obj = None
                try:
                    import json
                    obj = json.loads(line)
                except Exception:
                    continue
                ts = obj.get("ts") or obj.get("time") or obj.get("timestamp")
                vol = obj.get("volume") or obj.get("qty") or 0.0
                if ts is None:
                    continue
                t = pd.to_datetime(str(ts).replace("Z", "+00:00"), utc=True)
                rec: Dict[str, Any] = {"time": t, "volume": float(vol)}
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
    if ticks.empty:
        return ticks
    rule = f"{int(max(1, seconds))}S"
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
    df = pd.read_csv(path)
    cols_lower = {c: str(c).strip().lower() for c in df.columns}
    # Identify time column
    time_col = None
    for cand in ("time", "timestamp", "datetime", "date_time", "date"):
        if cand in cols_lower.values():
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
    if getattr(ts, 'dt', None) is not None and (ts.dt.tz is None):
        if csv_tz:
            ts = ts.dt.tz_localize(csv_tz).dt.tz_convert('UTC')
        else:
            ts = ts.dt.tz_localize('UTC')
    else:
        try:
            ts = ts.tz_convert('UTC')
        except Exception:
            ts = pd.to_datetime(ts, utc=True)
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
        bcols: Dict[str, Any] = {}
        for k in ("open", "high", "low", "close", "volume"):
            if k in lmap:
                bcols[k] = df[lmap[k]].astype(float)
        bars = pd.DataFrame(bcols)
        return bars, None, "bars"
    else:
        tcols: Dict[str, Any] = {}
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


def main():
    ap = argparse.ArgumentParser(description="Backtest EMA cross on FTMO NASDAQ 100 (US100) using CSV/ticks")
    ap.add_argument("--symbol", default="US100", help="Instrument symbol (default US100)")
    ap.add_argument("--csv", type=str, default=None, help="CSV with OHLCV bars or ticks (auto-detected)")
    ap.add_argument("--csv-type", type=str, choices=["auto", "bars", "ticks"], default="auto", help="Interpret CSV as bars or ticks")
    ap.add_argument("--csv-tz", type=str, default=None, help="Timezone for naive CSV timestamps")
    ap.add_argument("--ticks", type=str, default=None, help="JSONL ticks captured from your platform")
    ap.add_argument("--sec-bars", action="store_true", help="Use 1-second bars (from ticks) for indicators")
    ap.add_argument("--exit-sec", action="store_true", help="Use 1-second bars/ticks for exit logic")
    # Strategy
    ap.add_argument("--ema-short", type=int, default=9, help="EMA fast length (default 9)")
    ap.add_argument("--ema-long", type=int, default=21, help="EMA slow length (default 21)")
    ap.add_argument("--atr-length", type=int, default=14, help="ATR length (default 14)")
    ap.add_argument("--no-vwap", action="store_true", help="Disable VWAP gating")
    ap.add_argument("--long-only", action="store_true", help="Only take longs")
    ap.add_argument("--strict", action="store_true", help="Strict cross only")
    ap.add_argument("--no-reverse", action="store_true", help="Disable reverse-on-cross")
    # Instrument
    ap.add_argument("--tick-size", type=float, default=1.0, help="Tick size in index points (default 1.0 for US100)")
    ap.add_argument("--price-decimals", type=int, default=1, help="Price decimals (default 1 for US100)")
    ap.add_argument("--risk-per-point", type=float, default=1.0, help="PnL $ per point per unit size (default 1.0)")
    # Execution model
    ap.add_argument("--commission", type=float, default=0.0, help="Commission per unit per side")
    ap.add_argument("--slippage-ticks", type=int, default=0, help="Adverse slippage in ticks")
    ap.add_argument("--spread-points", type=float, default=0.0, help="Fixed bid/ask spread in points (half applied at entry/exit)")
    # Trailing
    ap.add_argument("--native-trailing", action="store_true", help="Enable native trailing simulation")
    ap.add_argument("--trail-ticks", type=int, default=None, help="Fixed trailing distance in ticks")
    ap.add_argument("--force-fixed-trail-ticks", action="store_true", help="Force fixed tick trailing distance (ignore ATR)")
    ap.add_argument("--trail-offset-ticks", type=int, default=None, help="Activation offset (ticks)")
    ap.add_argument("--atr-trail-k-long", type=float, default=1.5, help="ATR multiple for long trail distance")
    ap.add_argument("--atr-trail-k-short", type=float, default=1.0, help="ATR multiple for short trail distance")
    ap.add_argument("--atr-trail-offset-k", type=float, default=0.0, help="ATR multiple for trail activation offset")
    ap.add_argument("--pad-ticks", type=int, default=0, help="Pad ticks for BE before native trail")
    # Reporting
    ap.add_argument("--tz", type=str, default="Etc/UTC", help="Timezone for reporting and RTH gating")
    ap.add_argument("--rth-only", action="store_true", help="Filter to regular trading hours in --tz")
    ap.add_argument("--cash", type=float, default=50000.0, help="Initial cash for reporting")
    ap.add_argument("--print-count", type=int, default=50, help="Number of trades to print")
    ap.add_argument("--export-csv", type=str, default=None, help="Write trade ledger to CSV")
    ap.add_argument("--export-equity-csv", type=str, default=None, help="Write equity curve to CSV")
    ap.add_argument("--plot-equity", type=str, default=None, help="Save equity curve plot to PNG")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    # Load price data: prefer ticks if 1s bars or tick exits requested
    ticks_df: Optional[pd.DataFrame] = None
    if args.ticks:
        p = Path(args.ticks).expanduser()
        if not p.exists():
            raise SystemExit(f"Ticks file not found: {p}")
        ticks_df = _load_ticks_jsonl(p)
    bars_df: Optional[pd.DataFrame] = None
    if args.csv:
        p = Path(args.csv).expanduser()
        if not p.exists():
            raise SystemExit(f"CSV file not found: {p}")
        b_csv, t_csv, kind = _load_csv_autodetect(p, force_kind=args.csv_type, csv_tz=args.csv_tz)
        if b_csv is not None:
            bars_df = b_csv
        if t_csv is not None:
            if ticks_df is None:
                ticks_df = t_csv
            else:
                try:
                    ticks_df = pd.concat([ticks_df, t_csv]).sort_index()
                except Exception:
                    pass

    # Build main bars df
    df = None
    if args.sec_bars and (ticks_df is not None) and (not ticks_df.empty):
        df = _resample_ticks_to_bars(ticks_df, seconds=1)
    elif bars_df is not None:
        df = bars_df
    else:
        raise SystemExit("Provide --csv bars or --ticks for FTMO backtest; API fetch is not supported here.")
    if df is None or df.empty:
        raise SystemExit("No valid bars parsed for backtest.")

    # Prepare optional 1-second bars for exit logic
    sec_bars: Optional[pd.DataFrame] = None
    if bool(args.exit_sec) and (ticks_df is not None) and (not ticks_df.empty):
        sec_bars = _resample_ticks_to_bars(ticks_df, seconds=1)

    # Trailing resolution
    trail_ticks_fixed = int(args.trail_ticks) if args.trail_ticks is not None else None

    trades, eq = bt.backtest_week(
        df=df,
        ema_short=int(args.ema_short),
        ema_long=int(args.ema_long),
        ema_source="close",
        vwap_enabled=(not bool(args.no_vwap)),
        atr_length=int(args.atr_length),
        risk_per_trade=500.0,  # same default as Pine-style
        risk_per_point=float(args.risk_per_point),
        contract_size_max=15,
        short_size_factor=0.75,
        tick_size=float(args.tick_size),
        price_decimals=int(args.price_decimals),
        long_only=bool(args.long_only),
        strict_cross_only=bool(args.strict),
        reverse_on_cross=(not bool(args.no_reverse)),
        commission_per_contract_side=float(args.commission),
        use_native_trailing=bool(args.native_trailing),
        force_fixed_trail_ticks=bool(args.force_fixed_trail_ticks),
        trail_ticks_fixed=trail_ticks_fixed,
        atr_trail_k_long=float(args.atr_trail_k_long),
        atr_trail_k_short=float(args.atr_trail_k_short),
        trail_offset_ticks=(int(args.trail_offset_ticks) if args.trail_offset_ticks is not None else None),
        atr_trail_offset_k=float(args.atr_trail_offset_k),
        pad_ticks=int(args.pad_ticks),
        tzname=str(args.tz),
        rth_only=bool(args.rth_only),
        ticks=ticks_df,
        slippage_ticks=int(args.slippage_ticks or 0),
        spread_points=float(args.spread_points or 0.0),
        sec_bars=sec_bars,
        exit_on_sec=bool(args.exit_sec),
    )

    summary = bt._summarize(trades, initial_cash=float(args.cash))
    print("\nBacktest Summary – FTMO US100")
    print("----------------------------")
    print(f"Symbol: {args.symbol} | Bars: {len(df)}")
    print(f"Trades: {summary['trades']} | Wins: {summary['wins']} | Losses: {summary['losses']} | Win%: {summary['win_rate_pct']:.1f}")
    print(f"Net PnL: ${summary['net_pnl']:.2f} | End Cash: ${summary['ending_cash']:.2f}")
    print(f"Avg Win: ${summary['avg_win']:.2f} | Avg Loss: ${summary['avg_loss']:.2f} | R/R: {summary['reward_risk']}")

    # Optional: export equity to CSV
    if args.export_equity_csv and (eq is not None) and (not eq.empty):
        try:
            out_path_eq = Path(args.export_equity_csv).expanduser()
            out_path_eq.parent.mkdir(parents=True, exist_ok=True)
            eq_out = eq.copy()
            try:
                eq_out.index = eq_out.index.tz_convert(str(args.tz))
            except Exception:
                pass
            eq_out = eq_out.rename("equity")
            eq_out.to_csv(out_path_eq, header=True)
            print(f"Wrote equity CSV: {out_path_eq}")
        except Exception as e:
            print(f"Failed to export equity CSV: {e}")

    # Optional: save equity plot
    if args.plot_equity and (eq is not None) and (not eq.empty):
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            eq_plot = eq.copy()
            try:
                eq_plot.index = eq_plot.index.tz_convert(str(args.tz))
            except Exception:
                pass
            y = eq_plot + float(args.cash)
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.plot(y.index, y.values, label="Equity")
            ax.set_title(f"Equity Curve – {args.symbol}")
            ax.set_xlabel(f"Time ({args.tz})")
            ax.set_ylabel("Account Value ($)")
            ax.grid(True, alpha=0.3)
            ax.legend(loc="best")
            out_path_png = Path(args.plot_equity).expanduser()
            out_path_png.parent.mkdir(parents=True, exist_ok=True)
            fig.tight_layout()
            fig.savefig(out_path_png, dpi=150)
            plt.close(fig)
            print(f"Saved equity plot: {out_path_png}")
        except Exception as e:
            print(f"Failed to plot equity: {e}. If missing, install matplotlib in your venv.")

    # Optional: export trades to CSV
    if args.export_csv and trades:
        try:
            out_path = Path(args.export_csv).expanduser()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            rows = []
            for t in trades:
                try:
                    ts_local = t.time.tz_convert(str(args.tz)) if t.time.tzinfo is not None else t.time
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
                ts_local = t.time.tz_convert(str(args.tz)) if t.time.tzinfo is not None else t.time
            except Exception:
                ts_local = t.time
            ts_str = str(ts_local)
            print(f"{ts_str} | {t.side.upper()} size={t.size} entry={t.entry:.2f} exit={t.exit if t.exit is None else f'{t.exit:.2f}'} reason={t.reason} pnl={'NA' if t.pnl is None else f'${t.pnl:.2f}'}")


if __name__ == "__main__":
    main()

