"""MA crossover backtest (default 9/21), SMA or EMA.

Usage examples:
- Synthetic data, default 9/21 EMA:   python backtest/ma_9_21.py --ema
- SMA 9/21 on CSV:                    python backtest/ma_9_21.py --csv backtest/sample_data.csv
- Custom windows:                     python backtest/ma_9_21.py --fast 5 --slow 20 --sma
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

try:
    from backtesting import Backtest, Strategy
except ImportError as exc:
    raise SystemExit(
        "backtesting package not found. Install with `pip install backtesting pandas`."
    ) from exc

try:
    from backtesting.lib import crossover  # type: ignore
except Exception as exc:  # pragma: no cover
    raise SystemExit("backtesting.lib.crossover not available") from exc


def SMA(series: pd.Series | np.ndarray, n: int) -> pd.Series:
    s = pd.Series(series)
    return s.rolling(int(n)).mean()


def EMA(series: pd.Series | np.ndarray, n: int) -> pd.Series:
    s = pd.Series(series)
    return s.ewm(span=int(n), adjust=False).mean()


def load_data(csv: Optional[Path], min_len: int) -> pd.DataFrame:
    if csv is not None:
        if not csv.exists():
            raise SystemExit(f"CSV not found: {csv}")
        df = pd.read_csv(csv, parse_dates=True, index_col=0)
        required = {"Open", "High", "Low", "Close"}
        miss = required.difference(df.columns)
        if miss:
            raise SystemExit(f"CSV missing columns: {', '.join(sorted(miss))}")
        if len(df) < min_len:
            print(f"CSV length {len(df)} < required {min_len}; generating synthetic instead…")
        else:
            return df

    # Synthetic data for quick testing
    rng = pd.date_range("2022-01-01", periods=max(min_len, 400), freq="B")
    rs = np.random.default_rng(seed=42)
    steps = rs.normal(loc=0.03, scale=1.0, size=len(rng))
    close = 100 + steps.cumsum()
    open_ = close + rs.normal(scale=0.25, size=len(rng))
    high = np.maximum(open_, close) + rs.uniform(0.05, 0.5, size=len(rng))
    low = np.minimum(open_, close) - rs.uniform(0.05, 0.5, size=len(rng))
    vol = rs.integers(1_000, 5_000, size=len(rng))
    df = pd.DataFrame({
        "Open": open_,
        "High": high,
        "Low": low,
        "Close": close,
        "Volume": vol,
    }, index=rng)
    df.index.name = "Date"
    return df


class MaCross(Strategy):
    n_fast: int = 9
    n_slow: int = 21
    use_ema: bool = True
    long_only: bool = False

    def init(self) -> None:  # type: ignore[override]
        price = self.data.Close
        f = EMA if self.use_ema else SMA
        self.ma_fast = self.I(f, price, self.n_fast)
        self.ma_slow = self.I(f, price, self.n_slow)

    def next(self) -> None:  # type: ignore[override]
        fast, slow = self.ma_fast, self.ma_slow
        if crossover(fast, slow):
            self.position.close()
            self.buy()
        elif not self.long_only and crossover(slow, fast):
            self.position.close()
            self.sell()


def main() -> None:
    p = argparse.ArgumentParser(description="MA crossover backtest")
    p.add_argument("--csv", type=Path, default=None, help="CSV with OHLC columns")
    p.add_argument("--fast", type=int, default=9)
    p.add_argument("--slow", type=int, default=21)
    g = p.add_mutually_exclusive_group()
    g.add_argument("--ema", action="store_true", help="Use EMA (default)")
    g.add_argument("--sma", action="store_true", help="Use SMA")
    p.add_argument("--long-only", action="store_true")
    p.add_argument("--plot", action="store_true")
    p.add_argument("--out", type=Path, default=Path("backtest/plot_ma_9_21.html"))
    args = p.parse_args()

    if args.fast <= 0 or args.slow <= 0 or args.fast >= args.slow:
        raise SystemExit("Require 0 < fast < slow")

    df = load_data(args.csv, min_len=max(args.slow * 3, 100))

    class _Cfg(MaCross):
        n_fast = args.fast
        n_slow = args.slow
        use_ema = not args.sma  # default EMA
        long_only = args.long_only

    bt = Backtest(
        df,
        _Cfg,
        cash=50_000,
        commission=0.0005,
        exclusive_orders=True,
    )
    stats = bt.run()
    print(stats)
    if args.plot:
        try:
            bt.plot(filename=str(args.out), open_browser=False)
            print(f"Saved chart to: {args.out}")
        except Exception as exc:
            print(f"Plotting skipped: {exc}")


if __name__ == "__main__":
    main()
