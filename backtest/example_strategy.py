"""Minimal SMA crossover backtest using the `backtesting` package."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Backtesting API changed in recent versions: `SMA` may not be exported
try:
    from backtesting import Backtest, Strategy
except ImportError as exc:  # pragma: no cover - convenience guard
    raise SystemExit(
        "backtesting package not found. Install dependencies with `pip install backtesting pandas`."
    ) from exc

try:
    # Prefer library-provided helpers when available
    from backtesting.lib import SMA as _SMA, crossover  # type: ignore
    SMA = _SMA  # re-export as expected below
except Exception:
    # Fallback for environments where `SMA` isn't exposed by backtesting.lib
    from backtesting.lib import crossover  # type: ignore

    def SMA(series: pd.Series | np.ndarray, n: int) -> pd.Series:
        """Simple moving average compatible with Backtesting's `I` helper."""
        s = pd.Series(series)
        return s.rolling(int(n)).mean()

DATA_FILE = Path(__file__).with_name("sample_data.csv")


def load_data() -> pd.DataFrame:
    """Return OHLC data indexed by datetime, falling back to synthetic prices."""
    if DATA_FILE.exists() and DATA_FILE.stat().st_size > 0:
        df = pd.read_csv(DATA_FILE, parse_dates=True, index_col=0)
        required_cols = {"Open", "High", "Low", "Close"}
        missing = required_cols.difference(df.columns)
        if missing:
            raise SystemExit(
                f"`{DATA_FILE.name}` is missing required columns: {', '.join(sorted(missing))}"
            )
        return df

    rng = pd.date_range("2022-01-01", periods=300, freq="B")
    steps = np.random.normal(loc=0.05, scale=1.2, size=len(rng))
    close = 100 + steps.cumsum()
    open_ = close + np.random.normal(scale=0.3, size=len(rng))
    high = np.maximum(open_, close) + np.random.uniform(0.1, 0.6, size=len(rng))
    low = np.minimum(open_, close) - np.random.uniform(0.1, 0.6, size=len(rng))

    df = pd.DataFrame({
        "Open": open_,
        "High": high,
        "Low": low,
        "Close": close,
        "Volume": np.random.randint(1_000, 5_000, size=len(rng)),
    }, index=rng)
    df.index.name = "Date"
    return df


class SmaCross(Strategy):
    """Two moving averages trigger long/short entries."""

    n_fast = 10
    n_slow = 30

    def init(self) -> None:
        price = self.data.Close
        self.sma_fast = self.I(SMA, price, self.n_fast)
        self.sma_slow = self.I(SMA, price, self.n_slow)

    def next(self) -> None:
        if crossover(self.sma_fast, self.sma_slow):
            self.position.close()
            self.buy()
        elif crossover(self.sma_slow, self.sma_fast):
            self.position.close()
            self.sell()


def run_backtest(plot: bool = False) -> None:
    data = load_data()
    bt = Backtest(
        data,
        SmaCross,
        cash=50_000,
        commission=0.0005,
        exclusive_orders=True,
    )

    stats = bt.run()
    print(stats)

    if plot:
        try:
            bt.plot(open_browser=False)
        except Exception as exc:  # pragma: no cover - plotting optional
            print(f"Plotting skipped: {exc}", file=sys.stderr)


if __name__ == "__main__":
    run_backtest(plot="--plot" in sys.argv)
