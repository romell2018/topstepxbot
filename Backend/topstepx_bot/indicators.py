from typing import Optional
import pandas as pd
import numpy as np


class ATR:
    def __init__(self, period: int):
        self.period = int(period)
        self.mult = 1.0 / float(self.period)
        self.value: Optional[float] = None
        self.prev_close: Optional[float] = None

    @staticmethod
    def _tr(h: float, l: float, pc: Optional[float]) -> float:
        if pc is None:
            return float(h - l)
        return max(h - l, abs(h - pc), abs(l - pc))

    def update(self, h: float, l: float, c: float) -> Optional[float]:
        tr = self._tr(h, l, self.prev_close)
        if self.value is None:
            self.value = float(tr)
        else:
            self.value = (tr - self.value) * self.mult + self.value
        self.prev_close = float(c)
        return self.value


def _ema_pine(series: pd.Series, length: int) -> pd.Series:
    try:
        x = pd.to_numeric(series, errors='coerce').astype(float).values
        n = int(length)
        alpha = 2.0 / float(n + 1)
        out = np.full_like(x, np.nan, dtype=float)
        if len(x) == 0 or n <= 0:
            return pd.Series(out, index=series.index)
        if len(x) >= n:
            init = float(np.nanmean(x[:n]))
            out[n-1] = init
            for i in range(n, len(x)):
                prev = out[i-1]
                xi = x[i]
                if np.isnan(prev) or np.isnan(xi):
                    out[i] = prev
                else:
                    out[i] = alpha * xi + (1.0 - alpha) * prev
        return pd.Series(out, index=series.index)
    except Exception:
        return series.ewm(span=length, adjust=False).mean()


def compute_indicators(df: pd.DataFrame, ema_short: int, ema_long: int, ema_source: str,
                       rth_only: bool, tzname: str) -> pd.DataFrame:
    df = df.copy()
    # Ensure chronological order and no duplicate minutes
    try:
        df = df.sort_index()
    except Exception:
        pass
    try:
        df = df[~df.index.duplicated(keep='last')]
    except Exception:
        pass

    # Optional RTH-only filtering
    try:
        if rth_only and len(df.index) > 0 and df.index.tz is not None:
            from zoneinfo import ZoneInfo
            import datetime as dt
            idx_local = df.index.tz_convert(ZoneInfo(tzname))
            start_t = dt.time(9, 30)
            end_t = dt.time(16, 0)
            mask = (idx_local.weekday < 5) & (idx_local.time >= start_t) & (idx_local.time < end_t)
            df = df.loc[mask]
    except Exception:
        pass

    # Source series
    try:
        if ema_source == 'hlc3' and all(c in df.columns for c in ('high','low','close')):
            src = (df['high'] + df['low'] + df['close']) / 3.0
        elif ema_source == 'ohlc4' and all(c in df.columns for c in ('open','high','low','close')):
            src = (df['open'] + df['high'] + df['low'] + df['close']) / 4.0
        else:
            src = df['close']
    except Exception:
        src = df['close']

    df[f'ema{ema_short}'] = _ema_pine(src, ema_short)
    df[f'ema{ema_long}'] = _ema_pine(src, ema_long)

    # VWAP: cumulative intraday (simple cumulative variant)
    df['cum_vol'] = df['volume'].cumsum()
    df['cum_pv'] = (df['close'] * df['volume']).cumsum()
    df['vwap'] = df['cum_pv'] / df['cum_vol']

    return df

