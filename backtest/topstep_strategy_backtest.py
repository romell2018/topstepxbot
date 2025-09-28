"""Backtest the Topstep strategy logic from Backend/config.yaml.

It approximates the live bot's EMA(9/21) crossover with optional VWAP gating,
confirmation bars, long-only toggle, and ATR-based trailing stops or fixed TP/SL.

Examples:
- Synthetic data (EMA 9/21, trailing):
    python backtest/topstep_strategy_backtest.py
- With CSV (must have Open,High,Low,Close[,Volume]):
    python backtest/topstep_strategy_backtest.py --csv backtest/sample_data.csv
- SMA source override: n/a (uses EMA to match bot)
- Plot:
    python backtest/topstep_strategy_backtest.py --plot
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from math import ceil, floor
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd

try:
    from backtesting import Backtest, Strategy
except ImportError as exc:
    raise SystemExit(
        "backtesting package not found. Install with `pip install backtesting pandas`."
    ) from exc


# --- Config loading ----------------------------------------------------
try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None


def _ema_pine_series(series: pd.Series, length: int) -> pd.Series:
    try:
        x = pd.to_numeric(series, errors="coerce").astype(float).values
        n = int(length)
        alpha = 2.0 / float(n + 1)
        out = np.full_like(x, np.nan, dtype=float)
        if len(x) == 0 or n <= 0:
            return pd.Series(out, index=series.index)
        if len(x) >= n:
            init = float(np.nanmean(x[:n]))
            out[n - 1] = init
            for i in range(n, len(x)):
                prev = out[i - 1]
                xi = x[i]
                if np.isnan(prev) or np.isnan(xi):
                    out[i] = prev
                else:
                    out[i] = alpha * xi + (1.0 - alpha) * prev
        return pd.Series(out, index=series.index)
    except Exception:
        return series.ewm(span=length, adjust=False).mean()


def atr_series(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    h = pd.to_numeric(high, errors="coerce").astype(float).values
    l = pd.to_numeric(low, errors="coerce").astype(float).values
    c = pd.to_numeric(close, errors="coerce").astype(float).values
    n = int(max(1, period))
    mult = 1.0 / float(n)
    out = np.full_like(c, np.nan, dtype=float)
    prev_close: Optional[float] = None
    val: Optional[float] = None
    for i in range(len(c)):
        if np.isnan(h[i]) or np.isnan(l[i]) or np.isnan(c[i]):
            out[i] = val if val is not None else np.nan
            prev_close = c[i] if not np.isnan(c[i]) else prev_close
            continue
        tr = max(h[i] - l[i], abs(h[i] - (prev_close if prev_close is not None else h[i])), abs(l[i] - (prev_close if prev_close is not None else l[i])))
        if val is None:
            val = float(tr)
        else:
            val = (tr - val) * mult + val
        out[i] = val
        prev_close = c[i]
    return pd.Series(out, index=close.index)


def vwap_cumulative(close: pd.Series, volume: pd.Series) -> pd.Series:
    vol = pd.to_numeric(volume, errors="coerce").fillna(0.0)
    pv = pd.to_numeric(close, errors="coerce").fillna(method="ffill").fillna(0.0) * vol
    cum_vol = vol.cumsum().replace(0, np.nan)
    return (pv.cumsum() / cum_vol).fillna(method="ffill")


@dataclass
class StratParams:
    ema_short: int = 9
    ema_long: int = 21
    ema_source: str = "close"
    rth_only: bool = False
    use_vwap: bool = False
    confirm_bars: int = 0
    require_price_above: bool = False
    trailing_stop_enabled: bool = True
    atr_length: int = 14
    atr_k_long: float = 1.5
    atr_k_short: float = 1.0
    long_only: bool = False
    cooldown_bars: int = 0
    contract_size_max: int = 10
    short_size_factor: float = 0.75
    use_fixed_targets: bool = False
    tp_points: float = 3.0
    sl_points: float = 2.0
    trail_ticks_fixed: int = 5
    tick_size: float = 0.25
    risk_per_point: float = 5.0
    risk_per_trade: float = 500.0
    order_size: Optional[int] = None
    min_real_bars: int = 2


def load_params_from_yaml(path: Path) -> StratParams:
    if yaml is None:
        return StratParams()
    if not path.exists():
        return StratParams()
    data = yaml.safe_load(path.read_text()) or {}
    s = (data.get("strategy") or {})
    instr = (data.get("instrument") or {})
    trade = (data.get("trade") or {})
    risk = (data.get("risk") or {})
    # Cooldown seconds to bars (assume 1m bars)
    cooldown_sec = int((data.get("risk") or {}).get("trade_cooldown_sec", 10)) if isinstance(data.get("risk"), dict) else int((data.get("strategy") or {}).get("trade_cooldown_sec", 10))
    cooldown_bars = int(max(0, ceil(cooldown_sec / 60)))
    return StratParams(
        ema_short=int(s.get("emaShort", 9)),
        ema_long=int(s.get("emaLong", 21)),
        ema_source=str(s.get("emaSource", "close")).lower(),
        rth_only=bool(s.get("rthOnly", False)),
        use_vwap=bool(s.get("vwapEnabled", False)),
        confirm_bars=int(s.get("confirmBars", 0)),
        require_price_above=bool(s.get("requirePriceAboveEMAs", False)),
        trailing_stop_enabled=bool(s.get("trailingStopEnabled", True)),
        atr_length=int(s.get("atrLength", 14)),
        atr_k_long=float(s.get("atrTrailKLong", s.get("atrTrailK", 2.0))),
        atr_k_short=float(s.get("atrTrailKShort", s.get("atrTrailK", 2.0))),
        long_only=bool(s.get("longOnly", True)),
        cooldown_bars=cooldown_bars,
        contract_size_max=int(s.get("contractSizeMax", 10)),
        short_size_factor=float(s.get("shortSizeFactor", 0.75)),
        use_fixed_targets=bool(trade.get("useFixedTargets", False)),
        tp_points=float(trade.get("tpPoints", 0.0) or 0.0),
        sl_points=float(trade.get("slPoints", 0.0) or 0.0),
        trail_ticks_fixed=int(s.get("trailDistanceTicks", 5)),
        tick_size=float((data.get("instrument") or {}).get("tickSize", 0.25)),
        risk_per_point=float((data.get("instrument") or {}).get("riskPerPoint", 5.0)),
        risk_per_trade=float((data.get("strategy") or {}).get("riskPerTrade", 500.0)),
        order_size=int((data.get("trade") or {}).get("order_size", 0) or 0) or None,
        min_real_bars=int(s.get("minRealBarsBeforeTrading", 2) or 0),
    )


def load_data(csv: Optional[Path], min_len: int) -> pd.DataFrame:
    if csv is not None:
        if not csv.exists():
            raise SystemExit(f"CSV not found: {csv}")
        df = pd.read_csv(csv, parse_dates=True, index_col=0)
        required = {"Open", "High", "Low", "Close"}
        miss = required.difference(df.columns)
        if miss:
            raise SystemExit(f"CSV missing columns: {', '.join(sorted(miss))}")
        if "Volume" not in df.columns:
            df["Volume"] = 1
        if len(df) >= min_len:
            return df
        else:
            print(f"CSV length {len(df)} < required {min_len}; generating synthetic instead…")
    rng = pd.date_range("2022-01-01", periods=max(min_len, 400), freq="B")
    rs = np.random.default_rng(seed=1)
    steps = rs.normal(loc=0.02, scale=1.0, size=len(rng))
    close = 100 + steps.cumsum()
    open_ = close + rs.normal(scale=0.2, size=len(rng))
    high = np.maximum(open_, close) + rs.uniform(0.05, 0.5, size=len(rng))
    low = np.minimum(open_, close) - rs.uniform(0.05, 0.5, size=len(rng))
    vol = rs.integers(800, 5000, size=len(rng))
    df = pd.DataFrame({
        "Open": open_, "High": high, "Low": low, "Close": close, "Volume": vol
    }, index=rng)
    df.index.name = "Date"
    return df


class TopstepStrategy(Strategy):
    # Strategy parameters exposed to Backtesting.py (overridable via run(**kwargs))
    ema_short = 9
    ema_long = 21
    ema_source = "close"
    rth_only = False
    use_vwap = False
    confirm_bars = 0
    require_price_above = False
    trailing_stop_enabled = True
    atr_length = 14
    atr_k_long = 1.5
    atr_k_short = 1.0
    long_only = False
    cooldown_bars = 0
    contract_size_max = 10
    short_size_factor = 0.75
    use_fixed_targets = False
    tp_points = 3.0
    sl_points = 2.0
    trail_ticks_fixed = 5
    tick_size = 0.25
    risk_per_point = 5.0
    risk_per_trade = 500.0
    order_size = None
    min_real_bars = 2

    def init(self) -> None:  # type: ignore[override]
        # Snapshot params into a typed container for internal use
        p = StratParams(
            ema_short=int(self.ema_short),
            ema_long=int(self.ema_long),
            ema_source=str(self.ema_source),
            rth_only=bool(self.rth_only),
            use_vwap=bool(self.use_vwap),
            confirm_bars=int(self.confirm_bars),
            require_price_above=bool(self.require_price_above),
            trailing_stop_enabled=bool(self.trailing_stop_enabled),
            atr_length=int(self.atr_length),
            atr_k_long=float(self.atr_k_long),
            atr_k_short=float(self.atr_k_short),
            long_only=bool(self.long_only),
            cooldown_bars=int(self.cooldown_bars),
            contract_size_max=int(self.contract_size_max),
            short_size_factor=float(self.short_size_factor),
            use_fixed_targets=bool(self.use_fixed_targets),
            tp_points=float(self.tp_points),
            sl_points=float(self.sl_points),
            trail_ticks_fixed=int(self.trail_ticks_fixed),
            tick_size=float(self.tick_size),
            risk_per_point=float(self.risk_per_point),
            risk_per_trade=float(self.risk_per_trade),
            order_size=int(self.order_size) if (self.order_size is not None and int(self.order_size) > 0) else None,
            min_real_bars=int(self.min_real_bars),
        )
        self._sp = p
        # Build a DataFrame-like view for indicator calculations
        df = pd.DataFrame({
            "open": self.data.Open,  # type: ignore[attr-defined]
            "high": self.data.High,  # type: ignore[attr-defined]
            "low": self.data.Low,    # type: ignore[attr-defined]
            "close": self.data.Close,  # type: ignore[attr-defined]
            "volume": getattr(self.data, "Volume", np.ones_like(self.data.Close)),  # type: ignore[attr-defined]
        })
        # EMAs per Pine-style seeding
        self.ema_fast = self.I(lambda s=df["close"]: _ema_pine_series(s, p.ema_short))
        self.ema_slow = self.I(lambda s=df["close"]: _ema_pine_series(s, p.ema_long))
        # ATR and VWAP
        self.atr = self.I(lambda h=df["high"], l=df["low"], c=df["close"]: atr_series(h, l, c, p.atr_length))
        self.vwap = self.I(lambda c=df["close"], v=df["volume"]: vwap_cumulative(c, v))

        # State
        self._pending: Optional[Tuple[str, int]] = None  # ("long"|"short", bars_left)
        self._last_signal_bar: Optional[int] = None
        self._trail_sl: Optional[float] = None
        self._trail_side: Optional[int] = None  # 0 long, 1 short

    def _risk_size(self, side: int, entry: float, atr_val: float) -> int:
        p: StratParams = self._sp
        if p.order_size is not None and p.order_size > 0:
            size = int(p.order_size)
        else:
            # Stop distance in points
            stop_points: float
            if p.tick_size and p.trail_ticks_fixed and p.trail_ticks_fixed > 0:
                stop_points = float(p.trail_ticks_fixed) * float(p.tick_size)
            else:
                k = p.atr_k_long if side == 0 else p.atr_k_short
                stop_points = max(1e-6, float(abs(atr_val)) * float(k))
            rp = max(1e-6, float(p.risk_per_point))
            base = int(floor(float(p.risk_per_trade) / (stop_points * rp)))
            if side == 0:
                size = min(p.contract_size_max, max(0, base))
            else:
                size_long = min(p.contract_size_max, max(0, base))
                size = min(int(max(0, floor(size_long * p.short_size_factor))), p.contract_size_max)
        return max(0, size)

    def next(self) -> None:  # type: ignore[override]
        p: StratParams = self._sp
        i = len(self.data.Close) - 1  # current bar index
        if i < max(p.ema_long, p.min_real_bars):
            return
        close = float(self.data.Close[-1])
        ef = float(self.ema_fast[-1]) if not np.isnan(self.ema_fast[-1]) else None
        es = float(self.ema_slow[-1]) if not np.isnan(self.ema_slow[-1]) else None
        if ef is None or es is None:
            return
        prev_rel = None
        try:
            ef_prev = float(self.ema_fast[-2])
            es_prev = float(self.ema_slow[-2])
            if not (np.isnan(ef_prev) or np.isnan(es_prev)):
                prev_rel = ef_prev - es_prev
        except Exception:
            prev_rel = None
        rel = ef - es
        # Cross detection (non-strict by default)
        eps = 1e-9
        cross_up = (prev_rel is not None) and (prev_rel <= eps) and (rel > eps)
        cross_dn = (prev_rel is not None) and (prev_rel >= -eps) and (rel < -eps)

        # Gating
        vwap_ok_long = True
        vwap_ok_short = True
        vw = float(self.vwap[-1]) if not np.isnan(self.vwap[-1]) else None
        if p.use_vwap and vw is not None:
            vwap_ok_long = close > vw
            vwap_ok_short = close < vw
        ema_ok_long = (not p.require_price_above) or (close >= ef and close >= es)
        ema_ok_short = (not p.require_price_above) or (close <= ef and close <= es)

        # Cooldown by bars
        if self._last_signal_bar is not None and p.cooldown_bars > 0:
            if (i - self._last_signal_bar) < p.cooldown_bars:
                cross_up = False
                cross_dn = False

        # Confirmation logic
        if p.confirm_bars > 0:
            if cross_up:
                self._pending = ("long", int(p.confirm_bars))
            elif (not p.long_only) and cross_dn:
                self._pending = ("short", int(p.confirm_bars))
            if self._pending is not None:
                dir_, bars_left = self._pending
                still_valid = (rel > 0) if dir_ == "long" else (rel < 0)
                bars_left -= 1
                if (not still_valid) or bars_left < 0:
                    self._pending = None
                else:
                    self._pending = (dir_, bars_left)
                    if bars_left == 0:
                        if dir_ == "long" and vwap_ok_long and ema_ok_long:
                            self._enter(side=0)
                        elif dir_ == "short" and (not p.long_only) and vwap_ok_short and ema_ok_short:
                            self._enter(side=1)
                        self._pending = None
            # trailing updates even if waiting for confirm
            self._update_trailing()
            return

        # No confirmation: place on cross if passes gating
        placed = False
        if cross_up and vwap_ok_long and ema_ok_long:
            placed = self._enter(side=0)
        elif (not p.long_only) and cross_dn and vwap_ok_short and ema_ok_short:
            placed = self._enter(side=1)
        if placed:
            self._last_signal_bar = i
        self._update_trailing()

    def _enter(self, side: int) -> bool:
        p: StratParams = self._sp
        close = float(self.data.Close[-1])
        av = float(self.atr[-1]) if not np.isnan(self.atr[-1]) else 0.0
        size = self._risk_size(side, close, av)
        if size < 1:
            return False
        # Brackets: fixed TP/SL or initial SL from ATR/ticks
        sl = None
        tp = None
        if p.use_fixed_targets and p.tp_points and p.sl_points:
            if side == 0:
                sl = close - float(p.sl_points)
                tp = close + float(p.tp_points)
            else:
                sl = close + float(p.sl_points)
                tp = close - float(p.tp_points)
        elif p.trailing_stop_enabled:
            # initial protective stop before trailing updates
            if p.tick_size and p.trail_ticks_fixed and p.trail_ticks_fixed > 0:
                dist = float(p.trail_ticks_fixed) * float(p.tick_size)
            else:
                k = p.atr_k_long if side == 0 else p.atr_k_short
                dist = max(1e-6, float(abs(av)) * float(k))
            sl = close - dist if side == 0 else close + dist
        # Place order
        if side == 0:
            self.buy(size=size, sl=sl, tp=tp)
        else:
            self.sell(size=size, sl=sl, tp=tp)
        # Remember for monotonic trailing
        if p.trailing_stop_enabled:
            self._trail_sl = sl
            self._trail_side = side
        return True

    def _update_trailing(self) -> None:
        p: StratParams = self._sp
        if not p.trailing_stop_enabled:
            return
        if not self.position:
            self._trail_sl = None
            self._trail_side = None
            return
        av = float(self.atr[-1]) if not np.isnan(self.atr[-1]) else None
        if av is None:
            return
        close = float(self.data.Close[-1])
        if self.position.is_long:
            k = p.atr_k_long
            new_sl = close - max(1e-6, float(abs(av)) * float(k))
            prev = self._trail_sl
            if (prev is None) or (new_sl > prev):
                for tr in self.trades:
                    tr.sl = new_sl
                self._trail_sl = new_sl
        elif self.position.is_short:
            k = p.atr_k_short
            new_sl = close + max(1e-6, float(abs(av)) * float(k))
            prev = self._trail_sl
            if (prev is None) or (new_sl < prev):
                for tr in self.trades:
                    tr.sl = new_sl
                self._trail_sl = new_sl


def main() -> None:
    ap = argparse.ArgumentParser(description="Topstep strategy backtest")
    ap.add_argument("--csv", type=Path, default=None, help="CSV with Open,High,Low,Close[,Volume]")
    ap.add_argument("--config", type=Path, default=Path("Backend/config.yaml"))
    ap.add_argument("--plot", action="store_true")
    ap.add_argument("--out", type=Path, default=Path("backtest/plot_topstep.html"))
    params = ap.parse_args()

    sp = load_params_from_yaml(params.config)
    df = load_data(params.csv, min_len=max(sp.ema_long * 4, 200))

    # Backtesting expects columns Open/High/Low/Close/Volume, correct types
    bt = Backtest(
        df,
        TopstepStrategy,
        cash=50_000,
        commission=0.0,
        exclusive_orders=True,
    )
    stats = bt.run(**vars(sp))
    print(stats)
    if params.plot:
        try:
            # Backtesting.py can write HTML via the filename parameter
            bt.plot(filename=str(params.out), open_browser=False)
            print(f"Saved chart to: {params.out}")
        except Exception as exc:
            print(f"Plotting skipped: {exc}")


if __name__ == "__main__":
    main()
