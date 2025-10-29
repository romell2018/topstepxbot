# Duvenchy TopstepX Bot – Quick Usage Guide

This is a concise checklist to get the bot running safely, including where to put your credentials, how to discover your `account_id`, and the common commands to launch the runners and backtests.

## 1) Create a virtual environment and install deps

```bash
python3 -m venv Backend/.venv
source Backend/.venv/bin/activate    # Windows: Backend\.venv\Scripts\activate
pip install -r Backend/requirements.txt
```

## 2) Configure credentials and defaults

Edit `Backend/config.yaml` and set:
- `auth.username` (or `topstepx.username`)
- `auth.api_key` (or `topstepx.api_key`)
- `symbol` (e.g., `MNQ`)
- `tz` (time zone for gating, e.g., `Etc/GMT+4` for fixed UTC−4)

Optional but recommended:
- `hours.enable["HH"]` to allow/deny hours (00–23) for new entries.
- `runtime.flatten_on_disabled_hour: true` to force flat at the top of any disabled hour.

## 3) Discover your account_id

After setting `username` and `api_key`, run the discovery helper:

```bash
source Backend/.venv/bin/activate
python Backend/discover_account_id.py
```

Copy the reported `accountId` and update `account_id` in `Backend/config.yaml` (both under `topstepx.account_id` and top-level `account_id` if present).

## 4) Launch options

Pick one of the following ways to run the bot:

- Pine MNQ strategy runner (console):
  ```bash
  source Backend/.venv/bin/activate
  python Backend/strategies/pine_mnq_strategy.py
  ```
  Notes:
  - Uses your `config.yaml` (symbol, risk, hours, runtime flatten/reboot, etc.).
  - Auto-reconnects the stream and can auto-reboot on prolonged disconnect (see `runtime` section in config).


  ```
  Click “Settings” to enter credentials and “Start” to run the bot.

## 5) Backtesting (optional)

Run EMA(9/21) cross backtests with 1‑minute entries and 1‑second exits:

```bash
source Backend/.venv/bin/activate
python Backend/backtest.py --symbol MNQ --days 5 --exit-sec --tz 'Etc/GMT+4' --rth-only --print-count 50
```

Tips:
- Add `--ticks /path/to/ticks.jsonl` for tick-precise exits.
- Use `--export-csv Backend/trades_export.csv` and `--plot-equity Backend/equity.png` to save outputs.
- Daily PnL prints at the end of a run.

FTMO NASDAQ 100 (US100) backtest from your own data:

```bash
python Backend/backtest_ftmo_us100.py \
  --csv /path/to/us100_bars.csv \
  --tick-size 1.0 --price-decimals 1 --risk-per-point 1.0 \
  --spread-points 2.2 --commission 0.0 --slippage-ticks 0 \
  --ema-short 9 --ema-long 21 --atr-length 14 --exit-sec
```

Notes:
- Provide either `--csv` (bars or ticks CSV) or `--ticks` (JSONL ticks). API fetch is not used for FTMO.
- Tune `--tick-size`, `--price-decimals`, and `--risk-per-point` to match your FTMO platform.

## 6) Common config toggles (in `Backend/config.yaml`)

- `strategy.confirmBars`: 1–3 to reduce chopped crosses.
- `strategy.vwapEnabled: true` and `strategy.requirePriceAboveEMAs: true` to add trend filters.
- `risk.trade_cooldown_sec`: slow re‑entries (e.g., 30–60).
- `runtime.flatten_on_disabled_hour`: flatten at the top of disabled hours.
- `runtime.auto_reboot_on_disconnect`: restart process on prolonged stream loss.
- `runtime.reboot_only_when_flat`: avoid rebooting while in a trade.

## 7) Safety

- Start on a test or evaluation account first.
- Verify credentials and symbol mapping, and watch logs for API errors.
- Ensure your system time/zone is set correctly and matches your expectations for trading hours.
