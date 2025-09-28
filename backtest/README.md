# Backtesting Setup

This folder shows how to evaluate strategy performance with the `backtesting` Python package.

## Quick start
1. Install the dependency:
   ```bash
   pip install backtesting pandas
   ```
2. Prepare your historical market data as a CSV (columns: `Open`, `High`, `Low`, `Close`, optional `Volume`).
3. Update `example_strategy.py` if you need a different strategy or data source.
4. Run the example:
   ```bash
   python example_strategy.py
   ```

The script prints a performance report and opens a plot (if supported by your environment) showing equity curve, trades, and drawdowns.

## Folder contents
- `example_strategy.py` – minimal moving-average crossover strategy to demonstrate the API.
- `sample_data.csv` – placeholder; replace with your own history or generate programmatically.

Feel free to add more strategy scripts or notebooks here.
