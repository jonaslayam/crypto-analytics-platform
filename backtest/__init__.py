"""Honest backtest for the DAX signal rules described in the README.

The signals themselves (Confirmed Buy, Take Profit, Sell/Trend Reversal)
are fixed technical-indicator thresholds, not fit to this data -- there
is no model here and nothing to overfit, so this package doesn't need
the purged walk-forward machinery a trained-model backtest would.
What it does need, and provides: simulating trades on the same
already-computed RSI/Z-Score/moving-average columns the dashboard
reads, entering strictly on the candle AFTER a signal fires (never the
signal's own candle), and reporting a buy&hold benchmark alongside the
result so a number never stands alone.

Usage:
    python -m backtest --data <features.csv> --report
"""
