"""Statistics on the simulated trades.

Trades here don't overlap in time (only one position open at a time),
so the overlapping-window correction the crypto-streaming-app backtest
needed doesn't apply the same way. What's still true, and worth being
honest about: every trade comes from the SAME single asset over the
SAME roughly one-year stretch, so they aren't independent draws either
-- a market regime (a multi-week bull or bear run) can produce a
cluster of correlated wins or losses in a row. A one-sample t-test on
these trades is a legitimate first check, not a fully rigorous claim of
independence.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass

import pandas as pd


@dataclass
class TStatResult:
    n_trades: int
    mean_return: float
    std_return: float
    t_stat: float

    @property
    def significant(self) -> bool:
        return abs(self.t_stat) > 2.0

    def to_dict(self) -> dict:
        return {
            "n_trades": self.n_trades,
            "mean_return_pct": round(self.mean_return * 100, 4),
            "t_stat": round(self.t_stat, 3),
            "significant_at_95pct": self.significant,
        }


def t_stat(returns: list[float]) -> TStatResult:
    n = len(returns)
    if n < 2:
        return TStatResult(n, 0.0, 0.0, 0.0)
    mean = statistics.fmean(returns)
    std = statistics.pstdev(returns)
    t = mean / (std / (n ** 0.5)) if std > 0 else 0.0
    return TStatResult(n, mean, std, t)


def buy_and_hold(df: pd.DataFrame) -> float:
    """Equal to holding the asset from the first to the last row in `df`."""
    first_price = df["PRICE_USD"].iloc[0]
    last_price = df["PRICE_USD"].iloc[-1]
    return (last_price - first_price) / first_price
