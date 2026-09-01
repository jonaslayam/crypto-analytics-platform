"""Mirrors the DAX `Current_Market_Status` measure documented in the
README exactly -- same thresholds, same variables (RSI, Z-Score,
price vs. moving average). If the DAX ever changes, this needs to
change with it, or the backtest stops measuring what the dashboard
actually shows.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Thresholds:
    take_profit_rsi: float = 70.0
    take_profit_z: float = 2.0
    floor_rsi: float = 30.0
    floor_z: float = -2.0
    buy_rsi: float = 35.0
    sell_rsi: float = 65.0


def is_confirmed_buy(rsi: float, price: float, moving_average: float, t: Thresholds = Thresholds()) -> bool:
    """RSIValue <= 35 && CurrentPrice > MovingAverage"""
    return rsi <= t.buy_rsi and price > moving_average


def is_take_profit(rsi: float, z_score: float, t: Thresholds = Thresholds()) -> bool:
    """RSIValue >= 70 && ZScoreValue >= 2"""
    return rsi >= t.take_profit_rsi and z_score >= t.take_profit_z


def is_sell_reversal(rsi: float, price: float, moving_average: float, t: Thresholds = Thresholds()) -> bool:
    """RSIValue >= 65 && CurrentPrice < MovingAverage"""
    return rsi >= t.sell_rsi and price < moving_average
