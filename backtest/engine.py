"""Simulates following the DAX signals mechanically on real historical data.

Only one position open at a time (matching how a person reading the
dashboard would actually act on it), so trades never overlap in time by
construction -- there's no artificial inflation of the sample size from
counting the same holding period many times over, the way there would
be if a signal could fire on every candle.
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .signals import Thresholds, is_confirmed_buy, is_sell_reversal, is_take_profit


@dataclass
class Trade:
    entry_time: pd.Timestamp
    entry_price: float
    exit_time: pd.Timestamp | None = None
    exit_price: float | None = None
    exit_reason: str | None = None

    @property
    def is_closed(self) -> bool:
        return self.exit_time is not None

    def net_return(self, fee_bps: float) -> float | None:
        if not self.is_closed:
            return None
        gross = (self.exit_price - self.entry_price) / self.entry_price
        return gross - 2 * (fee_bps / 10_000.0)


@dataclass
class EngineConfig:
    max_holding_hours: float = 24.0
    fee_bps: float = 10.0
    thresholds: Thresholds = Thresholds()


@dataclass
class BacktestResult:
    trades: list[Trade]
    config: EngineConfig

    @property
    def n_opened(self) -> int:
        return len(self.trades)

    @property
    def n_closed(self) -> int:
        return sum(1 for t in self.trades if t.is_closed)

    @property
    def n_abandoned(self) -> int:
        return self.n_opened - self.n_closed


def simulate(df: pd.DataFrame, config: EngineConfig | None = None) -> BacktestResult:
    """`df` must be sorted ascending by EVENT_TIME, one row per hour, for a
    single asset -- columns EVENT_TIME, PRICE_USD, MA_24H_USD, RSI_24H, Z_SCORE_24H.
    """
    cfg = config or EngineConfig()
    t = cfg.thresholds

    times = df["EVENT_TIME"].to_numpy()
    prices = df["PRICE_USD"].to_numpy()
    mas = df["MA_24H_USD"].to_numpy()
    rsis = df["RSI_24H"].to_numpy()
    zscores = df["Z_SCORE_24H"].to_numpy()
    n = len(df)

    trades: list[Trade] = []
    open_trade: Trade | None = None

    i = 0
    while i < n:
        if open_trade is None:
            if is_confirmed_buy(rsis[i], prices[i], mas[i], t):
                # Execute on the NEXT candle -- the signal at i is only
                # fully known once candle i has closed.
                entry_idx = i + 1
                if entry_idx >= n:
                    break
                open_trade = Trade(
                    entry_time=pd.Timestamp(times[entry_idx]),
                    entry_price=float(prices[entry_idx]),
                )
                i = entry_idx + 1
                continue
            i += 1
            continue

        hours_held = (pd.Timestamp(times[i]) - open_trade.entry_time).total_seconds() / 3600.0
        if is_take_profit(rsis[i], zscores[i], t):
            open_trade.exit_time = pd.Timestamp(times[i])
            open_trade.exit_price = float(prices[i])
            open_trade.exit_reason = "take_profit"
            trades.append(open_trade)
            open_trade = None
        elif is_sell_reversal(rsis[i], prices[i], mas[i], t):
            open_trade.exit_time = pd.Timestamp(times[i])
            open_trade.exit_price = float(prices[i])
            open_trade.exit_reason = "sell_reversal"
            trades.append(open_trade)
            open_trade = None
        elif hours_held >= cfg.max_holding_hours:
            open_trade.exit_time = pd.Timestamp(times[i])
            open_trade.exit_price = float(prices[i])
            open_trade.exit_reason = "max_holding_period"
            trades.append(open_trade)
            open_trade = None
        i += 1

    if open_trade is not None:
        open_trade.exit_time = pd.Timestamp(times[-1])
        open_trade.exit_price = float(prices[-1])
        open_trade.exit_reason = "data_end"
        trades.append(open_trade)

    return BacktestResult(trades=trades, config=cfg)
