import pandas as pd
import pytest

from backtest.engine import EngineConfig, simulate


def _df(rows):
    """rows: list of (event_time_str, price, ma, rsi, z)"""
    return pd.DataFrame(
        rows, columns=["EVENT_TIME", "PRICE_USD", "MA_24H_USD", "RSI_24H", "Z_SCORE_24H"]
    ).assign(EVENT_TIME=lambda d: pd.to_datetime(d["EVENT_TIME"]))


def test_entry_executes_on_the_candle_after_the_signal_not_the_signal_candle():
    # Confirmed Buy fires at hour 0 (rsi<=35, price>ma). If the engine
    # entered on hour 0's own price it would be trading on a signal that
    # isn't fully known until the candle closes.
    rows = [
        ("2026-01-01 00:00", 100, 90, 30, 0),   # signal candle
        ("2026-01-01 01:00", 999, 90, 50, 0),   # entry should happen HERE, at 999
        ("2026-01-01 02:00", 999, 90, 50, 0),
    ]
    result = simulate(_df(rows), EngineConfig(max_holding_hours=100))
    assert result.n_opened == 1
    assert result.trades[0].entry_price == 999


def test_exits_on_take_profit():
    rows = [
        ("2026-01-01 00:00", 100, 90, 30, 0),
        ("2026-01-01 01:00", 100, 90, 50, 0),   # entry
        ("2026-01-01 02:00", 110, 90, 75, 2.5), # take profit
        ("2026-01-01 03:00", 999, 90, 50, 0),
    ]
    result = simulate(_df(rows), EngineConfig(max_holding_hours=100))
    trade = result.trades[0]
    assert trade.exit_reason == "take_profit"
    assert trade.exit_price == 110


def test_exits_on_sell_reversal():
    rows = [
        ("2026-01-01 00:00", 100, 90, 30, 0),
        ("2026-01-01 01:00", 100, 90, 50, 0),   # entry
        ("2026-01-01 02:00", 80, 90, 70, 0),    # sell reversal: rsi>=65, price<ma
        ("2026-01-01 03:00", 999, 90, 50, 0),
    ]
    result = simulate(_df(rows), EngineConfig(max_holding_hours=100))
    assert result.trades[0].exit_reason == "sell_reversal"


def test_exits_on_max_holding_period_when_no_other_exit_fires():
    rows = [
        ("2026-01-01 00:00", 100, 90, 30, 0),
        ("2026-01-01 01:00", 100, 90, 50, 0),   # entry
        ("2026-01-01 02:00", 101, 90, 50, 0),   # 1h held, no signal
        ("2026-01-01 03:00", 102, 90, 50, 0),   # 2h held, exceeds max_holding_hours=2 -> exit here
        ("2026-01-01 04:00", 999, 90, 50, 0),
    ]
    result = simulate(_df(rows), EngineConfig(max_holding_hours=2))
    assert result.trades[0].exit_reason == "max_holding_period"
    assert result.trades[0].exit_price == 102


def test_still_open_position_is_closed_at_data_end_not_dropped():
    rows = [
        ("2026-01-01 00:00", 100, 90, 30, 0),
        ("2026-01-01 01:00", 100, 90, 50, 0),   # entry, never exits before data ends
        ("2026-01-01 02:00", 105, 90, 50, 0),
    ]
    result = simulate(_df(rows), EngineConfig(max_holding_hours=1000))
    assert result.n_opened == 1
    assert result.n_closed == 1
    assert result.n_abandoned == 0
    assert result.trades[0].exit_reason == "data_end"
    assert result.trades[0].exit_price == 105


def test_only_one_position_open_at_a_time():
    rows = [
        ("2026-01-01 00:00", 100, 90, 30, 0),   # signal
        ("2026-01-01 01:00", 100, 90, 30, 0),   # entry here; also still a buy signal, but ignored while in-position
        ("2026-01-01 02:00", 100, 90, 30, 0),
        ("2026-01-01 03:00", 80, 90, 70, 0),    # sell reversal closes it
        ("2026-01-01 04:00", 100, 90, 30, 0),   # new signal after flat
        ("2026-01-01 05:00", 100, 90, 50, 0),   # second entry
    ]
    result = simulate(_df(rows), EngineConfig(max_holding_hours=100))
    assert result.n_opened == 2


def test_net_return_deducts_round_trip_fees():
    rows = [
        ("2026-01-01 00:00", 100, 90, 30, 0),
        ("2026-01-01 01:00", 100, 90, 50, 0),   # entry @ 100
        ("2026-01-01 02:00", 110, 90, 75, 2.5), # exit @ 110, take_profit
    ]
    result = simulate(_df(rows), EngineConfig(max_holding_hours=100, fee_bps=10.0))
    trade = result.trades[0]
    gross = (110 - 100) / 100
    expected_net = gross - 2 * (10.0 / 10_000.0)
    assert trade.net_return(10.0) == pytest.approx(expected_net)
