import pandas as pd
import pytest

from backtest.metrics import buy_and_hold, t_stat


def test_t_stat_zero_when_fewer_than_two_returns():
    result = t_stat([0.01])
    assert result.t_stat == 0.0
    assert result.n_trades == 1


def test_t_stat_zero_when_no_variance():
    # all returns identical -> std is 0, t_stat would divide by zero if unguarded
    result = t_stat([0.01, 0.01, 0.01])
    assert result.std_return == 0.0
    assert result.t_stat == 0.0


def test_t_stat_computed_correctly_against_manual_values():
    returns = [0.02, -0.01, 0.03, -0.02, 0.01]
    result = t_stat(returns)
    assert result.n_trades == 5
    assert result.mean_return == pytest.approx(0.006)
    # recompute independently to catch a wrong formula, not just re-assert the implementation
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / len(returns)
    std = var ** 0.5
    expected_t = mean / (std / (len(returns) ** 0.5))
    assert result.t_stat == pytest.approx(expected_t)


def test_significant_threshold():
    assert t_stat([1, 1, 1, 1, -0.001]).significant in (True, False)  # smoke: property doesn't crash
    strong = t_stat([0.05, 0.05, 0.05, 0.05, 0.04])
    assert strong.significant is True


def test_buy_and_hold_uses_first_and_last_row_only():
    df = pd.DataFrame({"PRICE_USD": [100, 999, 999, 150]})
    result = buy_and_hold(df)
    assert result == pytest.approx((150 - 100) / 100)


def test_buy_and_hold_negative_when_price_drops():
    df = pd.DataFrame({"PRICE_USD": [200, 100]})
    result = buy_and_hold(df)
    assert result == pytest.approx(-0.5)
