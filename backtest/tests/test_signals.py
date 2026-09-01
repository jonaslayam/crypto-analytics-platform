from backtest.signals import Thresholds, is_confirmed_buy, is_sell_reversal, is_take_profit

T = Thresholds()


def test_confirmed_buy_requires_both_conditions():
    assert is_confirmed_buy(rsi=30, price=105, moving_average=100, t=T) is True
    assert is_confirmed_buy(rsi=30, price=95, moving_average=100, t=T) is False  # price below MA
    assert is_confirmed_buy(rsi=40, price=105, moving_average=100, t=T) is False  # RSI too high


def test_confirmed_buy_boundary_is_inclusive():
    assert is_confirmed_buy(rsi=35, price=101, moving_average=100, t=T) is True


def test_take_profit_requires_both_conditions():
    assert is_take_profit(rsi=75, z_score=2.5, t=T) is True
    assert is_take_profit(rsi=75, z_score=1.0, t=T) is False
    assert is_take_profit(rsi=60, z_score=2.5, t=T) is False


def test_sell_reversal_requires_both_conditions():
    assert is_sell_reversal(rsi=70, price=95, moving_average=100, t=T) is True
    assert is_sell_reversal(rsi=70, price=105, moving_average=100, t=T) is False  # price above MA
    assert is_sell_reversal(rsi=50, price=95, moving_average=100, t=T) is False  # RSI too low
