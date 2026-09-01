"""CLI entry point.

    python -m backtest --data <features.csv> --report
"""
from __future__ import annotations

import argparse
import sys

import pandas as pd

from .engine import EngineConfig, simulate
from .metrics import buy_and_hold, t_stat


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="backtest")
    parser.add_argument("--data", required=True, help="CSV with EVENT_TIME, PRICE_USD, MA_24H_USD, RSI_24H, Z_SCORE_24H")
    parser.add_argument("--max-holding-hours", type=float, default=24.0)
    parser.add_argument("--fee-bps", type=float, default=10.0)
    parser.add_argument("--report", action="store_true", help="print a human-readable report")
    args = parser.parse_args(argv)

    df = pd.read_csv(args.data, parse_dates=["EVENT_TIME"])
    df = df.sort_values("EVENT_TIME").reset_index(drop=True)
    # Confirmed Buy needs RSI/MA/price, which are only defined once the
    # 24h moving-average window has data behind it -- drop the warm-up
    # rows rather than let NaN silently compare False everywhere.
    df = df.dropna(subset=["PRICE_USD", "MA_24H_USD", "RSI_24H", "Z_SCORE_24H"]).reset_index(drop=True)

    cfg = EngineConfig(max_holding_hours=args.max_holding_hours, fee_bps=args.fee_bps)
    result = simulate(df, cfg)

    closed = [tr for tr in result.trades if tr.is_closed]
    returns = [tr.net_return(cfg.fee_bps) for tr in closed]
    wins = [r for r in returns if r > 0]

    stats = t_stat(returns)
    bh = buy_and_hold(df)

    if args.report:
        print(f"rows analyzed:        {len(df)}")
        print(f"period:                {df['EVENT_TIME'].iloc[0]} -> {df['EVENT_TIME'].iloc[-1]}")
        print(f"trades opened:         {result.n_opened}")
        print(f"trades closed:         {result.n_closed}")
        print(f"trades abandoned:      {result.n_abandoned} (still open when data ran out)")
        print()
        if returns:
            win_rate = len(wins) / len(returns)
            total_return = 1.0
            for r in returns:
                total_return *= (1 + r)
            total_return -= 1.0
            print(f"win rate:              {win_rate:.1%} ({len(wins)}/{len(returns)})")
            print(f"mean return/trade:     {stats.mean_return:.4%} (net of {cfg.fee_bps} bps/side fees)")
            print(f"compounded return:     {total_return:.2%} (all trades chained, no reinvestment sizing assumed)")
            print(f"t-stat:                {stats.t_stat:.3f} ({'>2, naively significant' if stats.significant else 'not significant at naive 95%'})")
            print("  caveat: trades are single-asset and not independent draws -- a")
            print("  multi-week trend can cluster correlated wins or losses. Take the")
            print("  t-stat as a first check, not a rigorous significance claim.")
        else:
            print("no trades were generated -- the signal never fired on this data.")
        print()
        print(f"buy & hold over same period: {bh:.2%} (no fees, single entry at period start)")
        print()
        for tr in closed:
            r = tr.net_return(cfg.fee_bps)
            print(f"  {tr.entry_time} @ {tr.entry_price:.2f} -> {tr.exit_time} @ {tr.exit_price:.2f}  "
                  f"[{tr.exit_reason}]  {r:+.2%}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
