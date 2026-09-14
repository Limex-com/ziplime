"""Sweep the parameters vectorised, report the winner as ziplime.

This is what the hybrid is for. Trying forty-eight moving-average combinations bar by bar is
forty-eight full runs; the vector kernel computes each one over arrays, with no ledger and no
metrics in the way. The winning combination is then **replayed** through ziplime's ledger and
comes back as the same result an event-driven run produces: same table, same metrics, same report.

The sweep used to call `vectorbt.Portfolio.from_signals`. It now calls
`ziplime.vectorized.kernel`, and that is the only thing that changed -- the adapter, the ledger,
the metrics and the report are the same code. The numbers did not move and were not expected to:
`tests/reference/vectorbt/` holds the two engines to the same fills, and
`tests/test_vector_kernel_parity.py` holds the kernel to the same fills as the event-driven
engine.

Execution timing is now stated rather than hidden in a `.shift()` on the signals:
`ExecutionTiming.next_close()` is a decision taken on a bar's close and filled on the next one's.

Run:

    python examples/vectorized/sweep_then_report.py

Prices for SPY and QQQ come from Yahoo. Nothing extra needs installing.
"""
import asyncio
import datetime
import itertools
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ziplime.assets.domain.asset_type import AssetType  # noqa: E402
from ziplime.assets.entities.asset_symbol import AssetSymbol  # noqa: E402
from ziplime.core.ingest_data import get_asset_service  # noqa: E402
from ziplime.exchanges.simulation_exchange import SimulationExchange  # noqa: E402
from ziplime.finance.commission.no_commission import NoCommission  # noqa: E402
from ziplime.finance.slippage.no_slippage import NoSlippage  # noqa: E402
from ziplime.utils.bundle_utils import get_market_data_source  # noqa: E402
from ziplime.utils.calendar_utils import get_calendar  # noqa: E402
from ziplime.vectorized import to_execution_result  # noqa: E402
from ziplime.vectorized.kernel import ExecutionTiming, simulate_signals  # noqa: E402

ASSET_DB = str(Path(Path(__file__).parent.parent.parent, "data", "assets.sqlite").absolute())
UNIVERSE = [("SPY", "ARCX"), ("QQQ", "XNMS")]
CALENDAR = "XNYS"
CASH = 100_000.0

#: The grid. 8 x 6 = 48 combinations, which bar by bar would be 48 full runs.
FAST_WINDOWS = [5, 10, 15, 20, 25, 30, 40, 50]
SLOW_WINDOWS = [60, 80, 100, 120, 150, 200]

#: Shares per signal. Fixed: this example is about the sweep, not about sizing.
SIZE = 100


async def load_prices(asset_service, start: datetime.date, end: datetime.date):
    """Real daily closes, stamped at the calendar's session closes."""
    calendar = get_calendar(CALENDAR)
    listings = {}
    for ticker, mic in UNIVERSE:
        listing = await asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=ticker, mic=mic), asset_type=AssetType.EQUITY)
        if listing is None:
            raise SystemExit(f"{ticker}@{mic} is not in the asset database -- run "
                             f"`ziplime ingest-assets` first.")
        listings[ticker] = listing

    source = get_market_data_source("yahoo", assets=list(listings.values()))
    frame = await source.get_data(
        symbols=list(listings), frequency=datetime.timedelta(days=1),
        date_from=datetime.datetime.combine(start, datetime.time.min, tzinfo=calendar.tz),
        date_to=datetime.datetime.combine(end, datetime.time.max, tzinfo=calendar.tz))
    if frame.is_empty():
        raise SystemExit("Yahoo returned no bars.")

    closes = {session.date(): close.to_pydatetime() for session, close in calendar.schedule.loc[
        calendar.sessions_in_range(start, end), "close"].dt.tz_convert(calendar.tz).items()}
    wide = (frame.to_pandas()
            .assign(_d=lambda f: pd.to_datetime(f["date"]).dt.tz_convert(calendar.tz).dt.date)
            .query("_d in @closes")
            .pivot(index="_d", columns="symbol", values="close"))
    wide.index = pd.DatetimeIndex([closes[d] for d in wide.index])
    return wide.dropna(), listings


def sweep(prices: pd.DataFrame, listings: dict):
    """Every combination. This is the part a vector engine is worth having for.

    The signals are levels rather than edges: `fast > slow` is true for a run of bars, and the
    kernel with `accumulate=False` (the default) opens the position once instead of adding to it
    every bar. Edge-detecting by hand used to be necessary here precisely because that behaviour
    was decided somewhere else.
    """
    combos = [(f, s) for f, s in itertools.product(FAST_WINDOWS, SLOW_WINDOWS) if f < s]
    results = []
    for fast_window, slow_window in combos:
        fast = prices.rolling(fast_window).mean()
        slow = prices.rolling(slow_window).mean()
        result = simulate_signals(
            prices=prices,
            entries=(fast > slow).fillna(False),
            exits=(fast < slow).fillna(False),
            size=SIZE, initial_cash=CASH,
            execution=ExecutionTiming.next_close(),
            assets=listings)
        results.append(((fast_window, slow_window), result))
    return results


async def main():
    end = datetime.date.today() - datetime.timedelta(days=1)
    start = end - datetime.timedelta(days=365 * 4)
    asset_service = get_asset_service(db_path=ASSET_DB)
    try:
        prices, listings = await load_prices(asset_service, start, end)
        print(f"Loaded: {len(prices)} sessions, {len(prices.columns)} instruments "
              f"({prices.index[0].date()} .. {prices.index[-1].date()})")

        started = datetime.datetime.now()
        swept = sweep(prices, listings)
        elapsed = (datetime.datetime.now() - started).total_seconds()
        print(f"Vectorised sweep: {len(swept)} combinations in {elapsed:.1f}s "
              f"({elapsed / len(swept) * 1000:.0f} ms each)")

        ranked = sorted(swept, key=lambda item: float(item[1].portfolio_value.iloc[-1]),
                        reverse=True)
        print("\nBest five by ending value (the kernel's working numbers, not the reported ones):")
        for (fast_window, slow_window), result in ranked[:5]:
            print(f"   SMA {fast_window:>3}/{slow_window:<3}  "
                  f"ending {float(result.portfolio_value.iloc[-1]):>12,.2f}  "
                  f"fills {len(result.fills):>3}")

        (fast_window, slow_window), winner = ranked[0]
        print(f"\nReplaying SMA {fast_window}/{slow_window} through ziplime's ledger...")
        calendar = get_calendar(CALENDAR)
        exchange = SimulationExchange(
            name="VECTOR", country_code="US", trading_calendar=calendar, clock=None,
            cash_balance=CASH, equity_slippage=NoSlippage(), future_slippage=NoSlippage(),
            equity_commission=NoCommission(), future_commission=NoCommission(),
            account_id="vectorized_account", is_default=True)
        result = await to_execution_result(
            portfolio=winner, listings=listings, prices=prices,
            trading_calendar=calendar, exchange=exchange,
            emission_rate=datetime.timedelta(days=1))

        perf = result.perf
        print("\nThe ziplime result -- the same metrics an event-driven run reports:")
        print(f"   sessions      {len(perf)}")
        print(f"   return        {float(perf['algorithm_period_return'].iloc[-1]):+.2%}")
        print(f"   sharpe        {float(perf['sharpe'].iloc[-1]):.2f}")
        print(f"   volatility    {float(perf['algo_volatility'].iloc[-1]):.2%}")
        print(f"   max drawdown  {float(perf['max_drawdown'].iloc[-1]):.2%}")
        print(f"   transactions  {sum(len(t) for t in perf['transactions'])}")
        print(f"   ending value  {float(perf['portfolio_value'].iloc[-1]):,.2f}")

        print(f"\nKernel: {len(winner.fills)} fills, {winner.no_ops} signals landed on a position "
              f"that was already open (level signals; this is the normal case).")
        if winner.rejects:
            reasons = {}
            for reject in winner.rejects:
                reasons[reject.reason] = reasons.get(reject.reason, 0) + 1
            print("Rejected: "
                  + ", ".join(f"{reason} x{count}" for reason, count in sorted(reasons.items())))
            print("Rejections are observable on purpose: a strategy that stopped trading because "
                  "it ran out of money looks exactly like one that stopped signalling.")

        print("\nReconciled -- the ledger and the kernel agreed about the portfolio's value on "
              "every bar, or to_execution_result() would have refused to return a result.")
    finally:
        await asset_service._asset_repository.engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
