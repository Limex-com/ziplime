"""Vectorised signals inside an event-driven run: the same strategy, written twice.

Runs `strategies/v01_vectorised.py` and `strategies/v01_bar_by_bar.py` over the same real data,
checks them against each other trade for trade, and times both. The agreement is the point: if
lifting the arithmetic out of the loop changes the result, the speed-up is worth nothing.

    python examples/vectorized/signals_in_run.py

SPY and QQQ bars come from Yahoo. vectorbt is not needed for any of this.
"""
import asyncio
import cProfile
import datetime
import pstats
import sys
import time
from pathlib import Path

import pandas as pd
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ziplime.assets.domain.asset_type import AssetType  # noqa: E402
from ziplime.assets.entities.asset_symbol import AssetSymbol  # noqa: E402
from ziplime.constants.data_type import DataType  # noqa: E402
from ziplime.core.ingest_data import get_asset_service  # noqa: E402
from ziplime.core.run_simulation import run_simulation  # noqa: E402
from ziplime.data.domain.data_bundle import DataBundle  # noqa: E402
from ziplime.finance.commission.no_commission import NoCommission  # noqa: E402
from ziplime.finance.slippage.no_slippage import NoSlippage  # noqa: E402
from ziplime.utils.bundle_utils import get_market_data_source  # noqa: E402
from ziplime.utils.calendar_utils import get_calendar  # noqa: E402

HERE = Path(__file__).parent
ROOT = HERE.parent.parent
ASSET_DB = str((ROOT / "data" / "assets.sqlite").absolute())
UNIVERSE = [("SPY", "ARCX"), ("QQQ", "XNMS")]
CALENDAR = "XNYS"
CASH = 100_000.0

#: The run, and the history behind it. The indicators need 63 sessions before the first bar, so
#: the bundle starts well before the run does.
RUN_START = datetime.date(2021, 1, 4)
BUNDLE_START = datetime.date(2020, 1, 2)


async def load_bundle(asset_service, calendar, end: datetime.date):
    """Real daily bars from Yahoo, assembled into a ziplime bundle."""
    listings = {}
    for ticker, mic in UNIVERSE:
        listing = await asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=ticker, mic=mic), asset_type=AssetType.EQUITY)
        if listing is None:
            raise SystemExit(f"{ticker}@{mic} is not in the asset database -- "
                             f"run `ziplime ingest-assets` first.")
        listings[ticker] = listing

    source = get_market_data_source("yahoo", assets=list(listings.values()))
    frame = await source.get_data(
        symbols=list(listings), frequency=datetime.timedelta(days=1),
        date_from=datetime.datetime.combine(BUNDLE_START, datetime.time.min, tzinfo=calendar.tz),
        date_to=datetime.datetime.combine(end, datetime.time.max, tzinfo=calendar.tz))
    if frame.is_empty():
        raise SystemExit("Yahoo returned no bars.")

    sessions = calendar.sessions_in_range(BUNDLE_START, end)
    closes = {stamp.date(): stamp.to_pydatetime() for stamp in
              calendar.schedule.loc[sessions, "close"].dt.tz_convert(calendar.tz)}
    wide = frame.to_pandas()
    wide["_d"] = pd.to_datetime(wide["date"]).dt.tz_convert(calendar.tz).dt.date
    wide = wide[wide["_d"].isin(closes)]

    rows = [{"date": closes[r["_d"]], "sid": listings[r["symbol"]].sid, "symbol": r["symbol"],
             "mic": dict(UNIVERSE)[r["symbol"]], "open": r["open"], "high": r["high"],
             "low": r["low"], "close": r["close"], "price": r["close"], "volume": r["volume"]}
            for r in wide.to_dict("records")]
    data = pl.DataFrame(rows).sort(["sid", "date"])
    spans = data.with_row_index().group_by("sid", maintain_order=True).agg(
        [pl.col("index").first().alias("first"), pl.col("index").last().alias("last")])
    bundle = DataBundle(
        name="signals-example", version="1", start_date=data["date"].min(),
        end_date=data["date"].max(), trading_calendar=calendar,
        frequency=datetime.timedelta(days=1), original_frequency=datetime.timedelta(days=1),
        data_type=DataType.MARKET_DATA, timestamp=data["date"].max(), data=data,
        sid_indexes={r["sid"]: (r["first"], r["last"] + 1) for r in spans.iter_rows(named=True)})
    return bundle, listings, data["date"].max()


async def run(strategy: str, asset_service, calendar, bundle, end):
    started = time.perf_counter()
    result = await run_simulation(
        start_date=datetime.datetime.combine(RUN_START, datetime.time.min, tzinfo=calendar.tz),
        end_date=end, trading_calendar=CALENDAR, emission_rate=datetime.timedelta(days=1),
        total_cash=CASH, market_data_source=bundle, custom_data_sources=[],
        algorithm_file=str(HERE / "strategies" / strategy), stop_on_error=True,
        asset_service=asset_service, benchmark_asset_symbol=None, benchmark_returns=None,
        equity_commission=NoCommission(), equity_slippage=NoSlippage(), max_leverage=1.0,
        print_algo=False)
    return time.perf_counter() - started, result


def strategy_seconds(profile: pstats.Stats, filename: str) -> float:
    """Time spent in the strategy itself, separated from the engine's own overhead."""
    for (path, _, func), (_, _, _, cumulative, _) in profile.stats.items():
        if path.endswith(filename) and func == "handle_data":
            return cumulative
    return float("nan")


def trades(result):
    return [(t.asset.symbol, t.amount, round(float(t.price), 6))
            for row in result.perf["transactions"] for t in row]


async def main():
    calendar = get_calendar(CALENDAR)
    asset_service = get_asset_service(db_path=ASSET_DB)
    try:
        end = datetime.date.today() - datetime.timedelta(days=1)
        bundle, listings, last_bar = await load_bundle(asset_service, calendar, end)
        sessions = len(calendar.sessions_in_range(RUN_START, last_bar.date()))
        print(f"Loaded {len(bundle.data)} bars over {len(listings)} instruments; "
              f"the run is {sessions} sessions from {RUN_START}")

        timings, results, profiles = {}, {}, {}
        for label, strategy in (("vectorised", "v01_vectorised.py"),
                                ("bar by bar", "v01_bar_by_bar.py")):
            profiler = cProfile.Profile()
            profiler.enable()
            elapsed, result = await run(strategy, asset_service, calendar, bundle, last_bar)
            profiler.disable()
            timings[label] = elapsed
            results[label] = result
            profiles[label] = strategy_seconds(pstats.Stats(profiler), strategy)

        placed = trades(results["vectorised"])
        same = placed == trades(results["bar by bar"])
        print(f"\n{len(placed)} trades. The two versions trade identically: "
              f"{'yes' if same else 'NO'}")
        if not same:
            raise SystemExit("The two versions disagree -- the speed-up was bought by trading a "
                             "different strategy.")

        print("\nTime inside the strategy itself (handle_data, engine excluded):")
        for label in timings:
            print(f"   {label:<12} {profiles[label]:6.2f}s")
        print(f"   speed-up     {profiles['bar by bar'] / profiles['vectorised']:6.1f}x")

        print("\nTime for the whole run:")
        for label in timings:
            print(f"   {label:<12} {timings[label]:6.2f}s")
        print(f"   speed-up     {timings['bar by bar'] / timings['vectorised']:6.1f}x")
        print("\nThe speed-up over the whole run is smaller than the one inside the strategy, and\n"
              "that is exactly right: the engine still does its work every session -- ledger,\n"
              "dividends, metrics -- and none of it is touched here. This removes the cost of the\n"
              "signals, not the cost of the simulation.")

        perf = results["vectorised"].perf
        print("\nResult (identical for both versions):")
        print(f"   return        {float(perf['algorithm_period_return'].iloc[-1]):+.2%}")
        print(f"   Sharpe        {float(perf['sharpe'].iloc[-1]):.2f}")
        print(f"   drawdown      {float(perf['max_drawdown'].iloc[-1]):.2%}")
    finally:
        await asset_service._asset_repository.engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
