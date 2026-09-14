"""Backtest the vectorised 0DTE straddle on real OPRA data, minute by minute.

    python examples/options/run_0dte_straddle.py

Needs `GRPC_TOKEN` and `GRPC_SERVER_URL` in the repository's `.env`: the prices here are real.

**Every session in the window, not just the last one.** A 0DTE contract expires on the day it is
listed, so the chain for a past session is one the feed's ordinary lookup no longer finds --
`GetSecurityInfo` answers for what is currently listed and raises for what is not, and its own
fallback addresses the candle service by ticker, which that service does not accept. The effect
is that an expired option looks like an option with no data. It is not:
`ziplime.data.data_sources.options.grpc_chain.ExpiredAwareFeed` resolves the same contracts
through `GetSecurityHistory` and the bars come back in full -- 405 minutes a session, measured.

The chain itself is built by *constructing* OCC symbols around each session's opening price and
keeping the ones that answer. `GetOptionFamilies` cannot help here: it lists what is listed now,
and a past 0DTE series is not.

What this still is not: a large sample. Nineteen sessions is nineteen, and a 0DTE edge is a
distribution over hundreds. Read the per-session table rather than the aggregate.
"""
import asyncio
import datetime
import os
import shutil
import sys
import tempfile
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ziplime.assets.domain.asset_type import AssetType  # noqa: E402
from ziplime.assets.domain.exercise_style import ExerciseStyle  # noqa: E402
from ziplime.assets.domain.option_type import OptionType  # noqa: E402
from ziplime.assets.domain.premium_style import PremiumStyle  # noqa: E402
from ziplime.assets.domain.settlement_type import SettlementType  # noqa: E402
from ziplime.assets.entities.asset_symbol import AssetSymbol  # noqa: E402
from ziplime.constants.data_type import DataType  # noqa: E402
from ziplime.core.ingest_data import get_asset_service  # noqa: E402
from ziplime.data.data_sources.options.grpc_chain import ExpiredAwareFeed  # noqa: E402
from ziplime.data.data_sources.options.ingest import register_contracts  # noqa: E402
from ziplime.data.data_sources.options.source import ContractSpec  # noqa: E402
from ziplime.data.domain.data_bundle import DataBundle  # noqa: E402
from ziplime.data.services.bar_alignment import clock_minutes  # noqa: E402
from ziplime.finance.commission.no_commission import NoCommission  # noqa: E402
from ziplime.finance.slippage.no_slippage import NoSlippage  # noqa: E402
from ziplime.utils.calendar_utils import get_calendar  # noqa: E402

REPO = Path(__file__).parent.parent.parent
CALENDAR = "XNYS"
UNDERLYING = ("SPY", "ARCX")
MINUTE = datetime.timedelta(minutes=1)

#: The window to run. Every trading session in it gets its own 0DTE chain.
FIRST_SESSION = datetime.date(2026, 8, 17)
LAST_SESSION = datetime.date(2026, 9, 11)
#: Strikes either side of each session's opening price. The strategy buys the at-the-money
#: straddle, so a narrow band is enough -- and every extra strike is two more requests per session.
STRIKE_SPAN = 3
CASH = 100_000.0
#: Also run the bar-by-bar twin and diff the fills. Doubles the runtime of the simulation half.
COMPARE_ENGINES = True


def load_env() -> None:
    for line in (REPO / ".env").read_text().splitlines():
        if "=" in line and not line.strip().startswith("#"):
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


def occ_padded(expiry: datetime.date, kind: str, strike: float) -> str:
    """The 21-character OCC symbol the feed speaks, root padded to six characters."""
    return f"{UNDERLYING[0]:<6}{expiry:%y%m%d}{kind}{int(round(strike * 1000)):08d}"


def fills_of(result) -> list[tuple]:
    """Every fill, as the tuple that decides whether two runs did the same thing."""
    return sorted([(t.dt, t.asset.symbol, round(float(t.amount), 8), round(float(t.price), 8))
                   for row in result.perf["transactions"] for t in row], key=str)


async def _compare(simulate, vectorised) -> None:
    """Run the bar-by-bar twin over the same bundle and require the same fills.

    This is what "options work event-driven" actually means, said in a way that can fail. Both
    runs use the same ledger, the same metric set and the same bars, so the only thing left that
    can differ is where the arithmetic happened -- and if it differs, the vectorised version is
    computing something the bar-by-bar one is not, and its speed is worthless.
    """
    print("\nrunning the bar-by-bar twin over the same bundle...")
    event_driven = await simulate("o04_event_0dte_straddle.py")

    left, right = fills_of(vectorised), fills_of(event_driven)
    print(f"  vectorised   {len(left)} fills")
    print(f"  event-driven {len(right)} fills")
    if left == right:
        print("  -> identical, fill for fill")
    else:
        only_left = [fill for fill in left if fill not in right]
        only_right = [fill for fill in right if fill not in left]
        print(f"  -> DIFFER: {len(only_left)} only vectorised, {len(only_right)} only event")
        for fill in only_left[:4]:
            print(f"       vectorised only  {fill[0]:%Y-%m-%d %H:%M}  {fill[1]}  x{fill[2]} @ {fill[3]}")
        for fill in only_right[:4]:
            print(f"       event only       {fill[0]:%Y-%m-%d %H:%M}  {fill[1]}  x{fill[2]} @ {fill[3]}")

    for column in ("portfolio_value", "ending_cash"):
        a = vectorised.perf[column].to_numpy(dtype=float)
        b = event_driven.perf[column].to_numpy(dtype=float)
        worst = float(max(abs(a - b))) if len(a) == len(b) else float("nan")
        print(f"  max |{column} difference|: {worst:,.10f}")
    print(f"  errors: vectorised {len(vectorised.errors or [])}, "
          f"event-driven {len(event_driven.errors or [])}")


async def main():
    load_env()
    token, server = os.environ.get("GRPC_TOKEN"), os.environ.get("GRPC_SERVER_URL")
    if not token or not server:
        raise SystemExit("GRPC_TOKEN and GRPC_SERVER_URL are needed: the prices here are real.")
    endpoint = server.replace("https://", "").replace("http://", "").rstrip("/")

    temp = tempfile.TemporaryDirectory(prefix="ziplime-0dte-")
    database = Path(temp.name) / "assets.sqlite"
    shutil.copy2(REPO / "data" / "assets.sqlite", database)
    asset_service = get_asset_service(db_path=str(database))
    calendar = get_calendar(CALENDAR)
    sessions = [s.date() for s in calendar.sessions_in_range(FIRST_SESSION, LAST_SESSION)]
    grid = clock_minutes(calendar, FIRST_SESSION, LAST_SESSION, MINUTE)
    zone = str(grid.dtype.time_zone)
    print(f"{len(sessions)} sessions, {len(grid):,} clock minutes "
          f"({sessions[0]} .. {sessions[-1]})")

    try:
        underlying = await asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=UNDERLYING[0], mic=UNDERLYING[1]),
            asset_type=AssetType.EQUITY)

        from ziplime_grpc_data_source_private.grpc_data_source import GrpcDataSource

        # Wrapped, so a session whose contracts have already expired resolves like any other.
        feed = ExpiredAwareFeed(GrpcDataSource(authorization_token=token, server_url=endpoint))

        async def minute_bars(symbol: str, first: datetime.date, last: datetime.date):
            return await feed.get_data(
                symbols=[symbol], frequency=MINUTE,
                date_from=datetime.datetime.combine(first, datetime.time.min,
                                                    tzinfo=datetime.timezone.utc),
                date_to=datetime.datetime.combine(last, datetime.time.max,
                                                  tzinfo=datetime.timezone.utc))

        spot_bars = await minute_bars(f"{UNDERLYING[0]}@{UNDERLYING[1]}",
                                      sessions[0], sessions[-1])
        if spot_bars.is_empty():
            raise SystemExit(f"No {UNDERLYING[0]} bars over the window.")
        spot_bars = spot_bars.with_columns(
            pl.col("date").dt.convert_time_zone(zone).alias("date"))
        # The group key gets its own name: grouping by `pl.col("date").dt.date()` keeps the name
        # "date", and aggregating a column of the same name collides with it.
        opens = {row["session"]: row["open"] for row in
                 spot_bars.sort("date")
                 .group_by(pl.col("date").dt.date().alias("session"), maintain_order=True)
                 .agg(pl.col("open").first()).iter_rows(named=True)}
        print(f"{UNDERLYING[0]}: {spot_bars.height:,} minute bars over "
              f"{len(opens)} sessions\n")

        # One chain per session, built by constructing OCC symbols around that session's open.
        specs, quoted = [], []
        print(f"{'session':<12}{'open':>9}{'contracts':>11}")
        print("-" * 34)
        for session in sessions:
            opening = opens.get(session)
            if opening is None:
                continue
            centre = round(float(opening))
            found = 0
            for strike in range(centre - STRIKE_SPAN, centre + STRIKE_SPAN + 1):
                for letter, option_type in (("C", OptionType.CALL), ("P", OptionType.PUT)):
                    symbol = occ_padded(session, letter, strike)
                    bars = await minute_bars(f"{symbol}@OPRA", session, session)
                    if bars.is_empty():
                        continue
                    specs.append(ContractSpec(
                        underlying_symbol=UNDERLYING[0], expiration_date=session,
                        option_type=option_type, strike=float(strike), mic="OPRA",
                        # Listed and expiring on the same session: that equality is the definition
                        # of the product, not an accident of this script.
                        listed_date=session, multiplier=100.0, tick_size=0.01,
                        exercise_style=ExerciseStyle.AMERICAN,
                        settlement_type=SettlementType.PHYSICAL,
                        premium_style=PremiumStyle.UPFRONT))
                    quoted.append((specs[-1], bars))
                    found += 1
            print(f"{str(session):<12}{float(opening):>9.2f}{found:>11}")
        if not specs:
            raise SystemExit("No option bars anywhere in the window.")
        print(f"\nchain: {len(specs)} contracts across {len(sessions)} sessions")

        await register_contracts(specs, asset_service=asset_service, underlying=underlying)
        sid_by_symbol = {}
        for session in sessions:
            for listing in await asset_service.get_exchange_option_contracts(
                    underlying_symbol=UNDERLYING[0], expiration_date=session,
                    mic=underlying.mic):
                sid_by_symbol[listing.symbol] = listing.sid
        print(f"registered: {len(sid_by_symbol)} listings")

        on_grid = set(grid.to_list())

        def on_the_clock(frame: pl.DataFrame, sid: int) -> pl.DataFrame:
            """Minute bars onto the clock's own instants. These arrive UTC-aware, so converting
            the zone is a relabelling rather than the day-shifting trap a *daily* bar falls into
            -- a daily bar is stamped at midnight UTC, and converting that lands it on the
            previous evening."""
            if "date" in frame.columns and frame["date"].dtype.time_zone != zone:
                frame = frame.with_columns(pl.col("date").dt.convert_time_zone(zone))
            return (frame.filter(pl.col("date").is_in(list(on_grid)))
                    .with_columns(pl.lit(sid).cast(pl.Int64).alias("sid")))

        parts = [on_the_clock(spot_bars, underlying.sid)]
        for spec, bars in quoted:
            sid = sid_by_symbol.get(spec.listing_symbol)
            if sid is not None:
                parts.append(on_the_clock(bars, sid).with_columns(
                    pl.lit(spec.listing_symbol).alias("symbol"), pl.lit("OPRA").alias("mic")))

        data = pl.concat(parts, how="diagonal").sort(["sid", "date"]).select(
            ["date", "sid", "symbol", "mic", "open", "high", "low", "close", "price", "volume"])
        spans = data.with_row_index().group_by("sid", maintain_order=True).agg(
            [pl.col("index").first().alias("first"), pl.col("index").last().alias("last")])
        bundle = DataBundle(
            name="opra-0dte", version="1", start_date=data["date"].min(),
            end_date=data["date"].max(), trading_calendar=calendar, frequency=MINUTE,
            original_frequency=MINUTE, data_type=DataType.MARKET_DATA,
            timestamp=data["date"].max(), data=data,
            sid_indexes={row["sid"]: (row["first"], row["last"] + 1)
                         for row in spans.iter_rows(named=True)},
            asset_service=asset_service)
        print(f"bundle: {data.height:,} rows across {len(spans)} instruments\n")

        from ziplime.core.run_simulation import run_simulation

        async def simulate(strategy_file: str):
            return await run_simulation(
            start_date=datetime.datetime.combine(sessions[0], datetime.time.min,
                                                 tzinfo=calendar.tz),
            end_date=datetime.datetime.combine(sessions[-1], datetime.time.max,
                                               tzinfo=calendar.tz),
            trading_calendar=CALENDAR, emission_rate=MINUTE, total_cash=CASH,
            market_data_source=bundle, custom_data_sources=[],
            algorithm_file=str(Path(__file__).parent / "strategies" / strategy_file),
            stop_on_error=True, asset_service=asset_service, benchmark_asset_symbol=None,
            benchmark_returns=None, equity_commission=NoCommission(),
            equity_slippage=NoSlippage(), option_commission=NoCommission(),
            option_slippage=NoSlippage(), max_leverage=10.0, print_algo=False,
            # Next-bar execution: a decision taken on a minute's close fills on the following
            # minute, which is the honest reading of an intraday signal.
            same_bar_execution=False, price_used_in_order_execution="close")

        result = await simulate("o03_vectorised_0dte_straddle.py")
        if COMPARE_ENGINES:
            await _compare(simulate, result)

        perf = result.perf
        print(f"=== 0DTE STRADDLE ON REAL DATA, {sessions[0]} .. {sessions[-1]} ===")
        print(f"{'session':<12}{'trades':>7}{'P&L':>12}{'return':>10}")
        print("-" * 41)
        previous = CASH
        for stamp, row in perf.iterrows():
            value = float(row["portfolio_value"])
            trades = len(row["transactions"])
            print(f"{str(stamp.date()):<12}{trades:>7}{value - previous:>12,.2f}"
                  f"{value / previous - 1.0:>9.3%}")
            previous = value
        total = sum(len(row) for row in perf["transactions"])
        print("-" * 41)
        print(f"  sessions      {len(perf)}")
        print(f"  transactions  {total}  ({total // 4} straddles)")
        print(f"  return        {float(perf['algorithm_period_return'].iloc[-1]):+.4%}")
        print(f"  ending value  {float(perf['portfolio_value'].iloc[-1]):,.2f}")
        print(f"  errors        {len(result.errors or [])}")
        print("\nNineteen sessions is nineteen. Read the table, not the total.")
    finally:
        await asset_service._asset_repository.engine.dispose()
        temp.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
