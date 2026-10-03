"""Every option structure, over one chain, side by side.

    python examples/options/run_structures.py
    python examples/options/run_structures.py --only o06

Real AAPL option prices from Yahoo Finance -- no credentials, no paid feed. Each strategy is run
over the *same* window and the *same* chain, so the table at the end compares structures rather
than comparing data.

**The comparison is the point, not any one number.** Nineteen sessions of one underlying tells you
nothing about which structure is better, and the table is not offered as evidence of that. What it
does show, and what a single run cannot, is how differently these things behave from the same
view: what each costs to open, what each can make, and what each can lose. The last column is the
one worth reading -- a risk reversal and a ratio spread look almost free at entry and are the two
that can hurt.
"""
import argparse
import asyncio
import datetime
import importlib.util
import shutil
import sys
import tempfile
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ziplime.assets.domain.asset_type import AssetType  # noqa: E402
from ziplime.assets.entities.asset_symbol import AssetSymbol  # noqa: E402
from ziplime.core.ingest_data import get_asset_service  # noqa: E402
from ziplime.data.data_sources.options.ingest import build_option_bundle  # noqa: E402
from ziplime.data.data_sources.options.yahoo_chain import YahooOptionChainSource  # noqa: E402
from ziplime.finance.commission.no_commission import NoCommission  # noqa: E402
from ziplime.finance.slippage.no_slippage import NoSlippage  # noqa: E402
from ziplime.utils.calendar_utils import get_calendar  # noqa: E402

REPO = Path(__file__).parent.parent.parent
STRATEGY_DIR = Path(__file__).parent / "strategies"
CALENDAR = "XNYS"
UNDERLYING = ("AAPL", "XNGS")
DAY = datetime.timedelta(days=1)

FIRST_SESSION = datetime.date(2026, 8, 20)
LAST_SESSION = datetime.date(2026, 9, 11)
#: The expiry every strategy here trades. It has to be one Yahoo still lists, and it has to match
#: the `EXPIRY` in the strategy files -- they pick their own legs out of this chain.
EXPIRY = datetime.date(2026, 10, 16)
#: Wide enough for a butterfly's wings and a risk reversal's offsets to find strikes.
STRIKE_WINDOW = 0.10
MINIMUM_OPEN_INTEREST = 50
CASH = 100_000.0

#: The structures that trade a live expiry out of a Yahoo chain. The 0DTE pair (o03, o04) needs
#: the gRPC feed and its own runner; o01 and o02 run on the synthetic chain through `run_all.py`.
STRATEGIES = [
    "o05_yahoo_call_spread.py",
    "o06_long_butterfly.py",
    "o07_short_iron_butterfly.py",
    "o08_risk_reversal.py",
    "o09_call_ratio_spread.py",
]


def headline(path: Path) -> str:
    """The strategy's own first line, which is the description it wrote for itself."""
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return (module.__doc__ or "").strip().splitlines()[0]


async def underlying_bars(symbol: str, first: datetime.date, last: datetime.date, zone: str):
    import yfinance

    frame = await asyncio.to_thread(
        lambda: yfinance.Ticker(symbol).history(
            start=first.isoformat(), end=(last + DAY).isoformat(), interval="1d",
            auto_adjust=False))
    if frame is None or frame.empty:
        raise SystemExit(f"Yahoo returned no {symbol} bars for {first}..{last}.")
    local = frame.tz_convert(zone) if frame.index.tz is not None else frame.tz_localize(zone)
    closes = local["Close"].astype(float)
    return pl.DataFrame({
        "date": list(local.index), "symbol": [symbol] * len(local),
        "mic": [UNDERLYING[1]] * len(local),
        "open": local["Open"].astype(float).to_list(),
        "high": local["High"].astype(float).to_list(),
        "low": local["Low"].astype(float).to_list(),
        "close": closes.to_list(), "price": closes.to_list(),
        "volume": local["Volume"].astype(float).to_list(),
    })


def money(value) -> str:
    if value is None:
        return "-"
    if value == float("inf"):
        return "unbounded"
    if value == float("-inf"):
        return "UNBOUNDED"
    return f"{value:,.0f}"


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", help="run one strategy, by its o0N prefix")
    arguments = parser.parse_args()

    chosen = [name for name in STRATEGIES
              if arguments.only is None or name.startswith(arguments.only)]
    if not chosen:
        raise SystemExit(f"No strategy matches {arguments.only!r}. "
                         f"Known: {', '.join(name[:3] for name in STRATEGIES)}")

    temp = tempfile.TemporaryDirectory(prefix="ziplime-structures-")
    database = Path(temp.name) / "assets.sqlite"
    shutil.copy2(REPO / "data" / "assets.sqlite", database)
    asset_service = get_asset_service(db_path=str(database))
    calendar = get_calendar(CALENDAR)
    in_range = calendar.sessions_in_range(FIRST_SESSION, LAST_SESSION)
    sessions = [session.date() for session in in_range]
    stamps = pl.Series(list(calendar.schedule.loc[in_range, "close"].dt.tz_convert(calendar.tz)))
    zone = str(calendar.tz)

    try:
        underlying = await asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=UNDERLYING[0], mic=UNDERLYING[1]),
            asset_type=AssetType.EQUITY)
        if underlying is None:
            raise SystemExit(f"{UNDERLYING[0]}@{UNDERLYING[1]} is not in the asset database -- "
                             f"run `ziplime ingest-assets` first.")

        spot = await underlying_bars(UNDERLYING[0], sessions[0], sessions[-1], zone)
        spot = spot.with_columns(pl.lit(underlying.sid).cast(pl.Int64).alias("sid"))
        print(f"{UNDERLYING[0]}  {sessions[0]} .. {sessions[-1]}  "
              f"({len(sessions)} sessions)  "
              f"{float(spot['close'][0]):.2f} -> {float(spot['close'][-1]):.2f}")

        # One chain, built once, shared by every strategy. Rebuilding it per strategy would make
        # the table compare chains as much as structures.
        source = YahooOptionChainSource(
            venue="OPRA", strike_window=STRIKE_WINDOW, expiries=[EXPIRY],
            minimum_open_interest=MINIMUM_OPEN_INTEREST)
        bundle, listings = await build_option_bundle(
            source=source, asset_service=asset_service, underlying=underlying,
            underlying_bars=spot, sessions=sessions, timestamps=stamps,
            trading_calendar=calendar, emission_rate=DAY)
        strikes = sorted({listing.asset.strike for listing in listings})
        print(f"chain     expiry {EXPIRY}, {len(listings)} contracts, "
              f"strikes {strikes[0]:g}..{strikes[-1]:g}\n")

        from ziplime.core.run_simulation import run_simulation

        rows = []
        for name in chosen:
            path = STRATEGY_DIR / name
            print(f"--- {name[:3]}  {headline(path)}")
            result = await run_simulation(
                start_date=datetime.datetime.combine(sessions[0], datetime.time.min,
                                                     tzinfo=calendar.tz),
                end_date=datetime.datetime.combine(sessions[-1], datetime.time.max,
                                                   tzinfo=calendar.tz),
                trading_calendar=CALENDAR, emission_rate=DAY, total_cash=CASH,
                market_data_source=bundle, custom_data_sources=[],
                algorithm_file=str(path), stop_on_error=True, asset_service=asset_service,
                benchmark_asset_symbol=None, benchmark_returns=None,
                equity_commission=NoCommission(), equity_slippage=NoSlippage(),
                option_commission=NoCommission(), option_slippage=NoSlippage(),
                max_leverage=10.0, print_algo=False, same_bar_execution=False,
                price_used_in_order_execution="close")

            algorithm = result.trading_algorithm
            report = getattr(algorithm, "entry_report", None)
            refused = getattr(algorithm, "refused", None)
            transactions = [t for row in result.perf["transactions"] for t in row]
            for transaction in transactions:
                side = "buy " if transaction.amount > 0 else "sell"
                print(f"      {transaction.dt.date()}  {side} "
                      f"{transaction.asset.symbol:<22} x{transaction.amount:<3} "
                      f"@ {transaction.price}")
            if refused:
                print(f"      refused: {refused}")
            if not transactions and not refused:
                print("      no entry: the conditions in the file were never met")

            rows.append({
                "name": name[:3],
                "structure": (report or {}).get("name", "-"),
                "legs": len(transactions),
                "net": (report or {}).get("net"),
                "max_profit": (report or {}).get("max_profit"),
                "max_loss": (report or {}).get("max_loss"),
                "pnl": float(result.perf["portfolio_value"].iloc[-1]) - CASH,
                "errors": len(result.errors or []),
            })
            print()

        print(f"{'':<5}{'structure':<34}{'legs':>5}{'net':>9}{'max gain':>11}"
              f"{'max loss':>12}{'P&L':>10}")
        print("-" * 86)
        for row in rows:
            print(f"{row['name']:<5}{row['structure'][:33]:<34}{row['legs']:>5}"
                  f"{money(row['net']):>9}{money(row['max_profit']):>11}"
                  f"{money(row['max_loss']):>12}{row['pnl']:>10,.0f}")
        print("-" * 86)
        print("net is what the structure cost to open, in money: positive a debit, negative a "
              "credit.\nmax gain and max loss are at expiry, for the size actually traded -- and "
              "UNBOUNDED means it.")
        total_errors = sum(row["errors"] for row in rows)
        print(f"\nerrors: {total_errors}")
    finally:
        await asset_service._asset_repository.engine.dispose()
        temp.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
