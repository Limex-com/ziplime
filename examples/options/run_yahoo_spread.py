"""Backtest an option structure on free Yahoo Finance data.

    python examples/options/run_yahoo_spread.py

No credentials and no paid feed: `yfinance` is already a dependency, and everything below comes
from the same chain a browser shows at finance.yahoo.com/quote/AAPL/options.

**What that buys, and what it costs.** Yahoo carries the live chain in full -- strike, bid, ask,
volume, open interest, implied volatility -- and about a month of daily history behind each
contract that has been listed that long. It does not carry a chain as it stood in the past: a
contract that has expired is simply gone. So this runs over a live expiry and a short window, and
that is the honest shape of the source rather than a shortcut.

The structure is a bull call spread, which is the smallest thing that is genuinely an option
*structure*: two legs, opposite signs, one expiry, marked and closed together.
"""
import asyncio
import datetime
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
CALENDAR = "XNYS"
UNDERLYING = ("AAPL", "XNGS")
DAY = datetime.timedelta(days=1)

#: The window. Daily bars, because that is what Yahoo keeps for more than a session.
FIRST_SESSION = datetime.date(2026, 8, 20)
LAST_SESSION = datetime.date(2026, 9, 11)
#: The expiry to trade. Must still be listed -- see the module docstring.
EXPIRY = datetime.date(2026, 10, 16)
#: Strikes within this fraction of the underlying, and only those anyone holds. A full chain is
#: several hundred contracts, and a strike with no open interest prices off one stale trade.
STRIKE_WINDOW = 0.05
MINIMUM_OPEN_INTEREST = 50
CASH = 100_000.0


async def underlying_bars(symbol: str, first: datetime.date, last: datetime.date, zone: str):
    """The underlying's daily bars, from the same place the options come from."""
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
        "date": list(local.index),
        "symbol": [symbol] * len(local),
        "mic": [UNDERLYING[1]] * len(local),
        "open": local["Open"].astype(float).to_list(),
        "high": local["High"].astype(float).to_list(),
        "low": local["Low"].astype(float).to_list(),
        "close": closes.to_list(),
        "price": closes.to_list(),
        "volume": local["Volume"].astype(float).to_list(),
    })


async def main():
    temp = tempfile.TemporaryDirectory(prefix="ziplime-yahoo-options-")
    database = Path(temp.name) / "assets.sqlite"
    shutil.copy2(REPO / "data" / "assets.sqlite", database)
    asset_service = get_asset_service(db_path=str(database))
    calendar = get_calendar(CALENDAR)
    in_range = calendar.sessions_in_range(FIRST_SESSION, LAST_SESSION)
    sessions = [session.date() for session in in_range]
    stamps = pl.Series(list(calendar.schedule.loc[in_range, "close"].dt.tz_convert(calendar.tz)))
    zone = str(calendar.tz)
    print(f"{len(sessions)} sessions, {sessions[0]} .. {sessions[-1]}, expiry {EXPIRY}")

    try:
        underlying = await asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=UNDERLYING[0], mic=UNDERLYING[1]),
            asset_type=AssetType.EQUITY)
        if underlying is None:
            raise SystemExit(f"{UNDERLYING[0]}@{UNDERLYING[1]} is not in the asset database -- "
                             f"run `ziplime ingest-assets` first.")

        spot = await underlying_bars(UNDERLYING[0], sessions[0], sessions[-1], zone)
        spot = spot.with_columns(pl.lit(underlying.sid).cast(pl.Int64).alias("sid"))
        print(f"{UNDERLYING[0]}: {spot.height} daily bars, "
              f"{float(spot['close'][0]):.2f} -> {float(spot['close'][-1]):.2f}")

        source = YahooOptionChainSource(
            venue="OPRA", strike_window=STRIKE_WINDOW, expiries=[EXPIRY],
            minimum_open_interest=MINIMUM_OPEN_INTEREST)
        bundle, listings = await build_option_bundle(
            source=source, asset_service=asset_service, underlying=underlying,
            underlying_bars=spot, sessions=sessions, timestamps=stamps,
            trading_calendar=calendar, emission_rate=DAY)
        strikes = sorted({listing.asset.strike for listing in listings})
        print(f"chain: {len(listings)} contracts, strikes {strikes[0]:g}..{strikes[-1]:g}")
        print(f"bundle: {bundle.data.height:,} rows across "
              f"{bundle.data['sid'].n_unique()} instruments")

        from ziplime.core.run_simulation import run_simulation

        result = await run_simulation(
            start_date=datetime.datetime.combine(sessions[0], datetime.time.min,
                                                 tzinfo=calendar.tz),
            end_date=datetime.datetime.combine(sessions[-1], datetime.time.max,
                                               tzinfo=calendar.tz),
            trading_calendar=CALENDAR, emission_rate=DAY, total_cash=CASH,
            market_data_source=bundle, custom_data_sources=[],
            algorithm_file=str(Path(__file__).parent / "strategies"
                               / "o05_yahoo_call_spread.py"),
            stop_on_error=True, asset_service=asset_service, benchmark_asset_symbol=None,
            benchmark_returns=None, equity_commission=NoCommission(),
            equity_slippage=NoSlippage(), option_commission=NoCommission(),
            option_slippage=NoSlippage(), max_leverage=10.0, print_algo=False,
            same_bar_execution=False, price_used_in_order_execution="close")

        perf = result.perf
        transactions = [t for row in perf["transactions"] for t in row]
        print(f"\n=== BULL CALL SPREAD ON YAHOO DATA ===")
        print(f"  transactions  {len(transactions)}")
        for transaction in transactions:
            side = "buy " if transaction.amount > 0 else "sell"
            print(f"    {transaction.dt.date()}  {side} {transaction.asset.symbol:<22} "
                  f"x{transaction.amount:<3} @ {transaction.price}")
        if transactions:
            net = sum(t.amount * t.price * 100 for t in transactions)
            print(f"  net on the structure  {-net:,.2f}")
        print(f"  sessions      {len(perf)}")
        print(f"  return        {float(perf['algorithm_period_return'].iloc[-1]):+.4%}")
        print(f"  ending value  {float(perf['portfolio_value'].iloc[-1]):,.2f}")
        print(f"  errors        {len(result.errors or [])}")
        for error in (result.errors or [])[:3]:
            print(f"    {error.message[:200]}")
    finally:
        await asset_service._asset_repository.engine.dispose()
        temp.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
