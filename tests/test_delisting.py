"""What a backtest does when the prices run out.

Every failure pinned here was quiet, and every one of them moved the answer in the flattering
direction.

A run whose data stopped before its window ended did not stop with it. Each read past the last bar
raised, and those raises landed in the session-end branch *before* the performance row for the
session was recorded -- so no row was recorded, for that session or for any after it. The result
was a full-looking table over a shorter window: a Sharpe labelled 2021-2026 and computed over
2021-2024, with nothing in the output to say so.

One name stopping was worse, because nothing raised at all. Reference data does not record an
equity's delisting -- the listing keeps its row and an end date decades away -- so ``auto_close``
never fired, the position stayed on the book, and the spot-value lookup went on returning the last
bar the name ever printed, at full weight in exposure, leverage and returns for the rest of the
run.

And the branches that handle "this position has no price" crashed while composing their own
message about it, which is what :class:`NoPriceForAnOpenPositionTests` is for.

The third failure in this family -- a calendar that believes in days the exchange did not open --
lives with the connector that supplies that market's calendar.
"""
import datetime
import shutil
import tempfile
import unittest
from pathlib import Path

import polars as pl

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.constants.data_type import DataType
from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.utils.calendar_utils import get_calendar

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures"

CALENDAR = "XNYS"
#: The window every end-to-end case here runs, and the session the doomed name stops on.
START = datetime.date(2024, 1, 2)
END = datetime.date(2024, 6, 28)
CUTOFF = datetime.date(2024, 3, 28)
#: The first session after `CUTOFF`, when a name with no more bars should leave the book.
FIRST_DEAD_SESSION = datetime.date(2024, 4, 1)


def bars(asset, closes, price: float) -> list[dict]:
    return [{"date": close, "sid": asset.sid, "symbol": asset.symbol, "mic": asset.mic,
             "open": price, "high": price, "low": price, "close": price, "price": price,
             "volume": 1_000_000.0}
            for close in closes]


def make_bundle(rows: list[dict], calendar) -> DataBundle:
    """A bundle that declares the window its bars actually span.

    Which is how one is assembled from a price frame -- ``start_date`` and ``end_date`` come from
    the data, not from the window a caller later runs over. It matters here: a bundle that claims
    to reach the end of the run is never asked for anything past its own edge, and the failure
    these tests are about only happens when it is.
    """
    data = pl.DataFrame(rows).sort(["sid", "date"])
    spans = data.with_row_index().group_by("sid", maintain_order=True).agg(
        [pl.col("index").first().alias("first"), pl.col("index").last().alias("last")])
    return DataBundle(
        name="delisting-tests", version="1",
        start_date=data["date"].min(), end_date=data["date"].max(),
        trading_calendar=calendar, frequency=datetime.timedelta(days=1),
        original_frequency=datetime.timedelta(days=1), data_type=DataType.MARKET_DATA,
        timestamp=data["date"].max(), data=data,
        sid_indexes={r["sid"]: (r["first"], r["last"] + 1) for r in spans.iter_rows(named=True)})


class DelistingEndToEndTests(unittest.IsolatedAsyncioTestCase):
    """A universe of two equities, one of which stops trading a quarter of the way in."""

    async def asyncSetUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-delisting-")
        db_path = Path(self._temp.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db_path)
        self.asset_service = get_asset_service(db_path=str(db_path))
        self.alive = await self._listing("JNJ")
        self.doomed = await self._listing("KO")

        self.calendar = get_calendar(CALENDAR)
        self.start = datetime.datetime.combine(START, datetime.time.min,
                                               tzinfo=self.calendar.tz)
        self.end = datetime.datetime.combine(END, datetime.time.max, tzinfo=self.calendar.tz)
        sessions = self.calendar.sessions_in_range(START, END)
        self.closes = list(
            self.calendar.schedule.loc[sessions, "close"].dt.tz_convert(self.calendar.tz))
        self.truncated = [c for c in self.closes if c.date() <= CUTOFF]

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    async def _listing(self, symbol: str):
        listing = await self.asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=symbol, mic="XNYS"), asset_type=AssetType.EQUITY)
        self.assertIsNotNone(listing, symbol)
        return listing

    async def run_simulation(self, bundle: DataBundle, algorithm: str):
        return await run_simulation(
            start_date=self.start, end_date=self.end, trading_calendar=CALENDAR,
            emission_rate=datetime.timedelta(days=1), total_cash=100_000.0,
            market_data_source=bundle, custom_data_sources=[],
            algorithm_file=str(FIXTURES / algorithm),
            stop_on_error=False, asset_service=self.asset_service,
            benchmark_asset_symbol=None, benchmark_returns=None,
            equity_commission=NoCommission(), max_leverage=1.0, same_bar_execution=True,
            price_used_in_order_execution="close", print_algo=False)

    def bundle_with(self, doomed_closes) -> DataBundle:
        rows = bars(self.alive, self.closes, 100.0) + bars(self.doomed, doomed_closes, 50.0)
        return make_bundle(rows, self.calendar)

    async def test_one_name_stopping_does_not_shorten_the_record(self):
        """The universe loses a name; the backtest keeps its window, and nothing raises.

        The other names still price the tail of the window here, so this is the guard rather than
        the reproduction -- `test_a_run_whose_data_stops_short_still_records_its_whole_window` is
        the case where nothing does.
        """
        result = await self.run_simulation(self.bundle_with(self.truncated), "buy_two_and_hold.py")

        self.assertEqual(len(result.perf), len(self.closes))
        self.assertEqual(result.perf.index[-1].date(), END)
        self.assertEqual([error for error in result.errors if error.trace], [],
                         "no bar should have raised")

    async def test_a_name_with_no_more_bars_leaves_the_book(self):
        """Held to the end, it would be marked at a price months stale at full weight."""
        result = await self.run_simulation(self.bundle_with(self.truncated), "buy_two_and_hold.py")

        held = {position.asset.symbol
                for position in result.perf["positions"].iloc[-1]}
        self.assertEqual(held, {"JNJ"})

    async def test_it_is_closed_on_the_first_session_it_does_not_trade(self):
        """Not on its last trading session -- that bar is a real price -- and not later."""
        result = await self.run_simulation(self.bundle_with(self.truncated), "buy_two_and_hold.py")

        by_session = {index.date(): {p.asset.symbol for p in positions}
                      for index, positions in result.perf["positions"].items()}
        self.assertIn("KO", by_session[CUTOFF])
        self.assertNotIn("KO", by_session[FIRST_DEAD_SESSION])

    async def test_nothing_changes_while_both_names_still_trade(self):
        """The control: the same run with complete data keeps both positions."""
        result = await self.run_simulation(self.bundle_with(self.closes), "buy_two_and_hold.py")

        held = {position.asset.symbol for position in result.perf["positions"].iloc[-1]}
        self.assertEqual(held, {"JNJ", "KO"})
        self.assertEqual(len(result.perf), len(self.closes))

    async def test_a_dead_name_cannot_be_bought(self):
        """A strategy that keeps ordering it every session must not keep getting filled.

        The one fill allowed after the last bar is the engine's own liquidation, on the first
        session with no price -- that is the position leaving the book, not a new trade.
        """
        result = await self.run_simulation(self.bundle_with(self.truncated), "buy_two_daily.py")

        traded_after = [(index.date(), transaction.amount)
                        for index, transactions in result.perf["transactions"].items()
                        if index.date() > CUTOFF
                        for transaction in transactions
                        if transaction.asset.symbol == "KO"]
        self.assertEqual([session for session, _ in traded_after], [FIRST_DEAD_SESSION])
        self.assertLess(traded_after[0][1], 0, "the only fill left must be the liquidation")

    async def test_a_run_whose_data_stops_short_still_records_its_whole_window(self):
        """Nothing at all has a price for the last quarter. That used to end the record on the
        last bar and report the metrics of a quarter-shorter backtest under the full window's
        name; now the run keeps its window and says what happened to the data."""
        rows = bars(self.alive, self.truncated, 100.0) + bars(self.doomed, self.truncated, 50.0)
        bundle = make_bundle(rows, self.calendar)

        result = await self.run_simulation(bundle, "buy_two_and_hold.py")

        self.assertEqual(len(result.perf), len(self.closes))
        self.assertEqual(result.perf.index[-1].date(), END)

    async def test_it_says_which_listings_ran_out_of_prices(self):
        """`errors` is what a caller checks; a warning in a log of thousands is not."""
        result = await self.run_simulation(self.bundle_with(self.truncated), "buy_two_and_hold.py")

        stopped = result.trading_algorithm.data_delistings()
        self.assertEqual({asset.symbol: session for asset, session in stopped.items()},
                         {"KO": CUTOFF})
        reported = [error.message for error in result.errors if "KO@XNYS" in error.message]
        self.assertEqual(len(reported), 1, result.errors)
        self.assertIn(CUTOFF.isoformat(), reported[0])

    async def test_a_run_with_complete_data_reports_nothing(self):
        """The control for the report: no listing ran out, so there is nothing to say."""
        result = await self.run_simulation(self.bundle_with(self.closes), "buy_two_and_hold.py")

        self.assertEqual(result.trading_algorithm.data_delistings(), {})
        self.assertEqual(result.errors, [])


class DataCoverageTests(unittest.IsolatedAsyncioTestCase):
    """`last_available_bar` is where a delisting enters the engine, so it answers precisely."""

    def setUp(self):
        calendar = get_calendar(CALENDAR)
        self.calendar = calendar
        days = [datetime.datetime.combine(d, datetime.time(16), tzinfo=calendar.tz)
                for d in (datetime.date(2024, 1, 2), datetime.date(2024, 1, 3),
                          datetime.date(2024, 1, 4))]
        rows = []
        for sid, series in ((1, days), (2, days[:1])):
            rows += [{"date": d, "sid": sid, "symbol": f"S{sid}", "mic": "XNYS", "close": 1.0,
                      "price": 1.0, "open": 1.0, "high": 1.0, "low": 1.0, "volume": 1.0}
                     for d in series]
        self.bundle = make_bundle(rows, calendar)

    def test_it_answers_per_instrument(self):
        self.assertEqual(self.bundle.last_available_bar(1).date(), datetime.date(2024, 1, 4))
        self.assertEqual(self.bundle.last_available_bar(2).date(), datetime.date(2024, 1, 2))

    def test_without_a_sid_it_answers_for_the_bundle(self):
        self.assertEqual(self.bundle.last_available_bar().date(), datetime.date(2024, 1, 4))

    def test_an_instrument_the_bundle_does_not_carry_gets_no_opinion(self):
        """`None` is "cannot say", never "delisted" -- reading it the other way would stop a
        strategy trading anything priced from a source outside this bundle."""
        self.assertIsNone(self.bundle.last_available_bar(99))

    def test_a_source_that_is_not_bar_shaped_has_no_opinion(self):
        from ziplime.data.services.data_source import DataSource
        source = DataSource(name="events", start_date=datetime.date(2024, 1, 1),
                            end_date=datetime.date(2024, 1, 4),
                            frequency=datetime.timedelta(days=1),
                            original_frequency=datetime.timedelta(days=1),
                            data_type=DataType.CUSTOM)
        self.assertIsNone(source.last_available_bar())


class NoPriceForAnOpenPositionTests(unittest.TestCase):
    """The two places that handle "this position has no price", which used to raise there.

    Both read ``asset.asset_name``. ``ExchangeAsset`` has no such field -- ``asset_name`` belongs
    to the instrument it wraps, and a listing is named by ``symbol`` -- so each of these crashed
    while composing its own message about the missing price, turning a handled condition into an
    ``AttributeError`` from inside the error handler.

    Neither branch runs on ordinary daily bars, which is why they went unnoticed: reaching them
    needs a bar in which an instrument on the book did not trade. A halted market is one, an
    intraday rate is another, and a delisting is the third.
    """

    def setUp(self):
        from ziplime.assets.entities.currency import Currency
        from ziplime.assets.entities.equity import Equity
        from ziplime.assets.entities.exchange_asset import ExchangeAsset
        from ziplime.assets.entities.exchange_info import ExchangeInfo
        from ziplime.finance.domain.position_tracker import PositionTracker

        far_past, far_future = datetime.date(1900, 1, 1), datetime.date(2099, 1, 1)
        exchange = ExchangeInfo(mic="MISX", name="MOEX", canonical_name="MOEX", country_code="RU")
        self.asset = ExchangeAsset(
            sid=1, symbol="DSKY", start_date=far_past, end_date=far_future,
            first_traded=far_past, auto_close_date=far_future, external_id="", exchange=exchange,
            asset=Equity(id=11, isin=None, asset_name="DSKY", start_date=far_past,
                         end_date=far_future, first_traded=far_past, auto_close_date=far_future),
            quote=Currency(id=2, isin=None, asset_name="RUB", start_date=far_past,
                           end_date=far_future, first_traded=far_past,
                           auto_close_date=far_future))
        self.dt = datetime.datetime(2022, 2, 28, tzinfo=datetime.timezone.utc)
        self.tracker = PositionTracker(data_frequency=datetime.timedelta(days=1))
        self.tracker.update_position(asset=self.asset, exchange_name="MISX",
                                     trading_account_id="account-1", amount=100,
                                     last_sale_price=90.0, last_sale_date=self.dt,
                                     cost_basis=90.0)

    def test_a_bar_with_no_trade_leaves_the_previous_mark_alone(self):
        """No trade is not a price of zero, and it is not a crash either."""
        exchange = type("E", (), {"name": "MISX"})()

        self.tracker.sync_last_sale_prices(
            dt=self.dt + datetime.timedelta(days=1),
            prices={(self.asset.sid, exchange): None})

        self.assertAlmostEqual(
            self.tracker.get_position(self.asset, "MISX", "account-1").last_sale_price, 90.0)

    def test_closing_a_position_that_was_never_marked_declines_instead_of_raising(self):
        tracker = self.tracker
        tracker.get_position(self.asset, "MISX", "account-1").last_sale_price = float("nan")

        self.assertEqual(tracker.close_positions(asset=self.asset, dt=self.dt), [])

    def test_closing_a_marked_position_produces_the_liquidating_trade(self):
        """The control: with a mark, the same call returns the trade that flattens the book."""
        transactions = self.tracker.close_positions(asset=self.asset, dt=self.dt)

        self.assertEqual(len(transactions), 1)
        transaction = transactions[0]
        self.assertEqual(transaction.amount, -100)
        self.assertAlmostEqual(transaction.price, 90.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
