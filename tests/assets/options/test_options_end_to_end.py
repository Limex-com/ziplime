"""A 0DTE contract through a whole simulation: listed, bought, expired, settled.

These run the real engine over a two-session bundle rather than testing the pieces. What they pin
is the accounting, which for options is where a backtest is most easily and least visibly wrong:

* **The premium goes through the multiplier.** One contract at 1.20 costs 120, not 1.20. Getting
  this wrong is a hundredfold error in cash, and because the cost basis is set from it, every P&L
  measured afterwards is wrong by the same factor.
* **An option has value; a future does not.** A future is worth the variation margin since the last
  mark and the ledger settles it daily. An option was bought and paid for, and the position is an
  asset worth ``amount x price x multiplier`` -- or a liability, when short.
* **Expiry settles at intrinsic value, not at the last quote.** This is the one that only 0DTE
  makes obvious, because it happens every session. The two numbers part company in both directions
  on the same day: a wing that stops being quoted at lunchtime carries a mark of a few cents into a
  settlement of zero, and a strike that finishes ten cents in the money carries a mark of nothing
  into ten dollars a contract.
* **The contract then leaves the book.** A 0DTE position that survives its expiration session is
  not a position, it is a bug -- and one the delisting work in ``test_delisting.py`` would
  otherwise paper over by marking it at a price that never moves again.
"""
import datetime
import shutil
import tempfile
import unittest
from pathlib import Path

import polars as pl

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.domain.option_type import OptionType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.constants.data_type import DataType
from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.data.data_sources.options.ingest import register_contracts
from ziplime.data.data_sources.options.source import ContractSpec
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.utils.calendar_utils import get_calendar

PROJECT_ROOT = Path(__file__).resolve().parents[3]
FIXTURES = PROJECT_ROOT / "tests" / "fixtures"

CALENDAR = "XNYS"
#: Two sessions, so a contract expiring on the first is definitively gone on the second.
SESSIONS = [datetime.date(2024, 6, 13), datetime.date(2024, 6, 14)]
UNDERLYING_SYMBOL, UNDERLYING_MIC = "SPY", "ARCX"
MULTIPLIER = 100.0
STARTING_CASH = 100_000.0

#: Where the underlying finishes each session. The first is what the first session's contracts
#: settle against, and it is chosen so one strike finishes in the money and one does not.
FINAL_PRICE = {SESSIONS[0]: 523.41, SESSIONS[1]: 519.00}
#: Strikes listed on both sessions. 520 finishes 3.41 in the money as a call on session one.
STRIKES = [520.0, 525.0]


class OptionEndToEndTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-options-")
        db_path = Path(self._temp.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db_path)
        self.asset_service = get_asset_service(db_path=str(db_path))
        self.calendar = get_calendar(CALENDAR)
        self.underlying = await self.asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=UNDERLYING_SYMBOL, mic=UNDERLYING_MIC),
            asset_type=AssetType.EQUITY)
        self.assertIsNotNone(self.underlying, "SPY must be in the asset database")

        self.closes = {
            session: close.to_pydatetime()
            for session, close in self.calendar.schedule.loc[
                self.calendar.sessions_in_range(SESSIONS[0], SESSIONS[-1]), "close"
            ].dt.tz_convert(self.calendar.tz).items()
        }
        self.closes = {stamp.date(): stamp for stamp in self.closes.values()}
        await self._register_chains()

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    async def _register_chains(self):
        specs = [
            ContractSpec(underlying_symbol=UNDERLYING_SYMBOL, expiration_date=session,
                         option_type=option_type, strike=strike, mic=UNDERLYING_MIC,
                         listed_date=session, multiplier=MULTIPLIER)
            for session in SESSIONS
            for strike in STRIKES
            for option_type in (OptionType.CALL, OptionType.PUT)
        ]
        await register_contracts(specs, asset_service=self.asset_service,
                                 underlying=self.underlying)
        self.listings = {}
        for session in SESSIONS:
            for listing in await self.asset_service.get_exchange_option_contracts(
                    underlying_symbol=UNDERLYING_SYMBOL, expiration_date=session,
                    mic=UNDERLYING_MIC):
                self.listings[listing.symbol] = listing

    def listing(self, session: datetime.date, strike: float,
                option_type: OptionType):
        from ziplime.assets.entities.option_contract import format_occ_symbol
        return self.listings[format_occ_symbol(UNDERLYING_SYMBOL, session, option_type, strike)]

    def build_bundle(self, option_price: float) -> DataBundle:
        """A bundle with one bar per session: the close.

        Every option is quoted at ``option_price`` on every bar including the last, so the mark and
        the settlement value are deliberately different numbers and the tests can tell which one
        the engine used.
        """
        rows = []
        for session in SESSIONS:
            stamp = self.closes[session]
            price = FINAL_PRICE[session]
            rows.append({"date": stamp, "sid": self.underlying.sid,
                         "symbol": self.underlying.symbol, "mic": self.underlying.mic,
                         "open": price, "high": price, "low": price, "close": price,
                         "price": price, "volume": 1_000_000.0})
            for strike in STRIKES:
                for option_type in (OptionType.CALL, OptionType.PUT):
                    listing = self.listing(session, strike, option_type)
                    rows.append({"date": stamp, "sid": listing.sid, "symbol": listing.symbol,
                                 "mic": listing.mic, "open": option_price, "high": option_price,
                                 "low": option_price, "close": option_price,
                                 "price": option_price, "volume": 5_000.0})
        data = pl.DataFrame(rows).sort(["sid", "date"])
        spans = data.with_row_index().group_by("sid", maintain_order=True).agg(
            [pl.col("index").first().alias("first"), pl.col("index").last().alias("last")])
        return DataBundle(
            name="options-e2e", version="1",
            start_date=data["date"].min(), end_date=data["date"].max(),
            trading_calendar=self.calendar, frequency=datetime.timedelta(days=1),
            original_frequency=datetime.timedelta(days=1), data_type=DataType.MARKET_DATA,
            timestamp=data["date"].max(), data=data,
            sid_indexes={r["sid"]: (r["first"], r["last"] + 1)
                         for r in spans.iter_rows(named=True)},
            asset_service=self.asset_service)

    async def run_case(self, algorithm: str, option_price: float = 1.00):
        start = datetime.datetime.combine(SESSIONS[0], datetime.time.min, tzinfo=self.calendar.tz)
        end = datetime.datetime.combine(SESSIONS[-1], datetime.time.max, tzinfo=self.calendar.tz)
        return await run_simulation(
            start_date=start, end_date=end, trading_calendar=CALENDAR,
            emission_rate=datetime.timedelta(days=1), total_cash=STARTING_CASH,
            market_data_source=self.build_bundle(option_price), custom_data_sources=[],
            algorithm_file=str(FIXTURES / algorithm), stop_on_error=True,
            asset_service=self.asset_service, benchmark_asset_symbol=None,
            benchmark_returns=None, equity_commission=NoCommission(),
            option_commission=NoCommission(),
            # No costs at all: these tests are about what the accounting does with a price, and a
            # five-basis-point fill would put every figure below five cents off its exact value.
            equity_slippage=NoSlippage(), option_slippage=NoSlippage(),
            max_leverage=10.0, same_bar_execution=True,
            price_used_in_order_execution="close", print_algo=False)

    # -- the cases -------------------------------------------------------------------------

    async def test_the_premium_is_charged_through_the_multiplier(self):
        """One contract at 1.00 costs 100. The hundredfold error this catches is invisible in
        every other number the run produces."""
        result = await self.run_case("option_buy_one_call.py", option_price=1.00)
        cash_after_buy = float(result.perf["ending_cash"].iloc[0])
        settlement = max(FINAL_PRICE[SESSIONS[0]] - 520.0, 0.0) * MULTIPLIER
        self.assertAlmostEqual(cash_after_buy, STARTING_CASH - 100.0 + settlement, places=2)

    async def test_an_expiring_option_settles_at_intrinsic_value_not_at_its_mark(self):
        """The mark says 1.00 all day; the contract is worth 3.41 against where SPY finished.

        Settling at the mark would pay 100 instead of 341 -- and on the wings, where a quote of a
        few cents survives into a settlement of zero, it pays out on contracts worth nothing.
        """
        result = await self.run_case("option_buy_one_call.py", option_price=1.00)
        transactions = [t for row in result.perf["transactions"] for t in row]
        settlement = [t for t in transactions if t.amount < 0]
        self.assertEqual(len(settlement), 1, "the position is closed exactly once")
        self.assertAlmostEqual(settlement[0].price, FINAL_PRICE[SESSIONS[0]] - 520.0, places=2)

    async def test_a_worthless_option_settles_at_zero_however_it_was_quoted(self):
        result = await self.run_case("option_buy_one_otm_call.py", option_price=1.00)
        transactions = [t for row in result.perf["transactions"] for t in row]
        settlement = [t for t in transactions if t.amount < 0]
        self.assertEqual(len(settlement), 1)
        self.assertAlmostEqual(settlement[0].price, 0.0, places=6)
        # Paid 100 for it, got nothing back.
        self.assertAlmostEqual(float(result.perf["ending_cash"].iloc[0]),
                               STARTING_CASH - 100.0, places=2)

    async def test_the_contract_is_gone_the_session_after_it_expires(self):
        """A 0DTE position that outlives its session is not a position."""
        result = await self.run_case("option_buy_one_call.py", option_price=1.00)
        held = [len(row) for row in result.perf["positions"]]
        self.assertEqual(held, [0, 0], "flat at the end of every session")

    async def test_a_short_option_is_a_liability(self):
        result = await self.run_case("option_sell_one_call.py", option_price=1.00)
        # Sold at 1.00 (+100), settled 3.41 in the money (-341).
        settlement = max(FINAL_PRICE[SESSIONS[0]] - 520.0, 0.0) * MULTIPLIER
        self.assertAlmostEqual(float(result.perf["ending_cash"].iloc[0]),
                               STARTING_CASH + 100.0 - settlement, places=2)

    async def test_an_expiring_option_is_not_reported_as_an_unexplained_delisting(self):
        """The interaction with the delisting work, and it has to come out this way round.

        Both facts are true of an expired 0DTE contract on the next session: it is past its
        auto-close date, and its bars have stopped. Only the first explains it. Reporting every
        expiry as data that mysteriously ran out would put 170 lines a week in `errors` on a real
        chain and bury the one listing that genuinely went dark.
        """
        result = await self.run_case("option_buy_one_call.py", option_price=1.00)
        self.assertEqual(result.trading_algorithm.data_delistings(), {})
        self.assertEqual(result.errors, [])

    async def test_an_expired_contract_cannot_be_ordered_on_the_next_session(self):
        result = await self.run_case("option_order_expired.py", option_price=1.00)
        transactions = [t for row in result.perf["transactions"] for t in row]
        traded_on_second_session = [t for t in transactions if t.dt.date() == SESSIONS[1]]
        self.assertEqual(traded_on_second_session, [])


class ChainSourceSeamTests(unittest.IsolatedAsyncioTestCase):
    """A second implementation of :class:`OptionChainSource`, to show the seam is real.

    The whole point of the interface is that the synthetic generator can be swapped for a quote
    feed -- gRPC, a vendor REST API, a file of recorded quotes -- without anything downstream
    changing. Asserting that in a docstring proves nothing, so this implements one: a source whose
    contracts and bars come from a canned frame rather than from a model, run through exactly the
    same ingest and the same simulation.

    What a real gRPC source would add over this is a channel and a stream; the two methods, their
    signatures and the columns they return are what is pinned here.
    """

    async def asyncSetUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-options-seam-")
        db_path = Path(self._temp.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db_path)
        self.asset_service = get_asset_service(db_path=str(db_path))
        self.calendar = get_calendar(CALENDAR)
        self.underlying = await self.asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=UNDERLYING_SYMBOL, mic=UNDERLYING_MIC),
            asset_type=AssetType.EQUITY)

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    def make_source(self):
        from ziplime.data.data_sources.options.source import BAR_COLUMNS, OptionChainSource

        closes = {stamp.date(): stamp for stamp in self.calendar.schedule.loc[
            self.calendar.sessions_in_range(SESSIONS[0], SESSIONS[-1]), "close"
        ].dt.tz_convert(self.calendar.tz)}
        quoted_strike, quoted_price = 520.0, 2.50

        class RecordedQuoteSource(OptionChainSource):
            """Stands in for a feed: two contracts a session, quoted from a table."""

            is_real_market_data = True

            @property
            def name(self) -> str:
                return "recorded-quotes"

            async def contracts(self, underlying_symbol, mic, sessions):
                return [
                    ContractSpec(underlying_symbol=underlying_symbol, expiration_date=session,
                                 option_type=option_type, strike=quoted_strike, mic=mic,
                                 listed_date=session, multiplier=MULTIPLIER)
                    for session in sessions
                    for option_type in (OptionType.CALL, OptionType.PUT)
                ]

            async def bars(self, contracts, timestamps):
                rows = []
                for spec in contracts:
                    stamp = closes.get(spec.expiration_date)
                    if stamp is None:
                        continue
                    rows.append({
                        "date": stamp, "symbol": spec.occ_symbol, "mic": spec.mic,
                        "open": quoted_price, "high": quoted_price, "low": quoted_price,
                        "close": quoted_price, "price": quoted_price, "volume": 1_234.0,
                        "bid": quoted_price - 0.05, "ask": quoted_price + 0.05,
                        "implied_volatility": 0.19,
                        "underlying_price": FINAL_PRICE[spec.expiration_date],
                        "open_interest": 10_000.0,
                    })
                return pl.DataFrame(rows).select(BAR_COLUMNS)

        return RecordedQuoteSource()

    async def test_a_different_source_runs_through_the_same_pipeline(self):
        from ziplime.data.data_sources.options.ingest import build_option_bundle

        source = self.make_source()
        rows = [{"date": stamp, "sid": self.underlying.sid, "symbol": self.underlying.symbol,
                 "mic": self.underlying.mic, "open": price, "high": price, "low": price,
                 "close": price, "price": price, "volume": 1e6}
                for session, price in FINAL_PRICE.items()
                for stamp in [self.calendar.session_close(session).tz_convert(
                    self.calendar.tz).to_pydatetime()]]
        underlying_bars = pl.DataFrame(rows)

        bundle, listings = await build_option_bundle(
            source=source, asset_service=self.asset_service, underlying=self.underlying,
            underlying_bars=underlying_bars, sessions=SESSIONS,
            timestamps=underlying_bars["date"], trading_calendar=self.calendar,
            emission_rate=datetime.timedelta(days=1))

        self.assertEqual(bundle.name, "recorded-quotes")
        self.assertEqual(len(listings), 4, "two contracts on each of two sessions")
        self.assertIn("implied_volatility", bundle.data.columns)

    async def test_a_real_source_is_not_refused_a_performance_claim(self):
        """The guard is about synthetic prices, and must not fire on a feed."""
        from ziplime.data.data_sources.options.synthetic import refuse_performance_claims
        refuse_performance_claims(self.make_source())  # does not raise


class VendorIdentityTests(unittest.IsolatedAsyncioTestCase):
    """A source's own name for a contract survives into the listing.

    Real feeds do not speak OCC. Limex addresses an option by a numeric ``security_id``; others use
    a Bloomberg symbol or a RIC. ziplime stores the contract under its OCC symbol because that is
    the identifier that means the same thing everywhere, but every later request for that
    contract's bars has to be made in the vendor's terms -- so the vendor's id is kept on the
    listing's ``external_id``, and a source can read it back instead of rebuilding its own map.
    """

    async def asyncSetUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-options-vendor-")
        db_path = Path(self._temp.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db_path)
        self.asset_service = get_asset_service(db_path=str(db_path))
        self.underlying = await self.asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=UNDERLYING_SYMBOL, mic=UNDERLYING_MIC),
            asset_type=AssetType.EQUITY)

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    async def register(self, vendor_id):
        spec = ContractSpec(
            underlying_symbol=UNDERLYING_SYMBOL, expiration_date=SESSIONS[0],
            option_type=OptionType.CALL, strike=531.0, mic=UNDERLYING_MIC,
            listed_date=SESSIONS[0], multiplier=MULTIPLIER, vendor_id=vendor_id)
        await register_contracts([spec], asset_service=self.asset_service,
                                 underlying=self.underlying)
        listings = await self.asset_service.get_exchange_option_contracts(
            underlying_symbol=UNDERLYING_SYMBOL, expiration_date=SESSIONS[0],
            mic=UNDERLYING_MIC)
        return next(listing for listing in listings if listing.symbol == spec.occ_symbol)

    async def test_a_vendor_id_is_kept_on_the_listing(self):
        listing = await self.register(vendor_id="20250001")
        self.assertEqual(listing.external_id, "20250001")
        # The contract is still stored and found under its OCC symbol.
        self.assertEqual(listing.symbol, "SPY240613C00531000")

    async def test_a_source_without_one_falls_back_to_the_occ_symbol(self):
        listing = await self.register(vendor_id=None)
        self.assertEqual(listing.external_id, listing.symbol)


if __name__ == "__main__":
    unittest.main(verbosity=2)
