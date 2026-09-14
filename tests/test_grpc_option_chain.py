"""The real option chain source, against a stand-in feed.

No network and no credentials: the feed is replaced by objects shaped like the protobuf messages
it returns, which is enough because everything this source does with them is arithmetic and
mapping. What it cannot cover is the shape itself changing -- a renamed field, a different
nesting -- and that is what `_decimal`, `_date` and the enum tables are written to fail loudly on
rather than absorb.

The values below are the ones the live feed actually returned on 2026-09-13, copied rather than
invented: fixed-point decimals with `num`/`scale`, `type` 0 for a call, `style` 1 for American,
`settlement_type` 1 for physical, and a 21-character padded OCC ticker.
"""
import datetime
import types
import unittest

import polars as pl

from ziplime.assets.domain.exercise_style import ExerciseStyle
from ziplime.assets.domain.option_type import OptionType
from ziplime.assets.domain.premium_style import PremiumStyle
from ziplime.assets.domain.settlement_type import SettlementType
from ziplime.data.data_sources.options.grpc_chain import (
    GrpcChainError, GrpcOptionChainSource, _date, _decimal,
)
from ziplime.data.data_sources.options.source import BAR_COLUMNS
from ziplime.data.data_sources.options.venues import OptionVenue, register_venue

TZ = "America/New_York"


def decimal(value: float, scale: int = 0):
    """The feed's fixed-point decimal: an integer and the power of ten it is scaled by."""
    return types.SimpleNamespace(num=int(round(value * (10 ** scale))), scale=scale)


def date(value: datetime.date | None):
    if value is None:
        return types.SimpleNamespace(year=0, month=0, day=0)
    return types.SimpleNamespace(year=value.year, month=value.month, day=value.day)


def security(ticker: str, security_id: int, strike: float, option_type: int = 0,
             style: int = 1, settlement: int = 1, multiplier: float = 100.0,
             expiry: datetime.date = datetime.date(2026, 12, 18),
             listed: datetime.date | None = datetime.date(2025, 1, 2), mic: str = "OPRA"):
    """One `SecurityResponse`, nested the way the live service nests it."""
    option = types.SimpleNamespace(
        type=option_type, style=style, settlement_type=settlement,
        strike=decimal(strike), multiplier=decimal(multiplier),
        contract_size=decimal(100.0), tick_value=decimal(0.01, scale=2),
        trade_first_day=date(listed), trade_last_day=date(expiry),
        expiration_first_day=date(expiry), expiration_last_day=date(expiry), root="SPY")
    common = types.SimpleNamespace(
        security_id=security_id,
        ticker=types.SimpleNamespace(value=ticker),
        mic=types.SimpleNamespace(value=mic))
    return types.SimpleNamespace(
        security=types.SimpleNamespace(
            security=types.SimpleNamespace(option=option, common=common)))


#: ziplime ships OPRA alone; a margined venue reaches it through `register_venue`, which is what a
#: market's own connector does at import time. Registered here so the source can be asked for one.
REGISTERED = register_venue(OptionVenue(
    name="XTSTGRPC", mic="XTST", premium_style=PremiumStyle.MARGINED,
    exercise_style=ExerciseStyle.EUROPEAN, settlement_type=SettlementType.CASH,
    contract_size=100.0))


class FakeFeed:
    """A `GrpcDataSource` stand-in: the two private attributes the source reads, and `get_data`."""

    def __init__(self, bars_by_symbol: dict[str, pl.DataFrame] | None = None,
                 underlying_close: float | None = 750.0):
        self._server_url = "fake:443"
        self._authorization_token = "not-a-real-token"
        self._bars = bars_by_symbol or {}
        self._underlying_close = underlying_close
        self.asked_for: list[str] = []

    async def get_data(self, symbols, frequency, date_from, date_to, **kwargs):
        symbol = symbols[0]
        self.asked_for.append(symbol)
        if symbol in self._bars:
            return self._bars[symbol]
        if symbol.startswith("SPY@") and self._underlying_close is not None:
            return pl.DataFrame({
                "date": [datetime.datetime(2026, 9, 11, tzinfo=datetime.timezone.utc)],
                "close": [self._underlying_close]})
        return pl.DataFrame()


class DecimalAndDateTests(unittest.TestCase):
    """The two conversions every field goes through."""

    def test_a_scaled_decimal(self):
        self.assertEqual(_decimal(decimal(764.0, scale=3)), 764.0)
        self.assertEqual(_decimal(decimal(0.01, scale=2)), 0.01)

    def test_an_unscaled_decimal_is_a_whole_number_not_a_divisor(self):
        """`scale=0` is how the feed writes 100, and reading it as a divisor would give 1."""
        self.assertEqual(_decimal(decimal(100.0)), 100.0)

    def test_an_unset_date_is_none_rather_than_an_error(self):
        """An absent protobuf date reads as year 0, which `datetime.date` refuses. The field is
        optional, so that is a fact rather than a failure."""
        self.assertIsNone(_date(date(None)))
        self.assertEqual(_date(date(datetime.date(2026, 12, 18))),
                         datetime.date(2026, 12, 18))


class SpecMappingTests(unittest.TestCase):
    """Feed contracts to `ContractSpec`s."""

    def setUp(self):
        self.source = GrpcOptionChainSource(FakeFeed(), venue="OPRA", underlying_mic="ARCX",
                                            strike_window=None)
        self.window = (datetime.date(2026, 7, 1), datetime.date(2026, 9, 11))

    def convert(self, securities, reference_price=None):
        return self.source._to_specs(securities, "SPY", "ARCX", *self.window, reference_price)

    def test_a_call_maps_across(self):
        [spec] = self.convert([security("SPY   261218C00745000", 4503599940011335, 745.0)])
        self.assertEqual(spec.underlying_symbol, "SPY")
        self.assertEqual(spec.option_type, OptionType.CALL)
        self.assertEqual(spec.strike, 745.0)
        self.assertEqual(spec.expiration_date, datetime.date(2026, 12, 18))
        self.assertEqual(spec.multiplier, 100.0)
        self.assertEqual(spec.exercise_style, ExerciseStyle.AMERICAN)
        self.assertEqual(spec.settlement_type, SettlementType.PHYSICAL)
        self.assertEqual(spec.premium_style, PremiumStyle.UPFRONT)

    def test_a_put_maps_across(self):
        [spec] = self.convert([security("SPY   261218P00745000", 1, 745.0, option_type=1)])
        self.assertEqual(spec.option_type, OptionType.PUT)

    def test_the_vendor_id_is_carried_so_bars_can_be_asked_for(self):
        """It becomes the listing's `external_id`; without it a later bar request has no name to
        use but the OCC symbol, which this feed does not index by."""
        [spec] = self.convert([security("SPY   261218C00745000", 4503599940011335, 745.0)])
        self.assertEqual(spec.vendor_id, "4503599940011335")

    def test_the_contract_is_listed_on_its_own_venue_not_the_underlyings(self):
        """`mic` in the call names where the *underlying* trades. A real OPRA option is not listed
        on ARCX, and asking the feed for one there returns "Security not found"."""
        [spec] = self.convert([security("SPY   261218C00745000", 1, 745.0, mic="OPRA")])
        self.assertEqual(spec.mic, "OPRA")

    def test_the_feeds_listing_date_is_used_rather_than_a_convention(self):
        """A weekly lives days and a LEAPS lives years; only this field knows which."""
        [spec] = self.convert([security("SPY   261218C00745000", 1, 745.0,
                                        listed=datetime.date(2025, 1, 2))])
        self.assertEqual(spec.listed_date, datetime.date(2025, 1, 2))

    def test_a_contract_listed_after_the_window_is_dropped(self):
        """It did not exist during the run, so trading it would be trading an instrument that was
        not there."""
        self.assertEqual(self.convert([security("SPY   261218C00745000", 1, 745.0,
                                                listed=datetime.date(2026, 10, 1))]), [])

    def test_the_padded_ticker_is_remembered_for_the_bar_request(self):
        """ziplime stores the compact OCC symbol; the feed speaks the padded 21-character form."""
        [spec] = self.convert([security("SPY   261218C00745000", 1, 745.0)])
        self.assertEqual(spec.listing_symbol, "SPY261218C00745000")
        self.assertEqual(self.source._tickers[spec.listing_symbol], "SPY   261218C00745000")

    def test_an_unknown_option_type_is_refused_rather_than_guessed(self):
        with self.assertRaises(GrpcChainError) as caught:
            self.convert([security("SPY   261218X00745000", 1, 745.0, option_type=7)])
        self.assertIn("neither call nor put", str(caught.exception))

    def test_an_unknown_settlement_type_is_refused(self):
        """Settlement decides what happens at expiry; a default would be an invented one."""
        with self.assertRaises(GrpcChainError) as caught:
            self.convert([security("SPY   261218C00745000", 1, 745.0, settlement=99)])
        self.assertIn("settlement type", str(caught.exception))

    def test_a_zero_multiplier_falls_back_to_the_venues_contract_size(self):
        """Measured on a live feed, and documented in `venues`: at least one venue reports zero
        there and puts the real size in `contract_size`."""
        [spec] = self.convert([security("SPY   261218C00745000", 1, 745.0, multiplier=0.0)])
        self.assertEqual(spec.multiplier, 100.0)


class StrikeWindowTests(unittest.TestCase):
    """316 contracts in one SPY expiry, each needing its own bar request. The window is what makes
    that a usable number rather than a reason not to use the source."""

    def specs(self, strike_window, reference_price):
        source = GrpcOptionChainSource(FakeFeed(), strike_window=strike_window)
        securities = [security(f"SPY   261218C00{int(k)*1000:06d}", i, float(k))
                      for i, k in enumerate([600, 700, 745, 750, 800, 900], start=1)]
        return source._to_specs(securities, "SPY", "ARCX", datetime.date(2026, 7, 1),
                                datetime.date(2026, 9, 11), reference_price)

    def test_it_keeps_strikes_near_the_underlying(self):
        strikes = sorted(spec.strike for spec in self.specs(0.03, 750.0))
        self.assertEqual(strikes, [745.0, 750.0])

    def test_a_wider_window_keeps_more(self):
        # 15% of 750 is 112.50, so 700..800 survive and the 600 and 900 wings do not.
        strikes = sorted(spec.strike for spec in self.specs(0.15, 750.0))
        self.assertEqual(strikes, [700.0, 745.0, 750.0, 800.0])

    def test_no_window_keeps_the_whole_chain(self):
        self.assertEqual(len(self.specs(None, 750.0)), 6)

    def test_no_reference_price_keeps_the_whole_chain_rather_than_none_of_it(self):
        """A missing underlying price means slow and correct, not fast and empty."""
        self.assertEqual(len(self.specs(0.03, None)), 6)


class BarTests(unittest.IsolatedAsyncioTestCase):
    """Bars, and the timestamp handling that decides which session they land on."""

    def setUp(self):
        self.stamps = pl.Series([
            datetime.datetime(2026, 9, 10, 16, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2026, 9, 11, 16, 0, tzinfo=datetime.timezone.utc)])

    def feed_with(self, rows):
        return FakeFeed({"SPY   261218C00745000@OPRA": pl.DataFrame(rows)})

    def spec_for(self, source):
        [spec] = source._to_specs([security("SPY   261218C00745000", 1, 745.0)], "SPY", "ARCX",
                                  datetime.date(2026, 9, 1), datetime.date(2026, 9, 11), None)
        return spec

    async def test_bars_are_stamped_on_the_clocks_instants(self):
        feed = self.feed_with({
            "date": [datetime.datetime(2026, 9, 10, tzinfo=datetime.timezone.utc),
                     datetime.datetime(2026, 9, 11, tzinfo=datetime.timezone.utc)],
            "open": [40.0, 41.0], "high": [42.0, 43.0], "low": [39.0, 40.0],
            "close": [41.0, 42.0], "price": [41.0, 42.0], "volume": [100.0, 200.0],
            "symbol": ["x", "x"], "mic": ["OPRA", "OPRA"]})
        source = GrpcOptionChainSource(feed, strike_window=None)
        frame = await source.bars([self.spec_for(source)], self.stamps)
        self.assertEqual(frame.height, 2)
        self.assertEqual(frame["date"].to_list(), self.stamps.to_list())

    async def test_the_session_is_the_utc_date_not_the_converted_one(self):
        """The feed stamps a daily bar at midnight UTC of its own session. Converting that to the
        exchange's zone moves it onto the previous evening, and every bar then lands one session
        early -- with the last one falling off the run entirely."""
        feed = self.feed_with({
            "date": [datetime.datetime(2026, 9, 11, 0, 0, tzinfo=datetime.timezone.utc)],
            "open": [41.0], "high": [43.0], "low": [40.0], "close": [42.0], "price": [42.0],
            "volume": [200.0], "symbol": ["x"], "mic": ["OPRA"]})
        source = GrpcOptionChainSource(feed, strike_window=None)
        frame = await source.bars([self.spec_for(source)], self.stamps)
        self.assertEqual(frame.height, 1)
        self.assertEqual(frame["date"][0], self.stamps[1])

    async def test_the_listing_symbol_is_the_compact_occ_form(self):
        feed = self.feed_with({
            "date": [datetime.datetime(2026, 9, 11, tzinfo=datetime.timezone.utc)],
            "open": [41.0], "high": [43.0], "low": [40.0], "close": [42.0], "price": [42.0],
            "volume": [200.0], "symbol": ["whatever the feed called it"], "mic": ["OPRA"]})
        source = GrpcOptionChainSource(feed, strike_window=None)
        frame = await source.bars([self.spec_for(source)], self.stamps)
        self.assertEqual(frame["symbol"].to_list(), ["SPY261218C00745000"])

    async def test_the_feed_is_asked_in_its_own_padded_symbol(self):
        feed = self.feed_with({
            "date": [datetime.datetime(2026, 9, 11, tzinfo=datetime.timezone.utc)],
            "open": [41.0], "high": [43.0], "low": [40.0], "close": [42.0], "price": [42.0],
            "volume": [200.0], "symbol": ["x"], "mic": ["OPRA"]})
        source = GrpcOptionChainSource(feed, strike_window=None)
        await source.bars([self.spec_for(source)], self.stamps)
        self.assertIn("SPY   261218C00745000@OPRA", feed.asked_for)

    async def test_quote_columns_are_present_and_null_rather_than_absent(self):
        """The candle service carries trades, not quotes. Absent means "this source does not model
        quotes"; null means "it does and this bar had none". The first is the truth here."""
        feed = self.feed_with({
            "date": [datetime.datetime(2026, 9, 11, tzinfo=datetime.timezone.utc)],
            "open": [41.0], "high": [43.0], "low": [40.0], "close": [42.0], "price": [42.0],
            "volume": [200.0], "symbol": ["x"], "mic": ["OPRA"]})
        source = GrpcOptionChainSource(feed, strike_window=None)
        frame = await source.bars([self.spec_for(source)], self.stamps)
        self.assertEqual(list(frame.columns), list(BAR_COLUMNS))
        for column in ("bid", "ask", "implied_volatility", "open_interest"):
            self.assertIsNone(frame[column][0])

    async def test_a_contract_the_feed_has_nothing_for_contributes_no_rows(self):
        """An illiquid strike that never printed is an ordinary fact about a chain."""
        source = GrpcOptionChainSource(FakeFeed(), strike_window=None)
        frame = await source.bars([self.spec_for(source)], self.stamps)
        self.assertTrue(frame.is_empty())
        self.assertEqual(list(frame.columns), list(BAR_COLUMNS))

    async def test_one_failing_contract_does_not_fail_the_chain(self):
        class Exploding(FakeFeed):
            async def get_data(self, symbols, frequency, date_from, date_to, **kwargs):
                raise RuntimeError("the feed hung up")

        source = GrpcOptionChainSource(Exploding(), strike_window=None)
        frame = await source.bars([self.spec_for(source)], self.stamps)
        self.assertTrue(frame.is_empty())


class ConfigurationTests(unittest.TestCase):
    def test_it_declares_itself_real_market_data(self):
        """The synthetic source sets this False so nothing reports performance on a model. This
        one is the opposite claim, and it is the whole reason the source exists."""
        self.assertTrue(GrpcOptionChainSource(FakeFeed()).is_real_market_data)

    def test_the_name_says_which_venue_it_speaks_for(self):
        self.assertEqual(GrpcOptionChainSource(FakeFeed(), venue="OPRA").name, "grpc-opra")
        self.assertEqual(GrpcOptionChainSource(FakeFeed(), venue=REGISTERED.name).name,
                         f"grpc-{REGISTERED.name.lower()}")

    def test_an_unknown_venue_is_refused_by_name(self):
        with self.assertRaises(GrpcChainError) as caught:
            GrpcOptionChainSource(FakeFeed(), venue="NASDAQ-OPTIONS")
        self.assertIn("OPRA", str(caught.exception))

    def test_the_venues_conventions_are_taken_from_the_venue(self):
        """Including one registered from outside, which is how every venue but OPRA arrives."""
        other = GrpcOptionChainSource(FakeFeed(), venue=REGISTERED.name)
        self.assertEqual(other._venue.premium_style, PremiumStyle.MARGINED)
        opra = GrpcOptionChainSource(FakeFeed(), venue="OPRA")
        self.assertEqual(opra._venue.premium_style, PremiumStyle.UPFRONT)


class EmptyWindowTests(unittest.IsolatedAsyncioTestCase):
    async def test_no_sessions_is_an_empty_chain_not_a_call(self):
        source = GrpcOptionChainSource(FakeFeed())
        self.assertEqual(await source.contracts("SPY", "ARCX", []), [])


class ExpiredContractTests(unittest.IsolatedAsyncioTestCase):
    """Resolving contracts the ordinary lookup will not find.

    `GetSecurityInfo` answers for what is currently listed and raises for what is not, and the
    feed's own fallback then addresses the candle service by ticker -- which it mostly refuses.
    An expired option therefore looks like an option with no data. `GetSecurityHistory` does
    answer for them, and `ExpiredAwareFeed` is the bridge.
    """

    class Feed:
        """A `GrpcDataSource` stand-in with the private members the wrapper touches."""

        def __init__(self):
            self._server_url = "fake:443"
            self._authorization_token = "not-a-real-token"
            #: The cache the real source fills from its own lookup, and the wrapper seeds.
            self._security_id_tasks: dict = {}
            self.requested: list[str] = []

        async def get_token(self):
            return self._authorization_token

        async def get_data(self, symbols, frequency, date_from, date_to, **kwargs):
            self.requested.append(symbols[0])
            return pl.DataFrame({"date": [datetime.datetime(2026, 9, 10,
                                                            tzinfo=datetime.timezone.utc)],
                                 "close": [0.7]})

    def wrapped(self, resolves_to: int | None = 4503599966245258):
        from ziplime.data.data_sources.options.grpc_chain import ExpiredAwareFeed

        feed = self.Feed()
        wrapper = ExpiredAwareFeed(feed)

        async def resolve(channel, symbol):
            return resolves_to

        wrapper._resolve_expired = resolve

        async def prime(symbols):
            """The real `_prime` opens a gRPC channel; this keeps the seeding and drops the call."""
            import asyncio

            loop = asyncio.get_running_loop()
            for symbol in symbols:
                found = await resolve(None, symbol)
                if found is None:
                    continue
                wrapper._resolved[symbol] = found
                if symbol not in feed._security_id_tasks:
                    settled = loop.create_future()
                    settled.set_result(found)
                    feed._security_id_tasks[symbol] = settled

        wrapper._prime = prime
        return wrapper, feed

    async def test_it_seeds_the_sources_own_id_cache(self):
        """The seam. The source resolves a symbol once and caches it here; writing the id into
        that slot is what makes the candle request use it."""
        wrapper, feed = self.wrapped()
        symbol = "SPY   260910C00758000@OPRA"
        await wrapper.get_data(symbols=[symbol], frequency=datetime.timedelta(minutes=1),
                               date_from=None, date_to=None)
        self.assertIn(symbol, feed._security_id_tasks)
        self.assertEqual(feed._security_id_tasks[symbol].result(), 4503599966245258)

    async def test_the_wrapped_source_still_does_the_fetching(self):
        wrapper, feed = self.wrapped()
        symbol = "SPY   260910C00758000@OPRA"
        frame = await wrapper.get_data(symbols=[symbol], frequency=datetime.timedelta(minutes=1),
                                       date_from=None, date_to=None)
        self.assertEqual(feed.requested, [symbol])
        self.assertEqual(frame.height, 1)

    async def test_a_contract_it_cannot_resolve_is_passed_through_unchanged(self):
        """Not every miss is an expired contract; a symbol that simply does not exist has to
        reach the source and come back empty rather than raising here."""
        wrapper, feed = self.wrapped(resolves_to=None)
        symbol = "SPY   260910C99999000@OPRA"
        await wrapper.get_data(symbols=[symbol], frequency=datetime.timedelta(minutes=1),
                               date_from=None, date_to=None)
        self.assertEqual(feed._security_id_tasks, {})
        self.assertEqual(feed.requested, [symbol])

    async def test_each_symbol_is_resolved_once(self):
        """A chain is several hundred contracts; resolving each one per bar request would triple
        the traffic."""
        wrapper, feed = self.wrapped()
        symbol = "SPY   260910C00758000@OPRA"
        for _ in range(3):
            await wrapper.get_data(symbols=[symbol], frequency=datetime.timedelta(minutes=1),
                                   date_from=None, date_to=None)
        self.assertEqual(len(wrapper._resolved), 1)

    def test_unknown_attributes_reach_the_wrapped_source(self):
        wrapper, feed = self.wrapped()
        self.assertEqual(wrapper._server_url, "fake:443")

    def test_get_data_is_overridden_rather_than_delegated(self):
        """The pitfall this class was rewritten around. `get_data` internally calls
        `self.get_security_identifier`, so with plain `__getattr__` delegation `self` is the
        *wrapped* object and an override on the wrapper is never consulted -- the first version
        was bypassed exactly that way and the logs showed the source's own fallback instead."""
        from ziplime.data.data_sources.options.grpc_chain import ExpiredAwareFeed

        self.assertIn("get_data", vars(ExpiredAwareFeed))
