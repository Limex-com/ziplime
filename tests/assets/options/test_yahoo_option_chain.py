"""The Yahoo option chain source, against stand-in frames.

No network: `yfinance` is replaced by objects shaped like what it returns, which is enough because
everything this source does with them is filtering and mapping. The column names and values below
are copied from a live `Ticker.option_chain` on 2026-09-14 rather than invented -- in particular
`contractSymbol` in the compact OCC form, `contractSize` as the string ``"REGULAR"``, and
`openInterest` as a float that can be NaN.
"""
import datetime
import types
import unittest

import pandas as pd
import polars as pl

from ziplime.assets.domain.exercise_style import ExerciseStyle
from ziplime.assets.domain.option_type import OptionType
from ziplime.assets.domain.premium_style import PremiumStyle
from ziplime.data.data_sources.options.source import BAR_COLUMNS
from ziplime.data.data_sources.options.yahoo_chain import (
    YahooChainError, YahooOptionChainSource,
)

TZ = "America/New_York"


def chain_frame(strikes, option_type: str = "C", expiry: str = "261016",
                open_interest=1000.0, contract_size="REGULAR") -> pd.DataFrame:
    """A `Ticker.option_chain(...).calls` stand-in."""
    return pd.DataFrame([{
        "contractSymbol": f"AAPL{expiry}{option_type}{int(strike * 1000):08d}",
        "lastTradeDate": pd.Timestamp("2026-09-11 19:59:32+00:00"),
        "strike": float(strike),
        "lastPrice": 10.0,
        "bid": 9.9,
        "ask": 10.1,
        "volume": 100,
        "openInterest": open_interest,
        "impliedVolatility": 0.31,
        "inTheMoney": False,
        "contractSize": contract_size,
        "currency": "USD",
    } for strike in strikes])


class SpecMappingTests(unittest.TestCase):
    """A Yahoo chain row to a `ContractSpec`."""

    def setUp(self):
        self.source = YahooOptionChainSource(strike_window=None, minimum_open_interest=0)
        self.expiry = datetime.date(2026, 10, 16)
        self.first = datetime.date(2026, 8, 20)

    def convert(self, frame, option_type=OptionType.CALL, reference=None):
        return self.source._to_specs(frame, "AAPL", self.expiry, option_type, self.first,
                                     reference)

    def test_a_call_maps_across(self):
        [spec] = self.convert(chain_frame([330]))
        self.assertEqual(spec.underlying_symbol, "AAPL")
        self.assertEqual(spec.option_type, OptionType.CALL)
        self.assertEqual(spec.strike, 330.0)
        self.assertEqual(spec.expiration_date, self.expiry)
        self.assertEqual(spec.multiplier, 100.0)
        self.assertEqual(spec.mic, "OPRA")
        self.assertEqual(spec.exercise_style, ExerciseStyle.AMERICAN)
        self.assertEqual(spec.premium_style, PremiumStyle.UPFRONT)

    def test_a_put_maps_across(self):
        [spec] = self.convert(chain_frame([330], option_type="P"), OptionType.PUT)
        self.assertEqual(spec.option_type, OptionType.PUT)

    def test_yahoos_symbol_is_already_the_form_ziplime_stores(self):
        """Unlike the gRPC feed's padded 21-character ticker, Yahoo's `contractSymbol` is the
        compact OCC symbol -- so the listing name and the vendor id coincide, and nothing has to
        be translated on the way back for a bar request."""
        [spec] = self.convert(chain_frame([330]))
        self.assertEqual(spec.vendor_id, "AAPL261016C00330000")
        self.assertEqual(spec.listing_symbol, "AAPL261016C00330000")
        self.assertEqual(spec.occ_symbol, spec.vendor_id)

    def test_the_listed_date_is_the_windows_start_and_claims_nothing_more(self):
        """Yahoo does not say when a contract was listed. A session it has no bars for is one the
        strategy sits out anyway, because `data.current` returns nothing there."""
        [spec] = self.convert(chain_frame([330]))
        self.assertEqual(spec.listed_date, self.first)

    def test_an_adjusted_contract_is_refused_rather_than_assumed_to_be_a_hundred_shares(self):
        """A contract adjusted after a split is not 100 shares, and silently treating it as one
        would misstate every position in it."""
        with self.assertRaises(YahooChainError) as caught:
            self.convert(chain_frame([330], contract_size="ADJUSTED"))
        self.assertIn("ADJUSTED", str(caught.exception))


class FilterTests(unittest.TestCase):
    """A full chain is several hundred contracts and each one is a request."""

    def specs(self, strike_window=None, minimum_open_interest=0, open_interest=1000.0,
              reference=None):
        source = YahooOptionChainSource(strike_window=strike_window,
                                        minimum_open_interest=minimum_open_interest)
        frame = chain_frame([300, 320, 330, 340, 360], open_interest=open_interest)
        return source._to_specs(frame, "AAPL", datetime.date(2026, 10, 16), OptionType.CALL,
                                datetime.date(2026, 8, 20), reference)

    def test_the_strike_window_keeps_what_is_near_the_money(self):
        strikes = sorted(spec.strike for spec in self.specs(strike_window=0.05, reference=330.0))
        self.assertEqual(strikes, [320.0, 330.0, 340.0])

    def test_no_window_keeps_everything(self):
        self.assertEqual(len(self.specs(strike_window=None, reference=330.0)), 5)

    def test_no_reference_price_keeps_everything_rather_than_nothing(self):
        self.assertEqual(len(self.specs(strike_window=0.05, reference=None)), 5)

    def test_open_interest_filters_strikes_nobody_holds(self):
        """A strike with no open interest prices off one stale trade, and filling against it
        measures the quote rather than the strategy."""
        self.assertEqual(self.specs(minimum_open_interest=50, open_interest=10.0), [])
        self.assertEqual(len(self.specs(minimum_open_interest=50, open_interest=1000.0)), 5)

    def test_a_missing_open_interest_is_not_treated_as_zero(self):
        """NaN means Yahoo did not report it, which is different from reporting none."""
        self.assertEqual(len(self.specs(minimum_open_interest=50, open_interest=float("nan"))), 5)


class IntervalTests(unittest.TestCase):
    """Which bar sizes Yahoo actually serves for an option."""

    def stamps(self, step: datetime.timedelta, count: int = 4):
        start = datetime.datetime(2026, 9, 11, 10, 0, tzinfo=datetime.timezone.utc)
        return [start + step * i for i in range(count)]

    def interval(self, step):
        return YahooOptionChainSource()._interval(self.stamps(step))

    def test_daily_and_minute_are_understood(self):
        self.assertEqual(self.interval(datetime.timedelta(days=1)), "1d")
        self.assertEqual(self.interval(datetime.timedelta(minutes=1)), "1m")
        self.assertEqual(self.interval(datetime.timedelta(minutes=5)), "5m")

    def test_hourly_is_refused_by_name(self):
        """It comes back from Yahoo as an empty frame rather than an error, which would otherwise
        read as a chain where nothing traded."""
        with self.assertRaises(YahooChainError) as caught:
            self.interval(datetime.timedelta(hours=1))
        self.assertIn("hourly", str(caught.exception))

    def test_a_single_stamp_is_read_as_daily(self):
        self.assertEqual(YahooOptionChainSource()._interval(
            self.stamps(datetime.timedelta(days=1), count=1)), "1d")


class BarTests(unittest.IsolatedAsyncioTestCase):
    """Yahoo frames onto the simulation clock."""

    def setUp(self):
        self.source = YahooOptionChainSource(strike_window=None, minimum_open_interest=0)
        [self.spec] = self.source._to_specs(
            chain_frame([330]), "AAPL", datetime.date(2026, 10, 16), OptionType.CALL,
            datetime.date(2026, 9, 10), None)

    def yahoo_frame(self, index):
        return pd.DataFrame({
            "Open": [8.0, 9.0], "High": [10.0, 11.0], "Low": [7.0, 8.0],
            "Close": [9.5, 10.5], "Adj Close": [9.5, 10.5], "Volume": [100, 200],
        }, index=index)

    async def run_bars(self, yahoo_index, stamps):
        downloaded = {self.spec.vendor_id: self.yahoo_frame(yahoo_index)}
        self.source._download = staticmethod(lambda symbols, window: downloaded)
        return await self.source.bars([self.spec], pl.Series(stamps))

    async def test_daily_bars_land_on_the_sessions_close(self):
        """Yahoo stamps a daily bar at midnight of its session; the clock runs to the close."""
        yahoo = pd.DatetimeIndex(["2026-09-10 00:00", "2026-09-11 00:00"]).tz_localize(TZ)
        stamps = [datetime.datetime(2026, 9, 10, 16, 0), datetime.datetime(2026, 9, 11, 16, 0)]
        stamps = [s.replace(tzinfo=yahoo.tz) for s in stamps]
        frame = await self.run_bars(yahoo, stamps)
        self.assertEqual(frame.height, 2)
        self.assertEqual(frame["date"].to_list(), stamps)

    async def test_intraday_bars_match_the_instant(self):
        yahoo = pd.DatetimeIndex(["2026-09-11 10:00", "2026-09-11 10:01"]).tz_localize(TZ)
        stamps = list(yahoo.to_pydatetime())
        frame = await self.run_bars(yahoo, stamps)
        self.assertEqual(frame.height, 2)

    async def test_the_listing_symbol_is_what_the_bundle_carries(self):
        yahoo = pd.DatetimeIndex(["2026-09-10 00:00", "2026-09-11 00:00"]).tz_localize(TZ)
        stamps = [datetime.datetime(2026, 9, 10, 16, 0, tzinfo=yahoo.tz),
                  datetime.datetime(2026, 9, 11, 16, 0, tzinfo=yahoo.tz)]
        frame = await self.run_bars(yahoo, stamps)
        self.assertEqual(set(frame["symbol"]), {"AAPL261016C00330000"})

    async def test_quote_columns_are_present_and_null(self):
        """Yahoo's chain does carry bid, ask, implied volatility and open interest -- but as they
        stand *now*. Writing today's spread onto last week's bar would hand the strategy
        information that did not exist then."""
        yahoo = pd.DatetimeIndex(["2026-09-10 00:00", "2026-09-11 00:00"]).tz_localize(TZ)
        stamps = [datetime.datetime(2026, 9, 10, 16, 0, tzinfo=yahoo.tz),
                  datetime.datetime(2026, 9, 11, 16, 0, tzinfo=yahoo.tz)]
        frame = await self.run_bars(yahoo, stamps)
        self.assertEqual(list(frame.columns), list(BAR_COLUMNS))
        for column in ("bid", "ask", "implied_volatility", "open_interest"):
            self.assertIsNone(frame[column][0])

    async def test_price_is_the_close_because_that_is_what_marks_a_position(self):
        yahoo = pd.DatetimeIndex(["2026-09-10 00:00", "2026-09-11 00:00"]).tz_localize(TZ)
        stamps = [datetime.datetime(2026, 9, 10, 16, 0, tzinfo=yahoo.tz),
                  datetime.datetime(2026, 9, 11, 16, 0, tzinfo=yahoo.tz)]
        frame = await self.run_bars(yahoo, stamps)
        self.assertEqual(frame["price"].to_list(), frame["close"].to_list())

    async def test_no_contracts_is_an_empty_frame_with_the_right_columns(self):
        frame = await self.source.bars([], pl.Series([datetime.datetime(2026, 9, 11)]))
        self.assertTrue(frame.is_empty())
        self.assertEqual(list(frame.columns), list(BAR_COLUMNS))


class ExpiryTests(unittest.IsolatedAsyncioTestCase):
    """Yahoo lists the chain as it stands now, so a past expiry is simply not in it."""

    class Ticker:
        def __init__(self, expiries):
            self.options = tuple(expiries)

        def option_chain(self, text):
            return types.SimpleNamespace(calls=chain_frame([330]),
                                         puts=chain_frame([330], option_type="P"))

        def history(self, **kwargs):
            return pd.DataFrame({"Close": [330.0]},
                                index=pd.DatetimeIndex(["2026-09-11"]).tz_localize(TZ))

    async def contracts(self, expiries, sessions, **kwargs):
        import yfinance

        source = YahooOptionChainSource(strike_window=None, minimum_open_interest=0, **kwargs)
        original = yfinance.Ticker
        yfinance.Ticker = lambda symbol: self.Ticker(expiries)
        try:
            return await source.contracts("AAPL", "XNGS", sessions)
        finally:
            yfinance.Ticker = original

    async def test_an_expiry_after_the_window_is_used(self):
        specs = await self.contracts(["2026-10-16"],
                                     [datetime.date(2026, 9, 10), datetime.date(2026, 9, 11)])
        self.assertEqual(len(specs), 2)

    async def test_an_expiry_inside_the_window_is_refused_with_the_reason(self):
        """Expiring mid-run means the snapshot cannot describe the chain as it stood, so it is
        left out rather than half-served."""
        with self.assertRaises(YahooChainError) as caught:
            await self.contracts(["2026-09-10"],
                                 [datetime.date(2026, 9, 10), datetime.date(2026, 9, 11)])
        self.assertIn("has already passed", str(caught.exception))

    async def test_no_sessions_is_an_empty_chain_rather_than_a_call(self):
        self.assertEqual(await self.contracts(["2026-10-16"], []), [])

    async def test_an_underlying_with_no_options_says_so(self):
        with self.assertRaises(YahooChainError) as caught:
            await self.contracts([], [datetime.date(2026, 9, 11)])
        self.assertIn("no option expiries", str(caught.exception))


class ConfigurationTests(unittest.TestCase):
    def test_it_declares_itself_real_market_data(self):
        self.assertTrue(YahooOptionChainSource().is_real_market_data)

    def test_the_name_identifies_the_source(self):
        self.assertEqual(YahooOptionChainSource().name, "yahoo-options")

    def test_an_unknown_venue_is_refused(self):
        with self.assertRaises(YahooChainError):
            YahooOptionChainSource(venue="NASDAQ-OPTIONS")
