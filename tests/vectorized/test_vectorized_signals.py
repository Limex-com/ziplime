"""Vectorised signals inside an event-driven run.

The mechanism moves a strategy's arithmetic out of the bar loop and leaves its decisions in it.
That is only worth having if moving the arithmetic does not change the answer, so the test that
matters is :class:`EquivalenceTests`: the same strategy written both ways -- once with
``compute_signals`` over the whole array, once with ``data.history`` on every bar -- has to produce
the same performance table, order for order.

The rest of the file is about the danger the mechanism introduces. Handing a strategy the entire
price history is exactly how look-ahead bias gets into a backtest, and a backtest with look-ahead
does not fail: it succeeds, spectacularly, and the number it reports is unrelated to anything.
So two defences are pinned here. :class:`CausalityTests` covers computations that read the future
and have to be refused. :class:`AccessTests` covers the bar loop, where the future is not guarded
but absent -- there is no accessor that returns a later row.
"""
import datetime
import shutil
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.constants.data_type import DataType
from ziplime.core.ingest_data import get_asset_service
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.utils.calendar_utils import get_calendar
from ziplime.vectorized.signals import (
    LookAheadError, PricePanel, SignalPanel, normalise_signals, verify_causality,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = PROJECT_ROOT / "tests" / "fixtures"

CALENDAR = "XNYS"
#: The run, and the warm-up the slow average needs before it. The bundle covers both, so neither
#: version of the strategy spends the first month of the run computing averages of nothing.
START, END = datetime.date(2024, 3, 1), datetime.date(2024, 9, 30)
BUNDLE_START = datetime.date(2023, 11, 1)
CASH = 100_000.0
TICKERS = ("JNJ", "KO")


def price_path(index: pd.DatetimeIndex) -> pd.DataFrame:
    """Deterministic, and shaped to cross the averages several times.

    A path that never crosses would let a broken signal pass by trading nothing.
    """
    steps = np.arange(len(index))
    return pd.DataFrame({
        "JNJ": 100.0 + np.sin(steps / 11.0) * 9.0 + np.sin(steps / 3.0) * 2.0 + steps * 0.02,
        "KO": 50.0 + np.cos(steps / 8.0) * 5.0 + np.cos(steps / 2.5) * 1.5 - steps * 0.01,
    }, index=index)


def make_panel(frame: pd.DataFrame) -> PricePanel:
    return PricePanel({"close": frame, "open": frame, "high": frame, "low": frame,
                       "volume": frame * 0 + 1e9}, frame.index)


class SignalsTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-signals-")
        db = Path(self._temp.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db)
        self.asset_service = get_asset_service(db_path=str(db))
        self.calendar = get_calendar(CALENDAR)
        sessions = self.calendar.sessions_in_range(BUNDLE_START, END)
        self.index = pd.DatetimeIndex(
            self.calendar.schedule.loc[sessions, "close"].dt.tz_convert(self.calendar.tz))
        self.prices = price_path(self.index)
        self.listings = {}
        for ticker in TICKERS:
            self.listings[ticker] = await self.asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=ticker, mic="XNYS"), asset_type=AssetType.EQUITY)

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    def bundle(self) -> DataBundle:
        rows = [{"date": stamp, "sid": self.listings[t].sid, "symbol": t, "mic": "XNYS",
                 "open": p, "high": p, "low": p, "close": p, "price": p, "volume": 1e9}
                for t in TICKERS for stamp, p in self.prices[t].items()]
        data = pl.DataFrame(rows).sort(["sid", "date"])
        spans = data.with_row_index().group_by("sid", maintain_order=True).agg(
            [pl.col("index").first().alias("first"), pl.col("index").last().alias("last")])
        return DataBundle(
            name="signals-test", version="1", start_date=data["date"].min(),
            end_date=data["date"].max(), trading_calendar=self.calendar,
            frequency=datetime.timedelta(days=1), original_frequency=datetime.timedelta(days=1),
            data_type=DataType.MARKET_DATA, timestamp=data["date"].max(), data=data,
            sid_indexes={r["sid"]: (r["first"], r["last"] + 1)
                         for r in spans.iter_rows(named=True)})

    async def run_strategy(self, fixture: str, commission=None, slippage=None, **kwargs):
        from ziplime.core.run_simulation import run_simulation

        return await run_simulation(
            start_date=datetime.datetime.combine(START, datetime.time.min, tzinfo=self.calendar.tz),
            end_date=datetime.datetime.combine(END, datetime.time.max, tzinfo=self.calendar.tz),
            trading_calendar=CALENDAR, emission_rate=datetime.timedelta(days=1),
            total_cash=CASH, market_data_source=self.bundle(), custom_data_sources=[],
            algorithm_file=str(FIXTURES / fixture), stop_on_error=True,
            asset_service=self.asset_service, benchmark_asset_symbol=None, benchmark_returns=None,
            equity_commission=commission or NoCommission(),
            equity_slippage=slippage or NoSlippage(), max_leverage=10.0,
            print_algo=False, **kwargs)


class EquivalenceTests(SignalsTestCase):
    """Vectorising the arithmetic must not change the strategy.

    This is the claim the mechanism rests on. If these fail, the speed has been bought by
    computing something other than what the author wrote.
    """

    async def test_both_versions_place_the_same_orders(self):
        vectorised = await self.run_strategy("signals_vectorised.py")
        bar_by_bar = await self.run_strategy("signals_bar_by_bar.py")

        def signatures(result):
            return [(t.asset.symbol, t.amount, round(float(t.price), 8))
                    for row in result.perf["transactions"] for t in row]

        placed = signatures(vectorised)
        self.assertGreater(len(placed), 4, "a strategy that never traded proves nothing")
        self.assertEqual(placed, signatures(bar_by_bar))

    async def test_both_versions_agree_bar_for_bar(self):
        vectorised = await self.run_strategy("signals_vectorised.py")
        bar_by_bar = await self.run_strategy("signals_bar_by_bar.py")
        for column in ("portfolio_value", "ending_cash", "ending_exposure", "returns"):
            np.testing.assert_allclose(
                vectorised.perf[column].to_numpy(dtype=float),
                bar_by_bar.perf[column].to_numpy(dtype=float), rtol=0, atol=1e-9,
                err_msg=f"{column} differs between the vectorised and bar-by-bar versions")

    async def test_both_versions_report_the_same_metrics(self):
        vectorised = await self.run_strategy("signals_vectorised.py")
        bar_by_bar = await self.run_strategy("signals_bar_by_bar.py")
        for metric in ("sharpe", "max_drawdown", "algo_volatility", "algorithm_period_return"):
            self.assertAlmostEqual(float(vectorised.perf[metric].iloc[-1]),
                                   float(bar_by_bar.perf[metric].iloc[-1]), places=9,
                                   msg=f"{metric} differs between the two versions")


class CostModelTests(SignalsTestCase):
    """Vectorising the signals must not touch execution.

    This is the half of the hybrid where ziplime still fills every order, so its commission and
    slippage models have to apply exactly as they do in an ordinary run -- and identically to the
    bar-by-bar twin, since the two place the same trades. It is worth pinning rather than assuming:
    the whole selling point of this path over a vectorbt replay is that costs are still modelled.
    """

    def costs(self):
        from ziplime.finance.commission.per_share import PerShare
        from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage

        return PerShare(cost=0.005, min_trade_cost=1.0), FixedBasisPointsSlippage(basis_points=5)

    async def test_commissions_are_charged_on_the_vectorised_path(self):
        commission, slippage = self.costs()
        free = await self.run_strategy("signals_vectorised.py")
        costed = await self.run_strategy("signals_vectorised.py", commission=commission,
                                         slippage=slippage)
        self.assertLess(float(costed.perf["portfolio_value"].iloc[-1]),
                        float(free.perf["portfolio_value"].iloc[-1]),
                        "costs were configured and changed nothing -- they never ran")

    async def test_both_versions_pay_the_same_costs(self):
        """Same trades, same models, same bill. A difference here would mean the two paths fill
        differently, which is the one thing the split is not allowed to change."""
        commission, slippage = self.costs()
        vectorised = await self.run_strategy("signals_vectorised.py", commission=commission,
                                             slippage=slippage)
        bar_by_bar = await self.run_strategy("signals_bar_by_bar.py", commission=commission,
                                             slippage=slippage)
        np.testing.assert_allclose(
            vectorised.perf["portfolio_value"].to_numpy(dtype=float),
            bar_by_bar.perf["portfolio_value"].to_numpy(dtype=float), rtol=0, atol=1e-9)

        def fills(result):
            return [(t.asset.symbol, t.amount, round(float(t.price), 8))
                    for row in result.perf["transactions"] for t in row]

        def charged(result):
            # The event-driven engine books the commission on the order, not on the transaction;
            # the vectorised replay has no orders and carries it on the transaction instead.
            return [round(float(o.commission), 8)
                    for row in result.perf["orders"] for o in row]

        self.assertEqual(fills(vectorised), fills(bar_by_bar))
        billed = charged(vectorised)
        self.assertGreater(sum(billed), 0.0, "nothing was charged")
        self.assertEqual(billed, charged(bar_by_bar))

    async def test_slippage_moves_the_fill_away_from_the_close(self):
        """Not just a fee taken off the top -- the traded price itself has to move."""
        from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage

        clean = await self.run_strategy("signals_vectorised.py")
        slipped = await self.run_strategy("signals_vectorised.py",
                                          slippage=FixedBasisPointsSlippage(basis_points=25))
        first_clean = next(t for row in clean.perf["transactions"] for t in row)
        first_slipped = next(t for row in slipped.perf["transactions"] for t in row)
        self.assertEqual(first_clean.amount, first_slipped.amount)
        self.assertNotAlmostEqual(float(first_clean.price), float(first_slipped.price), places=6)


class FuturesTests(unittest.IsolatedAsyncioTestCase):
    """Futures through the vectorised-signal path.

    Worth its own class because futures are where a shortcut would show. A contract's position is
    worth ``price x multiplier x amount`` and costs margin rather than cash, and none of that is
    visible in a price series -- so if the mechanism had quietly taken over any part of execution,
    a contract with a multiplier of 1000 would be off by three orders of magnitude. It does not,
    and these pin that: the signals are arithmetic, the accounting stays the engine's.
    """

    MULTIPLIER = 1000.0

    async def asyncSetUp(self):
        from ziplime.core.ingest_data import get_asset_service
        from tests.futures_fixtures import EXCHANGE

        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-signals-fut-")
        db = Path(self._temp.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db)
        self.asset_service = get_asset_service(db_path=str(db))
        self.calendar = get_calendar(CALENDAR)

        self.start = datetime.date(2023, 2, 1)
        self.end = datetime.date(2023, 6, 30)
        bundle_start = datetime.date(2023, 1, 3)
        sessions = self.calendar.sessions_in_range(bundle_start, self.end)
        self.index = pd.DatetimeIndex(
            self.calendar.schedule.loc[sessions, "close"].dt.tz_convert(self.calendar.tz))
        steps = np.arange(len(self.index))
        self.prices = pd.Series(75.0 + np.sin(steps / 9.0) * 6.0 + np.sin(steps / 2.5) * 1.5,
                                index=self.index)

        from ziplime.assets.entities.commodity import Commodity
        from ziplime.assets.entities.currency import Currency

        # id=None so the database assigns one. The module-level fixtures carry hard-coded ids that
        # collide with the rows the seeded database already has.
        far_past, far_future = datetime.date(1900, 1, 1), datetime.date(2099, 1, 1)
        await self.asset_service.save_exchanges(exchanges=[EXCHANGE])
        [quote] = await self.asset_service.save_currencies([Currency(
            id=None, isin=None, asset_name="USD", start_date=far_past, end_date=far_future,
            first_traded=far_past, auto_close_date=far_future)])
        [root_asset] = await self.asset_service.save_commodities([Commodity(
            id=None, isin=None, asset_name="CL", start_date=far_past, end_date=far_future,
            first_traded=far_past, auto_close_date=far_future)])
        self.listing = await self._store_contract(quote, root_asset, bundle_start)

    async def _store_contract(self, quote, root_asset, first_session):
        from ziplime.assets.domain.settlement_type import SettlementType
        from ziplime.assets.entities.exchange_asset import ExchangeAsset
        from ziplime.assets.entities.futures_contract import FuturesContract
        from tests.futures_fixtures import EXCHANGE

        expiration = datetime.date(2023, 12, 20)
        [contract] = await self.asset_service.save_futures_contracts([FuturesContract(
            id=None, isin=None, asset_name="CLZ23", start_date=first_session,
            end_date=expiration, first_traded=first_session,
            auto_close_date=expiration + datetime.timedelta(days=1),
            root_exchange_asset=None, root_asset=root_asset, root_symbol="CL",
            notice_date=expiration, expiration_date=expiration,
            multiplier=self.MULTIPLIER, tick_size=0.01,
            settlement_type=SettlementType.CASH, margin_currency="USD")])
        [listing] = await self.asset_service.save_exchange_assets(exchange_assets=[ExchangeAsset(
            sid=None, symbol="CLZ23", start_date=first_session, end_date=expiration,
            first_traded=first_session, auto_close_date=expiration + datetime.timedelta(days=1),
            external_id="clz23", exchange=EXCHANGE, asset=contract, quote=quote)])
        return listing

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    def bundle(self):
        rows = [{"date": stamp, "sid": self.listing.sid, "symbol": "CLZ23", "mic": "XCME",
                 "open": p, "high": p, "low": p, "close": p, "price": p, "volume": 1e6}
                for stamp, p in self.prices.items()]
        data = pl.DataFrame(rows).sort(["sid", "date"])
        return DataBundle(
            name="futures-signals", version="1", start_date=data["date"].min(),
            end_date=data["date"].max(), trading_calendar=self.calendar,
            frequency=datetime.timedelta(days=1), original_frequency=datetime.timedelta(days=1),
            data_type=DataType.MARKET_DATA, timestamp=data["date"].max(), data=data,
            sid_indexes={self.listing.sid: (0, len(data))})

    async def run_strategy(self, fixture: str):
        from ziplime.core.run_simulation import run_simulation

        return await run_simulation(
            start_date=datetime.datetime.combine(self.start, datetime.time.min,
                                                 tzinfo=self.calendar.tz),
            end_date=datetime.datetime.combine(self.end, datetime.time.max,
                                               tzinfo=self.calendar.tz),
            trading_calendar=CALENDAR, emission_rate=datetime.timedelta(days=1),
            total_cash=1_000_000.0, market_data_source=self.bundle(), custom_data_sources=[],
            algorithm_file=str(FIXTURES / fixture), stop_on_error=True,
            asset_service=self.asset_service, benchmark_asset_symbol=None, benchmark_returns=None,
            equity_commission=NoCommission(), equity_slippage=NoSlippage(), max_leverage=10.0,
            print_algo=False)

    async def test_the_multiplier_is_applied_to_the_position(self):
        """The contract is worth 1000x its price. If the mechanism had touched accounting, the
        exposure would come out at the bare price instead."""
        result = await self.run_strategy("signals_futures.py")
        exposures = result.perf["ending_exposure"].to_numpy(dtype=float)
        held = [e for e in exposures if abs(e) > 0.0]
        self.assertTrue(held, "the strategy never held the contract")
        # Two contracts around $75 with a multiplier of 1000: order 150,000, not 150.
        self.assertGreater(max(abs(e) for e in held), 50_000.0)

    async def test_it_matches_the_bar_by_bar_version(self):
        vectorised = await self.run_strategy("signals_futures.py")
        bar_by_bar = await self.run_strategy("signals_futures_bar_by_bar.py")
        for column in ("portfolio_value", "ending_cash", "ending_exposure", "returns"):
            np.testing.assert_allclose(
                vectorised.perf[column].to_numpy(dtype=float),
                bar_by_bar.perf[column].to_numpy(dtype=float), rtol=0, atol=1e-9,
                err_msg=f"{column} differs on futures")

    async def test_cash_is_not_charged_the_notional(self):
        """Futures are margined: taking a position does not spend its notional value."""
        result = await self.run_strategy("signals_futures.py")
        cash = result.perf["ending_cash"].to_numpy(dtype=float)
        self.assertGreater(min(cash), 500_000.0,
                           "cash fell by something like the notional -- that is equity accounting")


class CausalityTests(unittest.TestCase):
    """Computations that read the future have to be refused, not run.

    Each of these is a real way people write indicators without noticing. They all produce a
    plausible-looking signal and an equity curve that cannot be achieved.
    """

    def setUp(self):
        index = pd.date_range("2024-01-01", periods=120, freq="B", tz="America/New_York")
        self.panel = make_panel(price_path(index))

    def check(self, compute):
        computed = normalise_signals(compute(self.panel), self.panel.index)
        verify_causality(compute, panel=self.panel, computed=computed, warmup=20)

    def test_a_rolling_mean_is_causal(self):
        self.check(lambda prices: {"sma": prices.close.rolling(20).mean()})

    def test_an_expanding_window_is_causal(self):
        self.check(lambda prices: {"mean": prices.close.expanding().mean()})

    def test_a_positive_shift_is_causal(self):
        self.check(lambda prices: {"yesterday": prices.close.shift(1)})

    def test_a_cross_of_two_averages_is_causal(self):
        self.check(lambda prices: {
            "long": prices.close.rolling(5).mean() > prices.close.rolling(20).mean()})

    def test_a_negative_shift_is_refused(self):
        """Tomorrow's close, read today. The purest form of the mistake."""
        with self.assertRaises(LookAheadError) as raised:
            self.check(lambda prices: {"tomorrow": prices.close.shift(-1)})
        self.assertIn("looks ahead", str(raised.exception))

    def test_a_full_sample_z_score_is_refused(self):
        """Normalising by statistics of the whole period, which nobody had at the time."""
        with self.assertRaises(LookAheadError):
            self.check(lambda prices: {
                "z": (prices.close - prices.close.mean()) / prices.close.std()})

    def test_dividing_by_the_final_price_is_refused(self):
        with self.assertRaises(LookAheadError):
            self.check(lambda prices: {"rebased": prices.close / prices.close.iloc[-1]})

    def test_a_full_period_rank_is_refused(self):
        with self.assertRaises(LookAheadError):
            self.check(lambda prices: {"rank": prices.close.rank(pct=True)})

    def test_the_message_names_the_bar_and_both_values(self):
        """A refusal that does not say where it disagreed sends the author reading the whole file."""
        with self.assertRaises(LookAheadError) as raised:
            self.check(lambda prices: {"peek": prices.close.shift(-1)})
        message = str(raised.exception)
        self.assertIn("'peek'", message)
        self.assertIn("bar", message)
        self.assertIn("shift", message, "the message lists the usual causes")

    def test_warmup_rows_are_not_compared(self):
        """A slow average is NaN early in both runs; that agreement is not interesting, and
        comparing before the window fills would reject honest indicators."""
        self.check(lambda prices: {"slow": prices.close.rolling(60).mean()})

    def test_a_signal_that_appears_only_on_long_history_is_refused(self):
        def flaky(prices):
            out = {"always": prices.close.rolling(5).mean()}
            if len(prices) > 100:
                out["sometimes"] = prices.close.rolling(5).mean()
            return out

        with self.assertRaises(LookAheadError) as raised:
            self.check(flaky)
        self.assertIn("every bar it is read at", str(raised.exception))


class NormalisationTests(unittest.TestCase):
    """What a strategy is allowed to return, and what it is told when it returns something else."""

    def setUp(self):
        index = pd.date_range("2024-01-01", periods=40, freq="B", tz="America/New_York")
        self.panel = make_panel(price_path(index))

    def test_a_bare_frame_is_accepted(self):
        frames = normalise_signals(self.panel.close > 0, self.panel.index)
        self.assertEqual(list(frames), ["signal"])

    def test_a_reindexed_signal_is_refused_with_the_reason(self):
        """dropna is the usual culprit: it silently shortens the frame, and every later lookup
        lands on the wrong bar."""
        broken = self.panel.close.rolling(10).mean().dropna()
        with self.assertRaises(ValueError) as raised:
            normalise_signals({"sma": broken}, self.panel.index)
        self.assertIn("indexed differently", str(raised.exception))
        self.assertIn("dropna", str(raised.exception))

    def test_a_numpy_array_is_refused(self):
        with self.assertRaises(TypeError) as raised:
            normalise_signals({"sma": np.arange(len(self.panel.index))}, self.panel.index)
        self.assertIn("pandas", str(raised.exception))

    def test_returning_nothing_is_refused(self):
        with self.assertRaises(ValueError):
            normalise_signals(None, self.panel.index)


class AccessTests(unittest.TestCase):
    """At bar t, row t + 1 is not protected -- it is unreachable."""

    def setUp(self):
        self.index = pd.date_range("2024-01-01", periods=30, freq="B", tz="America/New_York")
        frame = pd.DataFrame({"JNJ": np.arange(30.0), "KO": np.arange(30.0) * 2},
                             index=self.index)
        self.now = None
        self.panel = SignalPanel({"level": frame}, clock=lambda: self.now)

    def at(self, position: int):
        self.now = self.index[position].to_pydatetime()

    def test_the_current_bar_is_the_last_row_visible(self):
        self.at(10)
        self.assertEqual(float(self.panel["level"]["JNJ"]), 10.0)

    def test_history_stops_at_the_current_bar(self):
        self.at(10)
        window = self.panel.history("level", 5)
        self.assertEqual(len(window), 5)
        self.assertEqual(float(window["JNJ"].iloc[-1]), 10.0)
        self.assertEqual(float(window["JNJ"].iloc[0]), 6.0)

    def test_history_is_short_rather_than_padded_early_in_the_run(self):
        self.at(2)
        self.assertEqual(len(self.panel.history("level", 10)), 3)

    def test_no_accessor_returns_a_later_row(self):
        """The property the whole design rests on, asserted over every public reader."""
        self.at(5)
        today = float(self.panel["level"]["JNJ"])
        self.assertEqual(today, 5.0)
        self.assertLessEqual(float(self.panel.history("level", 30)["JNJ"].max()), today)
        for name in dir(self.panel):
            self.assertNotIn(name, {"frame", "frames", "all", "future"},
                             "an accessor handing back the whole frame would undo the clock")

    def test_reading_before_the_run_starts_says_so(self):
        self.now = None
        with self.assertRaises(RuntimeError) as raised:
            _ = self.panel["level"]
        self.assertIn("initialize", str(raised.exception))

    def test_an_unknown_signal_lists_the_known_ones(self):
        self.at(3)
        with self.assertRaises(KeyError) as raised:
            _ = self.panel["momentum"]
        self.assertIn("level", str(raised.exception))

    def test_an_unknown_column_lists_the_known_ones(self):
        self.at(3)
        with self.assertRaises(KeyError) as raised:
            _ = self.panel["level"]["MSFT"]
        self.assertIn("JNJ", str(raised.exception))

    def test_a_per_instrument_signal_refuses_a_bare_truth_test(self):
        """`if context.signals["long"]:` across two instruments has no single answer, and pandas
        would raise something about ambiguous truth values several frames away."""
        self.at(3)
        with self.assertRaises(ValueError) as raised:
            bool(self.panel["level"])
        self.assertIn("ambiguous", str(raised.exception))

    def test_warmup_is_reported_as_not_ready(self):
        frame = pd.DataFrame({"JNJ": [np.nan] * 5 + list(np.arange(25.0))}, index=self.index)
        panel = SignalPanel({"sma": frame}, clock=lambda: self.now)
        self.at(2)
        self.assertFalse(panel.is_ready("sma"))
        self.at(20)
        self.assertTrue(panel.is_ready("sma"))


class PanelTests(unittest.TestCase):
    """The price panel handed to a computation."""

    def setUp(self):
        index = pd.date_range("2024-01-01", periods=50, freq="B", tz="America/New_York")
        self.frame = price_path(index)

    def test_fields_come_off_the_bundle_columns(self):
        rows = pl.DataFrame([
            {"date": stamp, "sid": 1 if t == "JNJ" else 2, "close": p, "open": p,
             "high": p, "low": p, "volume": 1e6}
            for t in TICKERS for stamp, p in self.frame[t].items()])
        panel = PricePanel.from_bundle_rows(rows, {1: "JNJ", 2: "KO"})
        self.assertEqual(panel.columns, ["JNJ", "KO"])
        self.assertEqual(len(panel), len(self.frame))
        np.testing.assert_allclose(panel.close["JNJ"].to_numpy(),
                                   self.frame["JNJ"].to_numpy())

    def test_an_absent_field_names_what_is_there(self):
        panel = make_panel(self.frame)
        with self.assertRaises(AttributeError) as raised:
            _ = panel.vwap
        self.assertIn("close", str(raised.exception))

    def test_truncation_keeps_every_field(self):
        panel = make_panel(self.frame).truncated(10)
        self.assertEqual(len(panel), 10)
        self.assertEqual(sorted(panel.fields), ["close", "high", "low", "open", "volume"])


class EndToEndRefusalTests(SignalsTestCase):
    """A leaky strategy has to fail before it trades, not report an impossible return."""

    async def test_a_look_ahead_strategy_stops_the_run(self):
        with self.assertRaises(LookAheadError) as raised:
            await self.run_strategy("signals_look_ahead.py")
        self.assertIn("looks ahead", str(raised.exception))

    async def test_a_strategy_without_a_universe_is_told_what_to_set(self):
        with self.assertRaises(ValueError) as raised:
            await self.run_strategy("signals_no_universe.py")
        self.assertIn("context.universe", str(raised.exception))

    async def test_a_short_bundle_warns_rather_than_starting_mid_window(self):
        """An indicator that never fills its window yields NaN, a comparison against NaN is False,
        and the strategy quietly does not trade. The run says so instead."""
        import structlog.testing

        with structlog.testing.capture_logs() as logged:
            await self.run_strategy("signals_deep_warmup.py")
        warnings = [entry for entry in logged if entry.get("log_level") == "warning"
                    and "WARMUP" in entry.get("event", "")]
        self.assertTrue(warnings, f"nothing warned about the warm-up: {logged}")
        self.assertEqual(warnings[0]["warmup_requested"], 5_000)
        self.assertLess(warnings[0]["warmup_available"], 5_000,
                        "the warning has to say how much history there actually was")

    async def test_a_strategy_without_compute_signals_is_untouched(self):
        """The mechanism is opt-in; every existing strategy has to keep running as it did."""
        result = await self.run_strategy("buy_two_and_hold.py")
        self.assertGreater(len(result.perf), 100)
        self.assertEqual(result.errors, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
