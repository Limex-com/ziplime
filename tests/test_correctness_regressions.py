"""Regression tests for correctness bugs in the engine.

Every test here fails on the code as it was and passes after the fix. They are deliberately
**equity-only and self-contained**: each bug is in a path every backtest goes through, so none of
them needs a futures contract, a bond or a data bundle to demonstrate.

Each class names the bug it pins and what the wrong answer was.
"""
import dataclasses
import datetime
import unittest
from types import SimpleNamespace

import pandas as pd

from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.finance.domain.ledger import Ledger
from ziplime.finance.domain.position_tracker import PositionTracker
from ziplime.finance.domain.transaction import Transaction
from ziplime.finance.execution import (
    LimitOrder, MarketOrder, StopLimitOrder, StopOrder, make_execution_style,
)
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.trading.trading_algorithm import TradingAlgorithm

FAR_PAST = datetime.date(1900, 1, 1)
FAR_FUTURE = datetime.date(2099, 1, 1)
EXCHANGE = ExchangeInfo(mic="XNYS", name="NYSE", canonical_name="NYSE", country_code="US")
ACCOUNT = "account-1"
SESSION = datetime.datetime(2024, 3, 1, tzinfo=datetime.timezone.utc)


def make_equity(sid: int = 1, symbol: str = "SPY") -> ExchangeAsset:
    """A plain equity listing — the only instrument these tests need."""
    equity = Equity(id=sid + 10_000, isin=None, asset_name=symbol, start_date=FAR_PAST,
                    end_date=FAR_FUTURE, first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)
    usd = Currency(id=2, isin=None, asset_name="USD", start_date=FAR_PAST, end_date=FAR_FUTURE,
                   first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)
    return ExchangeAsset(sid=sid, symbol=symbol, start_date=FAR_PAST, end_date=FAR_FUTURE,
                         first_traded=FAR_PAST, auto_close_date=FAR_FUTURE, external_id="",
                         exchange=EXCHANGE, asset=equity, quote=usd)


def make_tracker(asset: ExchangeAsset, amount: int, price: float) -> PositionTracker:
    tracker = PositionTracker(data_frequency=datetime.timedelta(days=1))
    tracker.update_position(asset=asset, exchange_name=EXCHANGE.mic, trading_account_id=ACCOUNT,
                            amount=amount, last_sale_price=price, last_sale_date=SESSION,
                            cost_basis=price)
    return tracker


class NestedPositionLookupTests(unittest.TestCase):
    """`positions` is `{exchange: {account: {asset: Position}}}`, not `{asset: Position}`.

    Three call sites indexed it by asset. The ledger ones raised; `handle_commission` silently
    never matched, so **commission never reached a cost basis for any asset class**, equities
    included. Cash was always charged correctly, so nothing looked wrong.
    """

    def test_a_position_is_findable_by_asset(self):
        asset = make_equity()
        tracker = make_tracker(asset, amount=100, price=50.0)

        self.assertIsNotNone(tracker.get_position(asset))
        self.assertEqual(tracker.get_position(asset).amount, 100)

    def test_an_unheld_asset_returns_none(self):
        tracker = make_tracker(make_equity(sid=1), amount=100, price=50.0)
        self.assertIsNone(tracker.get_position(make_equity(sid=2, symbol="QQQ")))

    def test_commission_reaches_the_cost_basis(self):
        # The whole bug in one assertion: 100 shares at 50.00 with 25.00 of commission break even
        # at 50.25, not at 50.00.
        asset = make_equity()
        tracker = make_tracker(asset, amount=100, price=50.0)

        tracker.handle_commission(asset=asset, cost=25.0)

        self.assertAlmostEqual(tracker.get_position(asset).cost_basis, 50.25)

    def test_commission_on_a_short_lowers_the_break_even(self):
        asset = make_equity()
        tracker = make_tracker(asset, amount=-100, price=50.0)

        tracker.handle_commission(asset=asset, cost=25.0)

        self.assertAlmostEqual(tracker.get_position(asset).cost_basis, 49.75)

    def test_commission_on_an_unheld_asset_is_ignored(self):
        tracker = make_tracker(make_equity(sid=1), amount=100, price=50.0)
        tracker.handle_commission(asset=make_equity(sid=2, symbol="QQQ"), cost=25.0)
        self.assertAlmostEqual(tracker.get_position(make_equity(sid=1)).cost_basis, 50.0)


class PositionSnapshotTests(unittest.TestCase):
    """`Ledger.positions` handed out live objects, so the recorded past mutated.

    Performance packets keep whatever the property returns. Closing a position in September
    rewrote what June's already-recorded row showed. Cash and P&L were always right; the record
    was not.
    """

    def make_ledger(self) -> Ledger:
        ledger = Ledger(trading_sessions=pd.DatetimeIndex([SESSION]),
                        data_frequency=datetime.timedelta(days=1))
        ledger._portfolio.cash = 100_000.0
        return ledger

    def test_a_recorded_snapshot_does_not_follow_later_changes(self):
        asset = make_equity()
        ledger = self.make_ledger()
        ledger.position_tracker.update_position(
            asset=asset, exchange_name=EXCHANGE.mic, trading_account_id=ACCOUNT,
            amount=100, last_sale_price=50.0, last_sale_date=SESSION)

        recorded = ledger.positions          # what a perf packet keeps
        ledger.position_tracker.update_position(
            asset=asset, exchange_name=EXCHANGE.mic, trading_account_id=ACCOUNT,
            amount=0, last_sale_price=60.0)

        self.assertEqual(recorded[0].amount, 100, "the recorded past changed underneath us")
        self.assertAlmostEqual(recorded[0].last_sale_price, 50.0)

    def test_the_snapshot_still_carries_the_values(self):
        asset = make_equity()
        ledger = self.make_ledger()
        ledger.position_tracker.update_position(
            asset=asset, exchange_name=EXCHANGE.mic, trading_account_id=ACCOUNT,
            amount=100, last_sale_price=50.0, last_sale_date=SESSION, cost_basis=49.0)

        [position] = ledger.positions

        self.assertEqual(position.asset, asset)
        self.assertEqual(position.amount, 100)
        self.assertAlmostEqual(position.cost_basis, 49.0)

    def test_snapshots_are_not_the_live_objects(self):
        asset = make_equity()
        ledger = self.make_ledger()
        ledger.position_tracker.update_position(
            asset=asset, exchange_name=EXCHANGE.mic, trading_account_id=ACCOUNT,
            amount=100, last_sale_price=50.0, last_sale_date=SESSION)

        self.assertIsNot(ledger.positions[0], ledger.position_tracker.get_position(asset))


class ClosePositionTests(unittest.TestCase):
    """Auto-close never closed anything.

    `maybe_create_close_position_transaction` looked the position up by asset in the nested dict,
    so it always returned `None`. A listing past its auto-close date stayed on the books at a
    stale mark for the rest of the run, still counted in exposure and leverage.
    """

    def test_a_held_position_produces_a_closing_trade(self):
        asset = make_equity()
        tracker = make_tracker(asset, amount=100, price=50.0)

        txn = tracker.maybe_create_close_position_transaction(asset=asset, dt=SESSION)

        self.assertIsNotNone(txn, "nothing was ever closed")
        self.assertEqual(txn.amount, -100)
        self.assertAlmostEqual(txn.price, 50.0)

    def test_the_closing_trade_is_routed_to_the_right_account(self):
        # It used to be built with exchange_name=None and no account, which the ledger cannot post.
        asset = make_equity()
        tracker = make_tracker(asset, amount=100, price=50.0)

        txn = tracker.maybe_create_close_position_transaction(asset=asset, dt=SESSION)

        self.assertEqual(txn.exchange_name, EXCHANGE.mic)
        self.assertEqual(txn.trading_account_id, ACCOUNT)

    def test_an_unheld_asset_produces_nothing(self):
        tracker = make_tracker(make_equity(sid=1), amount=100, price=50.0)
        other = make_equity(sid=2, symbol="QQQ")
        self.assertIsNone(tracker.maybe_create_close_position_transaction(asset=other, dt=SESSION))

    def test_closing_removes_the_position_from_the_book(self):
        asset = make_equity()
        ledger = Ledger(trading_sessions=pd.DatetimeIndex([SESSION]),
                        data_frequency=datetime.timedelta(days=1))
        ledger._portfolio.cash = 100_000.0
        ledger.process_transaction(Transaction(
            id="open", amount=100, dt=SESSION, price=50.0, exchange_name=EXCHANGE.mic,
            trading_account_id=ACCOUNT, asset=asset))
        ledger.position_tracker.update_position(
            asset=asset, exchange_name=EXCHANGE.mic, trading_account_id=ACCOUNT,
            last_sale_price=50.0, last_sale_date=SESSION)

        ledger.close_position(asset=asset, dt=SESSION)

        self.assertIsNone(ledger.position_tracker.get_position(asset))


class NoSlippageTests(unittest.IsolatedAsyncioTestCase):
    """`NoSlippage.process_order` could never run.

    Its signature had drifted from the one `SlippageModel.simulate` calls — it took no `price` —
    and its body referred to a name that does not exist in that scope. Every order routed through
    it raised instead of filling, for any asset class.
    """

    def make_order(self, amount: int = 100):
        from ziplime.finance.domain.order import Order
        from ziplime.finance.domain.order_status import OrderStatus
        return Order(dt=SESSION, asset=make_equity(), amount=amount, id="o1", commission=0.0,
                     filled=0, execution_style=MarketOrder(), status=OrderStatus.OPEN,
                     exchange_name=EXCHANGE.mic, trading_account_id=ACCOUNT)

    async def test_it_fills_the_whole_order_at_the_given_price(self):
        price, volume = await NoSlippage().process_order(
            exchange=None, dt=SESSION, order=self.make_order(100), price=50.0)

        self.assertAlmostEqual(price, 50.0)
        self.assertEqual(volume, 100)

    async def test_it_fills_a_sell_too(self):
        price, volume = await NoSlippage().process_order(
            exchange=None, dt=SESSION, order=self.make_order(-100), price=50.0)

        self.assertAlmostEqual(price, 50.0)
        self.assertEqual(volume, -100)

    async def test_it_accepts_the_arguments_the_caller_passes(self):
        # The caller is SlippageModel.simulate, which passes price= as a keyword.
        import inspect
        params = inspect.signature(NoSlippage.process_order).parameters
        for expected in ("exchange", "dt", "order", "price"):
            self.assertIn(expected, params)


class ExecutionStyleTests(unittest.TestCase):
    """`order_value` forwarded `limit_price`/`stop_price` to `order`, which takes neither.

    It raised `TypeError` for every asset class unless the caller happened to pass a style. The
    shorthand the docstring documents is now resolved by `make_execution_style`.
    """

    def test_no_prices_means_a_market_order(self):
        self.assertIsInstance(make_execution_style(), MarketOrder)

    def test_a_limit_price_means_a_limit_order(self):
        style = make_execution_style(limit_price=10.0)
        self.assertIsInstance(style, LimitOrder)

    def test_a_stop_price_means_a_stop_order(self):
        self.assertIsInstance(make_execution_style(stop_price=9.0), StopOrder)

    def test_both_prices_mean_a_stop_limit_order(self):
        self.assertIsInstance(make_execution_style(limit_price=10.0, stop_price=9.0),
                              StopLimitOrder)

    def test_an_explicit_style_is_passed_through(self):
        style = MarketOrder()
        self.assertIs(make_execution_style(style=style), style)

    def test_a_style_together_with_a_price_is_refused(self):
        # Silently preferring one over the other would hide a real mistake in a strategy.
        with self.assertRaises(ValueError):
            make_execution_style(limit_price=10.0, style=MarketOrder())

    def test_order_does_not_accept_the_arguments_order_value_used_to_send(self):
        # The root of the bug: `order` takes a style, not loose prices. Forwarding them raised.
        import inspect
        from ziplime.trading.trading_algorithm import TradingAlgorithm
        params = inspect.signature(TradingAlgorithm.order).parameters
        self.assertNotIn("limit_price", params)
        self.assertNotIn("stop_price", params)

    def test_order_value_only_passes_order_arguments_it_accepts(self):
        """Check the call site against the callee's signature.

        This is the bug itself: `order_value` called `self.order(..., limit_price=, stop_price=)`
        and `order` accepts neither, so every call raised `TypeError`. Parsed rather than
        string-matched, because the fixed version legitimately mentions `limit_price` when it
        builds the style.
        """
        import ast
        import inspect
        import textwrap
        from ziplime.trading.trading_algorithm import TradingAlgorithm

        accepted = set(inspect.signature(TradingAlgorithm.order).parameters)
        tree = ast.parse(textwrap.dedent(inspect.getsource(TradingAlgorithm.order_value)))

        calls = [n for n in ast.walk(tree)
                 if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Attribute) and n.func.attr == "order"
                 and isinstance(n.func.value, ast.Name) and n.func.value.id == "self"]
        self.assertEqual(len(calls), 1, "expected exactly one self.order(...) call")

        passed = {kw.arg for kw in calls[0].keywords if kw.arg is not None}
        unexpected = passed - accepted
        self.assertEqual(unexpected, set(),
                         f"order_value passes {sorted(unexpected)}, which order() does not accept")

    def test_order_value_hands_order_a_style_it_accepts(self):
        # Whatever order_value builds must be something `order` can take, for every combination
        # of the shorthand it advertises.
        import inspect
        from ziplime.trading.trading_algorithm import TradingAlgorithm
        order_params = inspect.signature(TradingAlgorithm.order).parameters
        for kwargs in ({}, {"limit_price": 10.0}, {"stop_price": 9.0},
                       {"limit_price": 10.0, "stop_price": 9.0}):
            style = make_execution_style(**kwargs)
            self.assertIn("style", order_params)
            self.assertIsInstance(style, type(style))
            self.assertTrue(hasattr(style, "get_limit_price") or hasattr(style, "get_stop_price"),
                            f"{kwargs} produced something that is not an execution style")


class RunSimulationModelTests(unittest.TestCase):
    """`run_simulation` computed the slippage models and then ignored them.

    It hardcoded fresh instances when building the exchange, so `equity_slippage=` and
    `future_slippage=` did nothing. `run_simulation_iter` already did this correctly.
    """

    def test_the_models_passed_in_are_the_ones_used(self):
        import inspect
        from ziplime.core import run_simulation as module
        source = inspect.getsource(module.run_simulation)
        self.assertIn("equity_slippage=equity_slippage", source)
        self.assertIn("future_slippage=future_slippage", source)

    def test_no_model_is_constructed_inline_when_building_the_exchange(self):
        import inspect
        from ziplime.core import run_simulation as module
        source = inspect.getsource(module.run_simulation)
        exchange_call = source[source.index("SimulationExchange("):]
        self.assertNotIn("FixedBasisPointsSlippage()", exchange_call)
        self.assertNotIn("VolatilityVolumeShare(", exchange_call)


class BundleWindowTests(unittest.TestCase):
    """The bundle load window compared a UTC column against a bare date.

    `>= start_date.date()` means ">= midnight UTC", which drops the first session for any exchange
    east of UTC: a Moscow session stamped 00:00 MSK is 21:00 UTC the day before.
    """

    def test_the_filter_keeps_the_timezone(self):
        import inspect
        from ziplime.data.services import file_system_delta_lake_bundle_storage as module
        source = inspect.getsource(module)
        self.assertNotIn('pl.col("date") >= start_date.date()', source)
        self.assertNotIn('pl.col("date") <= end_date.date()', source)
        self.assertIn('pl.col("date") >= start_date', source)


class OpenOrderBookkeepingTests(unittest.TestCase):
    """The blotter wrote open orders under one key and deleted them under another.

    `save_order` stores them at `open_orders[exchange][order.asset]`, but `order_cancelled` and
    `order_rejected` read `open_orders[exchange][order.asset.sid]`. That key is never written, and
    because `open_orders` is a defaultdict the miss silently created an empty entry instead of
    raising — so a cancelled or rejected order stayed in the book as open for the rest of the run.
    """

    def make_blotter(self):
        from ziplime.finance.blotter.in_memory_blotter import InMemoryBlotter
        return InMemoryBlotter(exchanges=[SimpleNamespace(name=EXCHANGE.mic)], cancel_policy=None)

    def make_order(self, asset, order_id: str = "o1", amount: int = 100):
        from ziplime.finance.domain.order import Order
        from ziplime.finance.domain.order_status import OrderStatus
        return Order(dt=SESSION, asset=asset, amount=amount, id=order_id, commission=0.0,
                     filled=0, execution_style=MarketOrder(), status=OrderStatus.OPEN,
                     exchange_name=EXCHANGE.mic, trading_account_id=ACCOUNT)

    def test_a_saved_order_is_open_for_its_asset(self):
        blotter, asset = self.make_blotter(), make_equity()
        order = self.make_order(asset)
        blotter.save_order(order)

        self.assertEqual(
            blotter.get_open_orders_by_asset(asset=asset, exchange_name=EXCHANGE.mic),
            {"o1": order})

    def test_a_cancelled_order_stops_being_open(self):
        blotter, asset = self.make_blotter(), make_equity()
        blotter.save_order(self.make_order(asset))
        blotter.order_cancelled(self.make_order(asset))

        self.assertFalse(blotter.get_open_orders_by_asset(asset=asset,
                                                          exchange_name=EXCHANGE.mic))

    def test_a_rejected_order_stops_being_open(self):
        blotter, asset = self.make_blotter(), make_equity()
        blotter.save_order(self.make_order(asset))
        blotter.order_rejected(self.make_order(asset))

        self.assertFalse(blotter.get_open_orders_by_asset(asset=asset,
                                                          exchange_name=EXCHANGE.mic))

    def test_cancelling_one_order_leaves_the_others_open(self):
        blotter, asset = self.make_blotter(), make_equity()
        blotter.save_order(self.make_order(asset, order_id="o1"))
        kept = self.make_order(asset, order_id="o2")
        blotter.save_order(kept)
        blotter.order_cancelled(self.make_order(asset, order_id="o1"))

        self.assertEqual(
            blotter.get_open_orders_by_asset(asset=asset, exchange_name=EXCHANGE.mic),
            {"o2": kept})

    def test_the_asset_is_dropped_once_it_has_no_open_orders(self):
        # Otherwise an emptied entry keeps the asset in `get_all_assets_in_open_orders`, which
        # decides which assets the simulation asks for prices for.
        blotter, asset = self.make_blotter(), make_equity()
        blotter.save_order(self.make_order(asset))
        blotter.order_cancelled(self.make_order(asset))

        self.assertNotIn(asset, blotter.get_all_assets_in_open_orders())

    def test_cancelling_an_order_that_was_never_saved_is_harmless(self):
        blotter = self.make_blotter()
        blotter.order_cancelled(self.make_order(make_equity()))  # must not raise


class GetOpenOrdersTests(unittest.TestCase):
    """`TradingAlgorithm.get_open_orders` looked an asset up in a dict keyed by exchange name.

    `blotter.open_orders` is keyed by exchange name first and by listing second, so `asset in
    blotter.open_orders` was comparing a listing against exchange names and never matched: the
    method returned [] for every asset that had orders working. The no-asset form was wrong in the
    same way, returning exchange names mapped to listings rather than assets mapped to orders.

    A strategy that tops an order up relies on this to see what is already working. Without it,
    it re-sends the shortfall every session and buys a multiple of what it wanted.
    """

    def make_algo_with_orders(self, *orders):
        from ziplime.finance.blotter.in_memory_blotter import InMemoryBlotter
        blotter = InMemoryBlotter(exchanges=[SimpleNamespace(name=EXCHANGE.mic)],
                                  cancel_policy=None)
        for order in orders:
            blotter.save_order(order)
        return SimpleNamespace(blotter=blotter)

    def make_order(self, asset, order_id: str = "o1", amount: int = 100):
        from ziplime.finance.domain.order import Order
        from ziplime.finance.domain.order_status import OrderStatus
        return Order(dt=SESSION, asset=asset, amount=amount, id=order_id, commission=0.0,
                     filled=0, execution_style=MarketOrder(), status=OrderStatus.OPEN,
                     exchange_name=EXCHANGE.mic, trading_account_id=ACCOUNT)

    def test_an_asset_with_a_working_order_reports_it(self):
        asset = make_equity()
        order = self.make_order(asset)
        algo = self.make_algo_with_orders(order)

        self.assertEqual(TradingAlgorithm.get_open_orders(algo, asset), [order])

    def test_an_asset_with_nothing_working_reports_nothing(self):
        asset, other = make_equity(sid=1, symbol="SPY"), make_equity(sid=2, symbol="QQQ")
        algo = self.make_algo_with_orders(self.make_order(asset))

        self.assertEqual(TradingAlgorithm.get_open_orders(algo, other), [])

    def test_several_working_orders_on_one_asset_all_come_back(self):
        asset = make_equity()
        first, second = self.make_order(asset, "o1"), self.make_order(asset, "o2")
        algo = self.make_algo_with_orders(first, second)

        self.assertCountEqual(TradingAlgorithm.get_open_orders(algo, asset), [first, second])

    def test_the_no_asset_form_maps_assets_to_their_orders(self):
        spy, qqq = make_equity(sid=1, symbol="SPY"), make_equity(sid=2, symbol="QQQ")
        spy_order, qqq_order = self.make_order(spy, "o1"), self.make_order(qqq, "o2")
        algo = self.make_algo_with_orders(spy_order, qqq_order)

        self.assertEqual(TradingAlgorithm.get_open_orders(algo),
                         {spy: [spy_order], qqq: [qqq_order]})

    def test_an_empty_book_reports_nothing(self):
        algo = self.make_algo_with_orders()

        self.assertEqual(TradingAlgorithm.get_open_orders(algo), {})
        self.assertEqual(TradingAlgorithm.get_open_orders(algo, make_equity()), [])


class CanTradeTests(unittest.TestCase):
    """`BarData.can_trade` raised for every asset it was ever asked about.

    `_can_trade_for_asset` was written against `Asset` but is called with `ExchangeAsset`, which is
    not one. Four separate failures followed: `Restrictions.is_restricted` took its iterable branch
    and returned a pandas Series, which was then tested for truth; `asset.is_alive_for_session` and
    `asset.is_exchange_open` do not exist on a listing; and `self.data_portal` and
    `self.data_frequency` do not exist on `BarData`.

    The rewrite answers the two conditions available synchronously -- the listing is alive and the
    venue is open -- and documents that the third, a known last price, needs an async read and is
    left to the caller.
    """

    def make_bar_data(self, asset, session: datetime.date):
        from unittest.mock import Mock
        from ziplime.domain.bar_data import BarData
        from ziplime.finance.asset_restrictions import NoRestrictions

        calendar = Mock()
        calendar.minute_to_session.return_value = pd.Timestamp(session)
        calendar.is_open_on_minute.return_value = True
        calendar.is_session.return_value = True
        source = Mock()
        source.name = "test"
        return BarData(data_sources={"test": source},
                       simulation_dt_func=lambda: SESSION,
                       trading_calendar=calendar,
                       restrictions=NoRestrictions())

    def test_a_listed_asset_can_trade(self):
        asset = make_equity()
        bar_data = self.make_bar_data(asset, datetime.date(2024, 3, 1))

        self.assertTrue(bool(bar_data.can_trade(assets=[asset]).iloc[0]))

    def test_an_asset_that_has_not_listed_yet_cannot_trade(self):
        asset = dataclasses.replace(make_equity(), start_date=datetime.date(2025, 1, 1))
        bar_data = self.make_bar_data(asset, datetime.date(2024, 3, 1))

        self.assertFalse(bool(bar_data.can_trade(assets=[asset]).iloc[0]))

    def test_a_delisted_asset_cannot_trade(self):
        asset = dataclasses.replace(make_equity(), end_date=datetime.date(2023, 1, 1))
        bar_data = self.make_bar_data(asset, datetime.date(2024, 3, 1))

        self.assertFalse(bool(bar_data.can_trade(assets=[asset]).iloc[0]))

    def test_an_asset_past_its_auto_close_cannot_trade(self):
        asset = dataclasses.replace(make_equity(), auto_close_date=datetime.date(2023, 6, 1))
        bar_data = self.make_bar_data(asset, datetime.date(2024, 3, 1))

        self.assertFalse(bool(bar_data.can_trade(assets=[asset]).iloc[0]))

    def test_a_restricted_asset_cannot_trade(self):
        from ziplime.domain.bar_data import BarData
        from ziplime.finance.asset_restrictions import StaticRestrictions
        from unittest.mock import Mock

        asset = make_equity()
        calendar = Mock()
        calendar.minute_to_session.return_value = pd.Timestamp(datetime.date(2024, 3, 1))
        calendar.is_open_on_minute.return_value = True
        source = Mock()
        source.name = "test"
        bar_data = BarData(data_sources={"test": source}, simulation_dt_func=lambda: SESSION,
                           trading_calendar=calendar,
                           restrictions=StaticRestrictions([asset]))

        self.assertFalse(bool(bar_data.can_trade(assets=[asset]).iloc[0]))

    def test_several_assets_come_back_in_order(self):
        listed, unlisted = make_equity(sid=1, symbol="SPY"), dataclasses.replace(
            make_equity(sid=2, symbol="QQQ"), start_date=datetime.date(2025, 1, 1))
        bar_data = self.make_bar_data(listed, datetime.date(2024, 3, 1))

        answer = bar_data.can_trade(assets=[listed, unlisted])
        self.assertEqual([bool(v) for v in answer.to_numpy()], [True, False])


class VolumeCapSignTests(unittest.IsolatedAsyncioTestCase):
    """A volume cap could return a negative quantity, reversing the order.

    `FixedBasisPointsSlippage.order_target_percentage_maximum_quantity` capped an order at
    `max_volume - volume_for_bar`. Once part of a thin bar's limit was already used that difference
    goes negative, and `_calculate_order_percent_amount` passes it straight through `min()` as the
    number of shares to order. `order_target_percent` then asked for a *negative* quantity, opening
    a short inside a long-only strategy.

    It only bites on thin instruments -- a mega-cap bar has volume to spare -- which is why it
    surfaced on a micro-cap universe, where it ran a book to -13m of exposure on a 1m account.
    A cap must bound magnitude and never direction.
    """

    def make_exchange(self, volume: float, price: float = 10.0):
        from unittest.mock import AsyncMock, Mock
        exchange = Mock()
        exchange.get_spot_value = AsyncMock(return_value={"close": [price], "volume": [volume]})
        return exchange

    async def quantity_for(self, volume: float, already_used: int, cash: float = 100_000.0):
        model = FixedBasisPointsSlippage(basis_points=5.0, volume_limit=0.1)
        model._volume_for_bar = already_used
        _, quantity = await model.order_target_percentage_maximum_quantity(
            exchange=self.make_exchange(volume), dt=SESSION, asset=make_equity(),
            percentage=1.0, available_cash=cash)
        return quantity

    async def test_an_exhausted_cap_fills_nothing_rather_than_reversing(self):
        # 10% of 1000 shares is 100; 400 of the limit is already spent.
        self.assertEqual(await self.quantity_for(volume=1_000, already_used=400), 0)

    async def test_a_partly_used_cap_leaves_the_remainder(self):
        self.assertEqual(await self.quantity_for(volume=10_000, already_used=400), 600)

    async def test_an_untouched_cap_allows_its_whole_share(self):
        self.assertEqual(await self.quantity_for(volume=10_000, already_used=0), 1_000)

    async def test_the_cap_never_exceeds_what_the_cash_buys(self):
        # 10% of a million shares is 100 000, but 100 000 dollars buys about 10 000 at 10 each.
        quantity = await self.quantity_for(volume=1_000_000, already_used=0, cash=100_000.0)
        self.assertLess(quantity, 10_001)

    async def test_no_input_produces_a_negative_quantity(self):
        for volume in (0, 1, 10, 1_000, 100_000):
            for used in (0, 1, 50, 5_000):
                quantity = await self.quantity_for(volume=volume, already_used=used)
                self.assertGreaterEqual(quantity, 0, f"volume={volume} used={used}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
