"""Equities, bonds and futures in one portfolio.

Each class settles by rules the other two contradict:

============ ============================== ======================= ==========================
class        cash paid on opening           position value          exposure
============ ============================== ======================= ==========================
equity       ``price × amount``             ``price × amount``      same as value
bond         ``dirty price × amount``       dirty value             same as value
futures      **nothing** -- margin only     **zero**                ``price × multiplier × qty``
============ ============================== ======================= ==========================

A backtester that gets one of these right in isolation can still get it wrong when they share a
ledger -- by applying a futures payout to a bond, valuing an equity against a face value, or
letting one class's schedule reach another's positions. These tests hold all three at once and
check that each keeps its own rules.
"""
import datetime
import unittest

from cross_asset_fixtures import (
    EXCHANGE, SESSION, StubService, make_bond, make_equity, make_future, make_ledger, mark, settle,
    trade,
)

from ziplime.assets.domain.bond_event_type import BondEventType
from ziplime.assets.entities.bond_event import BondEvent
from ziplime.assets.entities.dividend_payout import DividendPayout
from ziplime.finance.margin import FixedRateFuturesMarginModel

EQUITY_PRICE = 300.0
BOND_QUOTE = 55.0          # percent of a 1000 nominal
FUTURE_PRICE = 100_000.0


def coupon(bond_listing, date=datetime.date(2025, 3, 3), value=40.0, record_lag=1):
    return BondEvent(
        asset=bond_listing.asset, event_type=BondEventType.COUPON, date=date, value=value,
        currency="RUB", record_date=date - datetime.timedelta(days=record_lag),
        period_start_date=date - datetime.timedelta(days=182), face_value=1000.0)


def dividend(equity_listing, ex_date=datetime.date(2025, 3, 3),
             pay_date=datetime.date(2025, 3, 4), amount=30.0):
    return DividendPayout(
        asset=equity_listing.asset, amount=amount, declared_date=ex_date,
        record_date=ex_date, ex_date=ex_date, pay_date=pay_date, currency=None)


class OneBookThreeClassesTests(unittest.IsolatedAsyncioTestCase):
    """The three coexist, and each pays for itself the way its own class does."""

    def setUp(self):
        self.equity = make_equity()
        self.bond = make_bond()
        self.future = make_future()

    async def test_all_three_can_be_held_at_once(self):
        ledger = make_ledger()

        trade(ledger, self.equity, amount=1_000, price=EQUITY_PRICE)
        trade(ledger, self.bond, amount=1_000, price=BOND_QUOTE)
        trade(ledger, self.future, amount=10, price=FUTURE_PRICE)
        await settle(ledger)

        held = {p.asset.symbol for p in ledger.position_tracker.get_position_list()}
        self.assertEqual(held, {"SBER", "OFZ", "SiM5"})

    async def test_each_class_pays_for_itself_by_its_own_rule(self):
        ledger = make_ledger(cash=10_000_000.0)
        # On a coupon date accrued interest is zero, so the bond's cost is its clean value.
        ledger.bond_book.add(self.bond.asset.id, [coupon(self.bond, date=SESSION)])

        trade(ledger, self.equity, amount=1_000, price=EQUITY_PRICE)
        after_equity = ledger.portfolio.cash
        trade(ledger, self.bond, amount=1_000, price=BOND_QUOTE)
        after_bond = ledger.portfolio.cash
        trade(ledger, self.future, amount=10, price=FUTURE_PRICE)
        after_future = ledger.portfolio.cash

        # An equity costs its quote per share...
        self.assertAlmostEqual(10_000_000.0 - after_equity, 1_000 * EQUITY_PRICE)
        # ...a bond costs a percentage of its nominal, ten times what its quote suggests...
        self.assertAlmostEqual(after_equity - after_bond, 1_000 * 550.0)
        # ...and a future costs nothing at all.
        self.assertAlmostEqual(after_future, after_bond)

    async def test_value_and_exposure_follow_the_class(self):
        ledger = make_ledger()
        ledger.bond_book.add(self.bond.asset.id, [coupon(self.bond, date=SESSION)])
        trade(ledger, self.equity, amount=1_000, price=EQUITY_PRICE)
        trade(ledger, self.bond, amount=1_000, price=BOND_QUOTE)
        trade(ledger, self.future, amount=10, price=FUTURE_PRICE)
        await settle(ledger)

        stats = ledger.position_tracker.stats
        # Value: equity + bond only -- a future has none.
        self.assertAlmostEqual(stats.net_value, 1_000 * EQUITY_PRICE + 1_000 * 550.0)
        # Exposure: the same two, plus the future's notional.
        self.assertAlmostEqual(
            stats.net_exposure,
            1_000 * EQUITY_PRICE + 1_000 * 550.0 + 10 * FUTURE_PRICE * 1.0)

    async def test_opening_all_three_at_fair_value_leaves_wealth_unchanged(self):
        ledger = make_ledger(cash=10_000_000.0)
        ledger.bond_book.add(self.bond.asset.id, [coupon(self.bond, date=SESSION)])

        trade(ledger, self.equity, amount=1_000, price=EQUITY_PRICE)
        trade(ledger, self.bond, amount=1_000, price=BOND_QUOTE)
        trade(ledger, self.future, amount=10, price=FUTURE_PRICE)
        await settle(ledger)

        self.assertAlmostEqual(ledger.portfolio.portfolio_value, 10_000_000.0)

    async def test_a_futures_move_settles_without_touching_the_others(self):
        ledger = make_ledger()
        ledger.bond_book.add(self.bond.asset.id, [coupon(self.bond, date=SESSION)])
        trade(ledger, self.equity, amount=1_000, price=EQUITY_PRICE)
        trade(ledger, self.bond, amount=1_000, price=BOND_QUOTE)
        trade(ledger, self.future, amount=10, price=FUTURE_PRICE)
        await settle(ledger)
        cash_before = ledger.portfolio.cash
        value_before = ledger.position_tracker.stats.net_value

        mark(ledger, self.future, FUTURE_PRICE + 1_000.0)
        await settle(ledger)

        # Variation margin is cash, and only the future's.
        self.assertAlmostEqual(ledger.portfolio.cash - cash_before, 10 * 1_000.0 * 1.0)
        self.assertAlmostEqual(ledger.position_tracker.stats.net_value, value_before)

    async def test_an_equity_move_does_not_settle_as_cash(self):
        ledger = make_ledger()
        trade(ledger, self.equity, amount=1_000, price=EQUITY_PRICE)
        trade(ledger, self.future, amount=10, price=FUTURE_PRICE)
        await settle(ledger)
        cash_before = ledger.portfolio.cash

        mark(ledger, self.equity, EQUITY_PRICE + 10.0)
        await settle(ledger)

        self.assertAlmostEqual(ledger.portfolio.cash, cash_before)
        self.assertAlmostEqual(ledger.position_tracker.stats.net_value,
                               1_000 * (EQUITY_PRICE + 10.0))

    async def test_a_bond_move_is_read_against_its_face_value(self):
        ledger = make_ledger()
        ledger.bond_book.add(self.bond.asset.id, [coupon(self.bond, date=SESSION)])
        trade(ledger, self.bond, amount=1_000, price=BOND_QUOTE)
        trade(ledger, self.equity, amount=1_000, price=EQUITY_PRICE)
        await settle(ledger)
        value_before = ledger.position_tracker.stats.net_value

        mark(ledger, self.bond, BOND_QUOTE + 1.0)
        await settle(ledger)

        # One point on a 1000 nominal is 10 roubles a bond, not one.
        self.assertAlmostEqual(
            ledger.position_tracker.stats.net_value - value_before, 1_000 * 10.0)


class SchedulesAcrossClassesTests(unittest.IsolatedAsyncioTestCase):
    """A schedule must reach its own class and no other."""

    def setUp(self):
        self.equity = make_equity()
        self.bond = make_bond()
        self.future = make_future()
        self.coupon = coupon(self.bond, date=datetime.date(2025, 3, 5))
        self.dividend = dividend(self.equity, ex_date=datetime.date(2025, 3, 5),
                                 pay_date=datetime.date(2025, 3, 6))
        self.service = StubService(bond_events={self.bond.asset.id: [self.coupon]},
                                   dividends=[self.dividend])

    async def hold_everything(self, ledger):
        ledger.bond_book.add(self.bond.asset.id, [self.coupon])
        trade(ledger, self.equity, amount=1_000, price=EQUITY_PRICE)
        trade(ledger, self.bond, amount=1_000, price=BOND_QUOTE)
        trade(ledger, self.future, amount=10, price=FUTURE_PRICE)
        await settle(ledger)

    async def test_a_coupon_is_paid_while_holding_shares_and_futures(self):
        ledger = make_ledger()
        await self.hold_everything(ledger)
        cash_before = ledger.portfolio.cash

        await ledger.process_bond_events(self.coupon.record_date, self.service)
        await ledger.process_bond_events(self.coupon.date, self.service)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_before, 1_000 * self.coupon.value)

    async def test_a_dividend_is_paid_while_holding_bonds_and_futures(self):
        ledger = make_ledger()
        await self.hold_everything(ledger)
        cash_before = ledger.portfolio.cash

        await ledger.process_dividends(self.dividend.ex_date, self.service)
        await ledger.process_dividends(self.dividend.pay_date, self.service)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_before,
                               1_000 * self.dividend.amount)

    async def test_a_coupon_does_not_reach_the_equity_or_the_future(self):
        # Only the bond is held; the coupon must still be the only thing that pays.
        ledger = make_ledger()
        ledger.bond_book.add(self.bond.asset.id, [self.coupon])
        trade(ledger, self.equity, amount=1_000, price=EQUITY_PRICE)
        trade(ledger, self.future, amount=10, price=FUTURE_PRICE)
        await settle(ledger)
        cash_before = ledger.portfolio.cash

        await ledger.process_bond_events(self.coupon.record_date, self.service)
        await ledger.process_bond_events(self.coupon.date, self.service)

        self.assertAlmostEqual(ledger.portfolio.cash, cash_before)

    async def test_both_schedules_pay_over_the_same_run(self):
        # Walked session by session, the way once_a_day does: entitlement dates are calendar
        # dates and a run that skips a session cannot see the one that falls on it.
        ledger = make_ledger()
        await self.hold_everything(ledger)
        cash_before = ledger.portfolio.cash

        session = SESSION
        while session <= datetime.date(2025, 3, 6):
            await ledger.process_dividends(session, self.service)
            await ledger.process_bond_events(session, self.service)
            session += datetime.timedelta(days=1)

        self.assertAlmostEqual(
            ledger.portfolio.cash - cash_before,
            1_000 * self.coupon.value + 1_000 * self.dividend.amount)

    async def test_a_dividend_and_a_coupon_falling_together_are_both_paid(self):
        # Same date for both, which is where a shared unpaid-payments ledger would collide.
        equity, bond = make_equity(sid=711), make_bond(sid=712)
        pay_date = datetime.date(2025, 3, 6)
        both_coupon = coupon(bond, date=pay_date, value=40.0)
        both_dividend = dividend(equity, ex_date=datetime.date(2025, 3, 5), pay_date=pay_date,
                                 amount=30.0)
        service = StubService(bond_events={bond.asset.id: [both_coupon]},
                              dividends=[both_dividend])
        ledger = make_ledger()
        ledger.bond_book.add(bond.asset.id, [both_coupon])
        trade(ledger, equity, amount=1_000, price=EQUITY_PRICE)
        trade(ledger, bond, amount=1_000, price=BOND_QUOTE)
        await settle(ledger)
        cash_before = ledger.portfolio.cash

        session = SESSION
        while session <= pay_date:
            await ledger.process_dividends(session, service)
            await ledger.process_bond_events(session, service)
            session += datetime.timedelta(days=1)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_before,
                               1_000 * 40.0 + 1_000 * 30.0)

    async def test_redemption_leaves_the_other_positions_alone(self):
        matured = make_bond(sid=704, symbol="OFZSHORT",
                            maturity=datetime.date(2025, 3, 4))
        service = StubService(bond_events={matured.asset.id: []})
        ledger = make_ledger()
        ledger.bond_book.add(matured.asset.id, [])
        trade(ledger, self.equity, amount=1_000, price=EQUITY_PRICE)
        trade(ledger, self.future, amount=10, price=FUTURE_PRICE)
        trade(ledger, matured, amount=1_000, price=99.0)
        await settle(ledger)

        await ledger.redeem_matured_bonds(datetime.date(2025, 3, 4), service)

        held = {p.asset.symbol for p in ledger.position_tracker.get_position_list()}
        self.assertEqual(held, {"SBER", "SiM5"}, "redemption disturbed another class")


class MarginAcrossClassesTests(unittest.IsolatedAsyncioTestCase):
    """Margin is a futures concept and must not be charged to the other classes."""

    async def test_only_the_futures_leg_ties_up_margin(self):
        ledger = make_ledger(
            futures_margin_model=FixedRateFuturesMarginModel(initial_rate=0.15,
                                                             maintenance_rate=0.12))
        equity, bond, future = make_equity(), make_bond(), make_future()
        ledger.bond_book.add(bond.asset.id, [coupon(bond, date=SESSION)])
        trade(ledger, equity, amount=1_000, price=EQUITY_PRICE)
        trade(ledger, bond, amount=1_000, price=BOND_QUOTE)
        trade(ledger, future, amount=10, price=FUTURE_PRICE)
        await settle(ledger)

        margin = ledger.futures_margin_by_currency()

        self.assertEqual(set(margin), {"USD"})
        self.assertAlmostEqual(margin["USD"], 10 * FUTURE_PRICE * 1.0 * 0.15)


class SymbolAmbiguityTests(unittest.IsolatedAsyncioTestCase):
    """A ticker is unique only within an asset class.

    A database carrying more than one asset class will contain tickers that exist under two of
    them -- an equity vendor listing a futures ticker as an equity is the usual way it happens.
    Resolving such a ticker by name alone returns whichever class is looked up first, and the
    failure is silent: bars get tagged with the other instrument's sid, and a strategy then finds
    no prices for what it is holding.

    The clash is constructed here rather than relied upon in the shipped database, so the test
    states the condition it needs instead of depending on which instruments happen to be seeded.
    """

    async def asyncSetUp(self):
        import shutil
        import tempfile
        from pathlib import Path

        from ziplime.assets.entities.equity import Equity
        from ziplime.assets.entities.exchange_asset import ExchangeAsset
        from ziplime.core.ingest_data import get_asset_service

        project_root = Path(__file__).resolve().parents[1]
        self.temp_dir = tempfile.TemporaryDirectory(prefix="ziplime-ambiguity-")
        db_path = Path(self.temp_dir.name) / "assets.sqlite"
        shutil.copy2(project_root / "data" / "assets.sqlite", db_path)
        self.asset_service = get_asset_service(db_path=str(db_path))

        # One ticker, two asset classes, same venue.
        self.ticker = "CLASH1"
        exchange = EXCHANGE
        await self.asset_service.save_exchanges(exchanges=[exchange])

        far_past, far_future = datetime.date(1900, 1, 1), datetime.date(2099, 1, 1)
        equity = Equity(id=None, isin="CLASHEQUITY1", asset_name=self.ticker,
                        start_date=far_past, end_date=far_future,
                        first_traded=far_past, auto_close_date=far_future)
        [stored_equity] = await self.asset_service.save_equities([equity])

        from ziplime.assets.entities.bond import Bond
        # id=None so the database assigns one; a hard-coded id collides with the seeded rows.
        bond = Bond(id=None, isin="CLASHBOND1", asset_name=self.ticker,
                    start_date=far_past, end_date=datetime.date(2030, 1, 1),
                    first_traded=far_past, auto_close_date=datetime.date(2030, 1, 2),
                    face_value=1000.0, maturity_date=datetime.date(2030, 1, 2))
        [stored_bond] = await self.asset_service.save_bonds([bond])

        quote = stored_equity  # any stored asset works as the quote for this test
        await self.asset_service.save_exchange_assets(exchange_assets=[
            ExchangeAsset(sid=None, symbol=self.ticker, start_date=far_past, end_date=far_future,
                          first_traded=far_past, auto_close_date=far_future, external_id="eq",
                          exchange=exchange, asset=stored_equity, quote=quote),
            ExchangeAsset(sid=None, symbol=self.ticker, start_date=far_past, end_date=far_future,
                          first_traded=far_past, auto_close_date=far_future, external_id="bd",
                          exchange=exchange, asset=stored_bond, quote=quote),
        ])

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self.temp_dir.cleanup()

    async def test_a_symbol_in_two_classes_is_refused_rather_than_guessed(self):
        from ziplime.assets.domain.asset_type import AssetType
        from ziplime.assets.entities.asset_symbol import AssetSymbol
        from ziplime.errors import AmbiguousSymbol

        with self.assertRaises(AmbiguousSymbol):
            await self.asset_service.get_exchange_assets_by_symbols(
                symbols=[AssetSymbol(symbol=self.ticker, mic=EXCHANGE.mic)],
                asset_type=[AssetType.EQUITY, AssetType.BOND])

    async def test_a_single_asset_type_is_never_ambiguous(self):
        from ziplime.assets.domain.asset_type import AssetType
        from ziplime.assets.entities.asset_symbol import AssetSymbol

        [listing] = await self.asset_service.get_exchange_assets_by_symbols(
            symbols=[AssetSymbol(symbol=self.ticker, mic=EXCHANGE.mic)],
            asset_type=AssetType.BOND)

        self.assertIsNotNone(listing)
        self.assertEqual(type(listing.asset).__name__, "Bond")

    async def test_the_unambiguous_class_is_the_one_asked_for(self):
        from ziplime.assets.domain.asset_type import AssetType
        from ziplime.assets.entities.asset_symbol import AssetSymbol

        [listing] = await self.asset_service.get_exchange_assets_by_symbols(
            symbols=[AssetSymbol(symbol=self.ticker, mic=EXCHANGE.mic)],
            asset_type=AssetType.EQUITY)

        self.assertEqual(type(listing.asset).__name__, "Equity")


if __name__ == "__main__":
    unittest.main(verbosity=2)
