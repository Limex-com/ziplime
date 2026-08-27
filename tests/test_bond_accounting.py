"""Bond accounting in the ledger: what a trade costs, and what the schedule pays.

Organised around the cases a bond backtest has to get right:

* buying and selling -- the money that moves is the dirty price, not the quote;
* a bond that pays coupons, and one that does not;
* amortization, which repays principal early and shrinks the nominal;
* redemption at maturity, which is not a trade at whatever the last bar printed;
* short positions, which owe the coupon rather than receiving it.
"""
import datetime
import unittest

from bond_fixtures import (
    StubBondService, load_schedule, make_amortization_events, make_bond, make_coupon_events,
    make_ledger, make_zero_coupon_bond, mark, settle, trade,
)

from ziplime.assets.domain.bond_event_type import BondEventType
from ziplime.assets.domain.price_quotation import PriceQuotation
from ziplime.assets.entities.bond_event import BondEvent
from ziplime.finance.commission import PerBondTurnover


class PurchaseCashFlowTests(unittest.IsolatedAsyncioTestCase):
    """What leaves the account when a bond is bought."""

    async def test_buying_pays_the_face_value_not_the_quote(self):
        # 10 bonds at 100% of a 1000 nominal is 10 000 roubles. Reading the quote as money would
        # take 1000 -- a factor of ten, and the single most consequential bond bug there is.
        ledger = make_ledger(cash=1_000_000.0)
        listing = make_bond()
        # A coupon date, so accrued interest is zero and the clean price is the whole cost.
        coupon_date = make_coupon_events(listing)[2].date
        load_schedule(ledger, listing)

        trade(ledger, listing, amount=10, price=100.0, dt=coupon_date)

        self.assertAlmostEqual(ledger.portfolio.cash, 1_000_000.0 - 10_000.0)

    async def test_buying_below_par_costs_proportionally_less(self):
        ledger = make_ledger(cash=1_000_000.0)
        listing = make_bond()
        coupon_date = make_coupon_events(listing)[2].date
        load_schedule(ledger, listing)

        trade(ledger, listing, amount=10, price=98.5, dt=coupon_date)

        self.assertAlmostEqual(ledger.portfolio.cash, 1_000_000.0 - 9_850.0)

    async def test_buying_between_coupons_also_pays_accrued_interest(self):
        ledger = make_ledger(cash=1_000_000.0)
        listing = make_bond()
        events = make_coupon_events(listing)
        load_schedule(ledger, listing, events)
        coupon = events[3]
        dt = coupon.period_start_date + datetime.timedelta(days=30)
        accrued = ledger.bond_book.accrued_interest(listing.asset, dt)

        trade(ledger, listing, amount=10, price=100.0, dt=dt)

        self.assertGreater(accrued, 0.0)
        self.assertAlmostEqual(ledger.portfolio.cash, 1_000_000.0 - 10 * (1000.0 + accrued))

    async def test_selling_receives_the_accrued_interest(self):
        # The seller is compensated for the coupon that accrued while they held the bond. A round
        # trip at an unchanged clean price is therefore not a wash -- it earns the accrual.
        ledger = make_ledger(cash=1_000_000.0)
        listing = make_bond()
        events = make_coupon_events(listing)
        load_schedule(ledger, listing, events)
        coupon = events[3]
        bought_on = coupon.period_start_date
        sold_on = coupon.period_start_date + datetime.timedelta(days=60)

        trade(ledger, listing, amount=10, price=100.0, dt=bought_on)
        cash_after_buy = ledger.portfolio.cash
        trade(ledger, listing, amount=-10, price=100.0, dt=sold_on)

        accrued = ledger.bond_book.accrued_interest(listing.asset, sold_on)
        self.assertGreater(accrued, 0.0)
        self.assertAlmostEqual(ledger.portfolio.cash - cash_after_buy, 10 * (1000.0 + accrued))

    async def test_a_money_quoted_bond_is_not_rescaled(self):
        ledger = make_ledger(cash=1_000_000.0)
        listing = make_bond(sid=511, symbol="MONEYBOND",
                            price_quotation=PriceQuotation.MONEY)
        coupon_date = make_coupon_events(listing)[2].date
        load_schedule(ledger, listing)

        trade(ledger, listing, amount=10, price=985.0, dt=coupon_date)

        self.assertAlmostEqual(ledger.portfolio.cash, 1_000_000.0 - 9_850.0)


class PositionValueTests(unittest.IsolatedAsyncioTestCase):
    """What a bond position is worth on the books."""

    async def test_position_value_uses_the_face_value(self):
        ledger = make_ledger(cash=1_000_000.0)
        listing = make_bond()
        coupon_date = make_coupon_events(listing)[2].date
        load_schedule(ledger, listing)

        trade(ledger, listing, amount=10, price=100.0, dt=coupon_date)
        await settle(ledger)

        self.assertAlmostEqual(ledger.position_tracker.stats.net_value, 10_000.0)

    async def test_portfolio_value_survives_a_purchase_at_par(self):
        # Buying at par converts cash into an equally valuable position; the total must not move.
        ledger = make_ledger(cash=1_000_000.0)
        listing = make_bond()
        coupon_date = make_coupon_events(listing)[2].date
        load_schedule(ledger, listing)

        trade(ledger, listing, amount=10, price=100.0, dt=coupon_date)
        await settle(ledger)

        self.assertAlmostEqual(ledger.portfolio.portfolio_value, 1_000_000.0)

    async def test_a_price_move_shows_up_in_the_position_value(self):
        ledger = make_ledger(cash=1_000_000.0)
        listing = make_bond()
        coupon_date = make_coupon_events(listing)[2].date
        load_schedule(ledger, listing)
        trade(ledger, listing, amount=10, price=100.0, dt=coupon_date)
        await settle(ledger)

        mark(ledger, listing, 101.0, dt=coupon_date)
        await settle(ledger)

        # One point on ten 1000-nominal bonds is 100 roubles.
        self.assertAlmostEqual(ledger.position_tracker.stats.net_value, 10_100.0)

    async def test_a_short_position_has_negative_value(self):
        ledger = make_ledger(cash=1_000_000.0)
        listing = make_bond()
        coupon_date = make_coupon_events(listing)[2].date
        load_schedule(ledger, listing)

        trade(ledger, listing, amount=-10, price=100.0, dt=coupon_date)
        await settle(ledger)

        self.assertAlmostEqual(ledger.position_tracker.stats.net_value, -10_000.0)


class CouponPaymentTests(unittest.IsolatedAsyncioTestCase):
    """The paying case: a coupon-bearing bond credits cash on every payment date."""

    def setUp(self):
        self.listing = make_bond(coupon_rate=0.08, coupon_frequency=2)
        self.events = make_coupon_events(self.listing)
        self.service = StubBondService({self.listing.asset.id: self.events})
        self.coupon = self.events[3]

    async def test_a_held_bond_is_paid_its_coupon(self):
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, self.listing, self.events)
        trade(ledger, self.listing, amount=10, price=100.0,
              dt=self.coupon.period_start_date)
        cash_before = ledger.portfolio.cash

        await ledger.process_bond_events(self.coupon.record_date, self.service)
        await ledger.process_bond_events(self.coupon.date, self.service)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_before, 10 * self.coupon.value)

    async def test_a_bond_that_pays_nothing_credits_nothing(self):
        # The control case. Same trade, same dates, a zero-coupon bond: cash must not move.
        listing = make_zero_coupon_bond()
        service = StubBondService({listing.asset.id: []})
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, listing, [])
        trade(ledger, listing, amount=10, price=92.0, dt=self.coupon.period_start_date)
        cash_before = ledger.portfolio.cash

        await ledger.process_bond_events(self.coupon.record_date, service)
        await ledger.process_bond_events(self.coupon.date, service)

        self.assertAlmostEqual(ledger.portfolio.cash, cash_before)

    async def test_holding_nothing_earns_nothing(self):
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, self.listing, self.events)

        await ledger.process_bond_events(self.coupon.record_date, self.service)
        await ledger.process_bond_events(self.coupon.date, self.service)

        self.assertAlmostEqual(ledger.portfolio.cash, 1_000_000.0)

    async def test_selling_after_the_record_date_keeps_the_coupon(self):
        # That is what a record date is: entitlement is fixed on it, payment follows later.
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, self.listing, self.events)
        trade(ledger, self.listing, amount=10, price=100.0, dt=self.coupon.period_start_date)

        await ledger.process_bond_events(self.coupon.record_date, self.service)
        trade(ledger, self.listing, amount=-10, price=100.0, dt=self.coupon.record_date)
        cash_before = ledger.portfolio.cash
        await ledger.process_bond_events(self.coupon.date, self.service)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_before, 10 * self.coupon.value)

    async def test_buying_after_the_record_date_misses_the_coupon(self):
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, self.listing, self.events)

        await ledger.process_bond_events(self.coupon.record_date, self.service)
        trade(ledger, self.listing, amount=10, price=100.0, dt=self.coupon.record_date)
        cash_before = ledger.portfolio.cash
        await ledger.process_bond_events(self.coupon.date, self.service)

        self.assertAlmostEqual(ledger.portfolio.cash, cash_before)

    async def test_a_short_position_owes_the_coupon(self):
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, self.listing, self.events)
        trade(ledger, self.listing, amount=-10, price=100.0,
              dt=self.coupon.period_start_date)
        cash_before = ledger.portfolio.cash

        await ledger.process_bond_events(self.coupon.record_date, self.service)
        await ledger.process_bond_events(self.coupon.date, self.service)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_before, -10 * self.coupon.value)

    async def test_a_coupon_is_paid_once(self):
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, self.listing, self.events)
        trade(ledger, self.listing, amount=10, price=100.0,
              dt=self.coupon.period_start_date)
        cash_before = ledger.portfolio.cash

        await ledger.process_bond_events(self.coupon.record_date, self.service)
        await ledger.process_bond_events(self.coupon.date, self.service)
        await ledger.process_bond_events(self.coupon.date, self.service)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_before, 10 * self.coupon.value)

    async def test_positions_across_accounts_are_paid_in_full(self):
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, self.listing, self.events)
        trade(ledger, self.listing, amount=10, price=100.0,
              dt=self.coupon.period_start_date)
        ledger.position_tracker.update_position(
            asset=self.listing, exchange_name="MOEX", trading_account_id="account-2",
            amount=20, last_sale_price=100.0)
        cash_before = ledger.portfolio.cash

        await ledger.process_bond_events(self.coupon.record_date, self.service)
        await ledger.process_bond_events(self.coupon.date, self.service)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_before, 30 * self.coupon.value)

    async def test_every_coupon_over_a_year_is_paid(self):
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, self.listing, self.events)
        trade(ledger, self.listing, amount=10, price=100.0, dt=self.events[0].date)
        cash_before = ledger.portfolio.cash

        year = [event for event in self.events
                if self.events[0].date < event.date <= self.events[2].date]
        for event in year:
            await ledger.process_bond_events(event.record_date, self.service)
            await ledger.process_bond_events(event.date, self.service)

        # Two semi-annual coupons on ten bonds: a year of the nominal 8% on 10 000 of face value.
        self.assertEqual(len(year), 2)
        self.assertAlmostEqual(ledger.portfolio.cash - cash_before, 800.0)


class AmortizationPaymentTests(unittest.IsolatedAsyncioTestCase):
    """Amortization repays principal early: cash in, and a smaller nominal afterwards."""

    def setUp(self):
        # MOEX schedules amortization on coupon dates, so the fixture does too: on 2024-01-10 this
        # bond pays both a coupon and an instalment, which is the case worth defaulting to.
        self.listing = make_bond(sid=520, symbol="AMORTBOND", is_amortized=True)
        self.instalment_date = datetime.date(2024, 1, 10)
        self.coupon_amount = 40.0
        self.events = make_coupon_events(self.listing) + make_amortization_events(
            self.listing, [(self.instalment_date, 300.0)])
        self.service = StubBondService({self.listing.asset.id: self.events})

    async def pay_through(self, ledger, date):
        """Run the record date and the payment date, as a simulation session pair would."""
        await ledger.process_bond_events(date - datetime.timedelta(days=1), self.service)
        await ledger.process_bond_events(date, self.service)

    async def test_an_instalment_credits_cash_alongside_the_coupon(self):
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, self.listing, self.events)
        trade(ledger, self.listing, amount=10, price=100.0, dt=datetime.date(2023, 7, 10))
        cash_before = ledger.portfolio.cash

        await self.pay_through(ledger, self.instalment_date)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_before,
                               10 * (300.0 + self.coupon_amount))

    async def test_an_instalment_off_the_coupon_grid_pays_only_principal(self):
        # Isolating the instalment: an issue whose amortization does not fall on a coupon date
        # must credit exactly the principal repaid, and nothing else.
        listing = make_bond(sid=522, symbol="AMORTOFFGRID", is_amortized=True)
        instalment_date = datetime.date(2024, 4, 10)
        events = make_coupon_events(listing) + make_amortization_events(
            listing, [(instalment_date, 300.0)])
        service = StubBondService({listing.asset.id: events})
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, listing, events)
        trade(ledger, listing, amount=10, price=100.0, dt=datetime.date(2024, 1, 10))
        cash_before = ledger.portfolio.cash

        await ledger.process_bond_events(instalment_date - datetime.timedelta(days=1), service)
        await ledger.process_bond_events(instalment_date, service)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_before, 10 * 300.0)

    async def test_the_position_is_worth_less_after_the_instalment(self):
        # Principal came back as cash, so the paper left is worth less.
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, self.listing, self.events)
        trade(ledger, self.listing, amount=10, price=100.0, dt=datetime.date(2023, 7, 10))
        mark(ledger, self.listing, 100.0, dt=datetime.date(2023, 7, 10))
        await settle(ledger)
        value_before = ledger.position_tracker.stats.net_value

        await self.pay_through(ledger, self.instalment_date)
        mark(ledger, self.listing, 100.0, dt=self.instalment_date)
        await settle(ledger)

        self.assertAlmostEqual(value_before, 10_000.0)
        self.assertAlmostEqual(ledger.position_tracker.stats.net_value, 7_000.0)

    async def test_amortization_moves_wealth_from_the_position_into_cash(self):
        # The instalment itself creates no wealth: it converts principal into cash. The only real
        # gain over the interval is the coupon that was paid with it.
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, self.listing, self.events)
        trade(ledger, self.listing, amount=10, price=100.0, dt=datetime.date(2023, 7, 10))
        mark(ledger, self.listing, 100.0, dt=datetime.date(2023, 7, 10))
        await settle(ledger)
        wealth_before = ledger.portfolio.portfolio_value

        await self.pay_through(ledger, self.instalment_date)
        mark(ledger, self.listing, 100.0, dt=self.instalment_date)
        await settle(ledger)

        self.assertAlmostEqual(ledger.portfolio.portfolio_value - wealth_before,
                               10 * self.coupon_amount)


class RedemptionTests(unittest.IsolatedAsyncioTestCase):
    """Maturity: the issuer repays the outstanding principal, whatever the last quote was."""

    async def test_a_matured_bond_is_repaid_at_par(self):
        listing = make_bond()
        service = StubBondService({listing.asset.id: make_coupon_events(listing)})
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, listing)
        trade(ledger, listing, amount=10, price=98.0, dt=datetime.date(2025, 6, 10))
        cash_before = ledger.portfolio.cash

        await ledger.redeem_matured_bonds(listing.asset.maturity_date, service)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_before, 10_000.0)

    async def test_redemption_closes_the_position(self):
        listing = make_bond()
        service = StubBondService({listing.asset.id: make_coupon_events(listing)})
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, listing)
        trade(ledger, listing, amount=10, price=98.0, dt=datetime.date(2025, 6, 10))

        await ledger.redeem_matured_bonds(listing.asset.maturity_date, service)

        self.assertIsNone(ledger.position_tracker.get_position(listing))

    async def test_redemption_ignores_a_stale_quote(self):
        # A bond that last printed 95 still repays 100% of face. Closing it at the last bar --
        # what the auto-close path would do -- would book a loss that never happened.
        listing = make_bond()
        service = StubBondService({listing.asset.id: make_coupon_events(listing)})
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, listing)
        trade(ledger, listing, amount=10, price=98.0, dt=datetime.date(2025, 6, 10))
        mark(ledger, listing, 95.0, dt=datetime.date(2025, 12, 20))
        cash_before = ledger.portfolio.cash

        await ledger.redeem_matured_bonds(listing.asset.maturity_date, service)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_before, 10_000.0)

    async def test_an_amortized_bond_repays_only_what_is_left(self):
        listing = make_bond(sid=521, symbol="AMORTBOND2", is_amortized=True)
        events = make_coupon_events(listing) + make_amortization_events(
            listing, [(datetime.date(2024, 1, 10), 300.0), (datetime.date(2025, 1, 10), 300.0)])
        service = StubBondService({listing.asset.id: events})
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, listing, events)
        trade(ledger, listing, amount=10, price=100.0, dt=datetime.date(2025, 6, 10))
        cash_before = ledger.portfolio.cash

        await ledger.redeem_matured_bonds(listing.asset.maturity_date, service)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_before, 10 * 400.0)

    async def test_a_bond_is_never_redeemed_twice(self):
        listing = make_bond()
        service = StubBondService({listing.asset.id: make_coupon_events(listing)})
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, listing)
        trade(ledger, listing, amount=10, price=98.0, dt=datetime.date(2025, 6, 10))
        cash_before = ledger.portfolio.cash

        await ledger.redeem_matured_bonds(listing.asset.maturity_date, service)
        await ledger.redeem_matured_bonds(
            listing.asset.maturity_date + datetime.timedelta(days=1), service)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_before, 10_000.0)

    async def test_a_bond_still_alive_is_not_redeemed(self):
        listing = make_bond()
        service = StubBondService({listing.asset.id: make_coupon_events(listing)})
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, listing)
        trade(ledger, listing, amount=10, price=98.0, dt=datetime.date(2025, 6, 10))
        cash_before = ledger.portfolio.cash

        await ledger.redeem_matured_bonds(datetime.date(2025, 12, 31), service)

        self.assertAlmostEqual(ledger.portfolio.cash, cash_before)
        self.assertIsNotNone(ledger.position_tracker.get_position(listing))

    async def test_a_zero_coupon_bond_returns_the_discount_at_maturity(self):
        # The whole return of a discount bond: bought at 92, repaid at 100.
        listing = make_zero_coupon_bond()
        service = StubBondService({listing.asset.id: []})
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, listing, [])
        trade(ledger, listing, amount=10, price=92.0, dt=datetime.date(2025, 1, 10))

        await ledger.redeem_matured_bonds(listing.asset.maturity_date, service)

        self.assertAlmostEqual(ledger.portfolio.cash, 1_000_000.0 + 10 * 80.0)


class RedemptionEventTests(unittest.IsolatedAsyncioTestCase):
    """A schedule that carries an explicit redemption must not repay the principal twice.

    Real vendor calendars end in a principal repayment -- Finam sends it as a final amortization,
    which the connector marks as MATURITY. The ledger repays the outstanding principal on that
    date, so the event itself must not also credit cash.
    """

    def make_schedule(self, listing, final_value: float):
        events = make_coupon_events(listing)
        return events + [BondEvent(
            asset=listing.asset, event_type=BondEventType.MATURITY,
            date=listing.asset.maturity_date, value=final_value, currency="RUB",
            record_date=listing.asset.maturity_date - datetime.timedelta(days=1))]

    async def test_a_redemption_event_does_not_pay_on_top_of_the_repayment(self):
        listing = make_bond()
        events = self.make_schedule(listing, final_value=1000.0)
        service = StubBondService({listing.asset.id: events})
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, listing, events)
        trade(ledger, listing, amount=10, price=98.0, dt=datetime.date(2025, 6, 10))
        cash_before = ledger.portfolio.cash

        # The whole maturity-day sequence, in the order a session runs it.
        await ledger.process_bond_events(listing.asset.maturity_date, service)
        await ledger.redeem_matured_bonds(listing.asset.maturity_date, service)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_before, 10_000.0,
                               msg="the principal was counted twice")

    async def test_an_amortizing_issue_repays_its_remainder_once(self):
        listing = make_bond(sid=530, symbol="AMORTFINAL", is_amortized=True)
        events = (make_coupon_events(listing)
                  + make_amortization_events(listing, [(datetime.date(2024, 1, 10), 600.0)])
                  + [BondEvent(asset=listing.asset, event_type=BondEventType.MATURITY,
                               date=listing.asset.maturity_date, value=400.0, currency="RUB")])
        service = StubBondService({listing.asset.id: events})
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, listing, events)
        trade(ledger, listing, amount=10, price=100.0, dt=datetime.date(2025, 6, 10))
        cash_before = ledger.portfolio.cash

        await ledger.process_bond_events(listing.asset.maturity_date, service)
        await ledger.redeem_matured_bonds(listing.asset.maturity_date, service)

        # 1000 nominal less the 600 already repaid leaves 400 per bond.
        self.assertAlmostEqual(ledger.portfolio.cash - cash_before, 4_000.0)

    async def test_the_final_coupon_is_still_paid_at_maturity(self):
        listing = make_bond()
        events = self.make_schedule(listing, final_value=1000.0)
        final_coupon = max((e for e in events if e.event_type is BondEventType.COUPON),
                           key=lambda e: e.date)
        service = StubBondService({listing.asset.id: events})
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, listing, events)
        trade(ledger, listing, amount=10, price=98.0, dt=datetime.date(2025, 6, 10))
        cash_before = ledger.portfolio.cash

        await ledger.process_bond_events(final_coupon.record_date, service)
        await ledger.process_bond_events(listing.asset.maturity_date, service)
        await ledger.redeem_matured_bonds(listing.asset.maturity_date, service)

        # The last coupon plus the principal: a bond pays both on its final day.
        self.assertAlmostEqual(ledger.portfolio.cash - cash_before,
                               10 * final_coupon.value + 10_000.0)


class OfferTests(unittest.IsolatedAsyncioTestCase):
    """An offer window is information, not a payment."""

    async def test_an_offer_does_not_move_cash(self):
        listing = make_bond()
        offer_date = datetime.date(2024, 7, 10)
        events = make_coupon_events(listing) + [BondEvent(
            asset=listing.asset, event_type=BondEventType.OFFER, date=offer_date, value=0.0,
            currency="RUB", offer_type="put", offer_price=100.0,
            offer_start_date=offer_date - datetime.timedelta(days=7), offer_end_date=offer_date)]
        service = StubBondService({listing.asset.id: events})
        ledger = make_ledger(cash=1_000_000.0)
        load_schedule(ledger, listing, events)
        trade(ledger, listing, amount=10, price=100.0, dt=datetime.date(2024, 6, 10))
        cash_before = ledger.portfolio.cash

        await ledger.process_bond_events(offer_date, service)

        self.assertAlmostEqual(ledger.portfolio.cash, cash_before)

    async def test_the_next_offer_is_discoverable(self):
        listing = make_bond()
        offer_date = datetime.date(2024, 7, 10)
        events = make_coupon_events(listing) + [BondEvent(
            asset=listing.asset, event_type=BondEventType.OFFER, date=offer_date, value=0.0,
            currency="RUB", offer_type="put", offer_price=100.0)]
        ledger = make_ledger()
        load_schedule(ledger, listing, events)

        found = ledger.bond_book.next_offer(listing.asset, datetime.date(2024, 1, 1))

        self.assertIsNotNone(found)
        self.assertEqual(found.date, offer_date)
        self.assertEqual(found.offer_type, "put")


class CommissionTests(unittest.IsolatedAsyncioTestCase):
    """Bond commission is billed on turnover, which is the money, not the quote."""

    async def test_commission_is_a_fraction_of_the_money_transacted(self):
        ledger = make_ledger(cash=1_000_000.0)
        listing = make_bond()
        coupon_date = make_coupon_events(listing)[2].date
        load_schedule(ledger, listing)
        model = PerBondTurnover(cost=0.0003, bond_book=ledger.bond_book)

        cost = model.calculate_for_asset(
            asset=listing, quantity=10,
            transaction_amount=ledger.bond_book.dirty_value(listing.asset, 100.0, coupon_date) * 10)

        self.assertAlmostEqual(cost, 10_000.0 * 0.0003)

    async def test_commission_reduces_cash(self):
        ledger = make_ledger(cash=1_000_000.0)
        listing = make_bond()
        coupon_date = make_coupon_events(listing)[2].date
        load_schedule(ledger, listing)

        trade(ledger, listing, amount=10, price=100.0, dt=coupon_date, commission=3.0)

        self.assertAlmostEqual(ledger.portfolio.cash, 1_000_000.0 - 10_000.0 - 3.0)

    async def test_commission_lands_in_the_cost_basis_in_quote_units(self):
        # The cost basis is carried in quote units, so a commission in money has to be converted
        # or a three-rouble fee moves the basis by three percentage points.
        ledger = make_ledger(cash=1_000_000.0)
        listing = make_bond()
        coupon_date = make_coupon_events(listing)[2].date
        load_schedule(ledger, listing)

        trade(ledger, listing, amount=10, price=100.0, dt=coupon_date, commission=30.0)

        position = ledger.position_tracker.get_position(listing)
        # 30 roubles over ten bonds is 3 roubles each, which is 0.3% of a 1000 nominal.
        self.assertAlmostEqual(position.cost_basis, 100.3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
