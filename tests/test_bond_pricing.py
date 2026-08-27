"""Bond pricing arithmetic: quotation, accrued interest, amortization, yields.

These are the numbers every other bond test depends on. A quote is a percentage of face value,
a buyer owes the seller the accrued coupon, and an amortizing issue's face value shrinks -- get
any of the three wrong and every bond figure downstream is wrong by a constant factor.
"""
import datetime
import unittest

from bond_fixtures import (
    book_with, make_amortization_events, make_bond, make_coupon_events, make_zero_coupon_bond,
)

from ziplime.assets.domain.bond_event_type import BondEventType
from ziplime.assets.domain.day_count import DayCount
from ziplime.assets.domain.price_quotation import PriceQuotation
from ziplime.finance.bonds import (
    BondBook, coupon_schedule, current_yield, simple_yield_to_maturity,
)


class QuotationTests(unittest.TestCase):
    """A quote of 98.5 on a 1000 nominal is 985 roubles, not 98.50."""

    def test_percent_quote_is_a_share_of_face_value(self):
        self.assertAlmostEqual(
            PriceQuotation.PERCENT_OF_FACE.money_price(98.5, 1000.0), 985.0)

    def test_money_quote_passes_through(self):
        self.assertAlmostEqual(PriceQuotation.MONEY.money_price(98.5, 1000.0), 98.5)

    def test_quoted_price_inverts_money_price(self):
        self.assertAlmostEqual(
            PriceQuotation.PERCENT_OF_FACE.quoted_price(985.0, 1000.0), 98.5)

    def test_clean_value_uses_the_bonds_own_quotation(self):
        listing = make_bond()
        book = book_with(listing)
        self.assertAlmostEqual(
            book.clean_value(listing.asset, 98.5, datetime.date(2024, 3, 1)), 985.0)

    def test_a_money_quoted_bond_is_not_rescaled(self):
        listing = make_bond(price_quotation=PriceQuotation.MONEY)
        book = book_with(listing)
        self.assertAlmostEqual(
            book.clean_value(listing.asset, 985.0, datetime.date(2024, 3, 1)), 985.0)


class AccruedInterestTests(unittest.TestCase):
    """НКД: what the buyer hands the seller on top of the quote."""

    def setUp(self):
        self.listing = make_bond(coupon_rate=0.08, coupon_frequency=2)
        self.events = make_coupon_events(self.listing)
        self.book = book_with(self.listing, self.events)
        self.bond = self.listing.asset

    def test_accrual_is_zero_on_a_coupon_payment_date(self):
        coupon = self.events[3]
        self.assertAlmostEqual(self.book.accrued_interest(self.bond, coupon.date), 0.0)

    def test_accrual_is_linear_within_the_period(self):
        coupon = self.events[3]
        start = coupon.period_start_date
        period_days = (coupon.date - start).days
        midpoint = start + datetime.timedelta(days=period_days // 2)

        accrued = self.book.accrued_interest(self.bond, midpoint)

        expected = coupon.value * (period_days // 2) / period_days
        self.assertAlmostEqual(accrued, expected)

    def test_accrual_reaches_the_whole_coupon_the_day_before_payment(self):
        coupon = self.events[3]
        day_before = coupon.date - datetime.timedelta(days=1)
        period_days = (coupon.date - coupon.period_start_date).days

        accrued = self.book.accrued_interest(self.bond, day_before)

        self.assertAlmostEqual(accrued, coupon.value * (period_days - 1) / period_days)
        self.assertLess(accrued, coupon.value)

    def test_a_zero_coupon_bond_accrues_nothing(self):
        listing = make_zero_coupon_bond()
        book = book_with(listing, [])
        self.assertAlmostEqual(
            book.accrued_interest(listing.asset, datetime.date(2024, 6, 1)), 0.0)

    def test_nothing_accrues_after_maturity(self):
        self.assertAlmostEqual(
            self.book.accrued_interest(self.bond, self.bond.maturity_date), 0.0)
        self.assertAlmostEqual(
            self.book.accrued_interest(
                self.bond, self.bond.maturity_date + datetime.timedelta(days=30)), 0.0)

    def test_dirty_value_is_clean_plus_accrued(self):
        coupon = self.events[3]
        dt = coupon.period_start_date + datetime.timedelta(days=30)

        clean = self.book.clean_value(self.bond, 98.5, dt)
        accrued = self.book.accrued_interest(self.bond, dt)

        self.assertAlmostEqual(self.book.dirty_value(self.bond, 98.5, dt), clean + accrued)
        self.assertGreater(accrued, 0.0)

    def test_an_unloaded_bond_falls_back_to_its_own_terms(self):
        # No schedule was ever ingested. The bond still states a coupon and a frequency, and using
        # them beats pricing it as if it paid nothing.
        empty = BondBook()
        coupon = self.events[3]
        dt = coupon.period_start_date + datetime.timedelta(days=30)

        self.assertAlmostEqual(empty.accrued_interest(self.bond, dt),
                               self.book.accrued_interest(self.bond, dt))

    def test_an_unloaded_bond_still_gets_the_face_value_conversion(self):
        empty = BondBook()
        self.assertAlmostEqual(
            empty.clean_value(self.bond, 98.5, datetime.date(2024, 3, 1)), 985.0)


class AmortizationTests(unittest.TestCase):
    """An amortizing issue repays principal in instalments, so the nominal shrinks."""

    def setUp(self):
        self.listing = make_bond(sid=503, symbol="AMORTBOND", is_amortized=True)
        self.instalments = [
            (datetime.date(2024, 1, 10), 300.0),
            (datetime.date(2025, 1, 10), 300.0),
        ]
        events = make_coupon_events(self.listing) + make_amortization_events(
            self.listing, self.instalments)
        self.book = book_with(self.listing, events)
        self.bond = self.listing.asset

    def test_face_value_is_the_nominal_before_any_instalment(self):
        self.assertAlmostEqual(
            self.book.face_value(self.bond, datetime.date(2023, 6, 1)), 1000.0)

    def test_face_value_drops_on_the_instalment_date(self):
        self.assertAlmostEqual(
            self.book.face_value(self.bond, datetime.date(2024, 1, 10)), 700.0)

    def test_instalments_accumulate(self):
        self.assertAlmostEqual(
            self.book.face_value(self.bond, datetime.date(2025, 6, 1)), 400.0)

    def test_nothing_is_outstanding_after_maturity(self):
        self.assertAlmostEqual(
            self.book.face_value(self.bond,
                                 self.bond.maturity_date + datetime.timedelta(days=1)), 0.0)

    def test_a_quote_is_read_against_the_reduced_nominal(self):
        # The same quote is worth less money once principal has been repaid: that is the whole
        # point of amortization, and reading it against the original nominal overstates the
        # position by the amount already returned to the holder.
        before = self.book.clean_value(self.bond, 100.0, datetime.date(2023, 6, 1))
        after = self.book.clean_value(self.bond, 100.0, datetime.date(2024, 6, 1))
        self.assertAlmostEqual(before, 1000.0)
        self.assertAlmostEqual(after, 700.0)

    def test_face_value_is_derived_when_the_vendor_omits_the_new_nominal(self):
        from ziplime.assets.entities.bond_event import BondEvent
        partial = BondEvent(asset=self.bond, event_type=BondEventType.AMORTIZATION,
                            date=datetime.date(2024, 1, 10), value=300.0, currency="RUB",
                            initial_face_value=1000.0)
        book = BondBook()
        book.add(self.bond.id, [partial])
        self.assertAlmostEqual(book.face_value(self.bond, datetime.date(2024, 6, 1)), 700.0)


class CouponScheduleTests(unittest.TestCase):
    """The schedule generated from a bond's own terms."""

    def test_the_last_coupon_lands_on_maturity(self):
        listing = make_bond()
        schedule = coupon_schedule(listing.asset)
        self.assertEqual(schedule[-1].date, listing.asset.maturity_date)

    def test_periods_are_contiguous(self):
        schedule = coupon_schedule(make_bond().asset)
        for earlier, later in zip(schedule, schedule[1:]):
            self.assertEqual(later.period_start_date, earlier.date)

    def test_each_coupon_is_the_annual_rate_over_the_frequency(self):
        listing = make_bond(face_value=1000.0, coupon_rate=0.08, coupon_frequency=2)
        for event in coupon_schedule(listing.asset):
            self.assertAlmostEqual(event.value, 40.0)

    def test_a_zero_coupon_bond_has_no_schedule(self):
        self.assertEqual(coupon_schedule(make_zero_coupon_bond().asset), [])

    def test_a_month_end_maturity_does_not_drift(self):
        # Stepping back a month at a time clamps 31 -> 30 and never recovers, walking the whole
        # schedule off the anniversary. Every date is measured from maturity instead.
        listing = make_bond(issue_date=datetime.date(2020, 3, 31),
                            maturity_date=datetime.date(2026, 3, 31),
                            coupon_rate=0.08, coupon_frequency=2)
        for event in coupon_schedule(listing.asset):
            self.assertIn(event.date.day, (30, 31),
                          msg=f"{event.date} drifted off the payment anniversary")


class DayCountTests(unittest.TestCase):
    """Conventions used only when no schedule is available."""

    def test_act_365_counts_actual_days(self):
        self.assertAlmostEqual(
            DayCount.ACT_365.year_fraction(datetime.date(2024, 1, 1), datetime.date(2024, 7, 1)),
            182 / 365)

    def test_act_360_uses_a_360_day_year(self):
        self.assertAlmostEqual(
            DayCount.ACT_360.year_fraction(datetime.date(2024, 1, 1), datetime.date(2024, 7, 1)),
            182 / 360)

    def test_thirty_360_treats_every_month_as_thirty_days(self):
        self.assertAlmostEqual(
            DayCount.THIRTY_360.year_fraction(datetime.date(2024, 1, 31),
                                              datetime.date(2024, 7, 31)),
            0.5)

    def test_act_act_measures_against_a_leap_year(self):
        self.assertAlmostEqual(
            DayCount.ACT_ACT.year_fraction(datetime.date(2024, 1, 1), datetime.date(2025, 1, 1)),
            1.0)


class YieldTests(unittest.TestCase):
    def setUp(self):
        self.listing = make_bond(coupon_rate=0.08, coupon_frequency=2)
        self.book = book_with(self.listing)
        self.bond = self.listing.asset

    def test_current_yield_is_coupons_over_the_dirty_price(self):
        dt = datetime.date(2024, 4, 10)
        dirty = self.book.dirty_value(self.bond, 100.0, dt)
        self.assertAlmostEqual(current_yield(self.bond, self.book, 100.0, dt), 80.0 / dirty)

    def test_a_zero_coupon_bond_has_no_current_yield(self):
        listing = make_zero_coupon_bond()
        book = book_with(listing, [])
        self.assertAlmostEqual(
            current_yield(listing.asset, book, 92.0, datetime.date(2024, 6, 1)), 0.0)

    def test_a_discount_bond_yields_more_than_its_coupon(self):
        # Bought below par, the pull to par adds to the coupon income.
        dt = datetime.date(2024, 1, 10)
        at_par = simple_yield_to_maturity(self.bond, self.book, 100.0, dt)
        at_discount = simple_yield_to_maturity(self.bond, self.book, 90.0, dt)
        self.assertGreater(at_discount, at_par)

    def test_a_premium_bond_yields_less_than_its_coupon(self):
        dt = datetime.date(2024, 1, 10)
        at_par = simple_yield_to_maturity(self.bond, self.book, 100.0, dt)
        at_premium = simple_yield_to_maturity(self.bond, self.book, 110.0, dt)
        self.assertLess(at_premium, at_par)

    def test_a_zero_coupon_bond_bought_at_a_discount_still_yields(self):
        bought_on = datetime.date(2024, 1, 10)
        maturity = datetime.date(2026, 1, 10)
        listing = make_zero_coupon_bond(issue_date=bought_on, maturity_date=maturity)
        book = book_with(listing, [])
        # 900 paid, 1000 back, annualised over the actual days to maturity: 2024 is a leap year,
        # so that is 731/365 years rather than a round two.
        years = (maturity - bought_on).days / 365.0
        self.assertAlmostEqual(
            simple_yield_to_maturity(listing.asset, book, 90.0, bought_on),
            (1000.0 - 900.0) / 900.0 / years, places=6)

    def test_yield_is_zero_at_maturity(self):
        self.assertAlmostEqual(
            simple_yield_to_maturity(self.bond, self.book, 100.0, self.bond.maturity_date), 0.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
