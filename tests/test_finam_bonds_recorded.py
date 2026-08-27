"""The connector against **recorded live payloads**.

``tests/fixtures/finam/bond_payloads.json`` holds the real ``AllAssets`` row, ``GetAsset``
specification and full calendar of three MOEX issues, captured from the API. Nothing here touches
the network.

The unit tests next door check each rule in isolation against a hand-written shape. These check
that the rules together produce the right bond from what the vendor actually sends -- which is a
different question, and the one that caught every mistake in the first version of this connector.
The three issues are chosen because each breaks a different assumption:

``SU26238RMFS4`` (ОФЗ 26238)
    A plain bullet. Its redemption arrives as a single ``AMORTIZATION`` of 100% of the nominal, so
    it is the case that shows ``MATURITY`` is never sent and has to be recognised.
``RU000A0JRU20`` (СЗКК 03)
    Amortizing *and* floating. Its instalments all report the same ``new_face_value``, and its
    unfixed future coupons are published as zero.
``XS0114288789`` (RUS-30)
    A dollar issue with a nominal of 1.00 whose instalments sum to more than that, and whose
    ``lot_size`` is 1000 -- the trap that inverts the nominal if it is mistaken for one.
"""
import datetime
import json
import unittest
from pathlib import Path

from ziplime.assets.domain.bond_event_type import BondEventType
from ziplime.assets.domain.price_quotation import PriceQuotation
from ziplime.data.data_sources.finam.finam_bonds import build_bond, is_bond_listing
from ziplime.finance.bonds import BondBook

PAYLOADS = json.loads(
    (Path(__file__).parent / "fixtures" / "finam" / "bond_payloads.json").read_text())


def build(ticker: str):
    recorded = PAYLOADS[ticker]
    return build_bond(listing=recorded["listing"], events=recorded["events"],
                      details=recorded["details"])


def book_for(ticker: str):
    bond, events = build(ticker)
    book = BondBook()
    book.add(bond.id, events)
    return bond, events, book


class RecordedListingTests(unittest.TestCase):
    def test_every_recorded_listing_is_recognised_as_a_bond(self):
        for ticker, recorded in PAYLOADS.items():
            self.assertTrue(is_bond_listing(recorded["listing"]), ticker)

    def test_the_live_type_is_plural(self):
        self.assertEqual(PAYLOADS["SU26238RMFS4"]["listing"]["type"], "BONDS")


class OfzBulletTests(unittest.TestCase):
    """ОФЗ 26238: the redemption is an amortization of the whole nominal."""

    def setUp(self):
        self.bond, self.events, self.book = book_for("SU26238RMFS4")

    def test_the_nominal_is_a_thousand_roubles(self):
        self.assertAlmostEqual(self.bond.face_value, 1000.0)

    def test_the_currency_is_roubles_not_a_symbol(self):
        self.assertEqual(self.bond.quote_currency, "RUB")

    def test_the_coupon_is_the_published_rate(self):
        self.assertAlmostEqual(self.bond.coupon_rate, 0.071, places=4)
        self.assertEqual(self.bond.coupon_frequency, 2)

    def test_it_is_not_treated_as_amortizing(self):
        # One event repaying the whole nominal is a bullet redemption, not a repayment schedule.
        self.assertFalse(self.bond.is_amortized)

    def test_the_redemption_is_recognised(self):
        redemptions = [e for e in self.events if e.event_type is BondEventType.MATURITY]
        self.assertEqual(len(redemptions), 1)
        self.assertEqual(redemptions[0].date, self.bond.maturity_date)
        self.assertEqual(self.bond.maturity_date, datetime.date(2041, 5, 15))

    def test_the_redemption_does_not_pay_cash_of_its_own(self):
        redemption = next(e for e in self.events if e.event_type is BondEventType.MATURITY)
        self.assertFalse(redemption.pays_cash)

    def test_the_principal_stays_whole_until_maturity(self):
        self.assertAlmostEqual(self.book.face_value(self.bond, datetime.date(2030, 1, 1)), 1000.0)
        self.assertAlmostEqual(self.book.face_value(self.bond, self.bond.maturity_date), 1000.0)

    def test_a_quote_is_worth_ten_times_its_face(self):
        # The trade that motivates the whole quotation machinery: 66.5 on the tape is 665 roubles.
        self.assertAlmostEqual(
            self.book.clean_value(self.bond, 66.5, datetime.date(2024, 1, 3)), 665.0)

    def test_accrued_interest_matches_the_published_schedule(self):
        # The 2024-06-05 coupon of 35.40 covers 2023-12-06 to 2024-06-05.
        coupon = next(e for e in self.events
                      if e.event_type is BondEventType.COUPON
                      and e.date == datetime.date(2024, 6, 5))
        as_of = datetime.date(2024, 1, 3)
        elapsed = (as_of - coupon.period_start_date).days
        period = (coupon.date - coupon.period_start_date).days

        self.assertAlmostEqual(self.book.accrued_interest(self.bond, as_of),
                               coupon.value * elapsed / period)

    def test_the_quotation_is_a_percentage(self):
        self.assertEqual(self.bond.price_quotation, PriceQuotation.PERCENT_OF_FACE)


class AmortizingFloaterTests(unittest.TestCase):
    """СЗКК 03: instalments that all report the same nominal, and unfixed future coupons."""

    def setUp(self):
        self.bond, self.events, self.book = book_for("RU000A0JRU20")

    def test_the_nominal_at_issue_is_not_the_outstanding_one(self):
        # Every event reports 567.0 as new_face_value / coupon face_value; the nominal is 1000.
        self.assertAlmostEqual(self.bond.face_value, 1000.0)

    def test_it_is_recognised_as_amortizing(self):
        self.assertTrue(self.bond.is_amortized)

    def test_the_principal_shrinks_over_its_life(self):
        early = self.book.face_value(self.bond, datetime.date(2015, 1, 1))
        late = self.book.face_value(self.bond, datetime.date(2030, 1, 1))
        self.assertGreater(early, late)
        self.assertLessEqual(late, early)

    def test_the_outstanding_principal_agrees_with_the_specification(self):
        # GetAsset reports what is outstanding today; the schedule has to reproduce it.
        reported = float(PAYLOADS["RU000A0JRU20"]["details"]["bond_details"]
                         ["bond_face_value"]["value"])
        today = datetime.date(2026, 8, 27)
        self.assertAlmostEqual(self.book.face_value(self.bond, today), reported, delta=1.0)

    def test_the_rate_is_the_last_one_actually_fixed(self):
        # Its future coupons are published as zero because the rate is not set. Taking the last
        # coupon chronologically would report this bond as paying nothing at all.
        self.assertGreater(self.bond.coupon_rate, 0.0)

    def test_only_the_final_instalment_is_the_redemption(self):
        redemptions = [e for e in self.events if e.event_type is BondEventType.MATURITY]
        instalments = [e for e in self.events if e.event_type is BondEventType.AMORTIZATION]
        self.assertEqual(len(redemptions), 1)
        self.assertGreater(len(instalments), 1)
        self.assertEqual(redemptions[0].date, max(e.date for e in self.events))

    def test_the_instalments_and_the_redemption_account_for_the_principal(self):
        repaid = sum(e.value for e in self.events
                     if e.event_type is BondEventType.AMORTIZATION)
        redemption = next(e for e in self.events if e.event_type is BondEventType.MATURITY)
        self.assertAlmostEqual(repaid + redemption.value, self.bond.face_value, delta=1.0)


class DollarEurobondTests(unittest.TestCase):
    """RUS-30: a 1.00 nominal, a 1000 lot size, and instalments that overshoot."""

    def setUp(self):
        self.bond, self.events, self.book = book_for("XS0114288789")

    def test_the_nominal_is_one_not_the_trading_lot(self):
        # lot_size is 1000.0 here. Reading it as the nominal would inflate the bond a thousandfold.
        self.assertAlmostEqual(self.bond.face_value, 1.0)
        self.assertAlmostEqual(
            float(PAYLOADS["XS0114288789"]["details"]["lot_size"]["value"]), 1000.0)

    def test_the_currency_is_dollars(self):
        self.assertEqual(self.bond.quote_currency, "USD")

    def test_the_redemption_is_the_last_instalment_not_the_first_to_reach_par(self):
        # Its 47 one-kopeck instalments sum to more than the nominal, so a running total declares
        # it redeemed years early. The maturity has to come from the end of the schedule.
        redemption = next(e for e in self.events if e.event_type is BondEventType.MATURITY)
        self.assertEqual(redemption.date, max(e.date for e in self.events))
        self.assertEqual(self.bond.maturity_date, redemption.date)
        self.assertGreater(self.bond.maturity_date, datetime.date(2029, 1, 1))

    def test_the_principal_never_goes_negative(self):
        for year in range(2001, 2031):
            outstanding = self.book.face_value(self.bond, datetime.date(year, 6, 30))
            self.assertGreaterEqual(outstanding, 0.0, f"negative principal in {year}")
            self.assertLessEqual(outstanding, self.bond.face_value)


class EveryRecordedBondTests(unittest.TestCase):
    """Invariants that must hold for all three, whatever their shape."""

    def test_each_bond_builds(self):
        for ticker in PAYLOADS:
            self.assertIsNotNone(build(ticker), ticker)

    def test_each_has_exactly_one_redemption(self):
        for ticker in PAYLOADS:
            _, events, _ = book_for(ticker)
            redemptions = [e for e in events if e.event_type is BondEventType.MATURITY]
            self.assertEqual(len(redemptions), 1, ticker)

    def test_no_event_keeps_a_currency_symbol(self):
        for ticker in PAYLOADS:
            _, events, _ = book_for(ticker)
            for event in events:
                self.assertRegex(event.currency, r"^[A-Z]{3}$",
                                 f"{ticker} kept a raw currency symbol")

    def test_trading_stops_before_the_issuer_repays(self):
        for ticker in PAYLOADS:
            bond, _, _ = book_for(ticker)
            self.assertEqual(bond.auto_close_date, bond.maturity_date, ticker)
            self.assertLess(bond.end_date, bond.maturity_date, ticker)

    def test_accrued_interest_never_exceeds_a_coupon(self):
        for ticker in PAYLOADS:
            bond, events, book = book_for(ticker)
            biggest = max((e.value for e in events
                           if e.event_type is BondEventType.COUPON), default=0.0)
            for year in range(2015, 2030):
                accrued = book.accrued_interest(bond, datetime.date(year, 7, 15))
                self.assertGreaterEqual(accrued, 0.0, ticker)
                self.assertLessEqual(accrued, biggest + 1e-9, ticker)


if __name__ == "__main__":
    unittest.main(verbosity=2)
