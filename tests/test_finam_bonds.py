"""Parsing the Finam bond endpoints.

The payloads here are copied from **live API responses**, not from the documentation, because four
of the fields do not mean what their names suggest and each one silently corrupts a bond:

* the ``AllAssets`` type is ``BONDS``, not ``BOND``;
* redemption arrives as a terminal ``AMORTIZATION``, never as ``MATURITY``;
* ``new_face_value`` and ``coupon_details.face_value`` both carry *today's* outstanding nominal,
  not the nominal after the instalment or the basis the coupon was computed on;
* a coupon of zero means the rate has not been fixed yet, not that the bond pays nothing.

Constants below name the real instrument each shape came from.
"""
import datetime
import unittest

from ziplime.assets.domain.bond_event_type import BondEventType
from ziplime.assets.domain.price_quotation import PriceQuotation
from ziplime.data.data_sources.finam.finam_bonds import (
    build_bond, infer_bond_terms, initial_face_value, is_bond_listing, normalize_currency,
    normalize_events, parse_bond_event, parse_bond_events,
)


def decimal(value) -> dict:
    return {"value": str(value)}


def date(year: int, month: int, day: int) -> dict:
    return {"year": year, "month": month, "day": day}


def coupon_payload(year: int, month: int, day: int, value: float = 41.25,
                   face_value: float = 1000.0, value_percent: float = 8.25,
                   period_start: tuple[int, int, int] | None = None) -> dict:
    """One coupon, shaped like a live ``/v1/bonds/past`` entry (rouble symbol included)."""
    start = period_start or (year - 1 if month <= 6 else year, month + 6 if month <= 6 else month - 6, day)
    return {
        "date": date(year, month, day),
        "type": "COUPON",
        "value": decimal(value),
        "currency": "₽",
        "coupon_details": {
            "record_date": date(year, month, max(1, day - 1)),
            "start_date": date(*start),
            "face_value": decimal(face_value),
            "value_percent": decimal(value_percent),
        },
    }


#: An ``AllAssets`` row exactly as the live API returns it for OFZ 26238.
LISTING = {
    "symbol": "SU26238RMFS4@MISX",
    "id": "56658",
    "ticker": "SU26238RMFS4",
    "mic": "MISX",
    "isin": "RU000A1038V6",
    "type": "BONDS",
    "name": "ОФЗ 26238",
    "is_archived": False,
}

#: The final event of OFZ 26238, copied from ``/v1/bonds/future``. This is the redemption: a full
#: repayment typed as an amortization, whose ``new_face_value`` is the nominal at issue rather than
#: the nothing that is left afterwards.
OFZ_REDEMPTION = {
    "date": date(2041, 5, 15),
    "type": "AMORTIZATION",
    "value": decimal(1000.0),
    "currency": "₽",
    "amortization_details": {
        "new_face_value": decimal(1000.0),
        "initial_face_value": decimal(1000.0),
        "amortization_percent": decimal(100.0),
    },
}


def amortization_payload(year: int, month: int, day: int, value: float,
                         outstanding_today: float = 567.0,
                         initial: float = 1000.0) -> dict:
    """One instalment of an amortizing issue, shaped like ``СЗКК 03``.

    ``new_face_value`` is deliberately the same on every instalment: that is what the live API
    sends, and it is what makes reading it as "the nominal after this instalment" wrong.
    """
    return {
        "date": date(year, month, day),
        "type": "AMORTIZATION",
        "value": decimal(value),
        "currency": "₽",
        "amortization_details": {
            "new_face_value": decimal(outstanding_today),
            "initial_face_value": decimal(initial),
            "amortization_percent": decimal(value / initial * 100.0),
        },
    }


class ListingFilterTests(unittest.TestCase):
    def test_a_bond_row_is_recognised(self):
        # The live type is BONDS. Matching on "BOND" found nothing at all in 297 947 instruments.
        self.assertTrue(is_bond_listing(LISTING))

    def test_other_instrument_types_are_not(self):
        # Every other type the live AllAssets listing actually contains.
        for asset_type in ("FUTURES", "EQUITIES", "CURRENCIES", "INDICES", "FUNDS", "SWAPS",
                           "SPREADS", "OPTIONS", "OTHER", ""):
            self.assertFalse(is_bond_listing({**LISTING, "type": asset_type}))

    def test_the_type_is_matched_case_insensitively(self):
        self.assertTrue(is_bond_listing({**LISTING, "type": "bonds"}))


class EventParsingTests(unittest.TestCase):
    def setUp(self):
        from ziplime.assets.entities.bond import Bond
        self.bond = Bond(id=1, isin="RU000A1038V6", asset_name="SU26238RMFS4",
                         start_date=None, end_date=None, first_traded=None,
                         auto_close_date=None, face_value=1000.0,
                         maturity_date=datetime.date(2041, 5, 15))

    def test_a_coupon_carries_its_period_and_face_value(self):
        event = parse_bond_event(coupon_payload(2024, 5, 15), self.bond)

        self.assertEqual(event.event_type, BondEventType.COUPON)
        self.assertEqual(event.date, datetime.date(2024, 5, 15))
        self.assertAlmostEqual(event.value, 41.25)
        self.assertEqual(event.record_date, datetime.date(2024, 5, 14))
        self.assertEqual(event.period_start_date, datetime.date(2023, 11, 15))
        self.assertAlmostEqual(event.face_value, 1000.0)
        self.assertAlmostEqual(event.value_percent, 8.25)

    def test_an_amortization_carries_the_new_nominal(self):
        payload = {
            "date": date(2025, 3, 20),
            "type": "AMORTIZATION",
            "value": decimal(250.0),
            "currency": "RUB",
            "amortization_details": {
                "new_face_value": decimal(750.0),
                "initial_face_value": decimal(1000.0),
                "amortization_percent": decimal(25.0),
            },
        }

        event = parse_bond_event(payload, self.bond)

        self.assertEqual(event.event_type, BondEventType.AMORTIZATION)
        self.assertAlmostEqual(event.value, 250.0)
        self.assertAlmostEqual(event.new_face_value, 750.0)
        self.assertAlmostEqual(event.amortization_percent, 25.0)

    def test_an_offer_carries_its_window(self):
        payload = {
            "date": date(2026, 2, 10),
            "type": "OFFER",
            "value": decimal(0),
            "currency": "RUB",
            "offer_details": {
                "offer_type": "put",
                "price": decimal(100.0),
                "start_date": date(2026, 2, 3),
                "end_date": date(2026, 2, 10),
                "agent": "Some Broker",
            },
        }

        event = parse_bond_event(payload, self.bond)

        self.assertEqual(event.event_type, BondEventType.OFFER)
        self.assertEqual(event.offer_type, "put")
        self.assertAlmostEqual(event.offer_price, 100.0)
        self.assertEqual(event.offer_start_date, datetime.date(2026, 2, 3))
        self.assertFalse(event.pays_cash, "an offer window pays nothing by itself")

    def test_an_unknown_type_degrades_rather_than_raising(self):
        # A vendor adding an event kind must not abort an ingest of a thousand events.
        event = parse_bond_event(
            {"date": date(2024, 5, 15), "type": "SOMETHING_NEW", "value": decimal(1)}, self.bond)

        self.assertEqual(event.event_type, BondEventType.UNSPECIFIED)
        self.assertFalse(event.pays_cash)

    def test_a_prefixed_type_is_understood(self):
        event = parse_bond_event(
            {"date": date(2024, 5, 15), "type": "BOND_EVENT_TYPE_COUPON",
             "value": decimal(41.25)}, self.bond)
        self.assertEqual(event.event_type, BondEventType.COUPON)

    def test_a_dateless_event_is_dropped(self):
        self.assertIsNone(parse_bond_event({"type": "COUPON", "value": decimal(1)}, self.bond))

    def test_events_come_back_sorted(self):
        payloads = [coupon_payload(2025, 5, 15), coupon_payload(2024, 5, 15),
                    {"type": "COUPON", "value": decimal(1)}]

        events = parse_bond_events(payloads, self.bond)

        self.assertEqual([e.date for e in events],
                         [datetime.date(2024, 5, 15), datetime.date(2025, 5, 15)])


class TermInferenceTests(unittest.TestCase):
    """The terms the endpoints never state, read off the realised calendar."""

    def setUp(self):
        from ziplime.assets.entities.bond import Bond
        self.bond = Bond(id=1, isin=None, asset_name="TEST", start_date=None, end_date=None,
                         first_traded=None, auto_close_date=None, face_value=1000.0,
                         maturity_date=None)
        self.semi_annual = parse_bond_events(
            [coupon_payload(2023, 5, 15), coupon_payload(2023, 11, 15),
             coupon_payload(2024, 5, 15), coupon_payload(2024, 11, 15)], self.bond)

    def test_the_nominal_of_a_bullet_comes_from_its_coupons(self):
        # With no amortization, outstanding and initial are the same number, so the coupon's
        # face_value is a safe source for it.
        self.assertAlmostEqual(infer_bond_terms(self.semi_annual).face_value, 1000.0)

    def test_the_nominal_of_an_amortizing_issue_ignores_the_coupon_figure(self):
        # coupon_details.face_value carries today's outstanding (567), not the nominal at issue
        # (1000). Taking it would understate the bond by whatever has already been repaid.
        events = parse_bond_events(
            [coupon_payload(2026, 10, 2, value=25.19, face_value=567.0),
             coupon_payload(2027, 4, 2, value=25.19, face_value=567.0),
             amortization_payload(2026, 10, 2, 39.0),
             amortization_payload(2027, 4, 2, 43.0)], self.bond)

        self.assertAlmostEqual(infer_bond_terms(events).face_value, 1000.0)

    def test_the_nominal_at_issue_is_read_from_the_amortization_details(self):
        events = parse_bond_events([amortization_payload(2026, 10, 2, 39.0)], self.bond)
        self.assertAlmostEqual(initial_face_value(events), 1000.0)

    def test_the_frequency_comes_from_the_spacing_between_payments(self):
        self.assertEqual(infer_bond_terms(self.semi_annual).coupon_frequency, 2)

    def test_a_quarterly_schedule_is_recognised(self):
        quarterly = parse_bond_events(
            [coupon_payload(2023, 3, 20), coupon_payload(2023, 6, 20),
             coupon_payload(2023, 9, 20), coupon_payload(2023, 12, 20)], self.bond)
        self.assertEqual(infer_bond_terms(quarterly).coupon_frequency, 4)

    def test_a_shifted_payment_does_not_move_the_frequency(self):
        # A holiday pushes one payment by a few days. The median gap is used precisely so that one
        # shifted date cannot turn a semi-annual bond into something else.
        shifted = parse_bond_events(
            [coupon_payload(2023, 5, 15), coupon_payload(2023, 11, 20),
             coupon_payload(2024, 5, 15), coupon_payload(2024, 11, 15)], self.bond)
        self.assertEqual(infer_bond_terms(shifted).coupon_frequency, 2)

    def test_the_rate_comes_from_the_published_percentage(self):
        self.assertAlmostEqual(infer_bond_terms(self.semi_annual).coupon_rate, 0.0825)

    def test_the_rate_falls_back_to_the_money_paid(self):
        # No value_percent: the annual rate is one coupon times the frequency, over the nominal.
        payloads = [
            {**coupon_payload(2023, 5, 15), "coupon_details": {
                "start_date": date(2022, 11, 15), "face_value": decimal(1000.0)}},
            {**coupon_payload(2023, 11, 15), "coupon_details": {
                "start_date": date(2023, 5, 15), "face_value": decimal(1000.0)}},
        ]
        terms = infer_bond_terms(parse_bond_events(payloads, self.bond))
        self.assertAlmostEqual(terms.coupon_rate, 41.25 * 2 / 1000.0)

    def test_maturity_is_the_last_dated_event(self):
        self.assertEqual(infer_bond_terms(self.semi_annual).maturity_date,
                         datetime.date(2024, 11, 15))

    def test_an_explicit_redemption_wins_over_the_last_coupon(self):
        events = self.semi_annual + parse_bond_events(
            [{"date": date(2024, 11, 15), "type": "MATURITY", "value": decimal(1000.0)}],
            self.bond)
        self.assertEqual(infer_bond_terms(events).maturity_date, datetime.date(2024, 11, 15))

    def test_the_specification_supplies_a_maturity_the_calendar_lacks(self):
        terms = infer_bond_terms(self.semi_annual,
                                 details={"maturity_date": date(2041, 5, 15)})
        self.assertEqual(terms.maturity_date, datetime.date(2041, 5, 15))
        self.assertNotIn("maturity_date", terms.inferred)

    def test_the_calendar_outranks_the_specification_for_the_nominal(self):
        # GetAsset reports bond_details.bond_face_value, which is the nominal *outstanding* -- 0.04
        # for RUS-30 against a nominal of 1.00. Only the calendar states the nominal at issue, so
        # it wins wherever it says anything.
        events = self.semi_annual + parse_bond_events(
            [amortization_payload(2025, 5, 15, 250.0),
             amortization_payload(2026, 5, 15, 250.0)], self.bond)

        terms = infer_bond_terms(
            events, details={"bond_details": {"bond_face_value": decimal(500.0)}})

        self.assertAlmostEqual(terms.face_value, 1000.0)

    def test_the_specification_fills_in_a_nominal_for_a_bullet(self):
        # With no amortization there is nothing to have been repaid, so GetAsset's outstanding
        # figure is the nominal and is safe to use.
        coupons_without_face = parse_bond_events([
            {**coupon_payload(2024, 5, 15), "coupon_details": {"start_date": date(2023, 11, 15)}},
            {**coupon_payload(2024, 11, 15), "coupon_details": {"start_date": date(2024, 5, 15)}},
        ], self.bond)

        terms = infer_bond_terms(
            coupons_without_face,
            details={"bond_details": {"bond_face_value": decimal(500.0)}})

        self.assertAlmostEqual(terms.face_value, 500.0)

    def test_the_trading_lot_is_never_mistaken_for_the_nominal(self):
        # lot_size is 1.0 for an OFZ whose nominal is 1000 and 1000.0 for RUS-30 whose nominal is
        # 1.0 -- reading it gets both wrong, in opposite directions.
        coupons_without_face = parse_bond_events([
            {**coupon_payload(2024, 5, 15), "coupon_details": {"start_date": date(2023, 11, 15)}},
        ], self.bond)

        terms = infer_bond_terms(coupons_without_face,
                                 details={"lot_size": decimal(1.0), "decimals": 4})

        self.assertNotAlmostEqual(terms.face_value, 1.0)

    def test_derived_fields_are_reported_as_derived(self):
        terms = infer_bond_terms(self.semi_annual)
        self.assertIn("maturity_date", terms.inferred)
        self.assertIn("coupon_frequency", terms.inferred)

    def test_amortization_is_detected(self):
        events = self.semi_annual + parse_bond_events(
            [amortization_payload(2024, 5, 15, 250.0),
             amortization_payload(2024, 11, 15, 250.0)], self.bond)
        self.assertTrue(infer_bond_terms(events).is_amortized)

    def test_a_single_full_repayment_is_not_amortization(self):
        # An OFZ repays its whole nominal in one event. That is a bullet redemption; calling it
        # amortization would advertise a repayment schedule the bond does not have.
        events = self.semi_annual + parse_bond_events([OFZ_REDEMPTION], self.bond)
        self.assertFalse(infer_bond_terms(events).is_amortized)

    def test_undetermined_coupons_are_counted_not_believed(self):
        # A floater publishes its future coupon dates with a value of zero because the rate has
        # not been fixed. Treating those as "pays nothing" would understate the bond.
        floater = parse_bond_events(
            [coupon_payload(2026, 10, 2, value=25.19, value_percent=8.91),
             coupon_payload(2027, 4, 2, value=0.0, value_percent=0.0),
             coupon_payload(2027, 10, 1, value=0.0, value_percent=0.0)], self.bond)
        self.assertEqual(infer_bond_terms(floater).undetermined_coupons, 2)

    def test_a_calendar_with_one_coupon_states_no_frequency(self):
        # Two dates are the minimum for a spacing. Claiming a frequency from one would be a guess.
        terms = infer_bond_terms(parse_bond_events([coupon_payload(2024, 5, 15)], self.bond))
        self.assertEqual(terms.coupon_frequency, 0)


class CurrencyTests(unittest.TestCase):
    """Finam reports a currency symbol, not an ISO code."""

    def test_the_rouble_symbol_becomes_rub(self):
        self.assertEqual(normalize_currency("₽"), "RUB")

    def test_the_dollar_symbol_becomes_usd(self):
        self.assertEqual(normalize_currency("$"), "USD")

    def test_an_iso_code_passes_through(self):
        self.assertEqual(normalize_currency("EUR"), "EUR")
        self.assertEqual(normalize_currency("usd"), "USD")

    def test_an_unknown_symbol_falls_back(self):
        self.assertEqual(normalize_currency("¤", default="RUB"), "RUB")
        self.assertEqual(normalize_currency(None), "RUB")

    def test_an_event_carries_the_normalised_currency(self):
        from ziplime.assets.entities.bond import Bond
        bond = Bond(id=1, isin=None, asset_name="T", start_date=None, end_date=None,
                    first_traded=None, auto_close_date=None, face_value=1000.0,
                    maturity_date=None)
        event = parse_bond_event(coupon_payload(2024, 5, 15), bond)
        self.assertEqual(event.currency, "RUB")


class RedemptionNormalizationTests(unittest.TestCase):
    """The terminal instalment is the redemption, and must be booked exactly once."""

    def setUp(self):
        from ziplime.assets.entities.bond import Bond
        self.bond = Bond(id=1, isin=None, asset_name="TEST", start_date=None, end_date=None,
                         first_traded=None, auto_close_date=None, face_value=1000.0,
                         maturity_date=None)

    def test_a_full_repayment_becomes_a_maturity_event(self):
        events = normalize_events(parse_bond_events([OFZ_REDEMPTION], self.bond), face_value=1000.0)
        self.assertEqual(events[0].event_type, BondEventType.MATURITY)

    def test_a_maturity_event_does_not_pay_cash_of_its_own(self):
        # The ledger repays the principal outstanding on the maturity date. If this event also
        # paid, the principal would be counted twice.
        events = normalize_events(parse_bond_events([OFZ_REDEMPTION], self.bond), face_value=1000.0)
        self.assertFalse(events[0].pays_cash)

    def test_only_the_last_instalment_is_re_typed(self):
        payloads = [amortization_payload(2024, 3, 20, 250.0),
                    amortization_payload(2025, 3, 20, 250.0),
                    amortization_payload(2026, 3, 20, 250.0),
                    amortization_payload(2027, 3, 20, 250.0)]
        events = normalize_events(parse_bond_events(payloads, self.bond), face_value=1000.0)

        kinds = [event.event_type for event in events]
        self.assertEqual(kinds, [BondEventType.AMORTIZATION] * 3 + [BondEventType.MATURITY])

    def test_the_instalments_before_redemption_still_pay(self):
        payloads = [amortization_payload(2024, 3, 20, 250.0),
                    amortization_payload(2025, 3, 20, 750.0)]
        events = normalize_events(parse_bond_events(payloads, self.bond), face_value=1000.0)

        self.assertTrue(events[0].pays_cash)
        self.assertAlmostEqual(events[0].value, 250.0)
        self.assertFalse(events[1].pays_cash)

    def test_rounding_slack_still_finds_the_redemption(self):
        # RUS-30 repays a nominal of 1.00 in one-kopeck steps and overshoots to 1.08, so the
        # "fully repaid" test cannot be an equality.
        payloads = [amortization_payload(2026, 9, 30, 0.5, initial=1.0),
                    amortization_payload(2027, 3, 31, 0.49, initial=1.0)]
        events = normalize_events(parse_bond_events(payloads, self.bond), face_value=1.0)
        self.assertEqual(events[-1].event_type, BondEventType.MATURITY)

    def test_coupons_are_left_alone(self):
        payloads = [coupon_payload(2024, 5, 15), OFZ_REDEMPTION]
        events = normalize_events(parse_bond_events(payloads, self.bond), face_value=1000.0)
        coupons = [e for e in events if e.event_type is BondEventType.COUPON]
        self.assertEqual(len(coupons), 1)
        self.assertTrue(coupons[0].pays_cash)


class BuildBondTests(unittest.TestCase):
    """Assembling the listing, the calendar and the specification into a tradeable bond."""

    def setUp(self):
        # A whole calendar: past and future concatenated, ending in the redemption -- which is
        # what FinamClient.bond_events returns.
        self.events = [coupon_payload(2023, 5, 15), coupon_payload(2023, 11, 15),
                       coupon_payload(2024, 5, 15), coupon_payload(2024, 11, 15)]
        self.full = self.events + [OFZ_REDEMPTION]

    def test_a_bond_is_built_from_a_listing_and_its_calendar(self):
        bond, events = build_bond(LISTING, events=self.events)

        self.assertEqual(bond.asset_name, "SU26238RMFS4")
        self.assertEqual(bond.isin, "RU000A1038V6")
        self.assertAlmostEqual(bond.face_value, 1000.0)
        self.assertEqual(bond.coupon_frequency, 2)
        self.assertAlmostEqual(bond.coupon_rate, 0.0825)
        self.assertEqual(bond.price_quotation, PriceQuotation.PERCENT_OF_FACE)
        self.assertEqual(len(events), 4)

    def test_every_event_points_at_the_finished_bond(self):
        bond, events = build_bond(LISTING, events=self.events)
        for event in events:
            self.assertIs(event.asset, bond)

    def test_trading_stops_before_the_issuer_repays(self):
        bond, _ = build_bond(LISTING, events=self.events)
        self.assertEqual(bond.auto_close_date, bond.maturity_date)
        self.assertLess(bond.end_date, bond.maturity_date)

    def test_the_bonds_life_starts_at_the_earliest_coupon_period(self):
        bond, _ = build_bond(LISTING, events=self.events)
        # The first coupon covers a period that began before it was paid.
        self.assertEqual(bond.start_date, datetime.date(2022, 11, 15))

    def test_a_bond_with_no_calendar_is_skipped(self):
        # Every archived issue looks like this: both endpoints answer with nothing.
        self.assertIsNone(build_bond(LISTING, events=[]))

    def test_the_redemption_sets_the_maturity_date(self):
        bond, events = build_bond(LISTING, events=self.full)

        self.assertEqual(bond.maturity_date, datetime.date(2041, 5, 15))
        redemptions = [e for e in events if e.event_type is BondEventType.MATURITY]
        self.assertEqual(len(redemptions), 1)
        self.assertEqual(redemptions[0].date, bond.maturity_date)

    def test_a_bullet_issue_is_not_marked_as_amortizing(self):
        bond, _ = build_bond(LISTING, events=self.full)
        self.assertFalse(bond.is_amortized)

    def test_the_currency_comes_from_the_payments(self):
        bond, _ = build_bond(LISTING, events=self.full)
        self.assertEqual(bond.quote_currency, "RUB")

    def test_a_dollar_issue_is_recognised(self):
        dollar_events = [{**coupon_payload(2024, 5, 15), "currency": "$"},
                         {**coupon_payload(2024, 11, 15), "currency": "$"},
                         {**OFZ_REDEMPTION, "currency": "$"}]
        bond, _ = build_bond(LISTING, events=dollar_events)
        self.assertEqual(bond.quote_currency, "USD")

    def test_the_specification_supplies_a_maturity_the_calendar_lacks(self):
        bond, _ = build_bond(LISTING, events=self.events,
                             details={"maturity_date": date(2041, 5, 15)})
        self.assertEqual(bond.maturity_date, datetime.date(2041, 5, 15))


if __name__ == "__main__":
    unittest.main(verbosity=2)
