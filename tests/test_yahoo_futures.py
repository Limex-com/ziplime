"""The Yahoo futures specification table and contract-chain ticker handling.

Yahoo publishes prices for individual dated contracts and **no contract specification at all** --
no multiplier, no tick size, no settlement type. Those come from the table in
`ziplime.data.data_sources.yahoo.yahoo_futures`, transcribed from the exchanges' published terms.

A wrong multiplier passes every type check and silently misstates every position by a factor of a
hundred or a thousand, so the table is checked against the figures a trader would recognise: the
notional one contract represents at a realistic price, and the value of one tick. Those two are
published by the exchange and are what the table has to reproduce.

The rest of the module builds and reads the tickers of a chain -- ``CLX26.NYM`` -- which is how a
chain gets discovered at all, since Yahoo has no endpoint that lists one.

Nothing here touches the network.
"""
import datetime
import unittest

from ziplime.assets.domain.settlement_type import SettlementType
from ziplime.data.data_sources.yahoo.yahoo_futures import (
    GRAIN, MONTH_CODES, MONTHLY, QUARTERLY, ROOTS_BY_CONTINUOUS_TICKER, YAHOO_FUTURES_ROOTS,
    candidate_tickers, contract_ticker, fallback_expiry, is_continuous_ticker, parse_expiry,
    parse_contract_ticker, root_for_continuous, root_for_contract,
)

#: Realistic price and the figures the exchange publishes at it. Sourced from the contract
#: specifications, not from this table, so that agreement is a real check rather than a tautology.
#:
#:   root: (price, notional at that price, value of one tick)
PUBLISHED = {
    "ES": (5_916.00, 295_800.00, 12.50),     # E-mini S&P 500: $50 x index, 0.25 tick
    "NQ": (21_000.00, 420_000.00, 5.00),     # E-mini Nasdaq 100: $20 x index, 0.25 tick
    "CL": (60.79, 60_790.00, 10.00),         # WTI: 1 000 barrels, $0.01 tick
    "NG": (3.45, 34_500.00, 10.00),          # Henry Hub: 10 000 MMBtu, $0.001 tick
    "GC": (3_288.90, 328_890.00, 10.00),     # Gold: 100 troy oz, $0.10 tick
    "SI": (32.89, 164_450.00, 25.00),        # Silver: 5 000 troy oz, $0.005 tick
    "ZC": (444.00, 22_200.00, 12.50),        # Corn: 5 000 bu quoted in cents, 0.25 tick
    "6E": (1.14, 142_500.00, 6.25),          # Euro FX: EUR 125 000, 0.00005 tick
}

#: The exchange suffix Yahoo uses per root, and the listing cycle the exchange publishes.
LISTINGS = {
    "ES": (".CME", QUARTERLY), "NQ": (".CME", QUARTERLY), "6E": (".CME", QUARTERLY),
    "CL": (".NYM", MONTHLY), "NG": (".NYM", MONTHLY),
    "GC": (".CMX", MONTHLY), "SI": (".CMX", MONTHLY),
    "ZC": (".CBT", GRAIN),
}


class SpecificationTableTests(unittest.TestCase):
    def test_every_root_reproduces_its_published_notional(self):
        # The check that catches a wrong power of ten, which is the failure mode that matters.
        for root_symbol, (price, notional, _) in PUBLISHED.items():
            root = YAHOO_FUTURES_ROOTS[root_symbol]
            self.assertAlmostEqual(price * root.multiplier, notional, places=2,
                                   msg=f"{root_symbol} multiplier misstates the contract")

    def test_every_root_reproduces_its_published_tick_value(self):
        for root_symbol, (_, _, tick_value) in PUBLISHED.items():
            root = YAHOO_FUTURES_ROOTS[root_symbol]
            self.assertAlmostEqual(root.tick_size * root.multiplier, tick_value, places=4,
                                   msg=f"{root_symbol} tick value is wrong")

    def test_the_table_covers_every_root_the_tests_know(self):
        self.assertEqual(set(PUBLISHED), set(YAHOO_FUTURES_ROOTS),
                         "a root was added or removed without updating the published figures")

    def test_index_futures_are_cash_settled(self):
        # An index cannot be delivered, so these must never be marked deliverable.
        for root_symbol in ("ES", "NQ"):
            self.assertIs(YAHOO_FUTURES_ROOTS[root_symbol].settlement_type, SettlementType.CASH)

    def test_commodity_futures_are_deliverable(self):
        for root_symbol in ("CL", "NG", "GC", "SI", "ZC"):
            self.assertTrue(YAHOO_FUTURES_ROOTS[root_symbol].settlement_type.is_deliverable,
                            f"{root_symbol} should be physically delivered")

    def test_every_root_has_a_positive_multiplier_and_tick(self):
        for root_symbol, root in YAHOO_FUTURES_ROOTS.items():
            self.assertGreater(root.multiplier, 0, root_symbol)
            self.assertGreater(root.tick_size, 0, root_symbol)

    def test_every_root_declares_a_currency_and_a_venue(self):
        for root_symbol, root in YAHOO_FUTURES_ROOTS.items():
            self.assertRegex(root.quote_currency, r"^[A-Z]{3}$", root_symbol)
            self.assertRegex(root.mic, r"^X[A-Z]{3}$", root_symbol)

    def test_every_root_declares_its_exchange_suffix_and_cycle(self):
        # The suffix and the cycle together decide which tickers get probed, so a wrong one means
        # the chain is simply never found.
        for root_symbol, (suffix, cycle) in LISTINGS.items():
            root = YAHOO_FUTURES_ROOTS[root_symbol]
            self.assertEqual(root.exchange_suffix, suffix, root_symbol)
            self.assertEqual(root.contract_months, cycle, root_symbol)

    def test_every_listing_cycle_uses_real_month_codes(self):
        for root_symbol, root in YAHOO_FUTURES_ROOTS.items():
            self.assertTrue(root.contract_months, root_symbol)
            for code in root.contract_months:
                self.assertIn(code, MONTH_CODES, f"{root_symbol} lists a non-existent month")

    def test_the_continuous_ticker_index_matches_the_table(self):
        self.assertEqual({r.continuous_ticker for r in YAHOO_FUTURES_ROOTS.values()},
                         set(ROOTS_BY_CONTINUOUS_TICKER))

    def test_unreliable_continuous_volume_is_flagged(self):
        # Yahoo reports a few hundred contracts a day for the metals on the continuous series,
        # which is not the real figure. A volume-share slippage model on that is meaningless.
        self.assertFalse(YAHOO_FUTURES_ROOTS["GC"].reliable_continuous_volume)
        self.assertFalse(YAHOO_FUTURES_ROOTS["SI"].reliable_continuous_volume)
        self.assertTrue(YAHOO_FUTURES_ROOTS["ES"].reliable_continuous_volume)


class ContinuousTickerTests(unittest.TestCase):
    def test_a_continuous_ticker_is_recognised(self):
        self.assertTrue(is_continuous_ticker("ES=F"))
        self.assertFalse(is_continuous_ticker("ESZ26.CME"))
        self.assertFalse(is_continuous_ticker("AAPL"))

    def test_a_known_continuous_ticker_resolves_to_its_root(self):
        self.assertIs(root_for_continuous("CL=F"), YAHOO_FUTURES_ROOTS["CL"])

    def test_an_unknown_continuous_ticker_resolves_to_nothing(self):
        # Deliberately None rather than a default: without a multiplier the contract cannot be
        # priced, and assuming 1.0 would misstate every position silently.
        self.assertIsNone(root_for_continuous("XYZ=F"))


class ContractTickerTests(unittest.TestCase):
    def test_a_contract_ticker_is_built_from_root_month_and_year(self):
        self.assertEqual(contract_ticker(YAHOO_FUTURES_ROOTS["CL"], "X", 2026), "CLX26.NYM")
        self.assertEqual(contract_ticker(YAHOO_FUTURES_ROOTS["ES"], "Z", 2026), "ESZ26.CME")
        self.assertEqual(contract_ticker(YAHOO_FUTURES_ROOTS["ZC"], "H", 2027), "ZCH27.CBT")

    def test_a_single_digit_year_keeps_two_places(self):
        # 2030 is "30"; 2005 would be "05". Dropping the zero produces a ticker Yahoo rejects.
        self.assertEqual(contract_ticker(YAHOO_FUTURES_ROOTS["CL"], "F", 2030), "CLF30.NYM")
        self.assertEqual(contract_ticker(YAHOO_FUTURES_ROOTS["CL"], "F", 2005), "CLF05.NYM")

    def test_a_contract_ticker_round_trips(self):
        parsed = parse_contract_ticker("CLX26.NYM")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.root_symbol, "CL")
        self.assertEqual(parsed.month_code, "X")
        self.assertEqual(parsed.year, 2026)
        self.assertEqual(parsed.delivery_month, 11)

    def test_the_month_code_maps_to_the_delivery_month(self):
        # F is January and Z is December; the letters are not alphabetical by month.
        self.assertEqual(parse_contract_ticker("CLF27.NYM").delivery_month, 1)
        self.assertEqual(parse_contract_ticker("CLZ26.NYM").delivery_month, 12)
        self.assertEqual(parse_contract_ticker("CLM27.NYM").delivery_month, 6)

    def test_things_that_are_not_contract_tickers_parse_to_nothing(self):
        for ticker in ("ES=F", "AAPL", "CL.NYM", "CLX2026.NYM", "CLX26", "XYZX26.NYM",
                       "CL126.NYM", "CLXAB.NYM"):
            self.assertIsNone(parse_contract_ticker(ticker), ticker)

    def test_a_contract_ticker_resolves_to_its_specification(self):
        self.assertIs(root_for_contract("CLX26.NYM"), YAHOO_FUTURES_ROOTS["CL"])
        self.assertIs(root_for_contract("ESZ26.CME"), YAHOO_FUTURES_ROOTS["ES"])
        self.assertIsNone(root_for_contract("AAPL"))


class CandidateChainTests(unittest.TestCase):
    def test_a_monthly_root_yields_one_candidate_per_month(self):
        candidates = candidate_tickers(YAHOO_FUTURES_ROOTS["CL"],
                                       start=datetime.date(2026, 9, 1), months_ahead=3)
        self.assertEqual(candidates,
                         ["CLU26.NYM", "CLV26.NYM", "CLX26.NYM", "CLZ26.NYM"])

    def test_a_quarterly_root_yields_only_its_cycle_months(self):
        candidates = candidate_tickers(YAHOO_FUTURES_ROOTS["ES"],
                                       start=datetime.date(2026, 9, 1), months_ahead=12)
        self.assertEqual(candidates,
                         ["ESU26.CME", "ESZ26.CME", "ESH27.CME", "ESM27.CME", "ESU27.CME"])

    def test_a_grain_root_yields_only_the_grain_cycle(self):
        candidates = candidate_tickers(YAHOO_FUTURES_ROOTS["ZC"],
                                       start=datetime.date(2026, 9, 1), months_ahead=12)
        self.assertEqual(candidates,
                         ["ZCU26.CBT", "ZCZ26.CBT", "ZCH27.CBT", "ZCK27.CBT", "ZCN27.CBT",
                          "ZCU27.CBT"])

    def test_candidates_cross_the_year_boundary(self):
        candidates = candidate_tickers(YAHOO_FUTURES_ROOTS["CL"],
                                       start=datetime.date(2026, 11, 1), months_ahead=2)
        self.assertEqual(candidates, ["CLX26.NYM", "CLZ26.NYM", "CLF27.NYM"])

    def test_candidates_come_out_in_expiry_order(self):
        candidates = candidate_tickers(YAHOO_FUTURES_ROOTS["CL"],
                                       start=datetime.date(2026, 1, 1), months_ahead=24)
        parsed = [parse_contract_ticker(t) for t in candidates]
        keys = [(p.year, p.delivery_month) for p in parsed]
        self.assertEqual(keys, sorted(keys), "the chain must come out front-month first")

    def test_every_candidate_parses_back(self):
        for root in YAHOO_FUTURES_ROOTS.values():
            for ticker in candidate_tickers(root, start=datetime.date(2026, 9, 1),
                                            months_ahead=18):
                parsed = parse_contract_ticker(ticker)
                self.assertIsNotNone(parsed, ticker)
                self.assertEqual(parsed.root_symbol, root.root_symbol, ticker)


class ExpiryTests(unittest.TestCase):
    def test_a_unix_timestamp_becomes_a_date(self):
        # 1789689600 is 2026-09-18, the E-mini expiry Yahoo reported while this was written.
        self.assertEqual(parse_expiry({"expireDate": 1789689600}), datetime.date(2026, 9, 18))

    def test_a_missing_expiry_is_none(self):
        self.assertIsNone(parse_expiry({}))
        self.assertIsNone(parse_expiry({"expireDate": None}))

    def test_an_unusable_expiry_is_none_rather_than_a_crash(self):
        self.assertIsNone(parse_expiry({"expireDate": "not a timestamp"}))

    def test_the_fallback_expiry_for_an_index_is_the_third_friday(self):
        # The convention for cash-settled index futures. December 2026: 4, 11, 18, 25 are Fridays.
        self.assertEqual(fallback_expiry(YAHOO_FUTURES_ROOTS["ES"], "Z", 2026),
                         datetime.date(2026, 12, 18))
        self.assertEqual(datetime.date(2026, 12, 18).weekday(), 4)

    def test_the_fallback_expiry_for_a_commodity_is_mid_month(self):
        self.assertEqual(fallback_expiry(YAHOO_FUTURES_ROOTS["CL"], "X", 2026),
                         datetime.date(2026, 11, 20))

    def test_the_fallback_expiry_lands_in_the_delivery_month(self):
        # It is only a convention, but it must at least put the contract in the right month, or
        # the chain comes out in the wrong order.
        for root in YAHOO_FUTURES_ROOTS.values():
            for code in root.contract_months:
                expiry = fallback_expiry(root, code, 2027)
                parsed = parse_contract_ticker(contract_ticker(root, code, 2027))
                self.assertEqual(expiry.month, parsed.delivery_month,
                                 f"{root.root_symbol}{code}27")
                self.assertEqual(expiry.year, 2027)


if __name__ == "__main__":
    unittest.main(verbosity=2)
