import datetime
import unittest
from types import SimpleNamespace

from tests.exchanges.test_simulation_exchange import make_exchange
from ziplime.exchanges.allocation import Holding, allocated_cash, round_to_lots


class LiveVenueDelistingTests(unittest.TestCase):
    bundle_end = datetime.datetime(2026, 9, 16)

    def exchange_with_bundle(self, live: bool):
        exchange = make_exchange()
        exchange.data_source = SimpleNamespace(last_available_bar=lambda sid=None: self.bundle_end)
        exchange.live = live
        return exchange

    def test_a_simulated_venue_ends_where_its_bundle_ends(self):
        self.assertEqual(self.exchange_with_bundle(live=False).last_available_bar(1), self.bundle_end)

    def test_a_live_venue_has_no_opinion_even_with_a_bundle_behind_it(self):
        # Taking the warm-up bundle's end for the instrument's made the engine close
        # a live position as delisted and never trade it again.
        exchange = self.exchange_with_bundle(live=True)
        self.assertIsNone(exchange.last_available_bar(1))
        self.assertIsNone(exchange.last_available_bar())

    def test_venues_are_simulated_unless_they_say_otherwise(self):
        self.assertFalse(make_exchange().live)


class AllocationTests(unittest.TestCase):
    def test_cash_is_the_allocation_minus_what_the_positions_cost(self):
        holdings = [Holding("SBER@MISX", 100, 280.0), Holding("GAZP@MISX", 50, 120.0)]
        self.assertEqual(allocated_cash(capital=100_000, holdings=holdings), 66_000)

    def test_no_positions_leaves_the_whole_allocation(self):
        self.assertEqual(allocated_cash(capital=2_000, holdings=[]), 2_000)

    def test_positions_costing_more_than_the_allocation_leave_negative_cash(self):
        # The strategy then sells down; capping at zero would hide the overweight.
        self.assertEqual(allocated_cash(capital=20_000, holdings=[Holding("SBER@MISX", 100, 280.0)]),
                         -8_000)

    def test_orders_round_toward_zero_to_whole_lots(self):
        for quantity, lot, expected in ((6, 10, 0), (17, 10, 10), (-6, 10, 0), (-25, 10, -20),
                                        (1865, 1, 1865), (7, 0, 7)):
            with self.subTest(quantity=quantity, lot=lot):
                self.assertEqual(round_to_lots(quantity, lot), expected)


if __name__ == "__main__":
    unittest.main()
