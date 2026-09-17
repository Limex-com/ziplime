"""The vector kernel's own semantics, pinned so they cannot drift.

Nothing here imports vectorbt. That is the point of the file: these are ziplime's answers to
questions vectorbt also answers, and several of them are deliberately *different* answers -- a
repeated entry, a conflicting entry-and-exit, an order that does not fit. A test that only
compared against vectorbt could not tell a decision from an accident, so the decisions are written
down here and the comparison lives in `tests/vectorized/reference/vectorbt/`.

The case numbering follows the specification's §28.
"""
import datetime
import unittest

import numpy as np
import pandas as pd

from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.commission.per_dolar import PerDollar
from ziplime.finance.commission.per_share import PerShare
from ziplime.finance.commission.per_trade import PerTrade
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
from ziplime.finance.slippage.fixed_slippage import FixedSlippage
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.vectorized.kernel import (
    VECTOR_KERNEL_VERSION, ExecutionTiming, VectorKernelError, simulate_signals,
)

TZ = "America/New_York"


def bars(n: int, start: str = "2024-01-02") -> pd.DatetimeIndex:
    return pd.date_range(start, periods=n, freq="D", tz=TZ)


def frame(index, columns, value=False) -> pd.DataFrame:
    return pd.DataFrame(value, index=index, columns=list(columns))


def flag(index, columns, **rows) -> pd.DataFrame:
    """A boolean frame with named bars set. `flag(idx, "AB", A=[1, 3])` sets A on bars 1 and 3."""
    out = frame(index, columns)
    for column, positions in rows.items():
        for position in positions:
            out.iloc[position, out.columns.get_loc(column)] = True
    return out


def ramp(index, columns: dict[str, float]) -> pd.DataFrame:
    """Deterministic straight-line prices. A test that fails on some seeds is not a test."""
    steps = np.arange(len(index), dtype=float)
    return pd.DataFrame({name: base + steps for name, base in columns.items()}, index=index)


class BasicTests(unittest.TestCase):
    """§28 cases 1-5."""

    def setUp(self):
        self.index = bars(8)
        self.prices = ramp(self.index, {"A": 10.0})

    def run_it(self, entries, exits, **kwargs):
        kwargs.setdefault("execution", ExecutionTiming.same_close())
        return simulate_signals(prices=self.prices, entries=entries, exits=exits,
                                size=10, initial_cash=10_000.0, **kwargs)

    def test_case_1_flat_to_long_to_flat(self):
        result = self.run_it(flag(self.index, "A", A=[1]), flag(self.index, "A", A=[4]))
        fills = result.fills_frame()
        self.assertEqual(list(fills["intent"]), ["open_long", "close_long"])
        self.assertEqual(list(fills["amount"]), [10.0, -10.0])
        self.assertEqual(float(result.positions["A"].iloc[-1]), 0.0)

    def test_case_2_flat_to_short_to_flat(self):
        result = self.run_it(frame(self.index, "A"), frame(self.index, "A"),
                             short_entries=flag(self.index, "A", A=[1]),
                             short_exits=flag(self.index, "A", A=[4]), allow_short=True)
        fills = result.fills_frame()
        self.assertEqual(list(fills["intent"]), ["open_short", "close_short"])
        self.assertEqual(list(fills["amount"]), [-10.0, 10.0])
        self.assertEqual(float(result.positions["A"].iloc[-1]), 0.0)

    def test_case_3_a_position_is_held_across_bars(self):
        result = self.run_it(flag(self.index, "A", A=[1]), flag(self.index, "A", A=[6]))
        held = result.positions["A"].to_numpy()
        np.testing.assert_array_equal(held, [0, 10, 10, 10, 10, 10, 0, 0])

    def test_case_4_a_repeated_entry_is_ignored_by_default(self):
        """Not vectorbt's choice inherited -- ziplime's, and the reason is in `accumulate`'s
        docstring: `fast > slow` is true for a run of bars, and accumulating on each would turn an
        ordinary crossover into a position that grows until the account is gone."""
        result = self.run_it(flag(self.index, "A", A=[1, 2, 3]), frame(self.index, "A"))
        self.assertEqual(len(result.fills), 1)
        # Counted, not recorded: a repeat is a no-op rather than a refusal, and with level
        # signals there are many times more of them than there are fills.
        self.assertEqual(result.no_ops, 2)
        self.assertEqual(result.rejects, [])

    def test_case_4b_accumulate_adds_to_the_position(self):
        result = self.run_it(flag(self.index, "A", A=[1, 2, 3]), frame(self.index, "A"),
                             accumulate=True)
        self.assertEqual(len(result.fills), 3)
        self.assertEqual(float(result.positions["A"].iloc[3]), 30.0)

    def test_case_5_a_repeated_exit_is_a_no_op_not_a_refusal(self):
        """Flat already is what the exit asks for, and `fast < slow` is true for runs of bars --
        so these are counted rather than recorded, the same way repeated entries are."""
        result = self.run_it(flag(self.index, "A", A=[1]), flag(self.index, "A", A=[4, 5]))
        self.assertEqual(len(result.fills), 2)
        self.assertEqual(result.no_ops, 1)
        self.assertEqual(result.rejects, [])

    def test_an_exit_with_no_position_at_all_never_opens_a_short(self):
        """The dangerous reading of `exits`: an exit that opens a short is a strategy nobody
        wrote. Short positions are opened by `short_entries`, and only with `allow_short`."""
        result = self.run_it(frame(self.index, "A"), flag(self.index, "A", A=[2]))
        self.assertEqual(result.fills, [])
        self.assertEqual(result.no_ops, 1)

    def test_closing_a_position_held_on_the_other_side_is_a_real_refusal(self):
        """The half that stays a rejection: flat is the exit's own goal, but being short while
        asking to close a long is a mismatch, and burying it under the no-ops would hide it."""
        result = self.run_it(frame(self.index, "A"), flag(self.index, "A", A=[4]),
                             short_entries=flag(self.index, "A", A=[1]), allow_short=True)
        self.assertEqual([f.intent for f in result.fills], ["open_short"])
        self.assertEqual([r.reason for r in result.rejects], ["nothing_to_close"])
        self.assertIn("other side", result.rejects[0].detail)


class ReversalTests(unittest.TestCase):
    """§28 cases 6-7, and §14's requirement that a reversal is two transactions."""

    def setUp(self):
        self.index = bars(6)
        self.prices = ramp(self.index, {"A": 100.0})

    def test_case_6_long_to_short_is_a_close_then_an_open(self):
        result = simulate_signals(
            prices=self.prices, entries=flag(self.index, "A", A=[1]),
            exits=flag(self.index, "A", A=[3]),
            short_entries=flag(self.index, "A", A=[3]), allow_short=True,
            size=10, initial_cash=100_000.0, execution=ExecutionTiming.same_close())
        fills = result.fills_frame()
        self.assertEqual(list(fills["intent"]), ["open_long", "close_long", "open_short"])
        # Two transactions on the reversal bar, in that order, so a per-trade commission is
        # charged twice -- which is what actually happens at a broker.
        same_bar = fills[fills["timestamp"] == self.index[3]]
        self.assertEqual(len(same_bar), 2)
        self.assertLess(same_bar["sequence"].iloc[0], same_bar["sequence"].iloc[1])
        self.assertEqual(float(result.positions["A"].iloc[3]), -10.0)

    def test_case_7_short_to_long_is_a_cover_then_an_open(self):
        result = simulate_signals(
            prices=self.prices, entries=flag(self.index, "A", A=[3]),
            exits=frame(self.index, "A"),
            short_entries=flag(self.index, "A", A=[1]),
            short_exits=flag(self.index, "A", A=[3]), allow_short=True,
            size=10, initial_cash=100_000.0, execution=ExecutionTiming.same_close())
        fills = result.fills_frame()
        self.assertEqual(list(fills["intent"]), ["open_short", "close_short", "open_long"])
        self.assertEqual(float(result.positions["A"].iloc[3]), 10.0)

    def test_a_reversal_pays_two_commissions(self):
        result = simulate_signals(
            prices=self.prices, entries=flag(self.index, "A", A=[1]),
            exits=flag(self.index, "A", A=[3]),
            short_entries=flag(self.index, "A", A=[3]), allow_short=True,
            size=10, initial_cash=100_000.0, execution=ExecutionTiming.same_close(),
            commission=PerTrade(cost=1.0))
        on_reversal = [f for f in result.fills if f.timestamp == self.index[3]]
        self.assertEqual([f.commission for f in on_reversal], [1.0, 1.0])

    def test_shorting_without_permission_is_refused(self):
        with self.assertRaises(VectorKernelError) as caught:
            simulate_signals(prices=self.prices, entries=frame(self.index, "A"),
                             exits=frame(self.index, "A"),
                             short_entries=flag(self.index, "A", A=[1]))
        self.assertIn("allow_short", str(caught.exception))


class MultiAssetTests(unittest.TestCase):
    """§28 cases 8-11, and the ordering policy of §12."""

    def setUp(self):
        self.index = bars(6)
        self.prices = pd.DataFrame({"A": [10.0] * 6, "B": [20.0] * 6}, index=self.index)

    def run_it(self, entries, exits, cash, **kwargs):
        return simulate_signals(prices=self.prices, entries=entries, exits=exits, size=100,
                                initial_cash=cash, execution=ExecutionTiming.same_close(),
                                **kwargs)

    def test_case_8_simultaneous_buys_with_enough_cash_both_fill(self):
        result = self.run_it(flag(self.index, "AB", A=[1], B=[1]), frame(self.index, "AB"),
                             cash=10_000.0)
        self.assertEqual(len(result.fills), 2)
        self.assertEqual(float(result.cash.iloc[1]), 10_000.0 - 1_000.0 - 2_000.0)

    def test_case_9_insufficient_cash_rejects_whole_orders_in_a_fixed_order(self):
        """A + B costs 3,000; there is 2,500. A is filled, B is rejected whole -- not shrunk to
        what was left, which would be a different strategy nobody asked for."""
        result = self.run_it(flag(self.index, "AB", A=[1], B=[1]), frame(self.index, "AB"),
                             cash=2_500.0)
        self.assertEqual([f.instrument for f in result.fills], ["A"])
        self.assertEqual([(r.instrument, r.reason) for r in result.rejects],
                         [("B", "insufficient_cash")])
        self.assertGreaterEqual(float(result.cash.min()), 0.0)

    def test_case_10_a_buy_and_a_sell_on_one_bar(self):
        # `|`, not `combine_first`: False is not NA, so combining two boolean frames that way
        # keeps the caller's False and silently drops the other frame's True.
        entries = flag(self.index, "AB", A=[1]) | flag(self.index, "AB", B=[2])
        exits = flag(self.index, "AB", A=[2])
        result = simulate_signals(
            prices=self.prices, entries=entries, exits=exits, size=100, initial_cash=5_000.0,
            execution=ExecutionTiming.same_close())
        on_bar = [f for f in result.fills if f.timestamp == self.index[2]]
        # The close comes first: it releases the cash the open then spends.
        self.assertEqual([(f.instrument, f.intent) for f in on_bar],
                         [("A", "close_long"), ("B", "open_long")])

    def test_case_11_exits_are_processed_before_entries_across_instruments(self):
        """The policy of §12, and the reason for it: a fully-invested book has to be able to
        rotate without holding a cash buffer it was never asked to hold."""
        entries = flag(self.index, "AB", A=[1])
        exits = frame(self.index, "AB")
        first = simulate_signals(prices=self.prices, entries=entries, exits=exits, size=100,
                                 initial_cash=1_000.0, execution=ExecutionTiming.same_close())
        self.assertEqual(len(first.fills), 1)

        # Fully invested at bar 1, then rotated at bar 3 with no spare cash: A releases exactly
        # what B costs, so B only fills if the close was processed first. Sized per instrument
        # (A at 10, B at 20) so the two legs cost the same 500.
        rotate_entries = flag(self.index, "AB", A=[1], B=[3])
        rotate_exits = flag(self.index, "AB", A=[3])
        rotated = simulate_signals(prices=self.prices, entries=rotate_entries, exits=rotate_exits,
                                   size=pd.Series({"A": 50.0, "B": 25.0}), initial_cash=500.0,
                                   execution=ExecutionTiming.same_close())
        intents = [(f.instrument, f.intent) for f in rotated.fills]
        self.assertEqual(intents, [("A", "open_long"), ("A", "close_long"), ("B", "open_long")])
        self.assertEqual(float(rotated.cash.iloc[-1]), 0.0)

    def test_the_instrument_order_is_the_callers_column_order(self):
        """Not sorted, not hashed: whatever order the caller handed in, which is the only one
        they can predict."""
        reversed_prices = self.prices[["B", "A"]]
        result = simulate_signals(
            prices=reversed_prices, entries=flag(self.index, "BA", A=[1], B=[1]),
            exits=frame(self.index, "BA"), size=100, initial_cash=10_000.0,
            execution=ExecutionTiming.same_close())
        self.assertEqual([f.instrument for f in result.fills], ["B", "A"])

    def test_cash_sharing_false_is_refused_rather_than_guessed(self):
        with self.assertRaises(VectorKernelError) as caught:
            simulate_signals(prices=self.prices, entries=flag(self.index, "AB", A=[1]),
                             exits=frame(self.index, "AB"), cash_sharing=False)
        self.assertIn("cash_sharing=False", str(caught.exception))


class DeterminismTests(unittest.TestCase):
    """§11. The requirement that makes the rest of the file mean anything."""

    def setUp(self):
        self.index = bars(12)
        # Flat, so the cash arithmetic is readable: at size 100 the four orders cost 1,000,
        # 2,000, 3,000 and 4,000 against a 3,000 balance, and which of them fit is obvious.
        self.prices = pd.DataFrame(
            {"A": [10.0] * 12, "B": [20.0] * 12, "C": [30.0] * 12, "D": [40.0] * 12},
            index=self.index)

    def run_it(self, columns):
        prices = self.prices[list(columns)]
        entries = flag(prices.index, columns, **{c: [1, 5] for c in columns})
        exits = flag(prices.index, columns, **{c: [8] for c in columns})
        return simulate_signals(prices=prices, entries=entries, exits=exits, size=100,
                                initial_cash=3_000.0, execution=ExecutionTiming.same_close())

    def test_the_same_inputs_give_the_same_fills_every_time(self):
        runs = [self.run_it("ABCD") for _ in range(5)]
        signature = [[(f.instrument, f.sequence, f.amount, f.price) for f in run.fills]
                     for run in runs]
        for other in signature[1:]:
            self.assertEqual(signature[0], other)

    def test_who_gets_the_last_of_the_cash_is_decided_by_column_order(self):
        """Under-funded on purpose, so the constraint actually binds. The answer has to come from
        the caller's column order rather than from whatever order a dict happened to iterate."""
        result = self.run_it("ABCD")
        # Scoped to the first signal bar: the entries fire again later, and with the book already
        # full the same two names are refused a second time. Asserting the whole list would be
        # asserting how often the strategy signalled, not what the ordering policy decided.
        first_bar = self.index[1]
        filled = [f.instrument for f in result.fills
                  if f.intent == "open_long" and f.timestamp == first_bar]
        rejected = [r.instrument for r in result.rejects
                    if r.reason == "insufficient_cash" and r.timestamp == first_bar]
        # A takes 1,000 and B the remaining 2,000; C and D find nothing left.
        self.assertEqual(filled, ["A", "B"])
        self.assertEqual(rejected, ["C", "D"])

    def test_reordering_the_columns_reorders_the_winners(self):
        """The other half: the policy is column order, so changing it changes the outcome. A test
        that only asserted determinism would pass on an implementation that always sorted."""
        result = self.run_it("DCBA")
        filled = [f.instrument for f in result.fills
                  if f.intent == "open_long" and f.timestamp == self.index[1]]
        # D alone costs 4,000 and cannot fill at all; C takes the whole 3,000, and the two that
        # would have fitted under ABCD now get nothing. Same cash, same signals, different answer.
        self.assertEqual(filled, ["C"])

    def test_the_sequence_numbers_are_dense_and_ascending(self):
        result = self.run_it("ABCD")
        self.assertEqual([f.sequence for f in result.fills], list(range(len(result.fills))))


class CostTests(unittest.TestCase):
    """§28 cases 12-16, and §15's requirement that these are ziplime's models."""

    def setUp(self):
        self.index = bars(6)
        self.prices = pd.DataFrame({"A": [100.0] * 6}, index=self.index)
        self.entries = flag(self.index, "A", A=[1])
        self.exits = flag(self.index, "A", A=[3])

    def run_it(self, **kwargs):
        return simulate_signals(prices=self.prices, entries=self.entries, exits=self.exits,
                                size=10, initial_cash=100_000.0,
                                execution=ExecutionTiming.same_close(), **kwargs)

    def test_case_12_no_fees(self):
        result = self.run_it()
        self.assertEqual([f.commission for f in result.fills], [0.0, 0.0])
        self.assertEqual(float(result.cash.iloc[-1]), 100_000.0)

    def test_case_13_a_proportional_commission_is_charged_per_fill(self):
        result = self.run_it(commission=PerShare(cost=0.01, min_trade_cost=0.0))
        self.assertEqual([f.commission for f in result.fills], [0.10, 0.10])
        self.assertAlmostEqual(float(result.cash.iloc[-1]), 100_000.0 - 0.20, places=9)

    def test_case_13b_a_percentage_of_value_commission(self):
        result = self.run_it(commission=PerDollar(cost=0.001))
        self.assertEqual([round(f.commission, 9) for f in result.fills], [1.0, 1.0])

    def test_case_14_a_fixed_commission_is_charged_once_per_fill(self):
        result = self.run_it(commission=PerTrade(cost=5.0))
        self.assertEqual([f.commission for f in result.fills], [5.0, 5.0])

    def test_case_15_slippage_moves_the_fill_away_from_the_close(self):
        result = self.run_it(slippage=FixedBasisPointsSlippage(basis_points=10.0))
        buy, sell = result.fills
        self.assertAlmostEqual(buy.price, 100.0 * 1.001, places=9)
        self.assertAlmostEqual(sell.price, 100.0 * 0.999, places=9)

    def test_case_15b_a_fixed_spread(self):
        result = self.run_it(slippage=FixedSlippage(spread=0.10))
        buy, sell = result.fills
        self.assertAlmostEqual(buy.price, 100.05, places=9)
        self.assertAlmostEqual(sell.price, 99.95, places=9)

    def test_case_16_commission_and_slippage_together(self):
        result = self.run_it(commission=PerShare(cost=0.01, min_trade_cost=0.0),
                             slippage=FixedBasisPointsSlippage(basis_points=10.0))
        buy, sell = result.fills
        self.assertAlmostEqual(buy.price, 100.1, places=9)
        self.assertEqual(buy.commission, 0.10)
        expected = 100_000.0 - (10 * 100.1 + 0.10) + (10 * 99.9 - 0.10)
        self.assertAlmostEqual(float(result.cash.iloc[-1]), expected, places=9)

    def test_a_commission_is_paid_out_of_cash_not_just_recorded(self):
        """The mistake this guards is the one the replay adapter had to fix separately: recording
        a fee on the transaction without moving the cash leaves two sides apart by exactly the
        fees."""
        free = self.run_it()
        charged = self.run_it(commission=PerTrade(cost=25.0))
        self.assertAlmostEqual(float(free.cash.iloc[-1]) - float(charged.cash.iloc[-1]), 50.0,
                               places=9)

    def test_a_model_the_kernel_cannot_price_is_refused_by_name(self):
        """An unpaid cost does not show up as an error, it shows up as a better return."""
        from ziplime.finance.slippage.volume_share_slippage import VolumeShareSlippage

        with self.assertRaises(VectorKernelError) as caught:
            self.run_it(slippage=VolumeShareSlippage())
        message = str(caught.exception)
        self.assertIn("VolumeShareSlippage", message)
        self.assertIn("ziplime.vectorized.signals", message)


class SlippageParityTests(unittest.TestCase):
    """The kernel restates two slippage formulas; this is what keeps the restatement honest.

    `costs.resolve_slippage` reads a model's parameters rather than calling it, because
    `SlippageModel.process_order` needs an order and an exchange that a vector path does not have.
    That duplication is only safe while the two agree, so both are run here on the same inputs.
    """

    def test_fixed_basis_points_agrees_with_the_event_driven_formula(self):
        from ziplime.vectorized.kernel.costs import resolve_slippage

        model = FixedBasisPointsSlippage(basis_points=7.5)
        resolved = resolve_slippage(model)
        for price in (10.0, 99.99, 1234.5):
            for direction in (1, -1):
                # The event-driven model's own arithmetic, from `process_order`.
                expected = price + price * (model.percentage * direction)
                self.assertAlmostEqual(resolved.fill_price(price, direction), expected, places=12)

    def test_fixed_spread_agrees_with_the_event_driven_formula(self):
        from ziplime.vectorized.kernel.costs import resolve_slippage

        model = FixedSlippage(spread=0.25)
        resolved = resolve_slippage(model)
        for price in (10.0, 99.99):
            for direction in (1, -1):
                expected = price + (model.spread / 2.0 * direction)
                self.assertAlmostEqual(resolved.fill_price(price, direction), expected, places=12)

    def test_no_slippage_leaves_the_price_alone(self):
        from ziplime.vectorized.kernel.costs import resolve_slippage

        self.assertEqual(resolve_slippage(NoSlippage()).fill_price(100.0, 1), 100.0)
        self.assertEqual(resolve_slippage(None).fill_price(100.0, -1), 100.0)

    def test_the_volume_cap_never_turns_an_order_around(self):
        """The clamp the event-driven model needed: an exhausted limit means nothing more fills,
        not that the position reverses."""
        from ziplime.vectorized.kernel.costs import resolve_slippage

        resolved = resolve_slippage(FixedBasisPointsSlippage(basis_points=5.0, volume_limit=0.1))
        self.assertEqual(resolved.cap(1_000.0, volume=100.0), 10.0)
        self.assertGreaterEqual(resolved.cap(1_000.0, volume=0.0), 0.0)

    def test_no_volume_frame_means_no_opinion_rather_than_no_capacity(self):
        from ziplime.vectorized.kernel.costs import resolve_slippage

        resolved = resolve_slippage(FixedBasisPointsSlippage(basis_points=5.0))
        self.assertEqual(resolved.cap(1_000.0, volume=None), 1_000.0)


class DataTests(unittest.TestCase):
    """§28 cases 17-20, and the NaN rules of §22."""

    def setUp(self):
        self.index = bars(6)

    def test_case_17_a_nan_execution_price_rejects_rather_than_filling_at_zero(self):
        prices = pd.DataFrame({"A": [10.0, np.nan, 12.0, 13.0, 14.0, 15.0]}, index=self.index)
        result = simulate_signals(prices=prices, entries=flag(self.index, "A", A=[1]),
                                  exits=frame(self.index, "A"), size=10, initial_cash=1_000.0,
                                  execution=ExecutionTiming.same_close())
        self.assertEqual(result.fills, [])
        self.assertEqual([r.reason for r in result.rejects], ["nan_price"])
        self.assertEqual(float(result.cash.iloc[-1]), 1_000.0)

    def test_a_nan_price_never_reaches_cash_as_a_zero(self):
        prices = pd.DataFrame({"A": [10.0, 11.0, np.nan, 13.0, 14.0, 15.0]}, index=self.index)
        result = simulate_signals(prices=prices, entries=flag(self.index, "A", A=[1]),
                                  exits=frame(self.index, "A"), size=10, initial_cash=1_000.0,
                                  execution=ExecutionTiming.same_close())
        self.assertTrue(np.isfinite(result.cash.to_numpy()).all())
        self.assertTrue(np.isfinite(result.portfolio_value.to_numpy()).all())

    def test_case_18_a_signal_on_the_last_bar_has_nowhere_to_execute(self):
        prices = ramp(self.index, {"A": 10.0})
        result = simulate_signals(prices=prices, entries=flag(self.index, "A", A=[5]),
                                  exits=frame(self.index, "A"), size=10, initial_cash=1_000.0,
                                  execution=ExecutionTiming.next_close())
        self.assertEqual(result.fills, [])
        self.assertEqual([r.reason for r in result.rejects], ["no_execution_bar"])

    def test_case_19_an_irregular_calendar_is_walked_by_position_not_by_date(self):
        """Gaps, weekends and holidays are the caller's business. The kernel steps bar to bar, so
        a three-day gap is one bar and the fill lands on the next row that exists."""
        index = pd.DatetimeIndex([
            pd.Timestamp("2024-01-02", tz=TZ), pd.Timestamp("2024-01-03", tz=TZ),
            pd.Timestamp("2024-01-08", tz=TZ), pd.Timestamp("2024-02-15", tz=TZ),
        ])
        prices = pd.DataFrame({"A": [10.0, 11.0, 12.0, 13.0]}, index=index)
        result = simulate_signals(prices=prices, entries=flag(index, "A", A=[1]),
                                  exits=frame(index, "A"), size=10, initial_cash=1_000.0,
                                  execution=ExecutionTiming.next_close())
        self.assertEqual(len(result.fills), 1)
        self.assertEqual(result.fills[0].timestamp, index[2].to_pydatetime())
        self.assertEqual(result.fills[0].price, 12.0)

    def test_case_20_intraday_bars_are_bars_like_any_other(self):
        index = pd.date_range("2024-01-02 09:30", periods=12, freq="30min", tz=TZ)
        prices = ramp(index, {"A": 100.0})
        result = simulate_signals(prices=prices, entries=flag(index, "A", A=[2]),
                                  exits=flag(index, "A", A=[8]), size=10, initial_cash=10_000.0,
                                  execution=ExecutionTiming.same_close())
        self.assertEqual(len(result.fills), 2)
        self.assertEqual(result.fills[0].timestamp, index[2].to_pydatetime())

    def test_prices_out_of_order_are_refused(self):
        index = bars(4)[::-1]
        prices = pd.DataFrame({"A": [10.0, 11.0, 12.0, 13.0]}, index=index)
        with self.assertRaises(VectorKernelError) as caught:
            simulate_signals(prices=prices, entries=frame(index, "A"), exits=frame(index, "A"))
        self.assertIn("chronological", str(caught.exception))


class TimingTests(unittest.TestCase):
    """§28 cases 21-23, and §10's requirement that timing is declared rather than shifted."""

    def setUp(self):
        self.index = bars(6)
        self.closes = pd.DataFrame({"A": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0]}, index=self.index)
        self.opens = pd.DataFrame({"A": [9.5, 10.5, 11.5, 12.5, 13.5, 14.5]}, index=self.index)
        self.entries = flag(self.index, "A", A=[1])

    def run_it(self, timing, **kwargs):
        return simulate_signals(prices=self.closes, entries=self.entries,
                                exits=frame(self.index, "A"), size=10, initial_cash=10_000.0,
                                execution=timing, **kwargs)

    def test_case_21_same_close_fills_on_the_signal_bar(self):
        fill = self.run_it(ExecutionTiming.same_close()).fills[0]
        self.assertEqual(fill.timestamp, self.index[1].to_pydatetime())
        self.assertEqual(fill.price, 11.0)
        self.assertEqual(fill.signal_timestamp, fill.timestamp)

    def test_case_22_next_open_fills_on_the_following_bars_open(self):
        fill = self.run_it(ExecutionTiming.next_open(), opens=self.opens).fills[0]
        self.assertEqual(fill.timestamp, self.index[2].to_pydatetime())
        self.assertEqual(fill.price, 11.5)
        self.assertEqual(fill.signal_timestamp, self.index[1].to_pydatetime())

    def test_case_23_next_close_fills_on_the_following_bars_close(self):
        fill = self.run_it(ExecutionTiming.next_close()).fills[0]
        self.assertEqual(fill.timestamp, self.index[2].to_pydatetime())
        self.assertEqual(fill.price, 12.0)

    def test_next_open_without_opens_says_what_is_missing(self):
        with self.assertRaises(VectorKernelError) as caught:
            self.run_it(ExecutionTiming.next_open())
        message = str(caught.exception)
        self.assertIn("open prices", message)
        self.assertIn("next_close", message)

    def test_same_close_is_marked_as_look_ahead(self):
        """It is offered because the event engine offers it, not because it is realistic, and the
        contract says so where a caller can read it."""
        self.assertTrue(ExecutionTiming.same_close().is_look_ahead)
        self.assertFalse(ExecutionTiming.next_open().is_look_ahead)
        self.assertFalse(ExecutionTiming.next_close().is_look_ahead)

    def test_an_unknown_timing_mode_is_refused(self):
        with self.assertRaises(VectorKernelError):
            ExecutionTiming(execute_at="whenever")


class ConflictingSignalTests(unittest.TestCase):
    """An entry and an exit on the same instrument and bar. §30's "define our own and pin it"."""

    def setUp(self):
        self.index = bars(6)
        self.prices = pd.DataFrame({"A": [100.0] * 6}, index=self.index)

    def test_the_exit_wins_and_the_entry_is_recorded_as_dropped(self):
        result = simulate_signals(
            prices=self.prices, entries=flag(self.index, "A", A=[1, 3]),
            exits=flag(self.index, "A", A=[3]), size=10, initial_cash=10_000.0,
            execution=ExecutionTiming.same_close())
        self.assertEqual([f.intent for f in result.fills], ["open_long", "close_long"])
        self.assertEqual([r.reason for r in result.rejects], ["superseded_by_exit"])
        self.assertEqual(float(result.positions["A"].iloc[-1]), 0.0)

    def test_it_is_not_netted_into_a_position_nobody_asked_for(self):
        result = simulate_signals(
            prices=self.prices, entries=flag(self.index, "A", A=[2]),
            exits=flag(self.index, "A", A=[2]), size=10, initial_cash=10_000.0,
            execution=ExecutionTiming.same_close())
        self.assertEqual(result.fills, [])
        self.assertEqual(float(result.positions["A"].iloc[-1]), 0.0)


class InputValidationTests(unittest.TestCase):
    """§36: no silent fallbacks. Every refusal names the thing the caller can change."""

    def setUp(self):
        self.index = bars(5)
        self.prices = ramp(self.index, {"A": 10.0, "B": 20.0})

    def test_a_misaligned_signal_frame_is_refused_rather_than_reindexed(self):
        other = bars(5, start="2020-06-01")
        with self.assertRaises(VectorKernelError) as caught:
            simulate_signals(prices=self.prices,
                             entries=pd.DataFrame(False, index=other, columns=["A", "B"]),
                             exits=frame(self.index, "AB"))
        self.assertIn("indexed differently", str(caught.exception))

    def test_a_signal_frame_missing_an_instrument_is_refused(self):
        with self.assertRaises(VectorKernelError) as caught:
            simulate_signals(prices=self.prices,
                             entries=pd.DataFrame(False, index=self.index, columns=["A"]),
                             exits=frame(self.index, "AB"))
        self.assertIn("'B'", str(caught.exception))

    def test_an_unsupported_size_type_is_refused_and_points_at_the_spec(self):
        with self.assertRaises(VectorKernelError) as caught:
            simulate_signals(prices=self.prices, entries=frame(self.index, "AB"),
                             exits=frame(self.index, "AB"), size_type="targetpercent")
        self.assertIn("targetpercent", str(caught.exception))

    def test_an_empty_price_frame_is_refused(self):
        empty = pd.DataFrame({"A": []}, index=pd.DatetimeIndex([], tz=TZ))
        with self.assertRaises(VectorKernelError):
            simulate_signals(prices=empty, entries=None, exits=None)

    def test_a_series_of_prices_is_read_as_one_instrument(self):
        series = pd.Series([10.0, 11.0, 12.0, 13.0, 14.0], index=self.index, name="SPY")
        result = simulate_signals(prices=series, entries=flag(self.index, ["SPY"], SPY=[1]),
                                  exits=frame(self.index, ["SPY"]), size=10,
                                  initial_cash=1_000.0, execution=ExecutionTiming.same_close())
        self.assertEqual(result.instruments, ["SPY"])
        self.assertEqual(len(result.fills), 1)

    def test_a_per_instrument_size_series(self):
        sizes = pd.Series({"A": 10.0, "B": 5.0})
        result = simulate_signals(prices=self.prices, entries=flag(self.index, "AB", A=[1], B=[1]),
                                  exits=frame(self.index, "AB"), size=sizes,
                                  initial_cash=10_000.0, execution=ExecutionTiming.same_close())
        self.assertEqual([abs(f.amount) for f in result.fills], [10.0, 5.0])

    def test_a_size_series_indexed_by_time_is_one_value_per_bar(self):
        """Told apart from a per-instrument Series by its index, not by a flag. A timestamp index
        and a ticker index cannot be confused, so neither reading is a guess."""
        by_bar = pd.Series([10.0, 10.0, 5.0, 5.0, 5.0], index=self.index)
        result = simulate_signals(prices=self.prices, entries=flag(self.index, "AB", A=[1], B=[3]),
                                  exits=frame(self.index, "AB"), size=by_bar,
                                  initial_cash=10_000.0, execution=ExecutionTiming.same_close())
        self.assertEqual([abs(f.amount) for f in result.fills], [10.0, 5.0])

    def test_a_series_that_is_neither_shape_is_refused(self):
        wrong = pd.Series({"A": 1.0, "Z": 2.0})
        with self.assertRaises(VectorKernelError) as caught:
            simulate_signals(prices=self.prices, entries=frame(self.index, "AB"),
                             exits=frame(self.index, "AB"), size=wrong)
        self.assertIn("'B'", str(caught.exception))


class ResultContractTests(unittest.TestCase):
    """§18, §19 and §38: what comes out, and whether it can be traced."""

    def setUp(self):
        self.index = bars(6)
        self.prices = ramp(self.index, {"A": 10.0})
        self.result = simulate_signals(
            prices=self.prices, entries=flag(self.index, "A", A=[1]),
            exits=flag(self.index, "A", A=[3]), size=10, initial_cash=1_000.0,
            execution=ExecutionTiming.same_close())

    def test_the_fills_frame_carries_the_documented_columns(self):
        columns = set(self.result.fills_frame().columns)
        for required in ("instrument", "timestamp", "size", "price", "side", "fees",
                         "sequence", "intent", "signal_timestamp"):
            self.assertIn(required, columns)

    def test_an_empty_run_still_produces_the_documented_columns(self):
        empty = simulate_signals(prices=self.prices, entries=frame(self.index, "A"),
                                 exits=frame(self.index, "A"),
                                 execution=ExecutionTiming.same_close())
        self.assertEqual(empty.fills, [])
        self.assertIn("instrument", empty.fills_frame().columns)
        self.assertIn("reason", empty.rejects_frame().columns)

    def test_the_result_carries_the_kernel_version(self):
        self.assertEqual(self.result.kernel_version, VECTOR_KERNEL_VERSION)

    def test_the_result_carries_the_timing_it_ran_under(self):
        self.assertEqual(self.result.timing.execute_at, "same_close")

    def test_the_compatibility_view_speaks_the_adapters_vocabulary(self):
        portfolio = self.result.as_portfolio()
        self.assertTrue(portfolio.wrapper.index.equals(self.index))
        records = portfolio.orders.records_readable
        self.assertEqual(list(records.columns),
                         ["Column", "Timestamp", "Size", "Price", "Side", "Fees"])
        self.assertEqual(list(records["Side"]), ["Buy", "Sell"])
        self.assertEqual(len(portfolio.value()), len(self.index))

    def test_size_is_a_magnitude_and_amount_is_signed(self):
        frame_out = self.result.fills_frame()
        self.assertTrue((frame_out["size"] > 0).all())
        self.assertEqual(list(np.sign(frame_out["amount"])), [1.0, -1.0])


class CompiledAndInterpretedAgreeTests(unittest.TestCase):
    """The kernel's inner loop is compiled by Numba when it is installed and interpreted when it
    is not. There is one implementation, so the two cannot drift apart by editing -- but they can
    still differ by a typing accident, which is what Numba is for and what this catches.

    `njit` keeps the original function on the dispatcher as `.py_func`, so both can be run on the
    same inputs in one process. Without Numba installed the two are literally the same object and
    the comparison is trivially true, which is correct: there is nothing to disagree.
    """

    def scenario(self, seed: int):
        rng = np.random.default_rng(seed)
        n_bars, n_assets = 60, 4
        index = bars(n_bars)
        columns = [f"S{i}" for i in range(n_assets)]
        prices = pd.DataFrame(
            {c: 20.0 + np.abs(np.cumsum(rng.normal(0, 0.7, n_bars))) for c in columns},
            index=index)
        entries = pd.DataFrame(rng.random((n_bars, n_assets)) < 0.2, index=index, columns=columns)
        exits = pd.DataFrame(rng.random((n_bars, n_assets)) < 0.2, index=index, columns=columns)
        return prices, entries, exits

    def run_both(self, seed: int):
        from ziplime.vectorized.kernel import _core

        compiled = _core.run_events
        interpreted = getattr(compiled, "py_func", compiled)
        prices, entries, exits = self.scenario(seed)

        def once(implementation):
            original = _core.run_events
            _core.run_events = implementation
            try:
                return simulate_signals(
                    prices=prices, entries=entries, exits=exits, size=50,
                    initial_cash=5_000.0, commission=PerShare(cost=0.005, min_trade_cost=1.0),
                    slippage=FixedBasisPointsSlippage(basis_points=5.0),
                    execution=ExecutionTiming.same_close())
            finally:
                _core.run_events = original

        return once(compiled), once(interpreted)

    def test_the_two_paths_produce_identical_fills(self):
        for seed in (1, 2, 3, 17, 99):
            with self.subTest(seed=seed):
                fast, slow = self.run_both(seed)
                self.assertEqual(
                    [(f.instrument, f.timestamp, f.amount, f.price, f.commission)
                     for f in fast.fills],
                    [(f.instrument, f.timestamp, f.amount, f.price, f.commission)
                     for f in slow.fills])

    def test_the_two_paths_produce_identical_cash_and_positions(self):
        fast, slow = self.run_both(7)
        np.testing.assert_array_equal(fast.cash.to_numpy(), slow.cash.to_numpy())
        np.testing.assert_array_equal(fast.positions.to_numpy(), slow.positions.to_numpy())
        self.assertEqual(fast.no_ops, slow.no_ops)

    def test_the_two_paths_agree_about_rejections(self):
        """Under-funded on purpose, so the cash constraint actually produces rejections to
        compare rather than two empty lists."""
        fast, slow = self.run_both(5)
        self.assertEqual([(r.instrument, r.reason) for r in fast.rejects],
                         [(r.instrument, r.reason) for r in slow.rejects])


class CommissionParityTests(unittest.TestCase):
    """The kernel restates the commission formulas; this is what keeps the restatement honest.

    Same arrangement as `SlippageParityTests`, and needed for the same reason: the compiled loop
    charges commission inside the decision about whether the next order fits, so it cannot call
    a model. `charge_through_model` runs the model's own code for the comparison.
    """

    MODELS = [
        ("none", NoCommission()),
        ("per share", PerShare(cost=0.0075, min_trade_cost=0.0)),
        ("per share with a floor", PerShare(cost=0.001, min_trade_cost=2.50)),
        ("per trade", PerTrade(cost=4.95)),
        ("per dollar", PerDollar(cost=0.00125)),
    ]

    def test_each_model_charges_the_same_either_way(self):
        from ziplime.vectorized.kernel.costs import charge_through_model, resolve_commission

        for label, model in self.MODELS:
            resolved = resolve_commission(model)
            for quantity, price in ((1, 10.0), (100, 99.99), (7500, 1.23), (1, 10_000.0)):
                with self.subTest(model=label, quantity=quantity, price=price):
                    value = quantity * price
                    self.assertAlmostEqual(
                        resolved.charge(quantity, value),
                        charge_through_model(model, None, quantity, value), places=10)

    def test_a_model_the_kernel_cannot_reduce_is_refused_by_name(self):
        from ziplime.finance.commission.per_contract import PerContract
        from ziplime.vectorized.kernel.costs import resolve_commission

        with self.assertRaises(VectorKernelError) as caught:
            resolve_commission(PerContract(cost=0.85, exchange_fee={}, min_trade_cost=0.0))
        message = str(caught.exception)
        self.assertIn("PerContract", message)
        self.assertIn("ziplime.vectorized.signals", message)
