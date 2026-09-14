"""The kernel against vectorbt, on inputs where the two are supposed to agree.

vectorbt is a **reference oracle here, not a specification**. The spec's §30 is explicit about
what to do with a disagreement -- classify it before reproducing it -- and two of the
disagreements below are classified as "vectorbt's behaviour, deliberately not ours" and asserted
as differences rather than papered over. Those live in :class:`DeliberateDifferenceTests`.

This whole directory is optional. Once the kernel is trusted these tests are the only thing left
that needs vectorbt installed, which is why they sit under `tests/reference/` rather than beside
the kernel's own suite: `pip uninstall vectorbt && pytest tests/` has to stay green.
"""
import unittest

import numpy as np
import pandas as pd

try:
    import vectorbt as vbt
except ImportError:  # pragma: no cover - the point of the directory
    vbt = None

from ziplime.finance.commission.per_dolar import PerDollar
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
from ziplime.vectorized.kernel import ExecutionTiming, simulate_signals

requires_vectorbt = unittest.skipIf(vbt is None, "vectorbt is not installed")

TZ = "America/New_York"


def index_of(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2024-01-02", periods=n, freq="D", tz=TZ)


def vbt_fills(portfolio, columns: list[str]) -> list[tuple]:
    """vectorbt's orders as (timestamp, instrument, signed amount, price), rounded for comparison.

    `columns` is needed because vectorbt labels a portfolio built from a Series by integer
    position while the kernel labels it by the Series' name -- the same fill under two names.
    That is a difference in how the two libraries spell a column, not in what they executed, so
    it is normalised here rather than asserted on.
    """
    records = portfolio.orders.records_readable
    if records.empty:
        return []
    out = []
    for row in records.to_dict("records"):
        amount = float(row["Size"])
        if str(row["Side"]).lower().startswith("sell"):
            amount = -amount
        column = row["Column"]
        instrument = columns[column] if isinstance(column, (int, np.integer)) else column
        out.append((row["Timestamp"], instrument, round(amount, 8), round(float(row["Price"]), 8)))
    return sorted(out, key=lambda item: (str(item[0]), str(item[1])))


def kernel_fills(result) -> list[tuple]:
    return sorted([(pd.Timestamp(f.timestamp), f.instrument, round(f.amount, 8),
                    round(f.price, 8)) for f in result.fills],
                  key=lambda item: (str(item[0]), str(item[1])))


@requires_vectorbt
class DifferentialTests(unittest.TestCase):
    """§28's cases, run both ways.

    Every case fills at the signal bar's close, because that is what `from_signals` does by
    default -- so the kernel is asked for `same_close`. Comparing against a vectorbt run at one
    timing and a kernel run at another would measure the timing, not the kernel.
    """

    CASH = 100_000.0

    def both(self, prices, entries, exits, size=10, cash=None, **kwargs):
        cash = self.CASH if cash is None else cash
        portfolio = vbt.Portfolio.from_signals(
            prices, entries, exits, init_cash=cash, freq="1D", size=size,
            size_type="amount", group_by=True, cash_sharing=True, **kwargs)
        result = simulate_signals(
            prices=prices, entries=entries, exits=exits, size=size, initial_cash=cash,
            execution=ExecutionTiming.same_close())
        return portfolio, result

    def assert_same_fills(self, portfolio, result, msg=""):
        self.assertEqual(kernel_fills(result), vbt_fills(portfolio, result.instruments), msg)

    def test_case_1_flat_to_long_to_flat(self):
        index = index_of(8)
        prices = pd.Series(np.arange(10.0, 18.0), index=index, name="A")
        entries = pd.Series(False, index=index); entries.iloc[1] = True
        exits = pd.Series(False, index=index); exits.iloc[4] = True
        portfolio, result = self.both(prices, entries, exits)
        self.assert_same_fills(portfolio, result)
        self.assertEqual(len(result.fills), 2)

    def test_case_3_holding_across_bars(self):
        index = index_of(10)
        prices = pd.Series(np.linspace(50.0, 60.0, 10), index=index, name="A")
        entries = pd.Series(False, index=index); entries.iloc[2] = True
        exits = pd.Series(False, index=index); exits.iloc[7] = True
        portfolio, result = self.both(prices, entries, exits)
        self.assert_same_fills(portfolio, result)

    def test_case_4_a_repeated_entry_is_ignored_by_both(self):
        """The agreement worth having: `accumulate=False` is vectorbt's default and the kernel's,
        and they mean the same thing by it."""
        index = index_of(8)
        prices = pd.Series(np.arange(20.0, 28.0), index=index, name="A")
        entries = pd.Series(False, index=index); entries.iloc[[1, 2, 3]] = True
        exits = pd.Series(False, index=index); exits.iloc[5] = True
        portfolio, result = self.both(prices, entries, exits)
        self.assert_same_fills(portfolio, result)
        self.assertEqual(len(result.fills), 2)

    def test_case_5_a_repeated_exit_is_ignored_by_both(self):
        index = index_of(8)
        prices = pd.Series(np.arange(20.0, 28.0), index=index, name="A")
        entries = pd.Series(False, index=index); entries.iloc[1] = True
        exits = pd.Series(False, index=index); exits.iloc[[4, 5, 6]] = True
        portfolio, result = self.both(prices, entries, exits)
        self.assert_same_fills(portfolio, result)

    def test_case_8_multi_asset_with_enough_cash(self):
        index = index_of(8)
        prices = pd.DataFrame({"A": np.arange(10.0, 18.0), "B": np.arange(30.0, 38.0)},
                              index=index)
        entries = pd.DataFrame(False, index=index, columns=["A", "B"]); entries.iloc[1] = True
        exits = pd.DataFrame(False, index=index, columns=["A", "B"]); exits.iloc[5] = True
        portfolio, result = self.both(prices, entries, exits, size=100)
        self.assert_same_fills(portfolio, result)

    def test_case_10_a_buy_and_a_sell_on_one_bar(self):
        index = index_of(8)
        prices = pd.DataFrame({"A": np.full(8, 10.0), "B": np.full(8, 20.0)}, index=index)
        entries = pd.DataFrame(False, index=index, columns=["A", "B"])
        exits = pd.DataFrame(False, index=index, columns=["A", "B"])
        entries.loc[index[1], "A"] = True
        exits.loc[index[3], "A"] = True
        entries.loc[index[3], "B"] = True
        portfolio, result = self.both(prices, entries, exits, size=100)
        self.assert_same_fills(portfolio, result)

    def test_case_13_a_proportional_commission(self):
        """vectorbt's `fees` is a fraction of order value, which is exactly `PerDollar`."""
        index = index_of(8)
        prices = pd.Series(np.arange(100.0, 108.0), index=index, name="A")
        entries = pd.Series(False, index=index); entries.iloc[1] = True
        exits = pd.Series(False, index=index); exits.iloc[5] = True
        portfolio = vbt.Portfolio.from_signals(
            prices, entries, exits, init_cash=self.CASH, freq="1D", size=10,
            size_type="amount", fees=0.001, group_by=True, cash_sharing=True)
        result = simulate_signals(prices=prices, entries=entries, exits=exits, size=10,
                                  initial_cash=self.CASH, commission=PerDollar(cost=0.001),
                                  execution=ExecutionTiming.same_close())
        self.assert_same_fills(portfolio, result)
        reported = portfolio.orders.records_readable["Fees"].to_numpy(dtype=float)
        np.testing.assert_allclose([f.commission for f in result.fills], reported, rtol=0,
                                   atol=1e-9)

    def test_case_15_slippage(self):
        """vectorbt's `slippage` moves the price by a fraction, the same shape as basis points."""
        index = index_of(8)
        prices = pd.Series(np.arange(100.0, 108.0), index=index, name="A")
        entries = pd.Series(False, index=index); entries.iloc[1] = True
        exits = pd.Series(False, index=index); exits.iloc[5] = True
        portfolio = vbt.Portfolio.from_signals(
            prices, entries, exits, init_cash=self.CASH, freq="1D", size=10,
            size_type="amount", slippage=0.001, group_by=True, cash_sharing=True)
        result = simulate_signals(
            prices=prices, entries=entries, exits=exits, size=10, initial_cash=self.CASH,
            slippage=FixedBasisPointsSlippage(basis_points=10.0),
            execution=ExecutionTiming.same_close())
        self.assert_same_fills(portfolio, result)

    def test_case_16_commission_and_slippage_together(self):
        index = index_of(10)
        prices = pd.Series(np.linspace(100.0, 120.0, 10), index=index, name="A")
        entries = pd.Series(False, index=index); entries.iloc[[1, 6]] = True
        exits = pd.Series(False, index=index); exits.iloc[[4, 8]] = True
        portfolio = vbt.Portfolio.from_signals(
            prices, entries, exits, init_cash=self.CASH, freq="1D", size=10,
            size_type="amount", fees=0.0005, slippage=0.0005, group_by=True, cash_sharing=True)
        result = simulate_signals(
            prices=prices, entries=entries, exits=exits, size=10, initial_cash=self.CASH,
            commission=PerDollar(cost=0.0005),
            slippage=FixedBasisPointsSlippage(basis_points=5.0),
            execution=ExecutionTiming.same_close())
        self.assert_same_fills(portfolio, result)

    def test_the_ending_cash_agrees(self):
        index = index_of(12)
        prices = pd.DataFrame({"A": np.linspace(10.0, 20.0, 12), "B": np.linspace(40.0, 30.0, 12)},
                              index=index)
        entries = pd.DataFrame(False, index=index, columns=["A", "B"]); entries.iloc[[1, 7]] = True
        exits = pd.DataFrame(False, index=index, columns=["A", "B"]); exits.iloc[[4, 10]] = True
        portfolio, result = self.both(prices, entries, exits, size=100)
        self.assertAlmostEqual(float(result.cash.iloc[-1]),
                               float(np.asarray(portfolio.cash())[-1]), places=6)

    def test_the_equity_path_agrees_bar_for_bar(self):
        index = index_of(12)
        prices = pd.DataFrame({"A": np.linspace(10.0, 20.0, 12), "B": np.linspace(40.0, 30.0, 12)},
                              index=index)
        entries = pd.DataFrame(False, index=index, columns=["A", "B"]); entries.iloc[[1, 7]] = True
        exits = pd.DataFrame(False, index=index, columns=["A", "B"]); exits.iloc[[4, 10]] = True
        portfolio, result = self.both(prices, entries, exits, size=100)
        np.testing.assert_allclose(result.portfolio_value.to_numpy(dtype=float),
                                   np.asarray(portfolio.value(), dtype=float), rtol=0, atol=1e-6)


@requires_vectorbt
class RandomDifferentialTests(unittest.TestCase):
    """§29: many generated scenarios rather than the handful anyone thinks to write.

    Seeded, so a failure is reproducible from its case number alone. The generator stays inside
    the ground the two engines are meant to share -- long-only, sufficient cash, no conflicting
    signals on a bar -- because outside it they are *supposed* to differ, and a differential test
    that wanders there reports ziplime's deliberate choices as failures.
    """

    CASES = 200

    def test_generated_scenarios_produce_identical_fills(self):
        rng = np.random.default_rng(20260913)
        mismatches = []
        for case in range(self.CASES):
            n_bars = int(rng.integers(10, 40))
            n_assets = int(rng.integers(1, 4))
            index = index_of(n_bars)
            columns = [f"S{i}" for i in range(n_assets)]
            prices = pd.DataFrame(
                {c: 50.0 + np.cumsum(rng.normal(0, 1.0, n_bars)) + 20.0 for c in columns},
                index=index).abs() + 1.0
            entries = pd.DataFrame(rng.random((n_bars, n_assets)) < 0.15, index=index,
                                   columns=columns)
            exits = pd.DataFrame(rng.random((n_bars, n_assets)) < 0.15, index=index,
                                 columns=columns)
            # Where both fire on one bar the two engines disagree by design -- see
            # `DeliberateDifferenceTests`. Kept out of the random ground so this test measures
            # agreement rather than re-measuring a known difference.
            both = entries & exits
            entries = entries & ~both
            exits = exits & ~both
            size = float(rng.integers(1, 20))
            cash = 1_000_000.0     # generous on purpose: cash pressure is the other known gap

            portfolio = vbt.Portfolio.from_signals(
                prices, entries, exits, init_cash=cash, freq="1D", size=size,
                size_type="amount", group_by=True, cash_sharing=True)
            result = simulate_signals(prices=prices, entries=entries, exits=exits, size=size,
                                      initial_cash=cash,
                                      execution=ExecutionTiming.same_close())
            if kernel_fills(result) != vbt_fills(portfolio, result.instruments):
                mismatches.append(case)

        self.assertEqual(mismatches, [], f"{len(mismatches)} of {self.CASES} scenarios differ; "
                                         f"first few: {mismatches[:5]}")

    def test_generated_scenarios_agree_on_ending_equity(self):
        rng = np.random.default_rng(4711)
        for case in range(50):
            n_bars = int(rng.integers(10, 30))
            index = index_of(n_bars)
            prices = pd.Series(20.0 + np.abs(np.cumsum(rng.normal(0, 0.5, n_bars))), index=index,
                               name="A")
            entries = pd.Series(rng.random(n_bars) < 0.2, index=index)
            exits = pd.Series(rng.random(n_bars) < 0.2, index=index)
            both = entries & exits
            entries, exits = entries & ~both, exits & ~both
            portfolio = vbt.Portfolio.from_signals(
                prices, entries, exits, init_cash=50_000.0, freq="1D", size=10,
                size_type="amount", group_by=True, cash_sharing=True)
            result = simulate_signals(prices=prices, entries=entries, exits=exits, size=10,
                                      initial_cash=50_000.0,
                                      execution=ExecutionTiming.same_close())
            with self.subTest(case=case):
                np.testing.assert_allclose(
                    result.portfolio_value.to_numpy(dtype=float),
                    np.asarray(portfolio.value(), dtype=float), rtol=0, atol=1e-6)


@requires_vectorbt
class DeliberateDifferenceTests(unittest.TestCase):
    """Where ziplime answers differently on purpose. §30, classification C.

    Asserted as differences rather than left undocumented, so that a future change which
    accidentally adopts vectorbt's answer fails here instead of passing quietly.
    """

    def test_a_conflicting_entry_and_exit_is_resolved_differently(self):
        """Both signals on one bar. vectorbt nets them under its own conflict rules; the kernel
        takes the exit and records the entry as dropped, because a strategy contradicting itself
        should not have the contradiction resolved into a position by arithmetic."""
        index = index_of(8)
        prices = pd.Series(np.full(8, 100.0), index=index, name="A")
        entries = pd.Series(False, index=index); entries.iloc[[1, 4]] = True
        exits = pd.Series(False, index=index); exits.iloc[4] = True

        result = simulate_signals(prices=prices, entries=entries, exits=exits, size=10,
                                  initial_cash=100_000.0,
                                  execution=ExecutionTiming.same_close())
        self.assertEqual([f.intent for f in result.fills], ["open_long", "close_long"])
        self.assertEqual([r.reason for r in result.rejects], ["superseded_by_exit"])
        self.assertEqual(float(result.positions["A"].iloc[-1]), 0.0)

    def test_an_order_that_does_not_fit_is_rejected_whole_rather_than_shrunk(self):
        """vectorbt fills what the cash allows. The kernel rejects the order and says so: a
        half-filled order is a different strategy, and one nobody asked for."""
        index = index_of(6)
        prices = pd.DataFrame({"A": np.full(6, 100.0)}, index=index)
        entries = pd.DataFrame(False, index=index, columns=["A"]); entries.iloc[1] = True
        exits = pd.DataFrame(False, index=index, columns=["A"])

        result = simulate_signals(prices=prices, entries=entries, exits=exits, size=100,
                                  initial_cash=5_000.0,
                                  execution=ExecutionTiming.same_close())
        self.assertEqual(result.fills, [])
        self.assertEqual([r.reason for r in result.rejects], ["insufficient_cash"])

        portfolio = vbt.Portfolio.from_signals(
            prices, entries, exits, init_cash=5_000.0, freq="1D", size=100,
            size_type="amount", group_by=True, cash_sharing=True)
        partial = portfolio.orders.records_readable
        self.assertFalse(partial.empty, "vectorbt is expected to fill what the cash allows")
        self.assertLess(float(partial["Size"].iloc[0]), 100.0)
