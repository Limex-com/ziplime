"""Computations on history rows, replayed on the host (ziplime/sweep/lazy.py).

A TA-Lib function in WINDOW_LOCAL is computed once over the whole history instead of on every
bar's window; that is only right if it gives the window's number at every bar, which these tests
check for every function on the list, with the window shorter and longer than the function needs.
"""
import types
import unittest

import numpy as np

from ziplime.sweep import lazy
from ziplime.sweep.convert import ALL
from ziplime.sweep.market import HistoryRows

try:
    import talib
except ImportError:  # pragma: no cover
    talib = None

BARS = 320


def market(gap_at=None):
    rng = np.random.default_rng(3)
    close = 50 * np.exp(np.cumsum(rng.normal(0, 0.02, (BARS, 2)), axis=0))
    spread = np.abs(rng.normal(0, 0.01, (BARS, 2)))
    fields = {"close": close, "open": close * (1 + rng.normal(0, 0.005, (BARS, 2))),
              "high": close * (1 + spread), "low": close * (1 - spread),
              "volume": rng.integers(1_000, 9_000, (BARS, 2)).astype(float)}
    t = np.arange(BARS)
    # the second instrument starts 60 bars later: its first windows are short
    ends = np.stack([t, np.maximum(t - 60, 0)], axis=1)
    for name, rows in fields.items():
        if gap_at is not None:
            rows[gap_at, 1] = np.nan
        rows[:, 1] = np.concatenate([rows[60:, 1], np.full(60, np.nan)])
    return types.SimpleNamespace(rows=fields, history_end=ends,
                                 history_end_before=np.maximum(ends - 1, 0), row_days=None,
                                 n_bars=BARS, columns=["A", "B"])


def inputs_of(name):
    """The history fields a TA-Lib function reads, in order."""
    import talib.abstract
    named = talib.abstract.Function(name).input_names
    fields = []
    for value in named.values():
        fields += list(value) if isinstance(value, (list, tuple)) else [value]
    return fields


def last_value(function, length, k=None, column=ALL, output=None, **options):
    """``talib.<function>(rows..., **options)[-1]`` -- ``[output][-1]`` for one of several
    outputs -- over windows of the fields it reads, as a record's key and params."""
    windows = []
    for field in inputs_of(function.__name__):
        rows = lazy.leaf(None, column, field, length, False, False).to_numpy()
        windows.append(rows[-k:] if k is not None else rows)
    value = lazy.record(function, tuple(windows), options)
    if output is not None:
        value = value[output]
    template = lazy._Template(value[-1])
    return template.key, tuple(template.params)


def replayed(key, params, data):
    plan = lazy.compile_plan(key, HistoryRows(data))
    return lazy.replay_series(plan, data.n_bars, [0, 1], params)


@unittest.skipIf(talib is None, "needs TA-Lib")
class WholeHistoryTests(unittest.TestCase):
    def test_every_window_local_function_gives_the_windows_number(self):
        data = market()
        import talib.abstract
        for name in sorted(lazy.WINDOW_LOCAL):
            function = getattr(talib, name)
            described = talib.abstract.Function(name)
            options = {"timeperiod": 10} if "timeperiod" in described.parameters else {}
            outputs = range(len(described.output_names)) if len(described.output_names) > 1 \
                else [None]
            for (length, k), output in [(window, output) for window in
                                        ((60, None), (60, 20), (12, 40), (8, None))
                                        for output in outputs]:
                with self.subTest(function=name, length=length, k=k, output=output):
                    key, params = last_value(function, length, k, output=output, **options)
                    fast = lazy.fast_series(key, params, data, [0, 1])
                    self.assertIsNotNone(fast)
                    expected = replayed(key, params, data)
                    np.testing.assert_array_equal(np.isnan(fast), np.isnan(expected))
                    np.testing.assert_allclose(fast, expected, rtol=1e-9, atol=1e-9,
                                               equal_nan=True)

    def test_a_smoothed_average_inside_is_replayed(self):
        key, params = last_value(talib.BBANDS, 60, output=1, timeperiod=10, matype=1)  # EMA
        self.assertIsNone(lazy.fast_series(key, params, market(), [0, 1]))

    def test_smoothed_functions_are_replayed(self):
        data = market()
        for function in (talib.RSI, talib.EMA, talib.MACD):
            key, params = last_value(function, 60)
            self.assertIsNone(lazy.fast_series(key, params, data, [0, 1]), function.__name__)

    def test_arithmetic_on_them(self):
        data = market()
        first = lazy.leaf(None, 0, "close", 60, False, False).to_numpy()
        second = lazy.leaf(None, 1, "close", 30, False, False).to_numpy()
        roc = lambda rows: lazy.record(talib.ROC, (rows,), {"timeperiod": 20})[-1]  # noqa: E731
        value = (roc(first) - roc(second) - 1.5) / abs(roc(second)) * 100
        key = lazy._Template(value).key
        fast = lazy.fast_series(key, (), data, [0])
        np.testing.assert_allclose(fast, replayed(key, (), data)[:, :1], equal_nan=True)

    def test_a_gap_in_the_history_is_replayed(self):
        key, params = last_value(talib.SMA, 30, timeperiod=10)
        self.assertIsNone(lazy.fast_series(key, params, market(gap_at=100), [0, 1]))

    def test_a_parameter_of_the_config(self):
        data = market()
        rows = lazy.leaf(None, ALL, "close", 90, False, False).to_numpy()
        period = np.array([[14.0], [30.0]])  # a config field: one value per combination
        template = lazy._Template(lazy.record(talib.ROC, (rows,), {"timeperiod": period})[-1])
        for value in (14, 30):
            fast = lazy.fast_series(template.key, (value,), data, [0, 1])
            np.testing.assert_allclose(fast, replayed(template.key, (value,), data),
                                       equal_nan=True)


class RowOfWindowTests(unittest.TestCase):
    def test_a_row_of_the_window_by_index(self):
        data = market(gap_at=100)  # a NaN row is read as it is, not carried
        for length, k, position in ((60, None, -1), (60, None, 0), (60, 20, 0), (60, 20, -3),
                                    (12, 40, 5), (12, None, 11), (12, None, 12), (8, None, -9)):
            with self.subTest(length=length, k=k, position=position):
                rows = lazy.leaf(None, ALL, "close", length, False, False).to_numpy()
                if k is not None:
                    rows = rows[-k:]
                key = lazy._Template(rows[position]).key
                fast = lazy.fast_series(key, (), data, [0, 1])
                self.assertIsNotNone(fast)
                np.testing.assert_array_equal(fast, replayed(key, (), data))


class ExplainTests(unittest.TestCase):
    def test_a_parameter_that_changes_nothing(self):
        import pandas as pd
        from ziplime.sweep import explain

        table = pd.DataFrame({"fast": [5, 5, 10, 10], "level": [1.0, 2.0, 1.0, 2.0],
                              "fills": [3, 3, 7, 7], "final_value": [101.0, 101.0, 99.5, 99.5]})
        found = explain.influence(table, ["fast", "level"])
        self.assertEqual(len(found), 1)
        self.assertIn("level changes nothing", found[0])

    def test_a_computation_that_is_nan_at_every_bar(self):
        from ziplime.sweep import explain
        from ziplime.sweep.engine import Windows

        data = market()
        windows = Windows(data, None)
        rows = lazy.leaf(None, 0, "close", 30, False, False).to_numpy()
        dead = lazy._Template(lazy.record(np.mean, (rows[-31:],))).key   # fine
        nan = lazy._Template(rows[40]).key                                # 30 rows: no [40]
        for n, key in enumerate((dead, nan)):
            windows.lazy.records[key] = (f"lazy{n}", None, False)
            windows.lazy.where[f"lazy{n}"] = f"strategy.py, line {10 + n}"
            windows.lazy._cache[(key, ())] = lazy.fast_series(key, (), data, [0]) \
                if key == nan else replayed(key, (), data)[:, :1]
        found = explain.warnings(windows, data.columns)
        self.assertEqual(len(found), 1)
        self.assertIn("strategy.py:11", found[0])


@unittest.skipIf(talib is None, "needs TA-Lib")
class ReplayTests(unittest.TestCase):
    def test_a_process_of_its_own_replays_the_same(self):
        data = market()
        key, params = last_value(talib.RSI, 60, timeperiod=14)
        # _start_worker marks its process as a worker; this one is not
        previous = __import__("os").environ.get("ZIPLIME_SWEEP_REPLAY_WORKERS")
        self.addCleanup(lambda: __import__("os").environ.pop("ZIPLIME_SWEEP_REPLAY_WORKERS")
                        if previous is None else
                        __import__("os").environ.__setitem__("ZIPLIME_SWEEP_REPLAY_WORKERS",
                                                             previous))
        lazy._start_worker(dict(rows=data.rows, history_end=data.history_end,
                                history_end_before=data.history_end_before,
                                row_days=data.row_days, n_bars=data.n_bars))
        np.testing.assert_array_equal(lazy._replay_job((key, params, [0, 1])),
                                      replayed(key, params, data))

    def test_the_compiled_record_matches_the_walked_one(self):
        data = market()
        rows = HistoryRows(data)
        p = lazy.leaf(None, 0, "close", 60, False, False).to_numpy()
        value = (lazy.record(np.std, (lazy.record(np.diff, (p[-20:],)) / p[-20:-1],)) * 100)
        key = lazy._Template(value).key
        plan = lazy.compile_plan(key, rows)
        for t in (0, 5, 61, 200, BARS - 1):
            walked = lazy.replay(value, rows, t, 0, ())
            compiled = lazy._real(plan(t, 0, ()))
            if np.isnan(walked):
                self.assertTrue(np.isnan(compiled))
            else:
                self.assertEqual(walked, compiled)


if __name__ == "__main__":
    unittest.main()
