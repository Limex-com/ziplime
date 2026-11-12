"""The converter: every construct gives, on an array, what Python gives element by element."""
import asyncio
import sys
import types
import unittest
from pathlib import Path

import numpy as np

from ziplime.sweep.check import foreign_calls, problems
from ziplime.sweep.convert import NotConvertible, VectorUniverse, context_writes, convert

import importlib.util

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "sweep"
sys.path.insert(0, str(FIXTURES))  # constructs.py imports a module of its own folder


def load(name):
    spec = importlib.util.spec_from_file_location(f"sweep_fixture_{name}", FIXTURES / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONSTRUCTS = load("constructs")
XS = np.array([-2.0, 0.0, 0.5, 1.0, 2.0, 4.0, 7.0])
YS = np.array([1.0, 0.0, 2.0, 1.0, 3.0, np.nan, 0.5])


def run_converted(hook, x, y):
    from ziplime.sweep.convert import ALL

    call = convert(hook).bind(np)
    type(ALL)._columns = (["A", "B"], [1, 2], np.array)
    # One combination per element of x, the universe's two instruments across
    context = types.SimpleNamespace(x=x[:, None], y=y[:, None], out=np.nan,
                                    universe=VectorUniverse({"A": 1, "B": 2}))
    call(context, None)
    out = np.asarray(context.out, dtype=float)
    return np.broadcast_to(out.reshape(len(x), -1)[:, 0] if out.ndim else out, x.shape)


def run_python(hook, x, y):
    out = []
    for a, b in zip(x, y):
        context = types.SimpleNamespace(x=float(a), y=float(b), out=np.nan,
                                        universe={"A": 1, "B": 2})
        asyncio.run(hook(context, None))
        out.append(float(context.out))
    return np.array(out)


class ConstructTests(unittest.TestCase):
    def check(self, name):
        hook = getattr(CONSTRUCTS, name)
        np.testing.assert_array_equal(run_converted(hook, XS, YS), run_python(hook, XS, YS))

    def test_if_elif_else(self):
        self.check("branches")

    def test_return_in_a_branch(self):
        self.check("early_return")

    def test_and_or_not_and_ternary(self):
        self.check("logic")

    def test_assignment_under_a_mask(self):
        self.check("assign_in_branch")

    def test_helper_function_with_return(self):
        self.check("helper_with_return")

    def test_builtins_and_math(self):
        self.check("builtins_and_math")

    def test_chained_comparison(self):
        self.check("chained")

    def test_continue_in_a_loop(self):
        self.check("loop_with_continue")

    def test_while_on_a_shared_value(self):
        self.check("uniform_while")

    def test_comprehensions_and_selections_across_instruments(self):
        self.check("cross_sections")

    def test_functions_of_the_strategys_own_modules(self):
        self.check("own_module_helpers")

    def test_numpy_elementwise_functions(self):
        self.check("numpy_elementwise")

    def test_numpy_reductions_over_a_list_reduce_value_by_value(self):
        self.check("numpy_reductions")


class RefusalTests(unittest.TestCase):
    def test_a_value_carried_across_instruments_is_refused(self):
        with self.assertRaisesRegex(NotConvertible, "budget"):
            run_converted(CONSTRUCTS.carried_over_instruments, XS, YS)

    def test_try_is_refused_at_conversion(self):
        with self.assertRaisesRegex(NotConvertible, "try"):
            convert(CONSTRUCTS.uses_try)

    def test_numpy_reduction_with_an_axis_is_refused(self):
        with self.assertRaisesRegex(NotConvertible, "axis"):
            run_converted(CONSTRUCTS.numpy_with_axis, XS, YS)

    def test_break_on_data_is_refused(self):
        with self.assertRaisesRegex(NotConvertible, "break"):
            run_converted(CONSTRUCTS.break_on_data, XS, YS)


class AnalysisTests(unittest.TestCase):
    def test_unsupported_calls_are_named_with_their_line(self):
        async def hook(context, data):
            await context.cancel_order("x")
            context.account.leverage
            data.fetch_csv("x")

        found = problems([hook], set())
        self.assertEqual(len(found), 3)
        for name in ("context.cancel_order", "context.account", "data.fetch_csv"):
            self.assertTrue(any(name in line and ", line " in line for line in found), name)

    def test_other_libraries_are_refused_with_their_line(self):
        import statistics

        cases = {"uses_pandas": ({}, "pandas"),
                 "uses_a_model": ({"model": statistics.NormalDist()}, "statistics"),
                 "uses_statistics": ({}, "statistics"),
                 "numpy_unsupported": ({}, "cumsum"),
                 "polars_other": ({}, "lit")}
        for name, (state, word) in cases.items():
            with self.subTest(name=name):
                found = foreign_calls([getattr(CONSTRUCTS, name)], state)
                self.assertEqual(len(found), 1, found)
                self.assertIn(word, found[0])
                self.assertIn("constructs.py, line ", found[0])

    def test_supported_calls_pass(self):
        for name in ("numpy_elementwise", "numpy_reductions", "builtins_and_math",
                     "helper_with_return", "own_module_helpers", "polars_col"):
            with self.subTest(name=name):
                self.assertEqual(foreign_calls([getattr(CONSTRUCTS, name)], {}), [])

    def test_dict_comprehension_over_instruments_keeps_its_condition(self):
        from ziplime.sweep.convert import ALL, Runtime

        async def hook(context, data):
            values = {name: context.x for name, asset in context.universe.items()}
            kept = {name: values[name] for name in values if values[name] > 0}
            context.out = len(kept)

        type(ALL)._columns = (["A", "B"], [1, 2], np.array)
        call = convert(hook).bind(np, Runtime(np))
        context = types.SimpleNamespace(x=XS[:, None] + np.array([[0.0, -1.0]]),
                                        universe=VectorUniverse({"A": 1, "B": 2}))
        call(context, None)
        expected = (XS > 0).astype(int) + (XS - 1 > 0).astype(int)
        np.testing.assert_array_equal(np.asarray(context.out).ravel(), expected)

    def test_writes_and_compile_blockers(self):
        async def hook(context, data):
            context.i += 1
            context.seen.append(1)
            context.when = context.get_datetime()

        written, in_place, blockers = context_writes([hook])
        self.assertEqual(written, {"i", "when"})
        self.assertEqual(in_place, {"seen"})
        self.assertEqual(len(blockers), 1)


if __name__ == "__main__":
    unittest.main()
