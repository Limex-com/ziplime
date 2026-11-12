"""The ways to start a sweep: sweep_simulation with run_simulation's arguments, the command line's
helpers, and the best combination's config file read back by run_simulation."""
import datetime
import json
import unittest
from pathlib import Path

from tests.sweep.test_sweep_parity import END, FIXTURES, START, SweepParityCase
from ziplime.sweep import tools
from ziplime.sweep.params import Range, SweepError


class SweepSimulationTests(SweepParityCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.listings = [await self.listing(s) for s in ("JNJ@XNYS", "KO@XNYS", "PG@XNYS")]

    async def test_same_as_a_sweep_file_and_the_best_config_runs_in_run_simulation(self):
        from ziplime.sweep import sweep_simulation

        bundle = self.bundle(self.listings, seed=7)
        grid = {"fast": [3, 5], "slow": [12, 20]}
        output = self.folder / "api"
        table = await sweep_simulation(
            grid=grid, algorithm_file=FIXTURES / "trend.py", market_data_source=bundle,
            asset_service=self.asset_service, trading_calendar="XNYS",
            start_date=datetime.datetime.combine(START, datetime.time.min),
            end_date=datetime.datetime.combine(END, datetime.time.min), total_cash=100_000,
            emission_rate=datetime.timedelta(days=1), same_bar_execution=False,
            price_used_in_order_execution="close", backend="exact", verify=False,
            output_dir=str(output), say=lambda message: None)
        from_file = await self.sweep("trend.py", grid, bundle)
        self.assertEqual(sorted(table["final_value"].round(2)),
                         sorted(from_file["final_value"].round(2)))
        best = json.loads((output / "best_config.json").read_text())
        value, fills = await self.engine("trend.py", best, bundle)
        self.assertAlmostEqual(float(table.iloc[0]["final_value"]), value, delta=0.01)
        self.assertEqual(int(table.iloc[0]["fills"]), fills)

    def test_run_simulation_settings_become_the_timing(self):
        from ziplime.sweep.simulation import _rate, _timing
        self.assertEqual(_timing(True, "close"), "same_close")
        self.assertEqual(_timing(False, "open"), "next_open")
        with self.assertRaises(SweepError):
            _timing(True, "open")
        self.assertEqual(_rate(datetime.timedelta(days=1)), "1d")
        self.assertEqual(_rate(datetime.timedelta(minutes=5)), "5m")
        self.assertEqual(_rate(datetime.timedelta(hours=1)), "1h")


class CommandLineHelperTests(unittest.TestCase):
    def test_grid_and_set(self):
        self.assertEqual(tools.parse_grid("fast=5,10,20"), ("fast", [5, 10, 20]))
        self.assertEqual(tools.parse_grid("slow=30:90:10"),
                         ("slow", {"from": 30, "to": 90, "step": 10}))
        self.assertEqual(tools.parse_grid("band=0.0:0.02:0.01")[1]["step"], 0.01)
        self.assertEqual(tools.parse_grid("mode=fast,slow"), ("mode", ["fast", "slow"]))
        self.assertEqual(tools.parse_value("where fast < slow"), "where fast < slow")
        self.assertEqual(tools.parse_value("true"), True)
        with self.assertRaises(SweepError):
            tools.parse_assignment("no equals sign")

    def test_check_runs_the_grids_corners(self):
        grid = {"fast": Range.model_validate({"from": 2, "to": 30}), "slow": [10, 20, 40],
                "mode": "x", "a": [1, 2], "b": [1, 2], "c": [1, 2]}
        small = tools.corner_of(grid)
        self.assertEqual(small["fast"], [2, 30])
        self.assertEqual(small["slow"], [10, 40])
        self.assertEqual(small["mode"], ["x"])
        count = 1
        for values in small.values():
            count *= len(values)
        self.assertLessEqual(count, tools.CHECK_COMBINATIONS)

    def test_init_writes_a_grid_around_the_defaults(self):
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "sweep.json"
            content = tools.template(FIXTURES / "history.py", output, bundle=None,
                                     start=datetime.date(2024, 1, 2),
                                     end=datetime.date(2024, 6, 28), calendar="XNYS",
                                     backend="exact")
        self.assertEqual(content["grid"]["window"], [5, 10, 20])
        self.assertEqual(content["grid"]["band"], [0.5, 1.0, 1.5])
        self.assertNotIn("symbols", content["grid"])
        fractions = tools.suggested_grid(type("C", (), {"model_fields": {
            "invested": type("F", (), {"default": 0.95})}}))
        self.assertEqual(fractions["invested"], [0.475, 0.95, 1.0])

    def test_an_old_style_command_line_runs_the_file(self):
        import asyncio
        import inspect

        from ziplime.__main__ import sweep

        def resolve(args):
            result = sweep.resolve_command(None, args)
            return asyncio.run(result) if inspect.isawaitable(result) else result

        name, _, args = resolve(["sweep.json", "--backend", "cpu"])
        self.assertEqual(name, "run")
        self.assertEqual(args, ["sweep.json", "--backend", "cpu"])
        self.assertEqual(resolve(["doctor"])[0], "doctor")

    def test_suggested_backend_prefers_cupy_over_mlx_without_metal(self):
        """On NVIDIA with mlx[cuda] and CuPy both installed, MLX cannot run the entries/exits
        kernel, so the suggestion is CuPy; on Apple silicon it stays MLX."""
        from unittest import mock

        from ziplime.vectorized.kernel import _mlx

        def installed(*names):
            return lambda name: object() if name in names else None

        cases = [(True, ("mlx", "cupy"), "mlx"), (False, ("mlx", "cupy"), "cupy"),
                 (False, ("mlx",), "mlx"), (False, ("dpnp",), "dpnp"), (False, (), "cpu")]
        for metal, names, expected in cases:
            with self.subTest(metal=metal, installed=names), \
                    mock.patch.object(_mlx, "_has_metal", return_value=metal), \
                    mock.patch.object(tools.importlib.util, "find_spec", installed(*names)):
                self.assertEqual(tools.default_backend(), expected)


if __name__ == "__main__":
    unittest.main()
