"""Intraday sweeps: 5-minute bars against run_simulation at the same emission rate."""
import contextlib
import datetime
import io
import json
import unittest

import numpy as np
import polars as pl

from tests.sweep.test_sweep_parity import FIXTURES, GPU, SweepParityCase
from ziplime.constants.data_type import DataType
from ziplime.core.run_simulation import run_simulation
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.sweep import SweepParams, run_sweep

RATE = datetime.timedelta(minutes=5)
START, END = datetime.date(2024, 3, 4), datetime.date(2024, 3, 12)
HISTORY_FROM = datetime.date(2024, 2, 26)


class IntradayParityTests(SweepParityCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.listings = [await self.listing(s) for s in ("JNJ@XNYS", "KO@XNYS")]

    def minute_bundle(self, seed=3):
        rng = np.random.default_rng(seed)
        rows = []
        sessions = self.calendar.sessions_in_range(HISTORY_FROM, END)
        for k, listing in enumerate(self.listings):
            price = 50.0 * (1 + k * 0.4)
            for session in sessions:
                first = self.calendar.session_first_minute(session).tz_convert(self.calendar.tz)
                close = self.calendar.session_close(session).tz_convert(self.calendar.tz)
                for stamp in __import__("pandas").date_range(first, close, freq=RATE):
                    price *= float(np.exp(rng.normal(0, 0.002)))
                    p = round(price, 2)
                    rows.append({"date": stamp.to_pydatetime(), "sid": listing.sid,
                                 "symbol": listing.symbol, "mic": listing.mic, "open": p,
                                 "high": round(p * 1.001, 2), "low": round(p * 0.999, 2),
                                 "close": p, "price": p,
                                 "volume": float(rng.integers(5_000, 50_000))})
        data = pl.DataFrame(rows).sort(["sid", "date"])
        spans = data.with_row_index().group_by("sid", maintain_order=True).agg(
            [pl.col("index").first().alias("first"), pl.col("index").last().alias("last")])
        return DataBundle(
            name="sweep-intraday", version="1", start_date=data["date"].min(),
            end_date=data["date"].max(), trading_calendar=self.calendar, frequency=RATE,
            original_frequency=RATE, data_type=DataType.MARKET_DATA,
            timestamp=data["date"].max(), data=data,
            sid_indexes={r["sid"]: (r["first"], r["last"] + 1) for r in spans.iter_rows(named=True)})

    async def intraday_sweep(self, fixture, grid, bundle, backend="exact"):
        params = SweepParams.model_validate({
            "algorithm_file": str(FIXTURES / fixture), "start_date": str(START),
            "end_date": str(END), "trading_calendar": "XNYS", "total_cash": 100_000,
            "emission_rate": "5m", "grid": grid, "backend": backend, "verify": False,
            "refine_top": 0, "output_dir": str(self.folder / "results")})
        with contextlib.redirect_stdout(io.StringIO()):
            return await run_sweep(params, self.folder, say=lambda message: None,
                                   market_data=bundle, asset_service=self.asset_service)

    async def intraday_engine(self, fixture, config, bundle):
        path = self.folder / "config.json"
        path.write_text(json.dumps(config))
        with contextlib.redirect_stdout(io.StringIO()):
            result = await run_simulation(
                start_date=datetime.datetime.combine(START, datetime.time.min, tzinfo=self.calendar.tz),
                end_date=datetime.datetime.combine(END, datetime.time.max, tzinfo=self.calendar.tz),
                trading_calendar="XNYS", emission_rate=RATE, total_cash=100_000.0,
                market_data_source=bundle, custom_data_sources=[],
                algorithm_file=str(FIXTURES / fixture), config_file=str(path), stop_on_error=True,
                asset_service=self.asset_service, benchmark_asset_symbol=None,
                benchmark_returns=None, same_bar_execution=False,
                equity_commission=SweepParams.model_validate(
                    {"algorithm_file": "x", "start_date": str(START), "grid": {"x": [0]}}
                ).equity_commission.model(),
                equity_slippage=SweepParams.model_validate(
                    {"algorithm_file": "x", "start_date": str(START), "grid": {"x": [0]}}
                ).equity_slippage.model(),
                max_leverage=100.0, print_algo=False)
        self.assertFalse(result.errors, [e.message for e in result.errors][:3])
        perf = result.perf
        return (float(perf["portfolio_value"].iloc[-1]),
                sum(len(row) for row in perf["transactions"]))

    async def assert_intraday_parity(self, fixture, grid, bundle):
        table = await self.intraday_sweep(fixture, grid, bundle)
        traded = 0
        for _, row in table.iterrows():
            config = {name: row[name].item() if hasattr(row[name], "item") else row[name]
                      for name in grid}
            value, trades = await self.intraday_engine(fixture, config, bundle)
            traded += trades
            with self.subTest(config=config):
                self.assertAlmostEqual(row["final_value"], value, delta=0.01)
                self.assertEqual(int(row["fills"]), trades)
        self.assertGreater(traded, 0)
        return table

    async def test_scheduled_intraday_trading(self):
        await self.assert_intraday_parity("intraday_trend.py", {"fast": [4, 8], "wait": [30, 90]},
                                          self.minute_bundle(seed=5))

    async def test_daily_and_intraday_history_and_before_trading_start(self):
        await self.assert_intraday_parity("intraday_history.py", {"days": [2, 3], "bars": [6, 12]},
                                          self.minute_bundle(seed=7))


    @unittest.skipUnless(__import__("tests.sweep.test_sweep_parity", fromlist=["x"]).mlx_available(),
                         "MLX is not installed")
    async def test_gpu_matches_exact_intraday(self):
        for fixture, grid, seed in (("intraday_trend.py", {"fast": [4, 8], "wait": [30, 90]}, 5),
                                    ("intraday_history.py", {"days": [2, 3], "bars": [6, 12]}, 7)):
            bundle = self.minute_bundle(seed=seed)
            exact = (await self.intraday_sweep(fixture, grid, bundle)).sort_index()
            gpu = (await self.intraday_sweep(fixture, grid, bundle, backend=GPU)).sort_index()
            with self.subTest(fixture=fixture):
                np.testing.assert_array_equal(gpu["fills"].to_numpy(), exact["fills"].to_numpy())
                np.testing.assert_allclose(gpu["final_value"], exact["final_value"], rtol=1e-6)


if __name__ == "__main__":
    unittest.main()
