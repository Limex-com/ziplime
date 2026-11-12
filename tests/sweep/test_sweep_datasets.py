"""context.datasets in a sweep: the same as-of statements as the engine, combination by combination."""
import contextlib
import datetime
import io
import json
import unittest
from pathlib import Path

from tests.vectorized.test_vectorized_fundamentals import (
    CALENDAR, CASH, END, START, FundamentalsTestCase,
)
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.sweep import SweepParams, run_sweep

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "sweep" / "fundamentals.py"


class DatasetSweepTests(FundamentalsTestCase):
    async def engine(self, config: dict):
        from ziplime.core.run_simulation import run_simulation

        path = Path(self._temp.name) / "config.json"
        path.write_text(json.dumps(config))
        with contextlib.redirect_stdout(io.StringIO()):
            result = await run_simulation(
                start_date=datetime.datetime.combine(START, datetime.time.min, tzinfo=self.calendar.tz),
                end_date=datetime.datetime.combine(END, datetime.time.max, tzinfo=self.calendar.tz),
                trading_calendar=CALENDAR, emission_rate=datetime.timedelta(days=1),
                total_cash=CASH, market_data_source=self.bundle(),
                custom_data_sources=[await self.source()], algorithm_file=str(FIXTURE),
                config_file=str(path), stop_on_error=True, asset_service=self.asset_service,
                benchmark_asset_symbol=None, benchmark_returns=None,
                equity_commission=NoCommission(), equity_slippage=NoSlippage(),
                same_bar_execution=False, max_leverage=10.0, print_algo=False)
        self.assertFalse(result.errors)
        return (float(result.perf["portfolio_value"].iloc[-1]),
                sum(len(row) for row in result.perf["transactions"]))

    async def test_indicator_on_a_dataset_matches_the_engine(self):
        grid = {"size": [100, 250], "margin": [0.0, 0.1]}
        params = SweepParams.model_validate({
            "algorithm_file": str(FIXTURE), "start_date": str(START), "end_date": str(END),
            "trading_calendar": CALENDAR, "total_cash": CASH, "grid": grid, "backend": "exact",
            "verify": False, "refine_top": 0, "output_dir": str(Path(self._temp.name) / "out")})
        with contextlib.redirect_stdout(io.StringIO()):
            table = await run_sweep(params, Path(self._temp.name), say=lambda message: None,
                                    market_data=self.bundle(), asset_service=self.asset_service,
                                    custom_data_sources=[await self.source()])
        traded = 0
        for _, row in table.iterrows():
            config = {"size": int(row["size"]), "margin": float(row["margin"])}
            value, trades = await self.engine(config)
            traded += trades
            with self.subTest(config=config):
                self.assertAlmostEqual(row["final_value"], value, delta=0.01)
                self.assertEqual(int(row["fills"]), trades)
        self.assertGreater(traded, 0)


if __name__ == "__main__":
    unittest.main()
