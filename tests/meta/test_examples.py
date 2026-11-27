"""The metastrategy examples run as written, on synthetic SBER bars on the Moscow calendar."""
import datetime
import shutil
import tempfile
import unittest
from pathlib import Path

import numpy as np

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.finance.commission import PerDollar
from ziplime.utils.calendar_utils import get_calendar

from tests.core.test_delisting import PROJECT_ROOT, make_bundle

EXAMPLES = PROJECT_ROOT / "examples" / "metastrategy"
CALENDAR = "XMOS"


class MetastrategyExampleTests(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-meta-examples-")
        db_path = Path(self._temp.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db_path)
        self.asset_service = get_asset_service(db_path=str(db_path))
        sber = await self.asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol="SBER", mic="MISX"), asset_type=AssetType.EQUITY)
        self.calendar = get_calendar(CALENDAR)
        sessions = self.calendar.sessions_in_range(datetime.date(2023, 1, 3), datetime.date(2024, 12, 27))
        closes = list(self.calendar.schedule.loc[sessions, "close"].dt.tz_convert(self.calendar.tz))
        rng = np.random.default_rng(7)
        # A cycle with noise: something for both a trend and a mean-reversion strategy to do.
        steps = 0.0004 + 0.012 * np.sin(np.arange(len(closes)) / 15.0) / 6 + rng.normal(0, 0.015, len(closes))
        prices = 250.0 * np.cumprod(1.0 + steps)
        self.bundle = make_bundle([
            {"date": close, "sid": sber.sid, "symbol": sber.symbol, "mic": sber.mic, "open": p,
             "high": p, "low": p, "close": p, "price": p, "volume": 1e9}
            for close, p in zip(closes, prices)], self.calendar)
        tz = self.calendar.tz
        self.start = datetime.datetime(2023, 9, 1, tzinfo=tz)
        self.end = datetime.datetime(2024, 12, 27, 23, 59, tzinfo=tz)

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    async def simulate(self, name: str):
        return await run_simulation(
            start_date=self.start, end_date=self.end, trading_calendar=CALENDAR,
            emission_rate=datetime.timedelta(days=1), total_cash=1_000_000.0,
            market_data_source=self.bundle, custom_data_sources=[],
            algorithm_file=str(EXAMPLES / name), stop_on_error=True,
            asset_service=self.asset_service, equity_commission=PerDollar(cost=0.0005),
            same_bar_execution=False, print_algo=False)

    async def test_sharpe_switch(self):
        result = await self.simulate("meta_sharpe_switch.py")
        self.assertEqual(result.errors, [])
        self.assertEqual(set(result.strategies), {"bollinger", "trend"})
        for strategy in result.strategies.values():
            self.assertEqual(strategy.errors, [])
            self.assertGreater(len(strategy.trading_algorithm._ledger.transaction_history), 0)
        self.assertTrue(set(result.perf["leader"].dropna()) <= {1, 2})
        context = result.trading_algorithm
        self.assertGreater(context.strategy1.units + context.strategy2.units, 0)

    async def test_cluster(self):
        result = await self.simulate("meta_cluster.py")
        self.assertEqual(result.errors, [])
        self.assertEqual(len(result.strategies), 9)
        context = result.trading_algorithm
        held = [member for member in context.bb if member.units]
        self.assertEqual(len(held), 3)
        self.assertGreater(context.portfolio.positions_value, 0.9 * context.portfolio.portfolio_value)
        # Nine different strategies, not one strategy nine times.
        finals = {round(member.nav, 8) for member in context.bb}
        self.assertGreater(len(finals), 5)


if __name__ == "__main__":
    unittest.main()
