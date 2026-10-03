"""``order_target*`` under next-bar execution, where the fills land before ``handle_data``.

Next-bar execution fills the previous bar's orders and then calls ``handle_data``. The fills went
to the ledger, but ``context.portfolio`` is a cache refreshed only at the top of the bar, so the
strategy read positions from before the fills it had just received. Every ``order_target*`` call
then re-sent a difference that had already been traded: a long-only book that swaps two names
every session went short on one and doubled up on the other, and on real strategies gross
leverage ran past 4 with cash a hundred thousand dollars negative. Same-bar execution never saw
it, because there ``handle_data`` runs before the fills.
"""
import datetime
import shutil
import tempfile
import unittest
from pathlib import Path

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.utils.calendar_utils import get_calendar

from tests.core.test_delisting import CALENDAR, END, FIXTURES, PROJECT_ROOT, START, bars, make_bundle


class NextBarRebalanceTests(unittest.IsolatedAsyncioTestCase):
    """Two names at fixed prices, the whole book swapped between them every session."""

    async def asyncSetUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-next-bar-")
        db_path = Path(self._temp.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db_path)
        self.asset_service = get_asset_service(db_path=str(db_path))
        self.alive, self.doomed = [
            await self.asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=symbol, mic="XNYS"), asset_type=AssetType.EQUITY)
            for symbol in ("JNJ", "KO")
        ]
        self.calendar = get_calendar(CALENDAR)
        self.start = datetime.datetime.combine(START, datetime.time.min, tzinfo=self.calendar.tz)
        self.end = datetime.datetime.combine(END, datetime.time.max, tzinfo=self.calendar.tz)
        sessions = self.calendar.sessions_in_range(START, END)
        self.closes = list(
            self.calendar.schedule.loc[sessions, "close"].dt.tz_convert(self.calendar.tz))

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    async def run_rotation(self, *, same_bar_execution: bool):
        rows = bars(self.alive, self.closes, 100.0) + bars(self.doomed, self.closes, 50.0)
        return await run_simulation(
            start_date=self.start, end_date=self.end, trading_calendar=CALENDAR,
            emission_rate=datetime.timedelta(days=1), total_cash=100_000.0,
            market_data_source=make_bundle(rows, self.calendar), custom_data_sources=[],
            algorithm_file=str(FIXTURES / "rotate_daily.py"),
            stop_on_error=True, asset_service=self.asset_service,
            benchmark_asset_symbol=None, benchmark_returns=None,
            equity_commission=NoCommission(), max_leverage=1.0,
            same_bar_execution=same_bar_execution,
            price_used_in_order_execution="close", print_algo=False)

    def assert_long_only_and_unlevered(self, result):
        shorts = [(index.date(), position.asset.symbol, position.amount)
                  for index, positions in result.perf["positions"].items()
                  for position in positions if position.amount < 0]
        self.assertEqual(shorts, [], "a long-only rotation must never hold a short")
        self.assertLessEqual(result.perf["gross_leverage"].max(), 1.0 + 1e-9)
        self.assertGreaterEqual(result.perf["ending_cash"].min(), 0.0)

    async def test_next_bar_rotation_stays_long_and_unlevered(self):
        self.assert_long_only_and_unlevered(await self.run_rotation(same_bar_execution=False))

    async def test_same_bar_rotation_stays_long_and_unlevered(self):
        """The control: here handle_data always ran before the fills."""
        self.assert_long_only_and_unlevered(await self.run_rotation(same_bar_execution=True))

    async def test_each_session_holds_one_name(self):
        """With fixed prices the book is exactly one name at 90% once the first swap has filled."""
        result = await self.run_rotation(same_bar_execution=False)
        for index, positions in list(result.perf["positions"].items())[2:]:
            self.assertEqual(len(positions), 1, f"{index.date()}: {positions}")


if __name__ == "__main__":
    unittest.main()
