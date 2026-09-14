"""Skipping the metrics nobody reads, and the promise that it changes nothing.

At an intraday rate `end_of_bar` fires on every bar -- 390 times a session at one minute -- and
in the default metric set all but one of those metrics do nothing but fill the minute packet,
which the executor discards on its way to a table with one row per session. Recomputing a
cumulative Sharpe ratio, alpha and beta every minute to keep the last one of each session
measured at 86% of an intraday run.

`intraday_metrics=False` stops doing it. The whole case for that default is that the reported
numbers do not move, so that is what this file tests -- column by column, at a minute rate, with
a strategy that actually trades.

The exception is the reason the switch is not simply "skip end_of_bar": `MaxLeverage` keeps a
running maximum, and a peak reached at 11:32 is gone by the close. It still runs on every bar.
"""
import datetime
import shutil
import tempfile
import unittest
from pathlib import Path

import numpy as np
import polars as pl

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.constants.data_type import DataType
from ziplime.core.ingest_data import get_asset_service
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services.bar_alignment import clock_minutes
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.metrics import default_metrics
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.utils.calendar_utils import get_calendar

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures"
CALENDAR = "XNYS"
MINUTE = datetime.timedelta(minutes=1)


class MetricClassificationTests(unittest.TestCase):
    """Which metrics may be skipped, decided by what they do rather than by a list."""

    def test_only_metrics_that_keep_state_run_on_every_bar(self):
        """`packet_only` is a claim about a metric's `end_of_bar`: that it writes into the packet
        and nothing else. Checked here against the code, so the claim cannot rot."""
        import ast
        import inspect

        for metric in default_metrics():
            end_of_bar = getattr(metric, "end_of_bar", None)
            if end_of_bar is None:
                continue
            try:
                source = inspect.getsource(end_of_bar)
            except (OSError, TypeError):  # pragma: no cover - not reachable for the built-ins
                continue
            tree = ast.parse(source.lstrip() if source[0].isspace() else source)
            writes_state = any(
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name) and target.value.id == "self"
                for node in ast.walk(tree)
                for target in (node.targets if isinstance(node, ast.Assign)
                               else [node.target] if isinstance(node, (ast.AugAssign, ast.AnnAssign))
                               else [])
            )
            claimed = getattr(metric, "packet_only", False)
            self.assertEqual(
                claimed, not writes_state,
                f"{type(metric).__name__} claims packet_only={claimed} but its end_of_bar "
                f"{'assigns to self' if writes_state else 'does not assign to self'}")

    def test_an_unmarked_metric_keeps_running(self):
        """A metric nobody classified is assumed to keep state, so an unfamiliar one is slow
        rather than silently wrong."""
        from ziplime.finance.metrics_tracker import MetricsTracker

        self.assertFalse(getattr(object(), "packet_only", False))
        self.assertIn("packet_only", inspect_source(MetricsTracker))

    def test_max_leverage_is_not_skippable(self):
        from ziplime.finance.metrics.max_leverage import MaxLeverage

        self.assertFalse(getattr(MaxLeverage(), "packet_only", False),
                         "MaxLeverage carries a running maximum; skipping bars loses intraday peaks")


def inspect_source(obj) -> str:
    import inspect

    return inspect.getsource(obj)


class IntradayMetricsTests(unittest.IsolatedAsyncioTestCase):
    """The switch, end to end over minute bars."""

    SESSIONS = 4

    async def asyncSetUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-intraday-")
        db = Path(self._temp.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db)
        self.asset_service = get_asset_service(db_path=str(db))
        self.calendar = get_calendar(CALENDAR)
        days = self.calendar.sessions_in_range(datetime.date(2024, 3, 1),
                                               datetime.date(2024, 6, 28))[:self.SESSIONS + 1]
        self.bundle_start, self.start = days[0].date(), days[1].date()
        self.end = days[-1].date()
        grid = clock_minutes(self.calendar, self.bundle_start, self.end, MINUTE)
        steps = np.arange(len(grid))
        # Shaped to cross the averages several times a session, so the run trades and the
        # leverage moves between the closes rather than only at them.
        self.prices = (150.0 + np.sin(steps / 90.0) * 5.0 + np.sin(steps / 11.0) * 1.2)
        self.listing = await self.asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol="AAPL", mic="XNGS"), asset_type=AssetType.EQUITY)
        self.grid = grid

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    def bundle(self) -> DataBundle:
        data = pl.DataFrame({
            "date": self.grid, "sid": [self.listing.sid] * len(self.grid),
            "symbol": ["AAPL"] * len(self.grid), "mic": ["XNGS"] * len(self.grid),
            "open": self.prices, "high": self.prices, "low": self.prices,
            "close": self.prices, "price": self.prices,
            "volume": np.full(len(self.grid), 1e6)}).sort("date")
        return DataBundle(
            name="intraday-metrics", version="1", start_date=data["date"].min(),
            end_date=data["date"].max(), trading_calendar=self.calendar, frequency=MINUTE,
            original_frequency=MINUTE, data_type=DataType.MARKET_DATA,
            timestamp=data["date"].max(), data=data,
            sid_indexes={self.listing.sid: (0, len(data))})

    async def run_it(self, intraday_metrics: bool):
        from ziplime.core.run_simulation import run_simulation

        return await run_simulation(
            start_date=datetime.datetime.combine(self.start, datetime.time.min,
                                                 tzinfo=self.calendar.tz),
            end_date=datetime.datetime.combine(self.end, datetime.time.max,
                                               tzinfo=self.calendar.tz),
            trading_calendar=CALENDAR, emission_rate=MINUTE, total_cash=100_000.0,
            market_data_source=self.bundle(), custom_data_sources=[],
            algorithm_file=str(FIXTURES / "intraday_sma.py"), stop_on_error=True,
            asset_service=self.asset_service, benchmark_asset_symbol=None,
            benchmark_returns=None, equity_commission=NoCommission(),
            equity_slippage=NoSlippage(), max_leverage=10.0, print_algo=False,
            intraday_metrics=intraday_metrics)

    async def test_every_reported_column_is_identical(self):
        """The claim the default rests on."""
        cheap = await self.run_it(intraday_metrics=False)
        full = await self.run_it(intraday_metrics=True)

        # Compared by name rather than by position: `default_metrics()` returns a set, so the
        # column order of a performance table already varies between two identical runs. That is
        # a separate defect and not this switch's doing -- see `test_column_order_is_not_stable`.
        self.assertEqual(set(cheap.perf.columns), set(full.perf.columns))
        self.assertEqual(len(cheap.perf), self.SESSIONS)
        compared = 0
        for column in full.perf.columns:
            try:
                a = np.asarray(cheap.perf[column], dtype=float)
                b = np.asarray(full.perf[column], dtype=float)
            except (TypeError, ValueError):
                continue          # object columns: transactions, orders, positions
            np.testing.assert_allclose(a, b, rtol=0, atol=1e-12, equal_nan=True,
                                       err_msg=f"{column} moved when the intraday metrics stopped")
            compared += 1
        self.assertGreater(compared, 10, "almost nothing was actually compared")

    async def test_the_strategy_traded(self):
        """A run that never opened a position would agree about everything for the wrong reason."""
        result = await self.run_it(intraday_metrics=False)
        self.assertGreater(sum(len(row) for row in result.perf["transactions"]), 2)

    async def test_intraday_leverage_peaks_survive(self):
        """The one metric that has to keep running. Its maximum is reached mid-session, so a run
        that only looked at the closes would report a lower one."""
        result = await self.run_it(intraday_metrics=False)
        peak = float(np.nanmax(np.asarray(result.perf["max_leverage"], dtype=float)))
        self.assertGreater(peak, 0.0)
        closes_only = float(np.nanmax(np.asarray(result.perf["gross_leverage"], dtype=float)))
        self.assertGreaterEqual(round(peak, 10), round(closes_only, 10),
                                "max_leverage fell below the leverage seen at the closes")

    async def test_column_order_is_not_stable_either_way(self):
        """Not a consequence of this switch, and worth pinning so it is not blamed on it.

        `default_metrics()` returns a set, so the metrics are iterated in whatever order their
        object ids fall into and the packet's keys -- hence the frame's columns -- come out in a
        different order on every run. Two runs of the same backtest with the same flag already
        disagree about column order. Anything reading the table by position is reading a different
        column each time.
        """
        first = await self.run_it(intraday_metrics=False)
        second = await self.run_it(intraday_metrics=False)
        self.assertEqual(set(first.perf.columns), set(second.perf.columns))
        for column in first.perf.columns:
            try:
                np.testing.assert_allclose(
                    np.asarray(first.perf[column], dtype=float),
                    np.asarray(second.perf[column], dtype=float),
                    rtol=0, atol=1e-12, equal_nan=True)
            except (TypeError, ValueError):
                continue

    async def test_it_is_off_by_default(self):
        from ziplime.core.run_simulation import run_simulation
        import inspect

        default = inspect.signature(run_simulation).parameters["intraday_metrics"].default
        self.assertIs(default, False)


if __name__ == "__main__":
    unittest.main(verbosity=2)
