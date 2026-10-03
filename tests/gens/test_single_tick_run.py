"""One live tick through the whole engine: the clock, the signals, the performance packet.

The unit tests of :mod:`ziplime.gens.domain.single_tick_clock` pin the events it emits; this pins
what they are for. A tick has to run to completion and hand back its performance record -- the
closing event is what produces it -- and a strategy with ``compute_signals`` has to get its
signals at its own emission rate, over the warm-up it declared, exactly as in its backtest.
"""
import datetime

from tests.vectorized.test_vectorized_signals import (
    CALENDAR, CASH, END, FIXTURES, START, SignalsTestCase,
)
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.gens.domain.single_tick_clock import SingleTickClock


class SingleTickRunTests(SignalsTestCase):
    async def run_tick(self, tick_dt: datetime.datetime):
        from ziplime.core.run_simulation import run_simulation

        clock = SingleTickClock(trading_calendar=self.calendar,
                                emission_rate=datetime.timedelta(days=1), tick_dt=tick_dt)
        return await run_simulation(
            start_date=datetime.datetime.combine(START, datetime.time.min, tzinfo=self.calendar.tz),
            end_date=datetime.datetime.combine(END, datetime.time.max, tzinfo=self.calendar.tz),
            trading_calendar=CALENDAR, emission_rate=datetime.timedelta(days=1),
            total_cash=CASH, market_data_source=self.bundle(), custom_data_sources=[],
            algorithm_file=str(FIXTURES / "signals_vectorised.py"), stop_on_error=True,
            asset_service=self.asset_service, benchmark_asset_symbol=None, benchmark_returns=None,
            equity_commission=NoCommission(), equity_slippage=NoSlippage(), max_leverage=10.0,
            print_algo=False, clock=clock)

    async def test_a_daily_tick_runs_one_bar_to_completion(self):
        tick_dt = datetime.datetime(2024, 9, 30, 12, 0, tzinfo=self.calendar.tz)

        result = await self.run_tick(tick_dt)

        self.assertEqual(result.errors, [], "a tick covering its one session is not a short run")
        self.assertEqual(len(result.perf), 1)

    async def test_a_tick_on_a_non_session_day_runs_no_bar(self):
        saturday = datetime.datetime(2024, 9, 28, 12, 0, tzinfo=self.calendar.tz)

        result = await self.run_tick(saturday)

        self.assertEqual(len(result.perf), 0)
