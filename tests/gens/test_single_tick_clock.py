import datetime
import unittest

from exchange_calendars import get_calendar

from ziplime.gens.domain.single_tick_clock import SingleTickClock
from ziplime.trading.enums.simulation_event import SimulationEvent


def _midday_of_a_session(calendar) -> datetime.datetime:
    """Thirty minutes after the open of a fixed, known XMOS session."""
    session = datetime.date(2026, 9, 25)
    assert calendar.is_session(session)
    first_minute = calendar.session_first_minute(session).tz_convert(calendar.tz).to_pydatetime()
    return first_minute + datetime.timedelta(minutes=30)


class SingleTickClockTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.calendar = get_calendar("XMOS")
        cls.tick_dt = _midday_of_a_session(cls.calendar)

    def clock(self, emission_rate: datetime.timedelta, tick_dt: datetime.datetime | None = None):
        return SingleTickClock(trading_calendar=self.calendar, emission_rate=emission_rate,
                               tick_dt=tick_dt or self.tick_dt)

    def test_a_daily_bar_is_closed_by_session_end(self):
        # At a daily rate the engine builds the performance packet -- the one a run
        # finishes on -- on SESSION_END; without it the tick never produces one.
        events = [action for _, action in self.clock(datetime.timedelta(days=1))]
        self.assertEqual(events, [
            SimulationEvent.SESSION_START,
            SimulationEvent.BEFORE_TRADING_START_BAR,
            SimulationEvent.BAR,
            SimulationEvent.SESSION_END,
        ])

    def test_an_intraday_bar_is_closed_by_emission_rate_end(self):
        for minutes in (1, 5, 60):
            with self.subTest(minutes=minutes):
                events = [action for _, action in self.clock(datetime.timedelta(minutes=minutes))]
                self.assertEqual(events, [
                    SimulationEvent.SESSION_START,
                    SimulationEvent.BEFORE_TRADING_START_BAR,
                    SimulationEvent.BAR,
                    SimulationEvent.EMISSION_RATE_END,
                ])

    def test_the_bar_is_the_tick(self):
        events = {action: dt for dt, action in self.clock(datetime.timedelta(days=1))}
        self.assertEqual(events[SimulationEvent.BAR], self.tick_dt)
        self.assertEqual(events[SimulationEvent.SESSION_START], self.tick_dt.date())
        self.assertLess(events[SimulationEvent.BEFORE_TRADING_START_BAR], self.tick_dt)
        self.assertEqual(events[SimulationEvent.BEFORE_TRADING_START_BAR].date(), self.tick_dt.date())

    def test_the_run_is_exactly_the_ticks_session(self):
        # The engine reads clock.sessions as the run's length; a longer window turns
        # every tick into "a record covering 1 of N sessions".
        clock = self.clock(datetime.timedelta(days=1))
        self.assertEqual(list(clock.sessions), [self.tick_dt.date()])
        self.assertEqual(clock.end_session, self.tick_dt.date())

    def test_a_non_session_day_emits_nothing(self):
        saturday = datetime.datetime(2026, 9, 26, 12, 0, tzinfo=self.calendar.tz)
        self.assertEqual(list(self.clock(datetime.timedelta(days=1), tick_dt=saturday)), [])

    def test_it_is_single_shot_and_never_blocks(self):
        clock = self.clock(datetime.timedelta(days=1))
        first = list(clock)
        self.assertEqual(first, list(clock))
        self.assertEqual(len(first), 4)


if __name__ == "__main__":
    unittest.main()
