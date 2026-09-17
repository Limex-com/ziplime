import datetime

from ziplime.gens.domain.single_execution_clock import SingleExecutionClock
from ziplime.trading.enums.simulation_event import SimulationEvent
from ziplime.utils.calendar_utils import get_calendar

CALENDAR = "XNYS"


def test_single_execution_clock_executes_only_on_requested_session_end():
    cal = get_calendar(CALENDAR)
    start = datetime.datetime(2024, 1, 2, 0, 0, tzinfo=cal.tz)
    end = datetime.datetime(2024, 1, 5, 23, 59, tzinfo=cal.tz)

    day_end_clock = SingleExecutionClock(
        start_date=start,
        end_date=end,
        trading_calendar=cal,
        emission_rate=datetime.timedelta(minutes=15),
        execute_on_period_end_bool=True,
    )
    events = list(day_end_clock)
    assert events[0] == (day_end_clock.sessions[-1], SimulationEvent.SESSION_START)
    assert events[-1] == (day_end_clock.minutes_by_session[day_end_clock.sessions[-1]][-1], SimulationEvent.SESSION_END)
    assert len(events) == 5
    assert [event for _, event in events if event == SimulationEvent.BAR] == [
        SimulationEvent.BAR,
    ]

    day_start_clock = SingleExecutionClock(
        start_date=start,
        end_date=end,
        trading_calendar=cal,
        emission_rate=datetime.timedelta(minutes=15),
        execute_on_period_end_bool=False,
    )
    events = list(day_start_clock)
    assert events[0] == (day_start_clock.sessions[0], SimulationEvent.SESSION_START)
    assert events[-1] == (day_start_clock.minutes_by_session[day_start_clock.sessions[0]][0], SimulationEvent.SESSION_END)
    assert len(events) == 5
    assert [event for _, event in events if event == SimulationEvent.BAR] == [
        SimulationEvent.BAR,
    ]


def test_single_execution_clock_matches_calendar_session_bounds():
    cal = get_calendar(CALENDAR)
    start = datetime.datetime(2024, 1, 6, 12, 0, tzinfo=cal.tz)
    end = datetime.datetime(2024, 1, 10, 12, 0, tzinfo=cal.tz)

    clock = SingleExecutionClock(
        start_date=start,
        end_date=end,
        trading_calendar=cal,
        emission_rate=datetime.timedelta(minutes=30),
        execute_on_period_end_bool=True,
    )

    expected = [d.date() for d in cal.sessions_in_range(start.date(), end.date())]
    assert clock.start_session == expected[0]
    assert clock.end_session == expected[-1]
    assert list(clock.sessions) == expected
    assert all(cal.is_session(day) for day in clock.sessions)
