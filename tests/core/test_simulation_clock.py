import datetime

from ziplime.gens.domain.simulation_clock import SimulationClock
from ziplime.trading.enums.simulation_event import SimulationEvent
from ziplime.utils.calendar_utils import get_calendar

CALENDAR = "XNYS"


def _clock(start: datetime.datetime, end: datetime.datetime, rate: datetime.timedelta):
    tz = get_calendar(CALENDAR).tz
    return SimulationClock(
        start_date=start.replace(tzinfo=tz),
        end_date=end.replace(tzinfo=tz),
        trading_calendar=get_calendar(CALENDAR),
        emission_rate=rate,
    )


def test_simulation_clock_snaps_non_session_bounds_to_valid_trading_days():
    tz = get_calendar(CALENDAR).tz
    clock = _clock(
        start=datetime.datetime(2024, 1, 1, 0, 0),
        end=datetime.datetime(2024, 1, 8, 23, 59),
        rate=datetime.timedelta(minutes=5),
    )

    assert clock.start_session == datetime.date(2024, 1, 2)
    assert clock.end_session == datetime.date(2024, 1, 8)
    assert list(clock.sessions)[:2] == [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]
    assert list(clock.sessions)[-1] == datetime.date(2024, 1, 8)
    assert all(get_calendar(CALENDAR).is_session(day) for day in clock.sessions)
    assert clock.first_open.tzinfo == tz
    assert clock.last_close.tzinfo == tz


def test_simulation_clock_emits_session_boundaries_and_intraday_bars():
    clock = _clock(
        start=datetime.datetime(2024, 1, 2, 0, 0),
        end=datetime.datetime(2024, 1, 4, 23, 59),
        rate=datetime.timedelta(minutes=5),
    )

    events = list(clock)
    bar_events = [(stamp, event) for stamp, event in events if event == SimulationEvent.BAR]
    emission_events = [(stamp, event) for stamp, event in events if event == SimulationEvent.EMISSION_RATE_END]

    assert events[0] == (datetime.date(2024, 1, 2), SimulationEvent.SESSION_START)
    assert events[-1] == (clock.minutes_by_session[clock.sessions[-1]][-1], SimulationEvent.SESSION_END)
    assert len(bar_events) == len(emission_events)
    assert len(bar_events) == sum(len(minutes) for minutes in clock.minutes_by_session.values())
    assert [stamp for stamp, _ in bar_events] == [stamp for stamp, _ in emission_events]

    for session in clock.sessions:
        minutes = clock.minutes_by_session[session]
        assert minutes[0] in {stamp for stamp, event in events if event == SimulationEvent.BAR}
        assert minutes[-1] in {stamp for stamp, event in events if event == SimulationEvent.SESSION_END}


def test_simulation_clock_daily_rate_emits_one_bar_per_session():
    clock = _clock(
        start=datetime.datetime(2024, 1, 2, 0, 0),
        end=datetime.datetime(2024, 1, 5, 23, 59),
        rate=datetime.timedelta(days=1),
    )

    events = list(clock)
    session_starts = [(stamp, event) for stamp, event in events if event == SimulationEvent.SESSION_START]
    bar_events = [(stamp, event) for stamp, event in events if event == SimulationEvent.BAR]
    emission_events = [(stamp, event) for stamp, event in events if event == SimulationEvent.EMISSION_RATE_END]
    session_end_events = [(stamp, event) for stamp, event in events if event == SimulationEvent.SESSION_END]

    assert len(session_starts) == len(clock.sessions)
    assert len(bar_events) == len(clock.sessions)
    assert len(emission_events) == len(clock.sessions)
    assert len(session_end_events) == len(clock.sessions)
    assert [stamp for stamp, _ in bar_events] == [stamp for stamp, _ in emission_events]
    assert all(len(values) == 1 for values in clock.minutes_by_session.values())


def test_simulation_clock_session_list_matches_xnys_calendar_range():
    cal = get_calendar(CALENDAR)
    start = datetime.datetime(2024, 1, 2, 9, 30, tzinfo=cal.tz)
    end = datetime.datetime(2024, 1, 12, 15, 0, tzinfo=cal.tz)

    clock = SimulationClock(
        start_date=start,
        end_date=end,
        trading_calendar=cal,
        emission_rate=datetime.timedelta(minutes=5),
    )

    expected = [d.date() for d in cal.sessions_in_range(start.date(), end.date())]
    assert list(clock.sessions) == expected
    assert clock.start_session == expected[0]
    assert clock.end_session == expected[-1]


def test_simulation_clock_clips_non_session_bounds_to_next_and_previous_valid_days():
    cal = get_calendar(CALENDAR)
    start = datetime.datetime(2024, 1, 6, 12, 0, tzinfo=cal.tz)  # Saturday
    end = datetime.datetime(2024, 1, 10, 12, 0, tzinfo=cal.tz)  # Wednesday

    clock = SimulationClock(
        start_date=start,
        end_date=end,
        trading_calendar=cal,
        emission_rate=datetime.timedelta(minutes=30),
    )

    assert clock.start_session == datetime.date(2024, 1, 8)
    assert clock.end_session == datetime.date(2024, 1, 10)
    assert list(clock.sessions) == [
        datetime.date(2024, 1, 8),
        datetime.date(2024, 1, 9),
        datetime.date(2024, 1, 10),
    ]
    assert all(cal.is_session(day) for day in clock.sessions)
