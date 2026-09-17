import datetime

import ziplime.gens.domain.realtime_clock as realtime_clock_module
from ziplime.gens.domain.realtime_clock import RealtimeClock
from ziplime.trading.enums.simulation_event import SimulationEvent
from ziplime.utils.calendar_utils import get_calendar

CALENDAR = "XNYS"


def test_realtime_clock_matches_calendar_session_bounds():
    cal = get_calendar(CALENDAR)
    start = datetime.datetime(2024, 1, 6, 12, 0, tzinfo=cal.tz)  # Saturday
    end = datetime.datetime(2024, 1, 10, 12, 0, tzinfo=cal.tz)  # Wednesday

    clock = RealtimeClock(
        trading_calendar=cal,
        start_date=start,
        end_date=end,
        emission_rate=datetime.timedelta(minutes=30),
    )

    expected = [d.date() for d in cal.sessions_in_range(start.date(), end.date())]
    assert clock.start_session == expected[0]
    assert clock.end_session == expected[-1]
    assert list(clock.sessions) == expected
    assert all(cal.is_session(day) for day in clock.sessions)


def test_realtime_clock_emits_before_trading_start_session_and_close_events(monkeypatch):
    cal = get_calendar(CALENDAR)
    start = datetime.datetime(2024, 1, 2, 8, 0, tzinfo=cal.tz)
    end = datetime.datetime(2024, 1, 2, 17, 0, tzinfo=cal.tz)
    clock = RealtimeClock(
        trading_calendar=cal,
        start_date=start,
        end_date=end,
        emission_rate=datetime.timedelta(minutes=30),
    )

    fake_now = {"value": clock.before_trading_start_minutes[0]}

    class FrozenDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            value = fake_now["value"]
            return value.astimezone(tz) if tz is not None else value

    monkeypatch.setattr(realtime_clock_module.datetime, "datetime", FrozenDateTime)
    monkeypatch.setattr(realtime_clock_module.time, "sleep", lambda seconds: None)
    clock._sleep_and_increase_time = lambda sleep_seconds: fake_now.__setitem__("value", fake_now["value"] + datetime.timedelta(minutes=1)) or fake_now["value"]

    events = list(clock)

    assert events[0] == (clock.before_trading_start_minutes[0], SimulationEvent.BEFORE_TRADING_START_BAR)
    assert events[1] == (clock.sessions[0], SimulationEvent.SESSION_START)
    assert any(event == SimulationEvent.BAR for _, event in events)
    assert SimulationEvent.SESSION_END not in {event for _, event in events}
