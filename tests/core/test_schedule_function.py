"""`schedule_function` and the calendar rules behind it.

The mechanism was ported into this fork intact but unexercised, and four separate things had
rotted in the move: month rules could not be constructed at all, the period sets they compare
against were rebuilt on every bar, an algorithm that scheduled all of its work crashed on the
first one, and a pandas deprecation was being tripped once per bar. Nothing here is subtle -- the
first three make the API unusable in different ways -- which is exactly why they went unnoticed
without a test.
"""
import asyncio
import datetime
import warnings
from pathlib import Path

import pytest

from ziplime.core.algorithm_file import AlgorithmFile
from ziplime.utils.calendar_utils import get_calendar
from ziplime.utils.events import (
    NthTradingDayOfMonth, NthTradingDayOfQuarter, date_rules, make_eventrule, time_rules,
)

CALENDAR = "XNYS"


def fires(date_rule, start: str, end: str, calendar: str = CALENDAR) -> list[datetime.date]:
    """The sessions a date rule triggers on, driven the way the simulation clock drives it."""
    cal = get_calendar(calendar)
    rule = make_eventrule(date_rule=date_rule, time_rule=time_rules.every_minute(),
                          cal=cal, half_days=True)
    sessions = cal.sessions_in_range(start, end)
    closes = cal.schedule.loc[sessions, "close"].dt.tz_convert(cal.tz)
    return [c.to_pydatetime().date() for c in closes
            if rule.should_trigger(c.to_pydatetime())]


def test_month_start_fires_on_the_first_trading_day_of_each_month():
    """The rule that could not be built: ``n`` reached the bounds check as an uncalled curry."""
    assert fires(date_rules.month_start(), "2015-01-01", "2015-12-31") == [
        datetime.date(2015, 1, 2), datetime.date(2015, 2, 2), datetime.date(2015, 3, 2),
        datetime.date(2015, 4, 1), datetime.date(2015, 5, 1), datetime.date(2015, 6, 1),
        datetime.date(2015, 7, 1), datetime.date(2015, 8, 3), datetime.date(2015, 9, 1),
        datetime.date(2015, 10, 1), datetime.date(2015, 11, 2), datetime.date(2015, 12, 1)]


def test_month_end_fires_on_the_last_trading_day_of_each_month():
    fired = fires(date_rules.month_end(), "2015-01-01", "2015-03-31")
    assert fired == [datetime.date(2015, 1, 30), datetime.date(2015, 2, 27),
                     datetime.date(2015, 3, 31)]


def test_month_start_offset_skips_that_many_trading_days():
    fired = fires(date_rules.month_start(days_offset=1), "2015-01-01", "2015-02-28")
    assert fired == [datetime.date(2015, 1, 5), datetime.date(2015, 2, 3)]


def test_week_start_respects_holidays():
    """A week beginning on a market holiday starts on the first session that is not one."""
    fired = fires(date_rules.week_start(), "2015-01-01", "2015-02-28")
    # 19 January is Martin Luther King Day and 16 February is Presidents' Day.
    assert datetime.date(2015, 1, 20) in fired and datetime.date(2015, 1, 19) not in fired
    assert datetime.date(2015, 2, 17) in fired and datetime.date(2015, 2, 16) not in fired


def test_quarter_start_and_end():
    """Quarterly rules, which upstream does not carry -- fundamentals arrive four times a year."""
    assert fires(date_rules.quarter_start(), "2016-01-01", "2016-12-31") == [
        datetime.date(2016, 1, 4), datetime.date(2016, 4, 1),
        datetime.date(2016, 7, 1), datetime.date(2016, 10, 3)]
    assert fires(date_rules.quarter_end(), "2016-01-01", "2016-12-31") == [
        datetime.date(2016, 3, 31), datetime.date(2016, 6, 30),
        datetime.date(2016, 9, 30), datetime.date(2016, 12, 30)]


def test_quarter_start_fires_four_times_a_year_for_a_decade():
    fired = fires(date_rules.quarter_start(), "2006-01-01", "2015-12-31")
    assert len(fired) == 40
    assert {d.month for d in fired} == {1, 4, 7, 10}


@pytest.mark.parametrize("rule", [NthTradingDayOfMonth, NthTradingDayOfQuarter])
def test_integral_float_offsets_are_coerced_and_fractional_ones_refused(rule):
    """``month_start(1.0)`` means the second trading day. ``month_start(1.5)`` means nothing."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert rule(1.0).td_delta == 1
    assert any("Coercing to int" in str(w.message) for w in caught)
    with pytest.raises(TypeError):
        rule(1.5)


def test_period_values_are_computed_once_per_calendar():
    """The set is a groupby over every session the calendar knows -- 36 410 of them for XNYS.

    Rebuilt on every bar it cost 2.2ms a bar, which is most of a ten-year run's time in a rule
    that answers *is it the first of the month*.
    """
    cal = get_calendar(CALENDAR)
    rule = NthTradingDayOfMonth(0)
    rule.cal = cal
    first = rule.execution_period_values
    assert rule.execution_period_values is first

    # Threading a different calendar through must not answer for the old one.
    other = get_calendar("CMES")
    rule.cal = other
    assert rule.execution_period_values is not first


def test_rules_do_not_trip_pandas_deprecations():
    """A `FutureWarning` once per bar per rule is noise that hides real warnings."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        fires(date_rules.week_start(), "2015-01-01", "2015-03-31")
        fires(date_rules.month_start(), "2015-01-01", "2015-03-31")
        fires(date_rules.quarter_start(), "2015-01-01", "2015-12-31")


def test_an_algorithm_that_only_schedules_needs_no_handle_data(tmp_path: Path):
    """The ordinary zipline shape: everything in `initialize`, nothing at module level.

    The engine awaits both hooks, so defaulting them to a plain `def` handed it `None` to await
    and the run died on the first bar.
    """
    script = tmp_path / "algo.py"
    script.write_text("from ziplime.utils.events import date_rules\n"
                      "async def initialize(context):\n"
                      "    context.schedule_function(rebalance, date_rules.month_start())\n"
                      "async def rebalance(context, data):\n"
                      "    pass\n")
    algo = AlgorithmFile(algorithm_file=str(script))
    assert asyncio.iscoroutinefunction(algo.handle_data)
    assert asyncio.iscoroutinefunction(algo.initialize)
    asyncio.run(algo.handle_data(None, None))


def intraday_grid(day: str, emission_rate: datetime.timedelta, calendar: str = CALENDAR):
    """The minutes an intraday simulation clock emits for one session.

    Built the way :class:`~ziplime.gens.domain.simulation_clock.SimulationClock` builds them:
    from the session's first minute to its close, stepping by the emission rate.
    """
    import polars as pl

    cal = get_calendar(calendar)
    session = cal.sessions_in_range(day, day)
    opens = cal.first_minutes.loc[session].dt.tz_convert(cal.tz)
    closes = cal.schedule.loc[session, "close"].dt.tz_convert(cal.tz)
    return list(pl.datetime_range(opens.iloc[0], closes.iloc[0], interval=emission_rate,
                                  eager=True))


def times_fired(time_rule, emission_rate: datetime.timedelta, day: str = "2026-09-02"):
    cal = get_calendar(CALENDAR)
    rule = make_eventrule(date_rule=date_rules.every_day(), time_rule=time_rule,
                          cal=cal, half_days=True)
    return [minute.time().strftime("%H:%M")
            for minute in intraday_grid(day, emission_rate) if rule.should_trigger(minute)]


MINUTE = datetime.timedelta(minutes=1)
FIVE_MINUTES = datetime.timedelta(minutes=5)


def test_time_rules_on_a_one_minute_grid():
    """The resolution these rules were written for, and the behaviour that must not change."""
    assert times_fired(time_rules.market_open(minutes=1), MINUTE) == ["09:31"]
    assert times_fired(time_rules.market_open(minutes=30), MINUTE) == ["10:00"]
    assert times_fired(time_rules.market_close(minutes=30), MINUTE) == ["15:30"]
    assert times_fired(time_rules.market_close(minutes=1), MINUTE) == ["15:59"]


def test_time_rules_fire_on_the_first_bar_at_or_after_their_target():
    """A five-minute grid has bars at 09:31, 09:36, 09:41 -- and none at 10:00.

    Matching the target minute exactly meant `market_open(minutes=30)` never fired at any rate but
    one minute, silently: the scheduled function was simply never called and the run completed.
    """
    assert times_fired(time_rules.market_open(minutes=1), FIVE_MINUTES) == ["09:31"]
    assert times_fired(time_rules.market_open(minutes=30), FIVE_MINUTES) == ["10:01"]
    assert times_fired(time_rules.market_close(minutes=30), FIVE_MINUTES) == ["15:31"]


def test_an_offset_shorter_than_a_bar_has_no_bar_to_fire_on():
    """`market_close(minutes=1)` asks for 15:59 and the last five-minute bar is 15:56.

    Pinned rather than worked around: there is no bar in the last minute of the session, so the
    honest answer is that the rule does not fire. Ask for an offset at least one bar long.
    """
    assert times_fired(time_rules.market_close(minutes=1), FIVE_MINUTES) == []


def test_a_time_rule_fires_once_a_session_not_once_a_bar():
    """`>=` on its own would fire on every bar after the target; `OncePerDay` is what stops it."""
    cal = get_calendar(CALENDAR)
    rule = make_eventrule(date_rule=date_rules.every_day(),
                          time_rule=time_rules.market_open(minutes=30),
                          cal=cal, half_days=True)
    fired = [minute for day in ("2026-09-02", "2026-09-03", "2026-09-04")
             for minute in intraday_grid(day, FIVE_MINUTES) if rule.should_trigger(minute)]
    assert [m.date().isoformat() for m in fired] == ["2026-09-02", "2026-09-03", "2026-09-04"]
    assert {m.time().strftime("%H:%M") for m in fired} == {"10:01"}
