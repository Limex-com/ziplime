import datetime
import subprocess
import sys
from pathlib import Path

import pytest
import pytz

from ziplime.trading.base_trading_algorithm import BaseTradingAlgorithm
from ziplime.utils.calendar_utils import get_calendar
from ziplime.utils.event_rule import EventRule
from ziplime.utils.events import (
    AfterOpen,
    Always,
    BeforeClose,
    ComposedRule,
    Event,
    EventManager,
    Never,
    NotHalfDay,
    OncePerDay,
    _build_date,
    _build_offset,
    _check_if_not_called,
    _out_of_range_error,
    _td_check,
    calendars,
    date_rules,
    ensure_utc,
    lossless_float_to_int,
    make_eventrule,
    time_rules,
)


def test_base_algorithm_import_does_not_load_scheduler_or_concrete_algorithm():
    result = subprocess.run(
        [sys.executable, "-c", "\n".join([
            "import sys",
            "from ziplime.trading.base_trading_algorithm import BaseTradingAlgorithm",
            "assert 'ziplime.utils.events' not in sys.modules",
            "assert 'ziplime.trading.trading_algorithm' not in sys.modules",
        ])],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_event_rule_keeps_legacy_import_and_abstract_contract():
    from ziplime.utils.events import EventRule as LegacyEventRule

    assert LegacyEventRule is EventRule
    assert isinstance(Always(), LegacyEventRule)
    with pytest.raises(TypeError, match="abstract"):
        EventRule()


@pytest.mark.asyncio
async def test_custom_extracted_rule_dispatches_base_algorithm_context():
    threshold = datetime.datetime(2024, 1, 1, 10, 0, tzinfo=pytz.utc)

    class AtOrAfter(EventRule):
        def should_trigger(self, dt: datetime.datetime) -> bool:
            return dt >= threshold

    rule = AtOrAfter()
    assert rule.cal is None
    calendar = get_calendar("XNYS")
    rule.cal = calendar
    assert rule.cal is calendar

    context, data = BaseTradingAlgorithm(), object()
    called = []

    async def callback(algorithm, current_data):
        called.append((algorithm, current_data))

    manager = EventManager()
    manager.add_event(Event(rule=rule, callback=callback))

    await manager.handle_data(context, data, threshold - datetime.timedelta(minutes=1))
    assert called == []
    await manager.handle_data(context, data, threshold)
    assert called == [(context, data)]


def test_ensure_utc():
    naive = datetime.datetime(2024, 1, 1, 10, 0)
    aware = ensure_utc(naive, tz="UTC")
    assert aware.tzinfo == pytz.utc
    assert aware.hour == 10


def test_error_and_td_check():
    err = _out_of_range_error(5, var="offset")
    assert "offset must be in between 0 and 4 inclusive" in str(err)

    err2 = _out_of_range_error(2, 5, var="day")
    assert "day must be in between 2 and 4 inclusive" in str(err2)

    assert _td_check(datetime.timedelta(minutes=5)) == datetime.timedelta(minutes=5)
    with pytest.raises(ValueError, match="offset must be in between 1 minute and 12 hours"):
        _td_check(datetime.timedelta(seconds=10))


def test_build_offset_and_date():
    offset = _build_offset(None, {"minutes": 15}, default=datetime.timedelta(minutes=1))
    assert offset == datetime.timedelta(minutes=15)

    default_off = _build_offset(None, {}, default=datetime.timedelta(minutes=1))
    assert default_off == datetime.timedelta(minutes=1)

    with pytest.raises(ValueError, match="Cannot pass kwargs and an offset"):
        _build_offset(datetime.timedelta(minutes=10), {"minutes": 15}, default=None)

    with pytest.raises(TypeError, match="Must pass 'hours' and/or 'minutes' as keywords"):
        _build_offset("not-a-timedelta", {}, default=None)

    # _build_date
    d = _build_date(None, {"year": 2024, "month": 1, "day": 1})
    assert d == datetime.date(2024, 1, 1)

    with pytest.raises(ValueError, match="Must pass a date or kwargs"):
        _build_date(None, {})

    with pytest.raises(ValueError, match="Cannot pass kwargs and a date"):
        _build_date(datetime.date(2024, 1, 1), {"year": 2024})

    assert _build_date(datetime.date(2024, 1, 1), {}) == datetime.date(2024, 1, 1)


def test_lossless_float_to_int():
    assert lossless_float_to_int("func", "arg", 5) == 5
    with pytest.warns(UserWarning, match="Coercing to int"):
        assert lossless_float_to_int("func", "arg", 5.0) == 5

    with pytest.raises(TypeError):
        lossless_float_to_int("func", "arg", 5.5)


def test_always_and_never():
    dt = datetime.datetime(2024, 1, 1, 10, 0, tzinfo=pytz.utc)
    always = Always()
    never = Never()
    assert always.should_trigger(dt)
    assert not never.should_trigger(dt)

    composed_and = always & never
    assert not composed_and.should_trigger(dt)


@pytest.mark.asyncio
async def test_event_manager_and_event():
    manager = EventManager()
    called = []

    async def callback(context, data):
        called.append((context, data))

    event1 = Event(rule=Always(), callback=callback)
    manager.add_event(event1)

    # Prepend event
    event0 = Event(rule=Never(), callback=callback)
    manager.add_event(event0, prepend=True)

    dt = datetime.datetime(2024, 1, 1, 10, 0, tzinfo=pytz.utc)
    await manager.handle_data("ctx", "data", dt)
    assert called == [("ctx", "data")]


def test_make_eventrule_and_factories():
    cal = get_calendar("XNYS")

    rule = make_eventrule(
        date_rule=date_rules.every_day(),
        time_rule=time_rules.market_open(minutes=1),
        cal=cal,
        half_days=True,
    )
    assert isinstance(rule, OncePerDay)

    rule_no_half = make_eventrule(
        date_rule=date_rules.every_day(),
        time_rule=time_rules.market_close(minutes=1),
        cal=cal,
        half_days=False,
    )
    assert isinstance(rule_no_half, OncePerDay)

    # Error if uncalled rule passed
    with pytest.raises(TypeError, match="invalid rule.*did you mean"):
        make_eventrule(
            date_rule=Always,
            time_rule=time_rules.market_open(),
            cal=cal,
        )


def test_date_and_time_rules():
    cal = get_calendar("XNYS")

    ws = date_rules.week_start(days_offset=0)
    ws.cal = cal
    we = date_rules.week_end(days_offset=0)
    we.cal = cal
    ms = date_rules.month_start(days_offset=0)
    ms.cal = cal
    me = date_rules.month_end(days_offset=0)
    me.cal = cal

    assert ws.cal is cal
    assert we.cal is cal
    assert ms.cal is cal
    assert me.cal is cal

    ao = time_rules.market_open(minutes=5)
    bc = time_rules.market_close(minutes=10)
    ao.cal = cal
    bc.cal = cal

    assert ao.cal is cal
    assert bc.cal is cal
