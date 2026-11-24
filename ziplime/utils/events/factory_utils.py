"""Factory utils for events."""

import inspect
from exchange_calendars import ExchangeCalendar
from ziplime.utils.event_rule import EventRule
from ziplime.utils.events.not_half_day import NotHalfDay
from ziplime.utils.events.once_per_day import OncePerDay
from ziplime.utils.events.date_rule_factories import date_rules
from ziplime.utils.events.time_rule_factories import time_rules


def _invert(d):
    return dict(zip(d.values(), d.keys()))


_uncalled_rules = _invert(vars(date_rules))


_uncalled_rules.update(_invert(vars(time_rules)))


def _check_if_not_called(v):
    try:
        name = _uncalled_rules[v]
    except KeyError:
        if not (inspect.isclass(v) and issubclass(v, EventRule)):
            return

        name = getattr(v, "__name__", None)

    msg = "invalid rule: %r" % (v,)
    if name is not None:
        msg += " (hint: did you mean %s())" % name

    raise TypeError(msg)


def make_eventrule(date_rule: EventRule, time_rule: EventRule, cal: ExchangeCalendar, half_days: bool = True):
    """
    Constructs an event rule from the factory api.
    """
    _check_if_not_called(date_rule)
    _check_if_not_called(time_rule)

    if half_days:
        inner_rule = date_rule & time_rule
    else:
        inner_rule = date_rule & time_rule & NotHalfDay()

    opd = OncePerDay(rule=inner_rule)
    # This is where a scheduled function's rule is associated with a calendar.
    opd.cal = cal
    return opd
