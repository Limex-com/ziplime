"""Stateful rule for events."""

from exchange_calendars import ExchangeCalendar
from ziplime.utils.event_rule import EventRule
from ziplime.utils.events.always import Always


class StatefulRule(EventRule):
    """
    A stateful rule has state.
    This rule will give different results for the same datetimes depending
    on the internal state that this holds.
    StatefulRules wrap other rules as state transformers.
    """

    def __init__(self, rule: EventRule | None = None) -> None:
        self.rule = rule or Always()

    @property
    def cal(self):
        return self.rule.cal

    @cal.setter
    def cal(self, value: ExchangeCalendar) -> None:
        # Thread the calendar through to the underlying rule.
        self.rule.cal = value
