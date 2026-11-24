"""Composed rule for events."""

import datetime
from collections.abc import Callable
from exchange_calendars import ExchangeCalendar
from ziplime.utils.event_rule import EventRule
from ziplime.utils.events.stateless_rule import StatelessRule


class ComposedRule(StatelessRule):
    """
    A rule that composes the results of two rules with some composing function.
    The composing function should be a binary function that accepts the results
    first(dt) and second(dt) as positional arguments.
    For example, operator.and_.
    If lazy=True, then the lazy composer is used instead. The lazy composer
    expects a function that takes the two should_trigger functions and the
    datetime. This is useful of you don't always want to call should_trigger
    for one of the rules. For example, this is used to implement the & and |
    operators so that they will have the same short circuit logic that is
    expected.
    """

    def __init__(
        self,
        first: EventRule,
        second: EventRule,
        composer: Callable[
            [Callable[[datetime.datetime], bool], Callable[[datetime.datetime], bool], datetime.datetime],
            bool,
        ],
    ) -> None:
        if not (isinstance(first, StatelessRule) and isinstance(second, StatelessRule)):
            raise ValueError("Only two StatelessRules can be composed")

        self.first = first
        self.second = second
        self.composer = composer

    def should_trigger(self, dt: datetime.datetime):
        """
        Composes the two rules with a lazy composer.
        """
        return self.composer(self.first.should_trigger, self.second.should_trigger, dt)

    @staticmethod
    def lazy_and(
        first_should_trigger: Callable[[datetime.datetime], bool],
        second_should_trigger: Callable[[datetime.datetime], bool],
        dt: datetime.datetime,
    ):
        """
        Lazily ands the two rules. This will NOT call the should_trigger of the
        second rule if the first one returns False.
        """
        return first_should_trigger(dt) and second_should_trigger(dt)

    @property
    def cal(self):
        return self.first.cal

    @cal.setter
    def cal(self, value: ExchangeCalendar) -> None:
        # Thread the calendar through to the underlying rules.
        self.first.cal = self.second.cal = value
