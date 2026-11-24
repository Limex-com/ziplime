"""Stateless rule for events."""

from ziplime.utils.event_rule import EventRule
from ziplime.utils import events


class StatelessRule(EventRule):
    """
    A stateless rule has no observable side effects.
    This is reentrant and will always give the same result for the
    same datetime.
    Because these are pure, they can be composed to create new rules.
    """

    def and_(self, rule: EventRule) -> 'events.ComposedRule':
        """
        Logical and of two rules, triggers only when both rules trigger.
        This follows the short circuiting rules for normal and.
        """
        from ziplime.utils.events.composed_rule import ComposedRule

        return ComposedRule(self, rule, ComposedRule.lazy_and)

    __and__ = and_
