"""Always for events."""

import datetime
from ziplime.utils.events.stateless_rule import StatelessRule


class Always(StatelessRule):
    """
    A rule that always triggers.
    """

    @staticmethod
    def always_trigger(dt: datetime.datetime) -> bool:
        """
        A should_trigger implementation that will always trigger.
        """
        return True

    should_trigger = always_trigger
