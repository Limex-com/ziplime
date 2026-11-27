"""Never for events."""

import datetime
from ziplime.utils.events.stateless_rule import StatelessRule


class Never(StatelessRule):
    """
    A rule that never triggers.
    """

    @staticmethod
    def never_trigger(dt: datetime.datetime) -> bool:
        """
        A should_trigger implementation that will never trigger.
        """
        return False

    should_trigger = never_trigger
