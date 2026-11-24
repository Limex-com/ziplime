"""Not half day for events."""

import datetime
from ziplime.utils.events.stateless_rule import StatelessRule


class NotHalfDay(StatelessRule):
    """
    A rule that only triggers when it is not a half day.
    """

    def should_trigger(self, dt: datetime.datetime):
        return self.cal.minute_to_session(dt) not in self.cal.early_closes
