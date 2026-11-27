"""Once per day for events."""

import datetime
import pandas as pd
from ziplime.utils.event_rule import EventRule
from ziplime.utils.events.stateful_rule import StatefulRule


class OncePerDay(StatefulRule):
    def __init__(self, rule: EventRule | None = None) -> None:
        self.triggered = False

        self.date = None
        self.next_date = None

        super(OncePerDay, self).__init__(rule)

    def should_trigger(self, dt: datetime.datetime):
        if self.date is None or dt >= self.next_date:
            # initialize or reset for new date
            self.triggered = False
            self.date = dt

            # record the timestamp for the next day, so that we can use it
            # to know if we've moved to the next day
            self.next_date = dt + pd.Timedelta(1, unit="d")

        if not self.triggered and self.rule.should_trigger(dt):
            self.triggered = True
            return True
