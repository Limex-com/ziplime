"""Hooks for engine."""

import dataclasses
from ziplime.sweep.convert import Converted


@dataclasses.dataclass
class Hooks:
    handle_data: Converted
    before_trading_start: Converted | None
    #: Scheduled functions, in the order initialize scheduled them
    scheduled: list

    def all(self):
        return [self.handle_data] + ([self.before_trading_start] if self.before_trading_start
                                     else []) + list(self.scheduled)
