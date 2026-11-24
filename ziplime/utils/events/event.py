"""Event for events."""

from collections import namedtuple
import datetime
from collections.abc import Callable
from ziplime.domain.bar_data import BarData
from ziplime.trading.base_trading_algorithm import BaseTradingAlgorithm
from ziplime.utils.event_rule import EventRule


class Event(namedtuple("Event", ["rule", "callback"])):
    """
    An event is a pairing of an EventRule and a callable that will be invoked
    with the current algorithm context, data, and datetime only when the rule
    is triggered.
    """

    def __new__(cls, rule: EventRule | None, callback: Callable | None = None):
        callback = callback or (lambda *args, **kwargs: None)
        return super(cls, cls).__new__(cls, rule=rule, callback=callback)

    async def handle_data(
        self, context: BaseTradingAlgorithm, data: BarData, dt: datetime.datetime,
    ) -> None:
        """
        Calls the callable only when the rule is triggered.
        """
        if self.rule.should_trigger(dt):
            await self.callback(context, data)
