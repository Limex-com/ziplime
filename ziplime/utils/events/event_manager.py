"""Event manager for events."""

import datetime
from collections.abc import Callable
from ziplime.utils.context_tricks import nop_context
from ziplime.domain.bar_data import BarData
from ziplime.trading.base_trading_algorithm import BaseTradingAlgorithm
from ziplime.utils.events.event import Event


class EventManager:
    """Manages a list of Event objects.
    This manages the logic for checking the rules and dispatching to the
    handle_data function of the Events.

    Parameters
    ----------
    create_context : (BarData) -> context manager, optional
        An optional callback to produce a context manager to wrap the calls
        to handle_data. This will be passed the current BarData.
    """

    def __init__(self, create_context: Callable | None = None) -> None:
        self._events = []
        self._create_context = (
            create_context if create_context is not None else lambda *_: nop_context
        )

    def add_event(self, event: 'Event', prepend: bool = False) -> None:
        """
        Adds an event to the manager.
        """
        if prepend:
            self._events.insert(0, event)
        else:
            self._events.append(event)

    async def handle_data(
        self, context: BaseTradingAlgorithm, data: BarData, dt: datetime.datetime,
    ) -> None:
        with self._create_context(data):
            for event in self._events:
                await event.handle_data(
                    context=context,
                    data=data,
                    dt=dt,
                )
