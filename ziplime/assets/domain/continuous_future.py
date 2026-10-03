"""Specifier for a chain of futures contracts, exposed to algorithms."""
import datetime
from dataclasses import dataclass

from ziplime.assets.entities.exchange_info import ExchangeInfo

#: Roll styles a continuous future may be built with.
ROLL_STYLES = frozenset({"calendar", "volume"})


@dataclass(frozen=True)
class ContinuousFuture:
    """A rolling position in a futures chain, addressed by root symbol rather than by contract.

    Data requests for a continuous future are resolved to whichever contract is active on the
    requested date, and history is spliced across rolls using ``adjustment``.

    Attributes:
        sid: Synthetic identifier, encoded from the other fields so that the same specification
            always yields the same id. It is not an ``exchange_assets`` sid.
        root_symbol: Root symbol of the chain, e.g. ``Si``.
        offset: Distance from the front contract -- 0 is the front month, 1 the next, and so on.
        roll_style: ``'calendar'`` to roll a fixed number of days before expiry, ``'volume'`` to
            roll once the next contract trades more than the current one.
        adjustment: How to splice prices across a roll: ``'mul'`` scales earlier prices by the
            price ratio at the roll, ``'add'`` shifts them by the difference, ``None`` leaves the
            raw contract prices in place (which produces a jump at every roll).
        start_date: First date the chain has data for.
        end_date: Last date the chain has data for.
        exchange_info: Exchange the chain trades on.
    """

    sid: int
    root_symbol: str
    offset: int
    roll_style: str
    start_date: datetime.date
    end_date: datetime.date
    exchange_info: ExchangeInfo
    adjustment: str | None = None

    @property
    def exchange(self) -> str:
        return self.exchange_info.canonical_name

    @property
    def exchange_full(self) -> str:
        return self.exchange_info.name

    @property
    def mic(self) -> str:
        return self.exchange_info.mic

    def __int__(self) -> int:
        return self.sid

    def __index__(self) -> int:
        return self.sid

    def __hash__(self) -> int:
        return hash(self.sid)

    def __str__(self) -> str:
        return (f"ContinuousFuture({self.root_symbol}, offset={self.offset}, "
                f"roll={self.roll_style}, adjustment={self.adjustment})")

    def is_alive_for_session(self, session_label: datetime.date) -> bool:
        """Whether the chain has data for the given session."""
        if isinstance(session_label, datetime.datetime):
            session_label = session_label.date()
        return self.start_date <= session_label <= self.end_date
