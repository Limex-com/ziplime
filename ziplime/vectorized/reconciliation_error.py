"""Vectorized execution reconciliation failures."""

import datetime


class ReconciliationError(ValueError):
    """The replayed ledger and the source's own value series disagree about the same trades.

    One of the two is wrong, and which one is not knowable from here. Raised rather than warned:
    a hybrid backtester whose two halves disagree silently is worse than one that only has a
    single half, because it invites comparisons that mean nothing.
    """

    def __init__(
        self,
        session: datetime.date | datetime.datetime,
        replayed: float,
        reported: float,
        tolerance: float,
    ) -> None:
        super().__init__(
            f"On {session} the replayed ledger holds {replayed:,.6f} but the source reports "
            f"{reported:,.6f} -- a difference of {abs(replayed - reported):,.6f}, beyond the "
            f"{tolerance:g} tolerance.\n"
            f"Three things cause this, in the order they are worth checking: the exchange's "
            f"starting cash differs from the starting cash the fills were decided under; "
            f"`prices` is not the series the source valued the book with, so the positions are "
            f"marked at something else; or the source holds more than one book and the wrong one "
            f"is being compared."
        )
        self.session = session
        self.replayed = replayed
        self.reported = reported
