"""Signal panel for signals."""

import datetime
from collections.abc import Callable
import pandas as pd
from ziplime.vectorized.signals.look_ahead_error import LookAheadError
from ziplime.vectorized.signals.signal_row import SignalRow
from ziplime.vectorized.signals.date_utils import _as_stamp


class SignalPanel:
    """Precomputed signals, readable only as far as the simulation has run.

    The clock is not a filter applied to a frame the object also holds unsliced; every read goes
    through :meth:`_upto`, and there is no accessor that returns a later row. A strategy cannot
    look ahead here by being careless, only by reaching past this object entirely.
    """

    def __init__(self, frames: dict[str, pd.DataFrame | pd.Series], clock: Callable[[], datetime.datetime]) -> None:
        self._frames = frames
        self._clock = clock
        # Every signal shares the panel's index -- `normalise_signals` refuses one that does not --
        # so the cut is found once per bar and reused across signals.
        self._index = next(iter(frames.values())).index
        self._cached_at = None
        self._cached_cut = 0

    def _cut(self) -> int:
        """How many rows are at or before the current bar.

        A binary search rather than a mask: this is read several times on every bar of the run, and
        `index <= stamp` walks the whole array each time, which turns a constant-time lookup into
        the very per-bar cost the mechanism exists to remove.
        """
        now = self._clock()
        if now is None:
            raise RuntimeError(
                "Signals are readable once the simulation is running -- there is no current bar "
                "in initialize. Compute in compute_signals, read in handle_data.")
        if now != self._cached_at:
            self._cached_at = now
            self._cached_cut = int(self._index.searchsorted(_as_stamp(now, self._index),
                                                            side="right"))
        return self._cached_cut

    def _frame(self, name: str):
        try:
            return self._frames[name]
        except KeyError:
            raise KeyError(
                f"No signal named {name!r}. compute_signals returned: "
                f"{', '.join(sorted(self._frames))}.") from None

    def _upto(self, name: str):
        """The signal as of the current bar: rows stamped at or before the simulation time."""
        return self._frame(name).iloc[:self._cut()]

    def __getitem__(self, name: str) -> SignalRow:
        frame = self._frame(name)
        cut = self._cut()
        if cut == 0:
            raise LookAheadError(
                f"Signal {name!r} has no row at or before the current bar. The panel starts at "
                f"{frame.index[0]}, which is after the simulation does.")
        return SignalRow(name, frame.iloc[cut - 1])

    def __contains__(self, name: str) -> bool:
        return name in self._frames

    def __iter__(self):
        return iter(self._frames)

    @property
    def names(self) -> list[str]:
        return sorted(self._frames)

    def history(self, name: str, bar_count: int):
        """The trailing ``bar_count`` bars of a signal, ending at the current one.

        Shorter than asked for near the start of the run, exactly as ``data.history`` is.
        """
        if bar_count < 1:
            raise ValueError(f"bar_count must be positive, got {bar_count}.")
        cut = self._cut()
        return self._frame(name).iloc[max(0, cut - bar_count):cut]

    def is_ready(self, name: str) -> bool:
        """Whether this bar's value is a real number rather than indicator warm-up.

        Only a signal that carries its warm-up can report it. ``rolling(20).mean()`` is NaN until
        its window fills and answers honestly here; ``fast > slow`` is ``False`` there, because a
        comparison against NaN is False rather than NaN, and this cannot tell that apart from a
        genuine "no". Return the numbers from ``compute_signals`` and compare them at the bar, and
        the distinction survives.
        """
        cut = self._cut()
        if cut == 0:
            return False
        # The last visible row, not a slice of everything up to it: this is read on every bar, and
        # materialising the whole prefix to look at one row is most of what the mechanism saves.
        last = self._frame(name).iloc[cut - 1]
        return not (bool(pd.isna(last).all()) if isinstance(last, pd.Series) else bool(pd.isna(last)))
