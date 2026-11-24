"""History for frames."""

from ziplime.sweep.all_assets import ALL
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.api import Data
from ziplime.sweep import frames
from ziplime.sweep.frames.col import Col
from ziplime.sweep.frames.support import HINT
from ziplime.sweep.frames.pick import Pick
from ziplime.sweep.frames.validation import _refuse


class History:
    """``data.history`` over several instruments. ``assets``: [ALL] for the whole universe, or
    the instruments asked for."""

    def __init__(self, data: Data, assets: list, length, fields: list[str] | None, frequency) -> None:
        self._data, self._assets, self._length = data, assets, length
        self._fields, self._frequency = fields, frequency

    @property
    def _xp(self):
        return self._data._backend.xp

    def _read(self, field: str, op: str):
        if self._fields is not None and field not in self._fields:
            raise KeyError(f"Field {field!r} was not asked for; asked for {self._fields}.")
        return self._data._window(ALL, field, op, self._length, self._frequency)

    def _chosen(self):
        """(1 × instruments): the instruments of the frame -- True for the whole universe."""
        if self._assets == [ALL]:
            return True
        xp, chosen = self._xp, None
        for asset in self._assets:
            here = self._data._columns.only(asset)
            chosen = here if chosen is None else xp.logical_or(chosen, here)
        return chosen

    def _present(self, field: str | None = None):
        """Which instruments have rows in the frame: a group_by gives them a row."""
        field = field or (self._fields[0] if self._fields else "close")
        present = self._read(field, "count") >= 1
        chosen = self._chosen()
        return present if chosen is True else self._xp.logical_and(present, chosen)

    def filter(self, *predicates, **constraints):
        from ziplime.sweep.api import Window

        if constraints or len(predicates) != 1 or not isinstance(predicates[0], Pick):
            raise NotConvertible("A history frame of several instruments is filtered by sid in a "
                                 "sweep: frame.filter(pl.col('sid') == asset.sid).")
        picked = [self._resolve(value) for value in predicates[0].values]
        if len(picked) == 1:
            return Window(self._data, picked[0], self._length, self._fields, self._frequency)
        if any(asset is ALL for asset in picked):
            raise NotConvertible("pl.col('sid').is_in([...]) with the loop's instrument among "
                                 "others is not supported in a sweep.")
        return History(self._data, picked, self._length, self._fields, self._frequency)

    def _resolve(self, value):
        """A sid (or a listing) the filter compares with -> the instrument; the loop's
        instrument stands for every one."""
        columns = self._data._columns
        if value is ALL:
            if self._assets != [ALL]:
                raise NotConvertible("Inside a loop over context.universe, filter a frame that "
                                     "holds the whole universe: data.history(assets="
                                     "list(context.universe.values()), ...).")
            return ALL
        asset = columns.names[columns.index(value)]
        if self._assets != [ALL] and not any(
                other is not ALL and columns.index(other) == columns.index(value)
                for other in self._assets):
            raise NotConvertible(f"{value!r} is not in this history frame; the engine's filter "
                                 f"would come back empty.")
        return asset

    def group_by(self, *by, maintain_order: bool = False) -> 'frames.Groups':
        from ziplime.sweep.frames.groups import Groups

        keys = [key.name if isinstance(key, Col) else key for key in by]
        if keys == [["sid"]] or keys == [("sid",)]:
            keys = ["sid"]
        if keys != ["sid"]:
            raise NotConvertible("A history frame of several instruments is grouped by 'sid' in "
                                 "a sweep: frame.group_by('sid').agg(...).")
        return Groups(self)

    def is_empty(self):
        """No instrument has a row yet."""
        return self._xp.logical_not(self._xp.any(self._present(), axis=1, keepdims=True))

    def __getitem__(self, field: str):
        raise NotConvertible(f"frame[{field!r}] of several instruments mixes their rows; {HINT}.")

    def __len__(self):
        raise NotConvertible(f"len() of a frame of several instruments is not supported in a "
                             f"sweep; {HINT}.")

    def __iter__(self):
        raise NotConvertible(f"Iterating a frame of several instruments is not supported in a "
                             f"sweep; {HINT}.")

    def __getattr__(self, name: str):
        if name.startswith("__") or name.startswith("sweep_") or name in ("shape", "dtype"):
            raise AttributeError(name)
        _refuse(f"frame.{name}")
