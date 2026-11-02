"""``data.history`` over several instruments: the engine's frame, as a converted strategy reads it.

The engine returns one long frame -- a row per instrument per bar, with ``date``, ``sid`` and the
fields asked for -- and a strategy reads it per instrument in one of two ways::

    frame = await data.history(assets=list(context.universe.values()), bar_count=20,
                               fields=["close"])
    stats = frame.group_by("sid").agg(pl.col("close").mean().alias("mean"))
    for row in stats.iter_rows(named=True):
        ... row["sid"], row["mean"] ...
    for name, asset in context.universe.items():
        ... frame.filter(pl.col("sid") == asset.sid)["close"].mean() ...

The engine takes the last ``bar_count`` rows of each sid, so each instrument's rows are the window
``data.history(assets=[asset], ...)`` reads, and both forms come down to the per-instrument
windows a sweep already computes: arrays (combinations × instruments). ``pl`` inside converted
code is :class:`PolarsShim`, which knows ``pl.col``; the rest of polars is refused before the
first bar.
"""
import operator

from ziplime.sweep.convert import ALL, NotConvertible, Vectorized

#: polars aggregation -> the window statistic it is (polars' std and var have ddof=1)
AGGREGATIONS = {"mean": "mean", "sum": "sum", "std": "std", "var": "var", "min": "min",
                "max": "max", "median": "median", "first": "first", "last": "last",
                "count": "count", "len": "count"}

HINT = ("read a frame of several instruments per instrument -- "
        "frame.filter(pl.col('sid') == asset.sid)['close'].mean(), or "
        "frame.group_by('sid').agg(pl.col('close').mean()) and its iter_rows()")


class Unsupported(NotConvertible, AttributeError):
    """An attribute a sweep does not provide: refused, and still False for hasattr()."""


def _refuse(what: str):
    raise Unsupported(f"{what} is not supported in a sweep; {HINT}.")


class PolarsShim:
    """``pl`` inside a converted strategy."""

    def col(self, name):
        if not isinstance(name, str):
            raise NotConvertible("pl.col takes one column name in a sweep.")
        return Col(name)

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        _refuse(f"pl.{name}")


class Col:
    """``pl.col(name)``: compared on ``sid``, it picks instruments; aggregated, a statistic."""

    def __init__(self, name: str):
        self.name = name

    def _sid(self):
        if self.name != "sid":
            raise NotConvertible(f"A history frame is filtered by sid in a sweep, not by "
                                 f"{self.name!r}.")

    def __eq__(self, other):
        self._sid()
        return Pick([other])

    def is_in(self, values):
        self._sid()
        return Pick(list(values))

    __hash__ = object.__hash__

    def _statistic(self, op, ddof=1):
        if self.name in ("sid", "date"):
            raise NotConvertible(f"pl.col({self.name!r}).{op}() is not supported in a sweep.")
        if ddof != 1:
            raise NotConvertible(f"pl.col(...).{op}() is supported with ddof=1 only.")
        field, statistic = self.name, AGGREGATIONS[op]
        return Statistic(field, lambda read: read(field, statistic), field)

    def mean(self):
        return self._statistic("mean")

    def sum(self):
        return self._statistic("sum")

    def std(self, ddof: int = 1):
        return self._statistic("std", ddof)

    def var(self, ddof: int = 1):
        return self._statistic("var", ddof)

    def min(self):
        return self._statistic("min")

    def max(self):
        return self._statistic("max")

    def median(self):
        return self._statistic("median")

    def first(self):
        return self._statistic("first")

    def last(self):
        return self._statistic("last")

    def count(self):
        return self._statistic("count")

    def len(self):
        return self._statistic("len")

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        _refuse(f"pl.col(...).{name}")


class Statistic:
    """An aggregation in ``agg(...)``: its output name and how to compute it from the windows.
    ``compute(read)`` with ``read(field, op)`` -> (combinations × instruments)."""

    def __init__(self, name: str, compute, field: str):
        self.name, self.compute, self.field = name, compute, field

    def alias(self, name: str):
        return Statistic(name, self.compute, self.field)

    def _combine(self, other, op, reflected=False):
        if isinstance(other, Statistic):
            right = other.compute
        elif isinstance(other, (int, float)) and not isinstance(other, bool):
            right = lambda read: other  # noqa: E731
        else:
            raise NotConvertible("An aggregation combines with numbers and other aggregations "
                                 "only in a sweep.")
        left = self.compute
        if reflected:
            # polars names `1 - pl.col("x").mean()` after its left side: "literal"
            name = other.name if isinstance(other, Statistic) else "literal"
            return Statistic(name, lambda read: op(right(read), left(read)), self.field)
        return Statistic(self.name, lambda read: op(left(read), right(read)), self.field)

    def __add__(self, other):
        return self._combine(other, operator.add)

    def __radd__(self, other):
        return self._combine(other, operator.add, reflected=True)

    def __sub__(self, other):
        return self._combine(other, operator.sub)

    def __rsub__(self, other):
        return self._combine(other, operator.sub, reflected=True)

    def __mul__(self, other):
        return self._combine(other, operator.mul)

    def __rmul__(self, other):
        return self._combine(other, operator.mul, reflected=True)

    def __truediv__(self, other):
        return self._combine(other, operator.truediv)

    def __rtruediv__(self, other):
        return self._combine(other, operator.truediv, reflected=True)

    def __neg__(self):
        compute = self.compute
        return Statistic(self.name, lambda read: -compute(read), self.field)

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        _refuse(f"pl.col(...).<aggregation>().{name}")


class Pick:
    """``pl.col("sid") == x`` / ``.is_in([...])``: which instruments a filter keeps."""

    def __init__(self, values: list):
        self.values = values


class History:
    """``data.history`` over several instruments. ``assets``: [ALL] for the whole universe, or
    the instruments asked for."""

    def __init__(self, data, assets: list, length, fields, frequency):
        self._data, self._assets, self._length = data, assets, length
        self._fields, self._frequency = fields, frequency

    @property
    def _xp(self):
        return self._data._backend.xp

    def _read(self, field, op):
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

    def _present(self, field=None):
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

    def group_by(self, *by, maintain_order: bool = False):
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

    def __getitem__(self, field):
        raise NotConvertible(f"frame[{field!r}] of several instruments mixes their rows; {HINT}.")

    def __len__(self):
        raise NotConvertible(f"len() of a frame of several instruments is not supported in a "
                             f"sweep; {HINT}.")

    def __iter__(self):
        raise NotConvertible(f"Iterating a frame of several instruments is not supported in a "
                             f"sweep; {HINT}.")

    def __getattr__(self, name):
        if name.startswith("__") or name.startswith("sweep_") or name in ("shape", "dtype"):
            raise AttributeError(name)
        _refuse(f"frame.{name}")


class Groups:
    """``frame.group_by("sid")``."""

    def __init__(self, frame: History):
        self._frame = frame

    def agg(self, *expressions, **named):
        statistics = []
        for expression in expressions:
            statistics += list(expression) if isinstance(expression, (list, tuple)) else [expression]
        statistics += [expression.alias(name) if isinstance(expression, Statistic) else expression
                       for name, expression in named.items()]
        if not statistics or not all(isinstance(each, Statistic) for each in statistics):
            raise NotConvertible("group_by('sid').agg(...) takes aggregations of pl.col(...) in a "
                                 "sweep: .mean(), .sum(), .std(), .var(), .min(), .max(), "
                                 ".median(), .first(), .last(), .count(), .len().")
        return Table(self._frame, statistics)

    def _all_fields(self, op):
        fields = self._frame._fields
        if not fields:
            raise NotConvertible(f"group_by('sid').{op}() needs the fields asked for: "
                                 f"data.history(..., fields=[...]).")
        return Table(self._frame, [Col(field)._statistic(op) for field in fields])

    def mean(self):
        return self._all_fields("mean")

    def sum(self):
        return self._all_fields("sum")

    def std(self):
        return self._all_fields("std")

    def var(self):
        return self._all_fields("var")

    def min(self):
        return self._all_fields("min")

    def max(self):
        return self._all_fields("max")

    def median(self):
        return self._all_fields("median")

    def first(self):
        return self._all_fields("first")

    def last(self):
        return self._all_fields("last")

    def count(self):
        return self._all_fields("count")

    def __getattr__(self, name):
        if name.startswith("__") or name.startswith("sweep_"):
            raise AttributeError(name)
        _refuse(f"group_by('sid').{name}")


class Table:
    """``group_by("sid").agg(...)``: a row per instrument that has rows in the frame."""

    def __init__(self, frame: History, statistics: list):
        self.columns = ["sid"] + [each.name for each in statistics]
        if len(set(self.columns)) != len(self.columns):
            raise NotConvertible(f"group_by('sid').agg(...) makes the columns {self.columns}; "
                                 f"give each a different name with .alias(...).")
        self._xp = frame._xp
        self._values = {each.name: each.compute(frame._read) for each in statistics}
        self._present = frame._present(statistics[0].field)

    def iter_rows(self, *, named: bool = False):
        if named:
            return Vectorized([Row(self._values)], mask=self._present)
        return Vectorized([(ALL, *[self._values[name] for name in self.columns[1:]])],
                          mask=self._present)

    def rows(self, *, named: bool = False):
        return self.iter_rows(named=named)

    def is_empty(self):
        return self._xp.logical_not(self._xp.any(self._present, axis=1, keepdims=True))

    def __getitem__(self, name):
        raise NotConvertible(f"table[{name!r}] is not supported in a sweep; read the rows: "
                             f"for row in table.iter_rows(named=True): row['sid'], row[...].")

    def __len__(self):
        raise NotConvertible("len() of a grouped frame is not supported in a sweep.")

    def __iter__(self):
        raise NotConvertible("Iterate a grouped frame through iter_rows() in a sweep.")

    def __getattr__(self, name):
        if name.startswith("__") or name.startswith("sweep_") or name in ("shape", "dtype"):
            raise AttributeError(name)
        _refuse(f"group_by('sid').agg(...).{name}")


class Row:
    """A row of ``iter_rows(named=True)``: ``row["sid"]`` is the instrument, every one at once."""

    def __init__(self, values: dict):
        self._values = values

    def __getitem__(self, key):
        if key == "sid":
            return ALL
        try:
            return self._values[key]
        except KeyError:
            raise KeyError(f"No column {key!r}; there are {['sid', *self._values]}.") from None

    def get(self, key, default=None):
        return self[key] if key == "sid" or key in self._values else default
