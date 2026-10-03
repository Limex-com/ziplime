"""Vectorised signals inside an event-driven run.

A strategy is rarely all one thing. The indicators are array work -- a moving average, a z-score,
a cross -- and recomputing them bar by bar is the single most expensive thing an event-driven
engine does: ``data.history(bar_count=200)`` on every one of a thousand sessions re-reads two
hundred rows and re-averages them, a thousand times, for an answer that one ``rolling(200).mean()``
produces in a single pass. But the part that decides *what to do* with the signal usually cannot
be vectorised at all: it reads the portfolio, respects a risk limit, rolls a contract, or sizes
against cash that depends on every fill before it.

So this module splits the strategy rather than the run. ``compute_signals`` is computed once,
over whole arrays, before the first bar. ``handle_data`` then reads one row per bar and orders
through the ordinary blotter, with the ordinary slippage and commissions. Nothing about execution
changes; only the arithmetic moves.

Handing a strategy the entire price history and asking it to be careful is how look-ahead bias
gets into a backtest, so two things here make that structural rather than advisory:

* :class:`SignalPanel` slices by the simulation clock. At bar ``t`` there is no accessor, method
  or attribute that returns row ``t + 1``. The future is not guarded, it is absent.
* :func:`verify_causality` re-runs the strategy's own computation on truncated history and
  compares. A rolling mean gives the same answer either way; a full-sample z-score, a
  ``shift(-1)`` or a division by ``iloc[-1]`` does not, and says so before the first order.

The second is sound but not complete: it compares at sampled cut points, so it catches a
computation whose leakage shows up at those rows -- which is every accidental leak in practice,
since full-sample normalisation and negative shifts move nearly every row -- and cannot promise to
catch one contrived to hide between them. It is a smoke alarm, not a proof.
"""
import datetime

import numpy as np
import pandas as pd
import polars as pl

#: Cut points used to check that a signal computation only looks backwards.
DEFAULT_CUTS = 4

#: Fields lifted out of the bundle into the panel, when the bundle carries them.
PANEL_FIELDS = ("open", "high", "low", "close", "volume", "price")


class LookAheadError(ValueError):
    """A signal computation read data it could not have had at the time.

    Raised before the simulation places an order, because the alternative is a backtest whose
    every number is inflated by an amount nobody can estimate after the fact.
    """


class PricePanel:
    """The price history a vectorised computation runs over: wide frames, one column per name.

    ``close`` and the other fields are ordinary :class:`pandas.DataFrame` objects indexed by bar
    timestamp with the strategy's own names for columns, which is the shape pandas, numpy and
    vectorbt all expect. Nothing here is lazy or proxied -- a signal computation is meant to be
    array code, and array code should see arrays.
    """

    def __init__(self, frames: dict[str, pd.DataFrame], index: pd.DatetimeIndex,
                 datasets: dict[str, "PricePanel"] | None = None):
        self._frames = frames
        self.index = index
        self._datasets = datasets or {}

    def dataset(self, name: str) -> "PricePanel":
        """A mounted dataset as of each bar, on the same index as the prices.

        Fundamentals, disclosures and filings do not arrive once per bar -- a company reports four
        times a year -- so what this returns is the *as-of* view: at each bar, the newest thing
        known by then, carried forward until the next one replaces it. That is the same question
        ``data.current`` answers at a single bar, asked for the whole history at once, and the
        strategy reads it the same way a price panel is read::

            def compute_signals(context, prices):
                f = prices.dataset("fundamentals")
                return {"quality": f.gross_profit / f.total_assets}

        Declared in ``initialize`` as ``context.datasets = {"fundamentals": source}``.
        """
        try:
            return self._datasets[name]
        except KeyError:
            raise KeyError(
                f"No dataset named {name!r} on this panel. Declared: "
                f"{sorted(self._datasets) or 'none'}. Set them in initialize with "
                f"`context.datasets = {{'{name}': <source or address>}}`.") from None

    def __getattr__(self, field: str) -> pd.DataFrame:
        try:
            return self._frames[field]
        except KeyError:
            raise AttributeError(
                f"The bundle has no {field!r} column. It carries: "
                f"{', '.join(sorted(self._frames))}."
            ) from None

    def __dir__(self):
        return [*super().__dir__(), *self._frames]

    @property
    def fields(self) -> list[str]:
        return sorted(self._frames)

    @property
    def columns(self) -> list[str]:
        return list(next(iter(self._frames.values())).columns)

    def __len__(self) -> int:
        return len(self.index)

    def truncated(self, rows: int) -> "PricePanel":
        """The same panel as it stood ``rows`` bars in -- what :func:`verify_causality` replays."""
        return PricePanel(
            {name: frame.iloc[:rows] for name, frame in self._frames.items()},
            self.index[:rows],
            # The datasets are truncated alongside, or a causality check would replay the signal
            # over a short price history and a full set of filings.
            {name: panel.truncated(rows) for name, panel in self._datasets.items()})

    @classmethod
    def from_bundle_rows(cls, rows: pl.DataFrame, names: dict[int, str]) -> "PricePanel":
        """Pivot flat ``date``/``sid`` bundle rows into one wide frame per field."""
        frame = rows.to_pandas()
        frame["_name"] = frame["sid"].map(names)
        if frame["_name"].isna().any():
            unknown = sorted(set(frame.loc[frame["_name"].isna(), "sid"]))
            raise ValueError(f"Bundle rows for sids outside the declared universe: {unknown}.")

        index = pd.DatetimeIndex(sorted(frame["date"].unique()))
        frames = {}
        for field in PANEL_FIELDS:
            if field not in frame.columns:
                continue
            wide = frame.pivot(index="date", columns="_name", values=field)
            wide.index = pd.DatetimeIndex(wide.index)
            frames[field] = wide.reindex(index).sort_index(axis=1)
        if not frames:
            raise ValueError(
                f"The bundle carries none of {PANEL_FIELDS}; there is nothing to compute over.")
        return cls(frames, index)


def as_of_panel(rows: pl.DataFrame, names: dict[int, str], index: pd.DatetimeIndex,
                coalesce: bool) -> PricePanel:
    """Build the as-of view of an event dataset: at each bar, what was known by then.

    A dataset of filings, disclosures or statements has nothing to say on most bars and several
    things to say on a few, so it cannot be pivoted onto the bar grid the way prices can. What a
    strategy wants from it at a bar is what ``data.current`` gives: the newest state visible then.
    This computes that for every bar in one pass.

    Two details decide whether the answer is the same as reading it bar by bar, and getting either
    wrong produces a backtest that is merely optimistic rather than broken:

    **Strictly before the bar.** A filing accepted while a bar is being traded is not visible
    inside it -- the source reads ``date < dt``, and so does the join here. An inclusive join
    would hand the strategy a statement at the very bar it could first have acted on, which is
    between a few hours and a day of hindsight depending on the rate.

    **Coalesce or latest row.** A source that republishes revisions resolves to the newest
    *non-null value per column*, because a later filing restating a period repeats fewer line
    items than the original -- on SEC fundamentals, taking the newest row instead drops 78% of
    ``total_assets``. Forward-filling each column within an instrument before the join reproduces
    that exactly. A source whose rows are snapshots resolves to the newest *row*, nulls included,
    which is the join on its own.

    Args:
        rows: The dataset's rows, carrying ``date`` (knowledge time), ``sid`` and its fields.
        names: ``{sid: the strategy's name for it}``.
        index: The bar grid to answer for.
        coalesce: Whether the source resolves by column rather than by row.

    Returns:
        A panel with one frame per field, indexed by bar, one column per name, plus ``known_at``
        (when the row a bar is reading was filed) and ``age_days`` (how long ago that was).
    """
    fields = [column for column in rows.columns if column not in ("date", "sid")]
    if not fields:
        raise ValueError("The dataset carries no fields besides date and sid.")

    bars = pl.DataFrame({"date": pl.Series(list(index))}).sort("date")
    frames = {field: pd.DataFrame(index=index, columns=list(names.values()), dtype="object")
              for field in fields}
    known_at = pd.DataFrame(index=index, columns=list(names.values()), dtype="object")

    for sid, name in names.items():
        visible = rows.filter(pl.col("sid") == sid).sort("date")
        if visible.is_empty():
            continue
        if coalesce:
            # The newest non-null per column, which is what the source's COALESCE resolution does
            # at a single bar -- carried forward so every later bar sees it too.
            visible = visible.with_columns([pl.col(field).forward_fill() for field in fields])
        aligned = bars.join_asof(
            visible.select([pl.col("date"), pl.col("date").alias("known_at"), *fields]),
            on="date", strategy="backward",
            # `data.current` on these sources reads strictly before the current moment.
            allow_exact_matches=False)
        for field in fields:
            frames[field][name] = aligned[field].to_list()
        known_at[name] = aligned["known_at"].to_list()

    built = {field: frame.apply(_numeric_where_possible) for field, frame in frames.items()}
    # When the row a bar is reading was filed, and how long ago that was. Staleness is the first
    # question anyone asks of point-in-time data -- a company that stopped reporting in 2015 should
    # not sit in a 2024 book -- and computing it from the panel is fiddly enough that a strategy
    # doing it by hand is a strategy getting it slightly wrong.
    # Everything in UTC before subtracting. A company with no filing at all leaves its column
    # entirely null, which `to_datetime` hands back tz-naive, and subtracting that from a tz-aware
    # index raises several frames away from anything the author wrote.
    known = known_at.apply(lambda column: pd.to_datetime(column, utc=True))
    bar_times = pd.Series(index.tz_convert("UTC") if index.tz is not None
                          else index.tz_localize("UTC"), index=index)
    built["known_at"] = known
    built["age_days"] = known.rsub(bar_times, axis=0).apply(
        lambda column: column.dt.total_seconds() / 86_400.0)
    return PricePanel(built, index)


class SignalRow:
    """One bar of one signal: ``row["JNJ"]``, or a bare value for a per-portfolio signal."""

    def __init__(self, name: str, value):
        self._name = name
        self._value = value

    def __getitem__(self, column: str):
        if not isinstance(self._value, pd.Series):
            raise TypeError(
                f"Signal {self._name!r} is one value per bar, not one per instrument, so "
                f"[{column!r}] means nothing. Use it directly.")
        try:
            return self._value[column]
        except KeyError:
            raise KeyError(
                f"Signal {self._name!r} has no column {column!r}. It has: "
                f"{', '.join(map(str, self._value.index))}.") from None

    def __iter__(self):
        return iter(self._value.items()) if isinstance(self._value, pd.Series) else iter(())

    def __bool__(self) -> bool:
        if isinstance(self._value, pd.Series):
            raise ValueError(
                f"Signal {self._name!r} holds one value per instrument, so its truth is "
                f"ambiguous. Ask for one: context.signals[{self._name!r}]['TICKER'].")
        return bool(self._value)

    def __float__(self) -> float:
        return float(self._value)

    def __repr__(self) -> str:
        return f"SignalRow({self._name!r}, {self._value!r})"

    @property
    def value(self):
        """The underlying scalar or :class:`pandas.Series`."""
        return self._value


class SignalPanel:
    """Precomputed signals, readable only as far as the simulation has run.

    The clock is not a filter applied to a frame the object also holds unsliced; every read goes
    through :meth:`_upto`, and there is no accessor that returns a later row. A strategy cannot
    look ahead here by being careless, only by reaching past this object entirely.
    """

    def __init__(self, frames: dict[str, pd.DataFrame | pd.Series], clock):
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


def _as_stamp(now: datetime.datetime, index: pd.DatetimeIndex) -> pd.Timestamp:
    """The simulation time, made comparable with the panel's index."""
    stamp = pd.Timestamp(now)
    if index.tz is None:
        return stamp.tz_localize(None) if stamp.tzinfo is not None else stamp
    return stamp.tz_localize(index.tz) if stamp.tzinfo is None else stamp.tz_convert(index.tz)


def _numeric_where_possible(column: pd.Series) -> pd.Series:
    """Numbers as numbers, and anything else left alone -- a dataset may carry text fields."""
    try:
        return pd.to_numeric(column)
    except (TypeError, ValueError):
        return column


def normalise_signals(returned, index: pd.DatetimeIndex) -> dict[str, pd.DataFrame | pd.Series]:
    """Accept what a strategy naturally returns, and insist it is indexed like the panel.

    A frame whose index does not match the panel's cannot be read by bar at all -- every lookup
    would silently land on the wrong row or none -- so it is refused here rather than at the bar
    that trips over it.
    """
    if returned is None:
        raise ValueError("compute_signals returned None; it has to return the signals to use.")
    if isinstance(returned, (pd.DataFrame, pd.Series)):
        returned = {"signal": returned}
    if not isinstance(returned, dict):
        raise TypeError(
            f"compute_signals must return a DataFrame, a Series, or a dict of them, "
            f"got {type(returned).__name__}.")

    frames = {}
    for name, value in returned.items():
        if not isinstance(value, (pd.DataFrame, pd.Series)):
            raise TypeError(
                f"Signal {name!r} is a {type(value).__name__}; it has to be a pandas DataFrame "
                f"(one column per instrument) or Series (one value per bar).")
        if len(value.index) != len(index) or not value.index.equals(index):
            raise ValueError(
                f"Signal {name!r} is indexed differently from the prices it was computed from "
                f"({len(value.index)} rows against {len(index)}). Keep the panel's index -- "
                f"rolling, shift and comparisons all preserve it; reset_index, dropna and "
                f"groupby do not.")
        frames[name] = value
    if not frames:
        raise ValueError("compute_signals returned no signals.")
    return frames


def verify_causality(compute, panel: PricePanel, computed: dict[str, pd.DataFrame | pd.Series],
                     cuts: int = DEFAULT_CUTS, warmup: int = 0) -> None:
    """Re-run a signal computation on truncated history and refuse it if the answer changes.

    The check is the definition of causality made executable: a signal at bar ``t`` is honest only
    if computing it from the first ``t`` bars alone gives the same number. Anything that needed the
    rest of the array to produce its answer -- a mean over the whole sample, a rank against the
    full period, a negative shift -- gives a different one, and that difference is the bias, sitting
    in the open where it can be raised rather than compounded into the equity curve.

    Args:
        compute: The strategy's ``compute_signals``, called as ``compute(panel)``.
        panel: The full price panel the signals were computed from.
        computed: The normalised result of computing over the whole panel.
        cuts: How many truncation points to test.
        warmup: Rows at the front reserved for indicator warm-up; cuts are taken after them.

    Raises:
        LookAheadError: The truncated run disagrees with the full one.
    """
    total = len(panel)
    first = max(warmup + 1, 2)
    if cuts < 1 or total <= first:
        return

    points = sorted({int(round(p)) for p in np.linspace(first, total - 1, num=min(cuts, total - first))
                     if first <= int(round(p)) < total})
    for rows in points:
        partial = normalise_signals(compute(panel.truncated(rows)), panel.index[:rows])
        for name, full in computed.items():
            if name not in partial:
                raise LookAheadError(
                    f"compute_signals produced {name!r} over the full history but not over the "
                    f"first {rows} bars. A signal has to exist at every bar it is read at.")
            _compare_last_row(name, full.iloc[rows - 1], partial[name].iloc[-1],
                              rows=rows, stamp=panel.index[rows - 1])


def _compare_last_row(name: str, full, partial, rows: int, stamp) -> None:
    """Fail unless the truncated computation reproduces the full one's value at that bar."""
    full_values = np.asarray(pd.Series(full).to_numpy() if isinstance(full, pd.Series) else [full])
    part_values = np.asarray(pd.Series(partial).to_numpy() if isinstance(partial, pd.Series) else [partial])

    if full_values.shape != part_values.shape:
        raise LookAheadError(
            f"Signal {name!r} changes shape with the length of the history "
            f"({part_values.shape} at {rows} bars against {full_values.shape} over the whole run).")

    if full_values.dtype.kind in "OUSb" or part_values.dtype.kind in "OUSb":
        matched = np.asarray([a == b or (_is_nan(a) and _is_nan(b))
                              for a, b in zip(full_values, part_values)])
    else:
        matched = np.isclose(full_values.astype(float), part_values.astype(float),
                             rtol=1e-9, atol=1e-12, equal_nan=True)

    if bool(matched.all()):
        return

    where = int(np.argmin(matched))
    raise LookAheadError(
        f"Signal {name!r} looks ahead. At bar {rows} ({stamp}) the whole-history computation "
        f"gives {full_values[where]!r}, but computing from the first {rows} bars alone -- all "
        f"that was known then -- gives {part_values[where]!r}.\n"
        f"Something in compute_signals reads the whole array rather than a trailing window. The "
        f"usual causes are a statistic over the full sample (mean, std, min, max, quantile, rank, "
        f"or a z-score built from them), a negative shift, and dividing by a fixed element such "
        f"as iloc[-1]. Rolling and expanding windows, shift with a positive argument and "
        f"element-wise arithmetic are all safe."
    )


def _is_nan(value) -> bool:
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False
