"""Utils for signals."""

from collections.abc import Callable
import numpy as np
import pandas as pd
import polars as pl
from ziplime.vectorized.signals.support import DEFAULT_CUTS
from ziplime.vectorized.signals.look_ahead_error import LookAheadError
from ziplime.vectorized.signals.price_panel import PricePanel


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


def _numeric_where_possible(column: pd.Series) -> pd.Series:
    """Numbers as numbers, and anything else left alone -- a dataset may carry text fields."""
    try:
        return pd.to_numeric(column)
    except (TypeError, ValueError):
        return column


def normalise_signals(
    returned: pd.DataFrame | pd.Series | dict[str, pd.DataFrame | pd.Series],
    index: pd.DatetimeIndex,
) -> dict[str, pd.DataFrame | pd.Series]:
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


def verify_causality(
    compute: Callable[[PricePanel], dict[str, pd.DataFrame | pd.Series]],
    panel: PricePanel,
    computed: dict[str, pd.DataFrame | pd.Series],
    cuts: int = DEFAULT_CUTS,
    warmup: int = 0,
) -> None:
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


def _compare_last_row(name: str, full, partial, rows: int, stamp: pd.Timestamp) -> None:
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
