"""Indicators declared in ``initialize`` as polars expressions.

The alternative to writing ``compute_signals``::

    async def initialize(context):
        price = pl.col("close").forward_fill()
        context.indicators(
            fast=price.rolling_mean(context.algorithm.config.fast),
            slow=price.rolling_mean(context.algorithm.config.slow),
        )

    async def handle_data(context, data):
        if context.signals["fast"]["SBER"] > context.signals["slow"]["SBER"]: ...

An expression is a description, not a computation: nothing runs in ``initialize``. Before the first
bar the engine evaluates every declared expression once over the whole price history -- the same
panel ``compute_signals`` receives, the run plus ``WARMUP`` bars -- and ``handle_data`` reads the
row for the current bar through ``context.signals``, exactly as with ``compute_signals``.

A field of a dataset declared with ``context.datasets`` is a column too, named
``"label.field"``: ``pl.col("fundamentals.gross_profit") / pl.col("fundamentals.total_assets")``.

Each expression is evaluated per instrument (the engine adds ``.over("sid")``) unless it carries
its own ``.over(...)`` -- ``pl.col("momentum").rank(descending=True).over("date")`` ranks across
instruments on each date. A later expression may read an earlier one by its name.

The frame the expressions run over has one row per bar and instrument, on the calendar's full grid:
a bar the instrument did not trade is a row of nulls, not a missing row, so ``rolling_mean(20)`` is
always twenty sessions. Columns: ``date``, ``sid`` (the instrument's position in the universe) and
the price fields (``open``, ``high``, ``low``, ``close``, ``volume``, ``price`` where the bundle has
them).
"""
import numpy as np
import pandas as pd
import polars as pl


def long_frame(panel) -> pl.DataFrame:
    """A :class:`PricePanel` as one long frame: row per (instrument, bar), instrument-major."""
    names = panel.columns
    n_bars = len(panel.index)
    columns = {
        "sid": np.repeat(np.arange(len(names)), n_bars),
        "date": np.tile(panel.index.to_numpy(), len(names)),
    }
    for field in panel.fields:
        columns[field] = getattr(panel, field)[names].to_numpy(dtype=float).T.reshape(-1)
    # context.datasets, as of every bar: "fundamentals.gross_profit"
    for label, dataset in getattr(panel, "_datasets", {}).items():
        for field in dataset.fields:
            frame = getattr(dataset, field)[names]
            try:
                columns[f"{label}.{field}"] = frame.to_numpy(dtype=float).T.reshape(-1)
            except (TypeError, ValueError):
                continue  # text and dates stay in the dataset, out of the expressions
    # NaN in the panel means "no bar"; polars calls that null, and forward_fill fills only nulls
    return pl.DataFrame(columns).with_columns(pl.col(pl.Float64).fill_nan(None))


def per_instrument(expression: pl.Expr) -> pl.Expr:
    """The expression evaluated per instrument, unless it already says how to partition."""
    if '"Over"' in expression.meta.serialize(format="json"):
        return expression
    return expression.over("sid")


def to_wide(values: pl.Series, panel) -> pd.DataFrame:
    """One computed column of :func:`long_frame` back to (bars × instruments)."""
    array = values.cast(pl.Float64).to_numpy().reshape(len(panel.columns), len(panel.index)).T
    return pd.DataFrame(array, index=panel.index, columns=panel.columns)


def compute_indicators(expressions: dict[str, pl.Expr], panel) -> dict[str, pd.DataFrame]:
    """Every declared indicator over the whole panel, as the frames ``compute_signals`` returns."""
    if not expressions:
        raise ValueError("context.indicators() was called without any indicators.")
    for name, expression in expressions.items():
        if not isinstance(expression, pl.Expr):
            raise TypeError(f"Indicator {name!r} must be a polars expression, "
                            f"not {type(expression).__name__}.")
    frame = long_frame(panel)
    # One by one: an indicator may read the ones declared before it
    for name, expression in expressions.items():
        frame = frame.with_columns(per_instrument(expression).alias(name))
    return {name: to_wide(frame[name], panel) for name in expressions}
