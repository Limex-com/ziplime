"""Bar utils for synthetic."""

import copy
import datetime
import numpy as np
import pandas as pd
import polars as pl
from ziplime.sweep.params import SweepError
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.sweep.synthetic.bars import Bars
from ziplime.sweep.synthetic.support import FIELDS
from ziplime.sweep.synthetic.feature_values import Features


def side_by_side(data: pl.DataFrame, universe: dict, start: datetime.date, tz: datetime.tzinfo) -> Bars:
    sids = [listing.sid for listing in universe.values()]
    position = {sid: k for k, sid in enumerate(sids)}
    frame = data.with_row_index("_row").filter(pl.col("sid").is_in(sids))
    if frame.is_empty():
        raise SweepError("No bars of the universe to build synthetic paths from.")
    dates = frame["date"]
    if getattr(dates.dtype, "time_zone", None):
        dates = dates.dt.convert_time_zone("UTC")
    stamps = dates.dt.timestamp("ns").to_numpy()
    moments = np.unique(stamps)
    bar = np.searchsorted(moments, stamps)
    column = np.array([position[sid] for sid in frame["sid"].to_list()])
    fields = {}
    for field in FIELDS:
        name = field if field in frame.columns else "price" if field == "close" else None
        if name is None or name not in frame.columns:
            continue
        values = np.full((len(moments), len(sids)), np.nan)
        values[bar, column] = frame[name].cast(pl.Float64).to_numpy()
        fields[field] = values
    if "close" not in fields:
        raise SweepError("The bundle has neither close nor price: no synthetic paths without them.")
    local = pd.DatetimeIndex(pd.to_datetime(moments, utc=True)).tz_convert(tz).date
    after = np.nonzero(local >= start)[0]
    if len(after) == 0:
        raise SweepError(f"No bars from {start} on to build synthetic paths for.")
    # A path continues from a real close: the first bar of the run needs one before it
    return Bars(fields=fields, rows=frame["_row"].to_numpy(), bar=bar, column=column,
                start=max(1, int(after[0])))


def build(bars: Bars, feats: Features, source: np.ndarray, returns: np.ndarray) -> dict:
    """The path's bars ((bars, instruments) per field): real before the run, then each bar from
    its log return and the shape of the real bar ``source`` names."""
    start = bars.start
    close_real = feats.close
    base = close_real[start - 1].copy()
    returns = returns.copy()
    # An instrument whose rows begin inside the run starts at its own first close
    for column in np.flatnonzero(~np.isfinite(base)):
        finite = np.flatnonzero(np.isfinite(close_real[start:, column]))
        if len(finite):
            base[column] = close_real[start + finite[0], column]
            returns[:finite[0] + 1, column] = 0.0
    close = base * np.exp(np.cumsum(returns, axis=0))
    previous = np.vstack([base[None, :], close[:-1]])
    opened = previous * np.exp(feats.gap[source])
    high = np.maximum.reduce([close * np.exp(feats.high[source]), opened, close])
    low = np.minimum.reduce([close * np.exp(feats.low[source]), opened, close])
    made = {"close": close, "open": opened, "high": high, "low": low}
    if feats.volume is not None:
        drawn = feats.volume[source]
        made["volume"] = np.where(np.isfinite(drawn), drawn, feats.volume[start:])
    out = {}
    for field, real in bars.fields.items():
        values = real.copy()
        values[start:] = made[field]
        out[field] = values
    return out


def as_bundle(bundle: DataBundle, bars: Bars, path: dict):
    """The real bundle with the universe's rows rewritten to the path: the same rows, the same
    stamps -- only the numbers differ."""
    data = bundle.data
    columns = []
    for field in (*FIELDS, "price"):
        if field not in data.columns:
            continue
        values = path["close" if field == "price" else field]
        if field == "price" and "close" not in path:
            continue
        series = data[field]
        full = series.cast(pl.Float64).to_numpy().copy()
        written = values[bars.bar, bars.column]
        present = np.isfinite(full[bars.rows])
        full[bars.rows] = np.where(present, written, full[bars.rows])
        if series.dtype.is_integer():
            full = np.round(full)
        columns.append(pl.Series(field, full).cast(series.dtype))
    made = copy.copy(bundle)
    made.data = data.with_columns(columns)
    made._last_bars = {}
    made._roll_finders = {}
    return made
