"""Price panel for signals."""

from typing import Self
import pandas as pd
import polars as pl
from ziplime.vectorized.signals.support import PANEL_FIELDS


class PricePanel:
    """The price history a vectorised computation runs over: wide frames, one column per name.

    ``close`` and the other fields are ordinary :class:`pandas.DataFrame` objects indexed by bar
    timestamp with the strategy's own names for columns, which is the shape pandas, numpy and
    vectorbt all expect. Nothing here is lazy or proxied -- a signal computation is meant to be
    array code, and array code should see arrays.
    """

    def __init__(self, frames: dict[str, pd.DataFrame], index: pd.DatetimeIndex,
                 datasets: dict[str, "PricePanel"] | None = None) -> None:
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
    def from_bundle_rows(cls, rows: pl.DataFrame, names: dict[int, str]) -> Self:
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
