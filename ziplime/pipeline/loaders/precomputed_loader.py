from pandas import DataFrame
import numpy as np
import pandas as pd

from ziplime.pipeline.loaders.data_frame_loader import DataFrameLoader
from ziplime.pipeline.loaders.pipeline_loader import PipelineLoader
from ziplime.pipeline.domain import Domain
from ziplime.pipeline.terms.bound_column import BoundColumn


class PrecomputedLoader(PipelineLoader):
    """Synthetic PipelineLoader that uses a pre-computed array for each column.

    Parameters
    ----------
    values : dict
        Map from column to values to use for that column.
        Values can be anything that can be passed as the first positional
        argument to a DataFrame whose indices are ``dates`` and ``sids``
    dates : iterable[datetime-like]
        Row labels for input data.  Can be anything that pd.DataFrame will
        coerce to a DatetimeIndex.
    sids : iterable[int-like]
        Column labels for input data.  Can be anything that pd.DataFrame will
        coerce to an Int64Index.

    Notes
    -----
    Adjustments are unsupported by this loader.
    """

    def __init__(self, constants, dates, sids) -> None:
        loaders = {}
        for column, const in constants.items():
            frame = DataFrame(
                const,
                index=dates,
                columns=sids,
                dtype=column.dtype,
            )
            loaders[column] = DataFrameLoader(
                column=column,
                baseline=frame,
                adjustments=None,
            )

        self._loaders = loaders

    async def load_adjusted_array(
        self,
        domain: 'Domain',
        columns: 'list[BoundColumn]',
        dates: 'pd.DatetimeIndex',
        sids: 'pd.Index',
        mask: 'np.ndarray',
    ):
        """Load by delegating to sub-loaders."""
        out = {}
        for col in columns:
            try:
                loader = self._loaders.get(col)
                if loader is None:
                    loader = self._loaders[col.unspecialize()]
            except KeyError as exc:
                raise ValueError("Couldn't find loader for %s" % col) from exc
            out.update(loader.load_adjusted_array(domain, [col], dates, sids, mask))
        return out
