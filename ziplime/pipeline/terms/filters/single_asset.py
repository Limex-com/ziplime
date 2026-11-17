import numpy as np
import pandas as pd

from ziplime.errors import NonExistentAssetInTimeFrame
from ziplime.pipeline.terms.filters import Filter
from ziplime.utils.numpy_utils import repeat_first_axis
from ziplime.assets.domain.continuous_future import ContinuousFuture
from ziplime.assets.entities.exchange_asset import ExchangeAsset


class SingleAsset(Filter):
    """
    A Filter that computes to True only for the given asset.
    """

    inputs = []
    window_length = 1

    def __new__(cls, asset: 'ExchangeAsset | ContinuousFuture'):
        return super(SingleAsset, cls).__new__(cls, asset=asset)

    def _init(self, asset: 'ExchangeAsset | ContinuousFuture', *args, **kwargs):
        self._asset = asset
        return super(SingleAsset, self)._init(*args, **kwargs)

    @classmethod
    def _static_identity(cls, asset: 'ExchangeAsset | ContinuousFuture', *args, **kwargs):
        return (
            super(SingleAsset, cls)._static_identity(*args, **kwargs),
            asset,
        )

    def _compute(self, arrays: 'list[np.ndarray]', dates: 'pd.DatetimeIndex', assets: 'pd.Index', mask: 'np.ndarray'):
        is_my_asset = assets == self._asset.sid
        out = repeat_first_axis(is_my_asset, len(mask))
        # Raise an exception if `self._asset` does not exist for the entirety
        # of the timeframe over which we are computing.
        if (is_my_asset.sum() != 1) or ((out & mask).sum() != len(mask)):
            raise NonExistentAssetInTimeFrame(
                asset=self._asset,
                start_date=dates[0],
                end_date=dates[-1],
            )
        return out

    def graph_repr(self) -> str:
        # Graphviz interprets `\l` as "divide label into lines, left-justified"
        return "SingleAsset:\\l  asset: {!r}\\l".format(self._asset)
