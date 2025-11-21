from ziplime.errors import NonExistentAssetInTimeFrame
from ziplime.pipeline.terms.filters import Filter
from ziplime.utils.numpy_utils import repeat_first_axis


class SingleAsset(Filter):
    """
    A Filter that computes to True only for the given asset.
    """

    inputs = []
    window_length = 1

    def __new__(cls, asset):
        return super(SingleAsset, cls).__new__(cls, asset=asset)

    def _init(self, asset, *args, **kwargs):
        self._asset = asset
        return super(SingleAsset, self)._init(*args, **kwargs)

    @classmethod
    def _static_identity(cls, asset, *args, **kwargs):
        return (
            super(SingleAsset, cls)._static_identity(*args, **kwargs),
            asset,
        )

    def _compute(self, arrays, dates, assets, mask):
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

    def graph_repr(self):
        # Graphviz interprets `\l` as "divide label into lines, left-justified"
        return "SingleAsset:\\l  asset: {!r}\\l".format(self._asset)
