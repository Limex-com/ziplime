from ziplime.pipeline.terms.asset_exists import AssetExists
from ziplime.pipeline.terms.factors import RollingSpearman, Returns
from ziplime.pipeline.terms.filters import SingleAsset


class RollingSpearmanOfReturns(RollingSpearman):
    """
    Calculates the Spearman rank correlation coefficient of the returns of the
    given asset with the returns of all other assets.

    Parameters
    ----------
    target : ziplime.assets.Asset
        The asset to correlate with all other assets.
    returns_length : int >= 2
        Length of the lookback window over which to compute returns. Daily
        returns require a window length of 2.
    correlation_length : int >= 1
        Length of the lookback window over which to compute each correlation
        coefficient.
    mask : ziplime.pipeline.Filter, optional
        A Filter describing which assets should have their correlation with the
        target asset computed each day.

    Notes
    -----
    Computing this factor over many assets can be time consuming. It is
    recommended that a mask be used in order to limit the number of assets over
    which correlations are computed.

    See Also
    --------
    :class:`ziplime.pipeline.factors.RollingPearsonOfReturns`
    :class:`ziplime.pipeline.factors.RollingLinearRegressionOfReturns`
    """

    def __new__(cls, target, returns_length, correlation_length, mask=None):
        # Use the `SingleAsset` filter here because it protects against
        # inputting a non-existent target asset.
        returns = Returns(
            window_length=returns_length,
            mask=(AssetExists() | SingleAsset(asset=target)),
        )
        return super(RollingSpearmanOfReturns, cls).__new__(
            cls,
            base_factor=returns,
            target=returns[target],
            correlation_length=correlation_length,
            mask=mask,
        )
