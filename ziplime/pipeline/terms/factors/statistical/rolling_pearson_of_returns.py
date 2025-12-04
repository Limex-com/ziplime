from ziplime.pipeline.terms.asset_exists import AssetExists
from ziplime.pipeline.terms.factors import RollingPearson, Returns
from ziplime.pipeline.terms.filters import SingleAsset




class RollingPearsonOfReturns(RollingPearson):
    """
    Calculates the Pearson product-moment correlation coefficient of the
    returns of the given asset with the returns of all other assets.

    Pearson correlation is what most people mean when they say "correlation
    coefficient" or "R-value".

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

    Examples
    --------
    Let the following be example 10-day returns for three different assets::

                       SPY    MSFT     FB
        2017-03-13    -.03     .03    .04
        2017-03-14    -.02    -.03    .02
        2017-03-15    -.01     .02    .01
        2017-03-16       0    -.02    .01
        2017-03-17     .01     .04   -.01
        2017-03-20     .02    -.03   -.02
        2017-03-21     .03     .01   -.02
        2017-03-22     .04    -.02   -.02

    Suppose we are interested in SPY's rolling returns correlation with each
    stock from 2017-03-17 to 2017-03-22, using a 5-day look back window (that
    is, we calculate each correlation coefficient over 5 days of data). We can
    achieve this by doing::

        rolling_correlations = RollingPearsonOfReturns(
            target=sid(8554),
            returns_length=10,
            correlation_length=5,
        )

    The result of computing ``rolling_correlations`` from 2017-03-17 to
    2017-03-22 gives::

                       SPY   MSFT     FB
        2017-03-17       1    .15   -.96
        2017-03-20       1    .10   -.96
        2017-03-21       1   -.16   -.94
        2017-03-22       1   -.16   -.85

    Note that the column for SPY is all 1's, as the correlation of any data
    series with itself is always 1. To understand how each of the other values
    were calculated, take for example the .15 in MSFT's column. This is the
    correlation coefficient between SPY's returns looking back from 2017-03-17
    (-.03, -.02, -.01, 0, .01) and MSFT's returns (.03, -.03, .02, -.02, .04).

    See Also
    --------
    :class:`ziplime.pipeline.factors.RollingSpearmanOfReturns`
    :class:`ziplime.pipeline.factors.RollingLinearRegressionOfReturns`
    """

    def __new__(cls, target, returns_length, correlation_length, mask=None):
        # Use the `SingleAsset` filter here because it protects against
        # inputting a non-existent target asset.
        returns = Returns(
            window_length=returns_length,
            mask=(AssetExists() | SingleAsset(asset=target)),
        )
        return super(RollingPearsonOfReturns, cls).__new__(
            cls,
            base_factor=returns,
            target=returns[target],
            correlation_length=correlation_length,
            mask=mask,
        )