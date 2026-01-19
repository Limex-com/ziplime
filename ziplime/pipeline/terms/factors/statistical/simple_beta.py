from ziplime.pipeline.terms.asset_exists import AssetExists
from ziplime.pipeline.terms.factors import CustomFactor, Returns
from ziplime.pipeline.terms.factors.statistical.vectorized_beta import vectorized_beta
from ziplime.pipeline.terms.filters import SingleAsset
from ziplime.pipeline.mixins import StandardOutputs
from ziplime.utils.numpy_utils import (
    float64_dtype,
)

from ziplime.assets.entities.asset import Asset






class SimpleBeta(CustomFactor, StandardOutputs):
    """Factor producing the slope of a regression line between each asset's daily
    returns to the daily returns of a single "target" asset.

    Parameters
    ----------
    target : ziplime.Asset
        Asset against which other assets should be regressed.
    regression_length : int
        Number of days of daily returns to use for the regression.
    allowed_missing_percentage : float, optional
        Percentage of returns observations (between 0 and 1) that are allowed
        to be missing when calculating betas. Assets with more than this
        percentage of returns observations missing will produce values of
        NaN. Default behavior is that 25% of inputs can be missing.
    """

    window_safe = True
    dtype = float64_dtype
    params = ("allowed_missing_count",)

    def __new__(cls, target: Asset, regression_length: int, allowed_missing_percentage: int | float = 0.25):
        if regression_length < 3:
            raise ValueError("regression_length must be greater than or equal to 3")
        if allowed_missing_percentage <= 0.0 or allowed_missing_percentage > 1.0:
            raise ValueError("allowed_missing_percentage must be between 0.0 and 1.0")
        daily_returns = Returns(
            window_length=2,
            mask=(AssetExists() | SingleAsset(asset=target)),
        )
        allowed_missing_count = int(allowed_missing_percentage * regression_length)
        return super(SimpleBeta, cls).__new__(
            cls,
            inputs=[daily_returns, daily_returns[target]],
            window_length=regression_length,
            allowed_missing_count=allowed_missing_count,
        )

    def compute(
            self, today, assets, out, all_returns, target_returns, allowed_missing_count
    ):
        vectorized_beta(
            dependents=all_returns,
            independent=target_returns,
            allowed_missing=allowed_missing_count,
            out=out,
        )

    def graph_repr(self):
        return "{}({!r}, {}, {})".format(
            type(self).__name__,
            str(self.target.symbol),  # coerce from unicode to str in py2.
            self.window_length,
            self.params["allowed_missing_count"],
        )

    @property
    def target(self):
        """Get the target of the beta calculation."""
        return self.inputs[1].asset

    def __repr__(self):
        return "{}({}, length={}, allowed_missing={})".format(
            type(self).__name__,
            self.target,
            self.window_length,
            self.params["allowed_missing_count"],
        )