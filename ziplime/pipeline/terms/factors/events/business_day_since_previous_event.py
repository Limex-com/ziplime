from numpy import newaxis

from ziplime.pipeline.terms.factors.factor import Factor
from ziplime.utils.numpy_utils import (
    NaTD,
    busday_count_mask_NaT,
    datetime64D_dtype,
    float64_dtype,
)


class BusinessDaysSincePreviousEvent(Factor):
    """
    Abstract class for business days since a previous event.
    Returns the number of **business days** (not trading days!) since
    the most recent event date for each asset.

    This doesn't use trading days for symmetry with
    BusinessDaysUntilNextEarnings.

    Assets which announced or will announce the event today will produce a
    value of 0.0. Assets that announced the event on the previous business
    day will produce a value of 1.0.

    Assets for which the event date is `NaT` will produce a value of `NaN`.


    Example
    -------
    ``BusinessDaysSincePreviousEvent`` can be used to create an event-driven
    factor. For instance, you may want to only trade assets that have
    a data point with an asof_date in the last 5 business days. To do this,
    you can create a ``BusinessDaysSincePreviousEvent`` factor, supplying
    the relevant asof_date column from your dataset as input, like this::

        # Factor computing number of days since most recent asof_date
        # per asset.
        days_since_event = BusinessDaysSincePreviousEvent(
            inputs=[MyDataset.asof_date]
        )

        # Filter returning True for each asset whose most recent asof_date
        # was in the last 5 business days.
        recency_filter = (days_since_event <= 5)

    """

    window_length = 0
    dtype = float64_dtype

    def _compute(self, arrays, dates, assets, mask):

        # Coerce from [ns] to [D] for numpy busday_count.
        announce_dates = arrays[0].astype(datetime64D_dtype)

        # Set masked values to NaT.
        announce_dates[~mask] = NaTD

        # Convert row labels into a column vector for broadcasted comparison.
        reference_dates = dates.values.astype(datetime64D_dtype)[:, newaxis]
        return busday_count_mask_NaT(announce_dates, reference_dates)
