"""
Dataset representing OHLCV data.
"""
from ziplime.pipeline.data import DataSet, Column
from ziplime.utils.numpy_utils import float64_dtype, categorical_dtype



class EquityPricing(DataSet):
    """
    :class:`~ziplime.pipeline.data.DataSet` containing daily trading prices and
    volumes.
    """

    open = Column(float64_dtype, currency_aware=True)
    high = Column(float64_dtype, currency_aware=True)
    low = Column(float64_dtype, currency_aware=True)
    close = Column(float64_dtype, currency_aware=True)
    volume = Column(float64_dtype)
    currency = Column(categorical_dtype)
