"""Deprecated location. The Yahoo Finance connector lives in `ziplime.data.data_sources.yahoo`."""
import warnings

from ziplime.data.data_sources.yahoo.yahoo_finance_constants import *  # noqa: F401,F403
from ziplime.data.data_sources.yahoo.yahoo_finance_constants import __dict__ as _moved  # noqa: F401

warnings.warn(
    "ziplime.data.data_sources.yahoo_finance_constants has moved to "
    "ziplime.data.data_sources.yahoo.yahoo_finance_constants",
    DeprecationWarning,
    stacklevel=2,
)
