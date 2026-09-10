"""Deprecated location. The Yahoo Finance connector lives in `ziplime.data.data_sources.yahoo`."""
import warnings

from ziplime.data.data_sources.yahoo.yahoo_finance_asset_data_source import *  # noqa: F401,F403
from ziplime.data.data_sources.yahoo.yahoo_finance_asset_data_source import __dict__ as _moved  # noqa: F401

warnings.warn(
    "ziplime.data.data_sources.yahoo_finance_asset_data_source has moved to "
    "ziplime.data.data_sources.yahoo.yahoo_finance_asset_data_source",
    DeprecationWarning,
    stacklevel=2,
)
