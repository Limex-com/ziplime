"""Deprecated location. The Yahoo Finance connector lives in `ziplime.data.data_sources.yahoo`."""
import warnings


warnings.warn(
    "ziplime.data.data_sources.yahoo_finance_asset_data_source has moved to "
    "ziplime.data.data_sources.yahoo.yahoo_finance_asset_data_source",
    DeprecationWarning,
    stacklevel=2,
)
