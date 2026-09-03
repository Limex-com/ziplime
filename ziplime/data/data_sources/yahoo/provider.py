"""Registers the Yahoo Finance connector."""
from ziplime.assets.domain.asset_type import AssetType
from ziplime.data.data_sources.registry import DataProvider, register_provider
from ziplime.data.data_sources.yahoo.yahoo_finance_asset_data_source import (
    YahooFinanceAssetDataSource,
)
from ziplime.data.data_sources.yahoo.yahoo_finance_data_source import YahooFinanceDataSource

def _market_data_source(assets=None, **kwargs) -> YahooFinanceDataSource:
    """Build the bar source, pre-loading each listing's exchange when assets are given.

    Yahoo does not return the exchange with the bars, so without this the source falls back to one
    rate-limited ``Ticker.info`` call per symbol.
    """
    if assets:
        return YahooFinanceDataSource.for_assets(assets=assets, **kwargs)
    return YahooFinanceDataSource(**kwargs)


YAHOO = register_provider(DataProvider(
    name="yahoo",
    description="Global equities, ETFs and continuous futures series from Yahoo Finance "
                "(no credentials required)",
    asset_data_source_factory=YahooFinanceAssetDataSource,
    market_data_source_factory=_market_data_source,
    required_env=(),
    default_mic=None,          # Yahoo spans many exchanges; the MIC comes per instrument.
    default_calendar="XNYS",
    asset_types=(AssetType.EQUITY.value, AssetType.FUTURES_CONTRACT.value),
))
