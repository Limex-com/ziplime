"""Registers the Finam Trade API connector (MOEX FORTS futures)."""
from ziplime.assets.domain.asset_type import AssetType
from ziplime.data.data_sources.finam.finam_asset_data_source import FinamAssetDataSource
from ziplime.data.data_sources.finam.finam_client import FinamClient
from ziplime.data.data_sources.finam.finam_data_source import FinamDataSource
from ziplime.data.data_sources.finam.moex_futures import RTSX_MIC
from ziplime.data.data_sources.registry import DataProvider, register_provider


def _asset_data_source(**kwargs) -> FinamAssetDataSource:
    return FinamAssetDataSource.from_env(**kwargs)


def _market_data_source(assets=None, **kwargs) -> FinamDataSource:
    """Build the bar source, restricted to each listing's own lifetime when assets are given."""
    if assets:
        return FinamDataSource.for_assets(client=FinamClient.from_env(), assets=assets, **kwargs)
    return FinamDataSource.from_env(**kwargs)


FINAM = register_provider(DataProvider(
    name="finam",
    description="Russian markets via the Finam Trade API; MOEX FORTS futures and MOEX bonds "
                "with full contract and coupon history",
    asset_data_source_factory=_asset_data_source,
    market_data_source_factory=_market_data_source,
    required_env=("FINAM_API_SECRET",),
    default_mic=RTSX_MIC,
    default_calendar="XMOS",
    asset_types=(AssetType.FUTURES_CONTRACT.value, AssetType.BOND.value),
))
