import polars as pl

from ziplime.assets.entities.asset import Asset
from ziplime.assets.models.exchange_info import ExchangeInfo


class AssetDataSource:

    def __init__(self):
        pass

    async def get_assets(self, **kwargs) -> list[Asset]:
        pass

    async def get_exchanges(self, **kwargs) -> list[ExchangeInfo]:...

    async def get_constituents(self, index: str) -> pl.DataFrame: ...

    async def search_assets(self, query: str, **kwargs) -> pl.DataFrame: ...
