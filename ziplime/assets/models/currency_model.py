from functools import lru_cache

from sqlalchemy.orm import Mapped, relationship

from ziplime.assets.models.asset_model import AssetModel
from ziplime.assets.models.currency_symbol_mapping_model import CurrencySymbolMappingModel


class CurrencyModel(AssetModel):
    __tablename__ = "currencies"
    currency_symbol_mappings: Mapped[list[CurrencySymbolMappingModel]] = relationship("CurrencySymbolMappingModel")

    @lru_cache
    def get_symbol_by_exchange(self, exchange_name: str | None) -> str | None:
        if exchange_name is None:
            symbol_mapping = next(iter(self.currency_symbol_mappings), None)
        else:
            symbol_mapping = next(filter(lambda x: x.exchange == exchange_name, self.currency_symbol_mappings), None)
        if symbol_mapping:
            return symbol_mapping.symbol
        return None
