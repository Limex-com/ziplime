from dataclasses import dataclass

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.currency_symbol_mapping import CurrencySymbolMapping


@dataclass(frozen=True)
class Currency(Asset):
    symbol_mapping: dict[str, CurrencySymbolMapping]

    def get_symbol_by_exchange(self, exchange_name: str) -> str | None:
        return self.symbol_mapping.get(exchange_name, None)
