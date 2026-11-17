from dataclasses import dataclass

from ziplime.assets.entities.asset import Asset


@dataclass(frozen=True)
class Equity(Asset):
    # mapping exchange code -> symbol
    # symbol_mapping: dict[str, EquitySymbolMapping]

    # def get_symbol_by_exchange(self, exchange_name: str | None) -> str | None:
    #     if exchange_name is None:
    #         symbol_mapping = next(iter(self.symbol_mapping.values()), None)
    #     else:
    #         symbol_mapping = self.symbol_mapping.get(exchange_name, None)
    #     if symbol_mapping:
    #         return symbol_mapping.symbol
    #     return None

    def __hash__(self) -> int:
        return hash(self.id)
