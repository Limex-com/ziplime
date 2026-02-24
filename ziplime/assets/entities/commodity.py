from dataclasses import dataclass

from ziplime.assets.entities.asset import Asset


@dataclass(frozen=True)
class Commodity(Asset):
    # mapping exchange code -> symbol
    symbol_mapping: dict[str, str]

    def get_symbol_by_exchange(self, exchange_name: str) -> str | None:
        return self.symbol_mapping.get(exchange_name, None)
