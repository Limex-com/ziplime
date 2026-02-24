from dataclasses import dataclass

from ziplime.assets.entities.asset import Asset


@dataclass(frozen=True)
class TradingPair:
    base_asset: Asset
    quote_asset: Asset
    exchange_name: str
