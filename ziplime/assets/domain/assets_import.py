from dataclasses import dataclass

from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.futures_contract import FuturesContract


@dataclass
class AssetsImport:
    exchange_assets: list[ExchangeAsset]
    currencies: list[Currency] = None
    equities: list[Equity] = None
    futures: list[FuturesContract] = None
