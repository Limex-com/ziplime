from dataclasses import dataclass, field

from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.bond_event import BondEvent
from ziplime.assets.entities.commodity import Commodity
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.assets.entities.futures_root import FuturesRoot


@dataclass
class AssetsImport:
    """A batch of assets to persist together.

    Order matters on import: ``exchange_assets`` reference assets by identity, and a
    ``FuturesContract`` references both its underlying (a commodity, currency or equity) and its
    chain, so those have to be written first. ``bond_events`` reference their bond, so they go
    after ``bonds``.
    """

    exchange_assets: list[ExchangeAsset]
    currencies: list[Currency] = field(default_factory=list)
    equities: list[Equity] = field(default_factory=list)
    commodities: list[Commodity] = field(default_factory=list)
    bonds: list[Bond] = field(default_factory=list)
    bond_events: list[BondEvent] = field(default_factory=list)
    futures_roots: list[FuturesRoot] = field(default_factory=list)
    futures: list[FuturesContract] = field(default_factory=list)
