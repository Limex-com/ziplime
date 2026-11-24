"""The symbol universe domain value."""

from dataclasses import dataclass
from sqlalchemy.orm import Mapped
from ziplime.assets.entities.symbols_universe_asset import SymbolsUniverseAsset


@dataclass(frozen=True)
class SymbolsUniverse:
    assets: list[SymbolsUniverseAsset]
    name: Mapped[str]
    symbol: Mapped[str]
    universe_type: Mapped[str]
