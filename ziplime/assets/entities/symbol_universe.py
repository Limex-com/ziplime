from dataclasses import dataclass

from sqlalchemy.orm import Mapped

from ziplime.assets.entities.asset import Asset


@dataclass(frozen=True)
class SymbolsUniverse:
    assets: list[Asset]
    name: Mapped[str]
    symbol: Mapped[str]
    universe_type: Mapped[str]
