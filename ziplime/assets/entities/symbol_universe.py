from dataclasses import dataclass

from sqlalchemy.orm import Mapped

from ziplime.assets.entities.symbols_universe_asset import SymbolsUniverseAsset


@dataclass(frozen=True)
class SymbolsUniverse:
    assets: list[SymbolsUniverseAsset]
    name: Mapped[str]
    symbol: Mapped[str]
    universe_type: Mapped[str]


def universe_venue(universe: SymbolsUniverse | None) -> str | None:
    """The market a universe belongs to, when it says so.

    ``universe_type`` carries it after a colon -- ``index:MISX`` -- because an index is an index
    *of an exchange*, and its members are issuers rather than listings. IMOEX without MISX means
    "the companies in the Moscow index, on whatever market they happen to be listed", which puts
    AT&T in it: the asset import keys companies by ticker, and Moscow's T collides with it.

    A universe with no venue (``index``, as the American Q-universes are stored) answers ``None``
    and is resolved across every market, as before.
    """
    kind = getattr(universe, "universe_type", None) or ""
    _, _, venue = kind.partition(":")
    return venue.strip().upper() or None
