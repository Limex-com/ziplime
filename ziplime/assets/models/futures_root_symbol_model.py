from sqlalchemy.orm import Mapped

from ziplime.core.db.annotated_types import AssetRouterFK, ExchangeFKPK, StringPK
from ziplime.core.db.base_model import BaseModel


class FuturesRootSymbolModel(BaseModel):
    """Metadata shared by every contract in a futures chain, e.g. ``Si`` for ``SiZ6``.

    Continuous futures are specified by root symbol, so the chain needs an addressable identity of
    its own: which exchange it trades on, what the default contract specification is, and which
    underlying asset it settles against.

    A chain is identified by its root *and* its exchange: ``NG`` is natural gas on both the Moscow
    Exchange and NYMEX, with a hundredfold difference in contract size, and one asset database
    holds both.
    """

    __tablename__ = "futures_root_symbols"

    root_symbol: Mapped[StringPK]
    description: Mapped[str]
    mic: Mapped[ExchangeFKPK]
    root_asset_id: Mapped[AssetRouterFK]
    multiplier: Mapped[float]
    tick_size: Mapped[float]
    quote_currency: Mapped[str]
    settlement_type: Mapped[str]
    margin_currency: Mapped[str]
