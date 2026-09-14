import datetime

from sqlalchemy.orm import Mapped

from ziplime.assets.models.asset_model import AssetModel
from ziplime.core.db.annotated_types import AssetRouterFK, ExchangeAssetFK


class OptionContractModel(AssetModel):
    """One row per listed option contract.

    0DTE makes this table grow the way no other asset table does: a chain of 34 strikes listed
    every session is 34 new rows a day and roughly 8 500 a year, because each contract genuinely
    exists for one session only. The columns are therefore kept to what identifies and settles a
    contract, and the underlying is referenced rather than copied.
    """

    __tablename__ = "options_contracts"

    # A plain foreign key, deliberately: `futures_contracts` reuses the FK-*and-primary-key*
    # annotation for its root and ends up with a composite primary key of (root_asset_id, id),
    # which is not what identifies a contract. A contract is identified by its own id, and an
    # underlying this database does not carry leaves this null.
    underlying_asset_id: Mapped[AssetRouterFK | None]
    underlying_exchange_asset_sid: Mapped[ExchangeAssetFK | None]
    underlying_symbol: Mapped[str]
    option_type: Mapped[str]
    strike: Mapped[float]
    expiration_date: Mapped[datetime.date]
    multiplier: Mapped[float]
    tick_size: Mapped[float]
    exercise_style: Mapped[str]
    settlement_type: Mapped[str]
    premium_style: Mapped[str]
