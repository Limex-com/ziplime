import datetime
from dataclasses import dataclass
from typing import Literal

from ziplime.assets.entities.exchange_asset import ExchangeAsset

type InvestmentKey = tuple[str, str, ExchangeAsset]


@dataclass(frozen=True)
class InvestmentEvent:
    """Immutable native investment posting; fields are constructor arguments.

    Attributes
    ----------
    id : str
        Identity unique within ``key``. Identical repeats are idempotent;
        conflicting repeats are rejected by the investment book.
    dt : datetime.datetime
        Aware posting time. Events must be recorded in chronological order.
    kind : {'trade', 'fee', 'income', 'split'}
        Determines which of the monetary/quantity fields are used.
    key : InvestmentKey
        ``(exchange_name, trading_account_id, ExchangeAsset)``. Lots never
        cross these account, venue, or listing boundaries.
    currency : str
        ISO currency code for monetary amounts. Trades use the listing's quote
        currency; fees and income can use their actual payment currency.
    quantity : float, optional
        Signed fill quantity for trades: positive buys, negative sells.
    unit_value : float, optional
        Native monetary consideration per unit for trades, not necessarily the
        quoted price: includes option multipliers or bond dirty-value conventions.
    amount : float, optional
        Native fee charge (positive cost, negative rebate), signed income credit,
        or old-shares/new-shares split ratio. Unused for trades.
    transaction_id : str or None, optional
        For fees, the referenced trade's ``id`` in the same ``key``. Required
        to allocate the fee between realized PNL and remaining acquisition cost.
    """

    id: str
    dt: datetime.datetime
    kind: Literal["trade", "fee", "income", "split"]
    key: InvestmentKey
    currency: str
    quantity: float = 0.0
    unit_value: float = 0.0
    amount: float = 0.0
    transaction_id: str | None = None
