from dataclasses import dataclass

from ziplime.assets.domain.settlement_type import SettlementType
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.exchange_info import ExchangeInfo


@dataclass(frozen=True)
class FuturesRoot:
    """Identity and default specification of a futures chain, e.g. ``Si`` for ``SiZ6``.

    A continuous future is specified by root symbol rather than by contract, so the chain needs an
    entity of its own. ``multiplier`` and ``tick_size`` are the chain defaults; an individual
    contract may override them.
    """

    root_symbol: str
    description: str
    exchange: ExchangeInfo
    root_asset: Asset
    multiplier: float
    tick_size: float
    quote_currency: str
    #: Cash or physical delivery, inherited by every contract in the chain.
    settlement_type: SettlementType = SettlementType.CASH
    #: Currency the exchange collects margin in; not necessarily ``quote_currency``.
    margin_currency: str = "RUB"

    def __hash__(self):
        return hash(self.root_symbol)

    def __str__(self):
        return f"{self.root_symbol}-{self.exchange.mic}"

    @property
    def mic(self) -> str:
        return self.exchange.mic
