"""Yahoo futures root for yahoo_futures."""

from dataclasses import dataclass
from ziplime.assets.domain.settlement_type import SettlementType
from ziplime.data.data_sources.yahoo.yahoo_futures.support import CONTINUOUS_SUFFIX
from ziplime.data.data_sources.yahoo.yahoo_futures.support import QUARTERLY


@dataclass(frozen=True)
class YahooFuturesRoot:
    """A contract root Yahoo carries, with the specification Yahoo does not publish.

    Attributes:
        root_symbol: Short code used as the chain identifier and as the ticker prefix.
        description: What the contract is.
        mic: Exchange the contract lists on, as a MIC.
        exchange_suffix: What Yahoo appends to a dated ticker, e.g. ``.CME``.
        contract_months: Month codes this root lists contracts for.
        multiplier: Money one full price point is worth. **The field that must be right.**
        tick_size: Smallest price increment.
        quote_currency: Currency the quote and the multiplier are in.
        settlement_type: Cash or physical delivery, from the contract specification.
        reliable_continuous_volume: Whether the ``ROOT=F`` series reports usable volume. Dated
            contracts report almost none regardless.
    """

    root_symbol: str
    description: str
    mic: str
    exchange_suffix: str
    multiplier: float
    tick_size: float
    contract_months: str = QUARTERLY
    quote_currency: str = "USD"
    settlement_type: SettlementType = SettlementType.PHYSICAL
    reliable_continuous_volume: bool = True

    @property
    def continuous_ticker(self) -> str:
        """Yahoo's ticker for this root's continuous front-month series."""
        return f"{self.root_symbol}{CONTINUOUS_SUFFIX}"
