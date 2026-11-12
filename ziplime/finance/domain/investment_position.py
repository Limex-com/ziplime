from dataclasses import dataclass

from ziplime.finance.domain.investment_event import InvestmentKey
from ziplime.finance.domain.monetary_valuation import MonetaryValuation


@dataclass(frozen=True)
class InvestmentPosition:
    """Immutable lifetime security accounting for one scoped listing.

    All monetary fields use the enclosing snapshot's reporting currency.
    Fields are constructor arguments; closed positions retain realized PNL,
    paid income, and fees, but have zero quantity, cost, and market value.

    Attributes
    ----------
    key : InvestmentKey
        ``(exchange_name, trading_account_id, ExchangeAsset)`` position identity.
    quantity : float
        Signed open quantity after trades and splits.
    historical_cost : float
        Signed remaining FIFO cost translated at event-time FX, including
        retained acquisition fees; not native average cost at today's FX.
    market_value : float
        Signed current monetary value translated at valuation-time FX.
    realized_pnl : float
        Cumulative disposal PNL against historical FIFO cost, including
        disposal fees and acquisition fees attributable to closed units.
    unrealized_pnl : float
        ``market_value - historical_cost`` for the open holding.
    income : float
        Cumulative paid income at payment-time FX; short liabilities are negative.
    fees : float
        Cumulative fee charges at posting-time FX, informational only: already
        included in cost/realized PNL and must not be subtracted again.
    valuation : MonetaryValuation or None
        Current price and FX provenance, or ``None`` when the position is closed.
    """

    key: InvestmentKey
    quantity: float
    historical_cost: float
    market_value: float
    realized_pnl: float
    unrealized_pnl: float
    income: float
    fees: float
    valuation: MonetaryValuation | None

    @property
    def total_pnl(self) -> float:
        """Return realized plus unrealized PNL plus income, excluding uninvested cash FX."""
        return self.realized_pnl + self.unrealized_pnl + self.income
