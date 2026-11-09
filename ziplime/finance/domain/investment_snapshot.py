import datetime
from dataclasses import dataclass

from ziplime.finance.domain.investment_position import InvestmentPosition


@dataclass(frozen=True)
class InvestmentSnapshot:
    """Immutable as-of security accounting, including open and closed positions.

    Attributes
    ----------
    currency : str
        ISO currency code shared by all monetary fields and aggregate properties.
    valuation_at : datetime.datetime
        Aware snapshot timestamp; later events are excluded.
    positions : tuple[InvestmentPosition, ...]
        Scoped position records in first-event order, including closed positions
        with retained realized PNL, income, or fees.

    Notes
    -----
    These constructor fields describe lifetime security accounting, not daily
    increments or simulation-period portfolio PNL. Native cash FX and external
    funding are outside this snapshot.
    """

    currency: str
    valuation_at: datetime.datetime
    positions: tuple[InvestmentPosition, ...]

    @property
    def historical_cost(self) -> float:
        """Return total signed remaining FIFO cost, including retained acquisition fees."""
        return sum(position.historical_cost for position in self.positions)

    @property
    def market_value(self) -> float:
        """Return net current value of open holdings in ``currency``."""
        return sum(position.market_value for position in self.positions)

    @property
    def realized_pnl(self) -> float:
        """Return cumulative realized security PNL, including allocated fees."""
        return sum(position.realized_pnl for position in self.positions)

    @property
    def unrealized_pnl(self) -> float:
        """Return current open-holding value minus its historical cost."""
        return sum(position.unrealized_pnl for position in self.positions)

    @property
    def income(self) -> float:
        """Return cumulative paid income at payment-time FX, net of short liabilities."""
        return sum(position.income for position in self.positions)

    @property
    def fees(self) -> float:
        """Return cumulative fees already included in cost/PNL; do not subtract again."""
        return sum(position.fees for position in self.positions)

    @property
    def total_pnl(self) -> float:
        """Return realized plus unrealized PNL plus income, excluding native cash FX."""
        return self.realized_pnl + self.unrealized_pnl + self.income
