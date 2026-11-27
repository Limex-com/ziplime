"""Portfolio for api."""

from types import ModuleType
from ziplime.sweep import api
from ziplime.sweep.api.columns import Columns


class Portfolio:
    """The ledger's portfolio snapshot at this bar."""

    def __init__(self, columns: Columns) -> None:
        self._columns = columns

    def update(self, xp: ModuleType, cash, held, marks, multiplier, is_future) -> None:
        self.cash = cash
        self.held = held
        exposure = held * marks * multiplier
        self.positions_exposure = xp.sum(exposure, axis=1, keepdims=True)
        # Futures are not part of the portfolio's value: their gains arrive as cash
        self.positions_value = xp.sum(xp.where(is_future, 0.0, held * marks), axis=1, keepdims=True)
        self.portfolio_value = cash + self.positions_value

    def get_asset_positions_amount(self, asset, *args, **kwargs):
        return self._columns.select(self.held, asset)

    @property
    def positions(self) -> 'api.Positions':
        from ziplime.sweep.api.positions import Positions

        return Positions(self)
