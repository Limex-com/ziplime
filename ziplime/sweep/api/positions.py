"""Positions for api."""

from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.api.portfolio import Portfolio
from ziplime.sweep.api.position import Position


class Positions:
    """``context.portfolio.positions`` looked up by instrument, as the engine's map allows: only
    open positions are in it."""

    def __init__(self, portfolio: Portfolio) -> None:
        self._portfolio = portfolio

    def _amount(self, asset):
        if isinstance(asset, tuple):
            raise NotConvertible("portfolio.positions[(exchange, account, asset)] is not "
                                 "supported in a sweep; look the position up by the asset.")
        return self._portfolio.get_asset_positions_amount(asset)

    def get(self, asset, default=None) -> Position:
        # A position that is not open has amount 0, as the default's .amount would be read
        return Position(self._amount(asset))

    def __getitem__(self, asset) -> Position:
        return Position(self._amount(asset))

    def sweep_contains(self, asset):
        return self._amount(asset) != 0

    def sweep_len(self):
        xp = self._portfolio._columns.backend.xp
        return xp.sum(self._portfolio.held != 0, axis=1, keepdims=True)

    def sweep_truth(self):
        return self.sweep_len() > 0

    def _refuse(self, *args, **kwargs):
        raise NotConvertible("Iterating portfolio.positions is not supported in a sweep (the "
                             "engine's keys are (exchange, account, asset)); iterate "
                             "context.universe and read positions.get(asset).amount.")

    __iter__ = keys = values = items = _refuse
