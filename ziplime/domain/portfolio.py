import datetime
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.domain.position import Position


class Positions(dict):
    """``(exchange, trading account, asset) -> Position``, also looked up by the asset alone.

    The map is keyed by book, but strategies written for zipline ask
    ``context.portfolio.positions.get(asset)`` or ``positions[asset].amount``. Those lookups used
    to miss every time -- ``get`` returned its default and ``in`` was False whatever was held -- so
    a strategy that closed what it held by checking its position never closed anything. Here an
    asset key finds the asset's position; held in several books, it is ambiguous and raises
    rather than picking one. Iteration and the tuple keys are unchanged.
    """

    def _by_asset(self, asset):
        sid = getattr(asset, "sid", None)
        found = [position for key, position in dict.items(self)
                 if key[-1] == asset or (sid is not None and getattr(key[-1], "sid", None) == sid)]
        if len(found) > 1:
            raise ValueError(f"{asset} is held in {len(found)} books; look it up by "
                             f"(exchange, trading account, asset).")
        return found[0] if found else None

    def __getitem__(self, key):
        if isinstance(key, tuple) or dict.__contains__(self, key):
            return dict.__getitem__(self, key)
        position = self._by_asset(key)
        if position is None:
            raise KeyError(key)
        return position

    def get(self, key, default=None):
        if isinstance(key, tuple) or dict.__contains__(self, key):
            return dict.get(self, key, default)
        position = self._by_asset(key)
        return default if position is None else position

    def __contains__(self, key):
        if isinstance(key, tuple) or dict.__contains__(self, key):
            return dict.__contains__(self, key)
        return self._by_asset(key) is not None


@dataclass
class Portfolio:
    # capital_used: float
    cash_flow: float
    starting_cash: float
    portfolio_value: float
    pnl: float
    returns: float
    cash: float
    positions_value: float
    positions_exposure: float
    # exchange_portfolios: dict[str, Self]

    positions: Mapping[tuple[str, str, ExchangeAsset], Position] = field(default_factory=Positions)

    start_date: datetime.datetime | None = None

    def _all_positions(self) -> Iterable[Position]:
        """Return positions from the flat exchange/account/asset position map."""
        return self.positions.values()

    async def get_exchange_asset_positions(self, asset: ExchangeAsset, exchange_name: str | None = None,
                                           trading_account_id: str | None = None) -> list[Position]:
        all_positions = self._all_positions()
        filtered = list(
            pos
            for pos in all_positions
            if pos.asset.sid == asset.sid
            and (exchange_name is None or pos.exchange_name == exchange_name)
            and (trading_account_id is None or pos.trading_account_id == trading_account_id)
        )

        return filtered

    async def get_asset_positions(self, asset: ExchangeAsset, exchange_name: str | None = None,
                                  trading_account_id: str | None = None) -> list[Position]:
        all_positions = self._all_positions()
        filtered = list(
            pos
            for pos in all_positions
            if pos.asset.asset.id == asset.asset.id
            and (exchange_name is None or pos.exchange_name == exchange_name)
            and (trading_account_id is None or pos.trading_account_id == trading_account_id)
        )

        return filtered

    async def get_exchange_asset_positions_amount(self, asset: ExchangeAsset, exchange_name: str | None = None,
                                                  trading_account_id: str | None = None) -> int:
        all_positions = self._all_positions()
        filtered = sum(
            pos.amount
            for pos in all_positions
            if pos.asset.sid == asset.sid
            and (exchange_name is None or pos.exchange_name == exchange_name)
            and (trading_account_id is None or pos.trading_account_id == trading_account_id)
        )

        return filtered

    async def get_asset_positions_amount(self, asset: ExchangeAsset, exchange_name: str | None = None,
                                         trading_account_id: str | None = None) -> int:
        all_positions = self._all_positions()
        filtered = sum(
            pos.amount
            for pos in all_positions
            if pos.asset.asset.id == asset.asset.id
            and (exchange_name is None or pos.exchange_name == exchange_name)
            and (trading_account_id is None or pos.trading_account_id == trading_account_id)
        )

        return filtered

    async def get_exchange_asset_positions_value(self, asset: ExchangeAsset, exchange_name: str | None = None,
                                                 trading_account_id: str | None = None) -> float:
        all_positions = self._all_positions()
        filtered = sum(
            pos.amount * pos.last_sale_price
            for pos in all_positions
            if pos.asset.sid == asset.sid
            and (exchange_name is None or pos.exchange_name == exchange_name)
            and (trading_account_id is None or pos.trading_account_id == trading_account_id)
        )

        return filtered

    async def get_asset_positions_value(self, asset: ExchangeAsset, exchange_name: str | None = None,
                                        trading_account_id: str | None = None) -> float:
        all_positions = self._all_positions()
        filtered = sum(
            pos.amount * pos.last_sale_price
            for pos in all_positions
            if pos.asset.asset.id == asset.asset.id
            and (exchange_name is None or pos.exchange_name == exchange_name)
            and (trading_account_id is None or pos.trading_account_id == trading_account_id)
        )

        return filtered
