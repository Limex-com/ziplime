import datetime
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from ziplime.assets.domain.continuous_future import ContinuousFuture
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.domain.position import Position



def _in_chain(position, continuous_future: ContinuousFuture) -> bool:
    """Whether ``position`` is a contract of ``continuous_future``'s chain, on its exchange."""
    asset = position.asset
    return (getattr(asset.asset, "root_symbol", None) == continuous_future.root_symbol
            and asset.mic == continuous_future.mic)

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

    positions: Mapping[tuple[str, str, ExchangeAsset], Position] = field(default_factory=dict)

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
        if isinstance(asset, ContinuousFuture):
            # A continuous future is held through whichever contract of its chain is current.
            return sum(pos.amount for pos in all_positions if _in_chain(pos, asset)
                       and (exchange_name is None or pos.exchange_name == exchange_name)
                       and (trading_account_id is None or pos.trading_account_id == trading_account_id))
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
        if isinstance(asset, ContinuousFuture):
            # A continuous future is held through whichever contract of its chain is current.
            return sum(pos.amount for pos in all_positions if _in_chain(pos, asset)
                       and (exchange_name is None or pos.exchange_name == exchange_name)
                       and (trading_account_id is None or pos.trading_account_id == trading_account_id))
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
        if isinstance(asset, ContinuousFuture):
            # A continuous future is held through whichever contract of its chain is current.
            return sum(pos.amount * pos.last_sale_price for pos in all_positions if _in_chain(pos, asset)
                       and (exchange_name is None or pos.exchange_name == exchange_name)
                       and (trading_account_id is None or pos.trading_account_id == trading_account_id))
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
        if isinstance(asset, ContinuousFuture):
            # A continuous future is held through whichever contract of its chain is current.
            return sum(pos.amount * pos.last_sale_price for pos in all_positions if _in_chain(pos, asset)
                       and (exchange_name is None or pos.exchange_name == exchange_name)
                       and (trading_account_id is None or pos.trading_account_id == trading_account_id))
        filtered = sum(
            pos.amount * pos.last_sale_price
            for pos in all_positions
            if pos.asset.asset.id == asset.asset.id
            and (exchange_name is None or pos.exchange_name == exchange_name)
            and (trading_account_id is None or pos.trading_account_id == trading_account_id)
        )

        return filtered
