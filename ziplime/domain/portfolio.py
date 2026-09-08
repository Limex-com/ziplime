import datetime
from collections.abc import Iterable
from dataclasses import dataclass, field
from itertools import chain

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.domain.position import Position


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

    #: Nested exchange name -> trading account id -> asset -> position. **Not** the flat
    #: `{asset: Position}` upstream zipline used: code ported from there subscripts this by asset
    #: and gets a `KeyError`, which is how two trading controls came to be silently broken. Reach
    #: for `get_asset_positions_amount` and its siblings instead, which answer zero for an
    #: instrument that is not held.
    positions: dict[str, dict[str, dict[ExchangeAsset, Position]]] = field(default_factory=dict)

    start_date: datetime.datetime | None = None

    async def get_exchange_asset_positions(self, asset: ExchangeAsset, exchange_name: str | None = None,
                                     trading_account_id: str | None = None) -> list[Position]:
        all_accounts = chain.from_iterable(exchange.values() for exchange in self.positions.values())
        all_positions: Iterable[Position] = chain.from_iterable(account.values() for account in all_accounts)
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
        all_accounts = chain.from_iterable(exchange.values() for exchange in self.positions.values())
        all_positions: Iterable[Position] = chain.from_iterable(account.values() for account in all_accounts)
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
        all_accounts = chain.from_iterable(exchange.values() for exchange in self.positions.values())
        all_positions: Iterable[Position] = chain.from_iterable(account.values() for account in all_accounts)
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
        all_accounts = chain.from_iterable(exchange.values() for exchange in self.positions.values())
        all_positions: Iterable[Position] = chain.from_iterable(account.values() for account in all_accounts)
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
        all_accounts = chain.from_iterable(exchange.values() for exchange in self.positions.values())
        all_positions: Iterable[Position] = chain.from_iterable(account.values() for account in all_accounts)
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
        all_accounts = chain.from_iterable(exchange.values() for exchange in self.positions.values())
        all_positions: Iterable[Position] = chain.from_iterable(account.values() for account in all_accounts)
        filtered = sum(
            pos.amount * pos.last_sale_price
            for pos in all_positions
            if pos.asset.asset.id == asset.asset.id
            and (exchange_name is None or pos.exchange_name == exchange_name)
            and (trading_account_id is None or pos.trading_account_id == trading_account_id)
        )

        return filtered