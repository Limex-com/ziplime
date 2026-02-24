import asyncio
import datetime
import logging
from functools import partial

import structlog
from lime_trader.models.page import PageRequest
from lime_trader.utils.pagination import iterate_pages_async

from ziplime.assets.entities.asset import Asset
from ziplime.domain.bar_data import BarData

from ziplime.domain.portfolio import Portfolio as ZpPortfolio
from ziplime.domain.position import Position as ZpPosition
from ziplime.domain.account import Account as ZpAccount

from lime_trader.models.accounts import AccountDetails, TradeSide
from lime_trader.models.market import Period
from lime_trader.models.trading import Order as LimeTraderOrder, OrderSide, OrderDetails, \
    OrderStatus as LimeTraderOrderStatus, OrderType, TimeInForce
from ziplime.finance.execution import (MarketOrder,
                                       LimitOrder,
                                       )

from ziplime.finance.commission import CommissionModel
from ziplime.finance.domain.order import Order
from ziplime.finance.domain.order_status import OrderStatus
from ziplime.finance.domain.transaction import Transaction
import pandas as pd
import numpy as np
import uuid

from ziplime.finance.slippage.slippage_model import SlippageModel
from ziplime.exchanges.exchange import Exchange


class TradingSignalExchange(Exchange):

    def __init__(self, name: str):
        super().__init__(name=name)
        self._logger = structlog.get_logger(__name__)

    def get_positions(self) -> dict[Asset, ZpPosition]:
        return {}

    def get_portfolio(self) -> ZpPortfolio:
        return {}

    def get_account(self) -> ZpAccount:
        account = self.get_account_balance(account_number=self._account_id)
        z_account = ZpAccount()
        z_account.buying_power = float(account.cash)
        z_account.total_position_value = float(account.position_market_value)
        return z_account

    def get_account_balance(self, account_number: str) -> AccountDetails:
        acc = next(filter(lambda x: x.account_number == account_number, self._lime_sdk_client.account.get_balances()),
                   None)
        if acc is None:
            raise Exception(f"Invalid account number {account_number}. Not found.")
        return acc

    def get_time_skew(self) -> pd.Timedelta:
        return pd.Timedelta('0 sec')  # TODO: use clock API

    def is_alive(self) -> bool:
        try:
            self._lime_sdk_client.account.get_balances()
            return True
        except Exception as _:
            return False

    def _order2zp(self, order: OrderDetails, asset: Asset) -> Order | None:

        match order.order_status:
            case LimeTraderOrderStatus.CANCELED:
                order_status = OrderStatus.CANCELLED
            case LimeTraderOrderStatus.REJECTED:
                order_status = OrderStatus.REJECTED
            case LimeTraderOrderStatus.SUSPENDED:
                order_status = OrderStatus.HELD
            # case LimeTraderOrderStatus.REPLACED:
            #     zp_order.status = OrderStatus.REPLACED
            # case LimeTraderOrderStatus.PENDING_CANCEL:
            #     zp_order.status = OrderStatus.PENDING_CANCEL
            # case LimeTraderOrderStatus.DONE_FOR_DAY:
            #     zp_order.status = OrderStatus.DONE_FOR_DAY
            case LimeTraderOrderStatus.NEW:
                order_status = OrderStatus.OPEN
            case LimeTraderOrderStatus.PENDING_NEW:
                order_status = OrderStatus.OPEN
            case LimeTraderOrderStatus.PARTIALLY_FILLED:
                order_status = OrderStatus.OPEN
            case LimeTraderOrderStatus.FILLED:
                order_status = OrderStatus.FILLED
            case _:
                raise Exception(f"Unknown order status: {order.order_status}")
        match order.order_type:
            case OrderType.MARKET:
                execution_style = MarketOrder()
            case OrderType.LIMIT:
                execution_style = LimitOrder(limit_price=float(order.price))
            case _:
                raise Exception(f"Unknown order type {order.order_type}")

        order_details = Order(
            id=order.client_order_id,
            asset=asset,
            amount=int(order.quantity) if order.order_side == OrderSide.BUY else -int(order.quantity),
            filled=int(order.executed_quantity),
            dt=order.executed_timestamp,
            commission=0.0,
            execution_style=execution_style,
            status=order_status,
            exchange_order_id=order.order_id
        )

        return order_details

    def get_orders(self) -> dict[str, Order]:
        # return {}
        current_active_orders = self._lime_sdk_client.trading.get_active_orders(
            account_number=self._account_id)

        for order in current_active_orders:
            self._tracked_orders[order.client_order_id] = order

        result = {}
        for o in self._tracked_orders.values():
            ziplime_order = self._order2zp(order=o)
            if ziplime_order is None:
                continue
            result[o.client_order_id] = ziplime_order

        return result

    def get_orders_by_ids(self, order_ids: list[str]) -> list[OrderDetails]:
        result = []
        for order_id in order_ids:
            order = self._lime_sdk_client.trading.get_order_details_by_client_order_id(client_order_id=order_id)
            result.append(order)
        return result

    async def get_transactions(self, orders: dict[Asset, dict[str, Order]], bar_data: BarData):
        closed_orders = []
        transactions = []
        commissions = []

        all_orders = []
        for asset, asset_orders_dict in orders.items():
            asset_orders = list(asset_orders_dict.values())
            all_orders.extend(asset_orders)
        if not all_orders:
            return transactions, commissions, closed_orders
        assets_from_orders = {order.asset.get_symbol_by_exchange(self.name): order.asset for order in all_orders}
        start_date_for_transactions = min(o.dt for o in all_orders)

        orders = [self._lime_sdk_client.trading.get_order_details(order_id=order.exchange_order_id) for
                  order in all_orders]
        order_details = await asyncio.gather(*orders)

        for order_sdk_raw, order in zip(order_details, all_orders):
            order_sdk = self._order2zp(order=order_sdk_raw, asset=order.asset)
            if order_sdk_raw.executed_timestamp is None:
                continue
            if not order_sdk.open:
                closed_orders.append(order)
        async for transaction_page in iterate_pages_async(start_page=PageRequest(page=1, size=20),
                                              func=partial(self._lime_sdk_client.account.get_trades,
                                                           account_number=self._account_id,
                                                           date=bar_data.current_dt.date())):
            # async for transaction_page in self._lime_sdk_client.account.iterate_trades(
            #         account_number=self._account_id,
            #         start_page=PageRequest(page=1, size=20),
            #         date=start_date_for_transactions.date()):

            for transaction_sdk in transaction_page.data:
                asset = assets_from_orders[transaction_sdk.symbol]
                if asset is None:
                    continue

                # total_commissions = sum(
                #     fee.amount for fee in transaction_sdk.fees
                # )
                total_commissions = 0.0

                if transaction_sdk.trade_id in self.processed_transaction_ids:
                    continue

                tx = Transaction(
                    id=uuid.uuid4().hex,
                    asset=asset,
                    amount=transaction_sdk.quantity if transaction_sdk.side == TradeSide.BUY else -transaction_sdk.quantity,
                    dt=transaction_sdk.timestamp,
                    price=float(transaction_sdk.price),
                    order_id=None,
                    commission=total_commissions,  # TODO: how to get commission
                )
                transactions.append(tx)
                self.processed_transaction_ids.add(transaction_sdk.trade_id)
                commissions.append(
                    {
                        "asset": asset,
                        # "order": order,
                        "cost": total_commissions,
                    }
                )
        return transactions, commissions, closed_orders

        #
        #     for order, txn in slippage.simulate(data=bar_data, assets={asset},
        #                                         orders_for_asset=asset_orders.values()):
        #         commission = self.get_commission_model(asset=asset)
        #         additional_commission = commission.calculate(order, txn)
        #
        #         if additional_commission > 0:
        #
        #
        #         order.filled += txn.amount
        #         order.commission += additional_commission
        #
        #         order.dt = txn.dt
        #
        #         transactions.append(txn)
        #
        #         if not order.open:
        #             closed_orders.append(order)
        #
        # for asset, asset_orders_dict in orders.items():
        #     asset_orders = list(asset_orders_dict.values())
        #     orders = [self._lime_sdk_client.trading.get_order_details_by_client_order_id(client_order_id=order.id) for
        #               order in asset_orders]
        #     order_details = await asyncio.gather(*orders)
        #     for order_sdk_raw, order in zip(order_details, asset_orders):
        #         order_sdk = self._order2zp(order=order_sdk_raw, asset=asset)
        #         if order_sdk_raw.executed_timestamp is None:
        #             continue
        #         tx = Transaction(
        #             asset=order.asset,
        #             amount=int(order_sdk_raw.executed_quantity),
        #             dt=order_sdk_raw.executed_timestamp,
        #             price=float(order_sdk_raw.price),
        #             order_id=order_sdk_raw.client_order_id,
        #             commission=order_sdk_raw,
        #         )
        #         if not order_sdk.open:
        #             closed_orders.append(order_sdk)
        #         results[order_sdk_raw.client_order_id] = tx
        return results

        # raise NotImplementedError("Use get_transactions_by_order_ids method.")

    # def get_transactions_by_order_ids(self, order_ids: list[str]):
    #     results = {}
    #
    #     for order_id in order_ids:
    #         order = self._lime_sdk_client.trading.get_order_details_by_client_order_id(client_order_id=order_id)
    #         # self._lime_sdk_client.account.iterate_trades(account_number=self._account_id,
    #         #                                          date=
    #         #
    #         #                                          ):
    #         if order.executed_timestamp is None:
    #             continue
    #         try:
    #             asset = symbol_lookup(order.symbol)
    #         except SymbolNotFound:
    #             continue
    #         tx = Transaction(
    #             asset=asset,
    #             amount=int(order.executed_quantity),
    #             dt=order.executed_timestamp,
    #             price=float(order.price),
    #             order_id=order.client_order_id,
    #             commission=0.0,
    #         )
    #         results[order.client_order_id] = tx
    #     return results

    def cancel_order(self, zp_order_id: str) -> None:
        try:
            order = self._lime_sdk_client.trading.get_order_details_by_client_order_id(order_id=zp_order_id)
            self._lime_sdk_client.trading.cancel_order(order_id=order.order_id)
        except Exception as e:
            self._logger.error(e)
            return

    def get_last_traded_dt(self, asset) -> datetime.datetime:
        quote = self._lime_sdk_client.market.get_current_quote(asset.symbol)
        return quote.date

    def get_spot_value(self, assets, field, dt, data_frequency):
        assert (field in (
            'open', 'high', 'low', 'close', 'volume', 'price'))
        assets_is_scalar = not isinstance(assets, (list, set, tuple))
        if assets_is_scalar:
            symbols = [assets.symbol]
        else:
            symbols = [asset.symbol for asset in assets]
        if field in ('price',):
            quotes = self._lime_sdk_client.market.get_current_quotes(symbols=symbols)
            if assets_is_scalar:
                if field == 'price':
                    if len(quotes) == 0:
                        return np.nan
                    return float(quotes[-1].last)
                else:
                    if len(quotes) == 0:
                        return pd.NaT
                    return quotes[-1].last_timestamp
            else:
                return [
                    float(quote.last) if field == 'price' else quote.last_timestamp
                    for quote in quotes
                ]

        bars_list = self._lime_sdk_client.market.get_quotes_history(symbol=symbols[0], period=Period.MINUTE,
                                                                    from_date=dt,
                                                                    to_date=dt)
        if assets_is_scalar:
            if len(bars_list) == 0:
                return np.nan
            return getattr(bars_list[0], field)
        bars_map = {a.symbol: a for a in bars_list}
        return [
            getattr(bars_map[symbol], field)
            for symbol in symbols
        ]

    def get_realtime_bars(self, assets, data_frequency):
        # TODO: cache the result. The caller
        # (DataPortalLive#get_history_window) makes use of only one
        # column at a time.
        assets_is_scalar = not isinstance(assets, (list, set, tuple))
        is_daily = 'd' in data_frequency  # 'daily' or '1d'
        if assets_is_scalar:
            symbols = [assets.symbol]
        else:
            symbols = [asset.symbol for asset in assets]
        timeframe = Period.DAY if is_daily else Period.MINUTE
        if not symbols:
            return []
        dfs = []
        to_date = datetime.datetime.now(tz=datetime.timezone.utc)
        from_date = to_date - datetime.timedelta(days=1)
        for asset in assets if not assets_is_scalar else [assets]:

            bars_list = self._lime_sdk_client.market.get_quotes_history(symbol=asset.symbol, period=timeframe,
                                                                        from_date=from_date,
                                                                        to_date=to_date
                                                                        )
            bars_map = {a.symbol: a for a in bars_list}

            symbol = asset.symbol
            df = bars_map[symbol].df.copy()
            if df.index.tz is None:
                df.index = df.index.tz_localize(
                    'utc').tz_convert('America/New_York')
            df.columns = pd.MultiIndex.from_product([[asset, ], df.columns])
            dfs.append(df)
        return pd.concat(dfs, axis=1)

    async def submit_order(self, order: Order):
        symbol = order.asset.get_symbol_by_exchange(self.name)
        qty = order.amount if order.amount > 0 else -order.amount
        side = OrderSide.BUY if order.amount > 0 else OrderSide.SELL

        if isinstance(order.execution_style, MarketOrder):
            order_type = OrderType.MARKET
        elif isinstance(order.execution_style, LimitOrder):
            order_type = OrderType.LIMIT
        else:
            raise Exception(f"Unsupported order type: {order.execution_style}.")

        new_order_id = uuid.uuid4().hex
        sdk_order = LimeTraderOrder(
            symbol=symbol,
            quantity=float(qty),
            side=side,
            order_type=order_type,
            time_in_force=TimeInForce.DAY,
            price=order.limit,
            client_order_id=new_order_id,
            account_number=self._account_id
        )
        validated_order = await self._lime_sdk_client.trading.validate_order(order=sdk_order)
        if not validated_order.is_valid:
            raise Exception("Validation failed for order")

        submitted_order = await self._lime_sdk_client.trading.place_order(order=sdk_order)
        # await asyncio.sleep(5)  # TODO: check why order is not immediately available
        order_details_sdk = await self._lime_sdk_client.trading.get_order_details(
            order_id=submitted_order.order_id
        )
        order_details = self._order2zp(order=order_details_sdk, asset=order.asset)

        return order_details

    def get_commission_model(self, asset: Asset) -> CommissionModel:
        pass

    def get_slippage_model(self, asset: Asset) -> SlippageModel:
        pass
