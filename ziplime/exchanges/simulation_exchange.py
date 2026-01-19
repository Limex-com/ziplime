import datetime
from functools import lru_cache

import polars as pl
import uuid

from exchange_calendars import ExchangeCalendar

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.constants.period import Period
from ziplime.data.services.data_source import DataSource

from ziplime.domain.position import Position
from ziplime.domain.portfolio import Portfolio
from ziplime.domain.account import Account
from ziplime.finance.commission import EquityCommissionModel, FutureCommissionModel, CommissionModel
from ziplime.finance.domain.order import Order
from ziplime.finance.slippage.slippage_model import SlippageModel
from ziplime.exchanges.exchange import Exchange
from ziplime.gens.domain.trading_clock import TradingClock


class SimulationExchange(Exchange):

    def __init__(self,
                 name: str,
                 country_code: str,
                 trading_calendar: ExchangeCalendar,
                 clock: TradingClock,
                 cash_balance: float,
                 equity_slippage: SlippageModel,
                 future_slippage: SlippageModel,
                 equity_commission: EquityCommissionModel,
                 future_commission: FutureCommissionModel,
                 data_source: DataSource = None,
                 extra_data_sources: list = None
                 ):
        super().__init__(name=name,
                         canonical_name=name,
                         clock=clock,
                         data_source=data_source,
                         country_code=country_code,
                         trading_calendar=trading_calendar)
        self.slippage_models = {
            Equity: equity_slippage,
            FuturesContract: future_slippage,
        }
        self.commission_models = {
            Equity: equity_commission,
            FuturesContract: future_commission,
        }
        self.cash_balance = cash_balance

    def get_start_cash_balance(self) -> float:
        return self.cash_balance

    def get_current_cash_balance(self) -> float:
        return self.cash_balance

    def get_commission_model(self, asset: Asset) -> CommissionModel:
        return self.commission_models[type(asset)]

    def get_slippage_model(self, asset: Asset) -> SlippageModel:
        return self.slippage_models[type(asset)]

    async def submit_order(self, order: Order):
        order.id = uuid.uuid4().hex
        return order

    def get_positions(self) -> dict[Asset, Position]:
        pass

    def get_portfolio(self) -> Portfolio:
        pass

    def get_account(self) -> Account:
        pass

    def get_time_skew(self):
        pass

    def order(self, asset, amount, style):
        pass

    def is_alive(self):
        pass

    def get_orders(self) -> dict[str, Order]:
        pass

    async def get_transactions(self, orders: dict[Asset, dict[str, Order]],
                               current_dt: datetime.datetime):
        """
        Creates a list of transactions based on the current open orders,
        slippage model, and commission model.

        Parameters
        ----------
        bar_data: ziplime._protocol.BarData

        Notes
        -----
        This method book-keeps the blotter's open_orders dictionary, so that
         it is accurate by the time we're done processing open orders.

        Returns
        -------
        transactions_list: List
            transactions_list: list of transactions resulting from the current
            open orders.  If there were no open orders, an empty list is
            returned.

        commissions_list: List
            commissions_list: list of commissions resulting from filling the
            open orders.  A commission is an object with "asset" and "cost"
            parameters.

        closed_orders: List
            closed_orders: list of all the orders that have filled.
        """

        closed_orders = []
        transactions = []
        commissions = []

        for asset, asset_orders in orders.items():
            slippage = self.get_slippage_model(asset=asset)

            for order, txn in slippage.simulate(exchange=self, assets=frozenset({asset}),
                                                orders_for_asset=asset_orders.values(),
                                                current_dt=current_dt
                                                ):
                commission = self.get_commission_model(asset=asset)
                additional_commission = commission.calculate(order=order, transaction=txn)

                if additional_commission > 0:
                    commissions.append(
                        {
                            "asset": order.asset,
                            "order": order,
                            "cost": additional_commission,
                        }
                    )

                order.filled += txn.amount
                order.commission += additional_commission

                order.dt = txn.dt

                transactions.append(txn)

                if not order.open:
                    closed_orders.append(order)

        return transactions, commissions, closed_orders

    def get_orders_by_ids(self, order_ids: list[str]):
        pass

    def get_transactions_by_order_ids(self, order_ids: list[str]):
        pass

    def cancel_order(self, order_param):
        pass

    def get_last_traded_dt(self, asset):
        pass

    def get_spot_value(self, assets: frozenset[Asset], fields: frozenset[str], dt: datetime.datetime,
                       data_frequency: datetime.timedelta) -> pl.DataFrame:
        pass

    def get_realtime_bars(self, assets, frequency):
        pass

    async def get_scalar_asset_spot_value(self, asset: Asset, field: str, dt: datetime.datetime,
                                          frequency: datetime.timedelta):
        return self.get_data_by_limit(
            fields=frozenset({field}),
            limit=1,
            end_date=dt,
            frequency=frequency,
            assets=frozenset({asset}),
            include_end_date=True,
        )

    def get_scalar_asset_spot_value_sync(self, asset: Asset, field: str, dt: datetime.datetime,
                                         frequency: datetime.timedelta):

        return  self.get_data_by_limit(
            fields=frozenset({field}),
            limit=1,
            end_date=dt,
            frequency=frequency,
            assets=frozenset({asset}),
            include_end_date=True,
        )

    # @lru_cache(maxsize=100)
    def current(self, assets: frozenset[Asset], fields: frozenset[str], dt: datetime.datetime):
        data = {}
        # print(f"Getting current: {assets}, fields={fields}, dt={dt}")
        # TODO: check this, uncomment adjust_minutes
        # if not self._adjust_minutes:
        # return self.data_source.get_spot_value(
        #     assets=assets,
        #     fields=fields,
        #     dt=dt,
        #     frequency=self.data_source.frequency
        # )

        return self.get_data_by_limit(
            fields=fields,
            limit=1,
            end_date=dt,
            frequency=self.data_source.frequency,
            assets=assets,
            include_end_date=True,
        )
        # return df_raw

    @lru_cache(maxsize=100)
    def get_data_by_period(self,
                           fields: frozenset[str],
                          start_date: datetime.datetime,
                          end_date: datetime.datetime,
                          frequency: datetime.timedelta,
                          assets: frozenset[Asset],
                          include_end_date: bool,
                          source: str
                          ) -> pl.DataFrame:
        return self.data_source.get_data_by_limit(fields=fields,
                                                  limit=limit,
                                                  end_date=end_date,
                                                  frequency=frequency,
                                                  assets=assets,
                                                  include_end_date=include_end_date,
                                                  )

    @lru_cache(maxsize=100)
    def get_data_by_limit(self, fields: frozenset[str],
                          limit: int,
                          end_date: datetime.datetime,
                          frequency: datetime.timedelta | Period,
                          assets: frozenset[Asset],
                          include_end_date: bool,
                          ) -> pl.DataFrame:
        return self.data_source.get_data_by_limit(fields=fields,
                                                  limit=limit,
                                                  end_date=end_date,
                                                  frequency=frequency,
                                                  assets=assets,
                                                  include_end_date=include_end_date,
                                                  )
