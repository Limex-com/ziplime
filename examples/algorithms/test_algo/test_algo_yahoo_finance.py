import datetime
import logging

import numpy as np
import polars as pl
import structlog
from pydantic import BaseModel

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder, LimitOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

logger = structlog.get_logger(__name__)


class EquityToTrade(BaseModel):
    symbol: str
    target_percentage: float


class AlgorithmConfig(BaseAlgorithmConfig):
    currency: str
    equities_to_trade: list[EquityToTrade]


async def initialize(context: TradingAlgorithm):
    # context.asset = await context.symbol("AAPL")
    # No hardcoded MIC: the same ticker sits on a different exchange depending on which asset
    # database you ingested, and leaving it out resolves the symbol wherever it is listed.
    context.assets = [
        await context.symbol("AAPL"),
        await context.symbol("META"),
        await context.symbol("NFLX"),
    ]
    # read config file
    logger.info("Algorithm config: ", config=context.algorithm.config)
    context.i = 0


async def handle_data(context: TradingAlgorithm, data: BarData):
    num_assets = len(context.assets)
    target_percent = 1.0 / num_assets
    if context.i == 0:
        await context.order_target_percent(asset=context.assets[0], style=MarketOrder(), target=1)
    if context.i == 1:
        await context.order_target_percent(asset=context.assets[0], style=MarketOrder(), target=0.5)
    context.i+=1

    # print(context.portfolio)
    # result = await context.order(exchange_name="grpc_exchange",
    #               asset=context.assets[1],
    #               style=MarketOrder(),
    #               amount=-1,
    #               ),
    # await context.order_target(asset=context.assets[1], exchange_name="grpc_exchange",
    #                      style=LimitOrder(limit_price=current_prices.filter(pl.col("symbol") == "GMKN")[0]["price"][0]), target=1)
    # await context.order_target(asset=context.assets[1], exchange_name="grpc_exchange",
    #                      style=LimitOrder(limit_price=current_prices.filter(pl.col("symbol") == "MGNT")[0]["price"][0]), target=2)
    # await context.order_target(asset=context.assets[1], exchange_name="grpc_exchange",
    #                      style=MarketOrder(), target=1)

    # await context.order_target(asset=context.assets[1], exchange_name="grpc_exchange",
    #                      style=MarketOrder(), target=1)

    # for asset in context.assets:
    #     print(f"Ordering asset={asset}, target_percent={target_percent}")

    # open_orders = context.get_open_orders()
    # for order in open_orders:
    # await context.cancel_all_orders_for_asset(asset=context.assets[0], exchange_name="grpc_exchange")
