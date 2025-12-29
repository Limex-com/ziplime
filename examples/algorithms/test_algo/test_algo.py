import datetime
import logging

import numpy as np
import structlog
from pydantic import BaseModel

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

logger = structlog.get_logger(__name__)


class EquityToTrade(BaseModel):
    symbol: str
    target_percentage: float


class AlgorithmConfig(BaseAlgorithmConfig):
    currency: str
    equities_to_trade: list[EquityToTrade]


async def initialize(context):
    context.assets = [
        await context.symbol("META"),
        await context.symbol("AMZN"),
        await context.symbol("NFLX"),
        await context.symbol("GOOGL")
    ]
    # read config file
    logger.info("Algorithm config: ", config=context.algorithm.config)


async def handle_data(context, data):
    num_assets = len(context.assets)
    target_percent = 1.0 / num_assets
    for asset in context.assets:
        await context.order_target_percent(asset=asset,
                                           target=target_percent, style=MarketOrder())
