import datetime
import logging

import numpy as np
import structlog

from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

logger = structlog.get_logger(__name__)
async def initialize(context):
    context.assets = [
        await context.symbol("META@NYSE"),
        await context.symbol("META@BATS"),
        await context.symbol("AMZN"),
        await context.symbol("NFLX"),
        await context.symbol("GOOGL") - GOOGL,MIC
    ]

    context.meta_symbols( [
        await context.symbol("META@NYSE"),
        await context.symbol("META@BATS"),
    ])

async def handle_data(context, data):
    num_assets = len(context.assets)
    target_percent = 1.0 / num_assets
    for asset in context.assets:
        await context.order_target_percent(asset=asset,
                                           target=target_percent, style=MarketOrder())

    data.history([context.meta, context.google])
