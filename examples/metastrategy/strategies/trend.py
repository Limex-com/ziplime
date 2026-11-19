"""Trend following: long while the fast average is above the slow one."""
import math

import polars as pl

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 100


class AlgorithmConfig(BaseAlgorithmConfig):
    fast: int = 10
    slow: int = 50
    symbol: str = "SBER@MISX"


async def initialize(context):
    config = context.config
    context.sber = await context.symbol(config.symbol)
    context.universe = {"SBER": context.sber}
    close = pl.col("close").forward_fill()
    context.indicators(close=pl.col("close"), fast=close.rolling_mean(config.fast),
                       slow=close.rolling_mean(config.slow))


async def handle_data(context, data):
    if not context.signals.is_ready("slow") or math.isnan(context.signals["close"]["SBER"]):
        return
    long = context.signals["fast"]["SBER"] > context.signals["slow"]["SBER"]
    await context.order_target_percent(context.sber, 0.95 if long else 0.0, style=MarketOrder())
