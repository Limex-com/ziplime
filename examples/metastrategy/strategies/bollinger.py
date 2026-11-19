"""Mean reversion on Bollinger bands: long below the lower band, flat above the middle."""
import math

import polars as pl

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 60


class AlgorithmConfig(BaseAlgorithmConfig):
    bb_period: int = 20
    bb_std: float = 2.0
    symbol: str = "SBER@MISX"


async def initialize(context):
    config = context.config
    context.sber = await context.symbol(config.symbol)
    context.universe = {"SBER": context.sber}
    close = pl.col("close").forward_fill()
    middle = close.rolling_mean(config.bb_period)
    width = close.rolling_std(config.bb_period) * config.bb_std
    context.indicators(close=pl.col("close"), middle=middle, lower=middle - width)


async def handle_data(context, data):
    if not context.signals.is_ready("middle"):
        return
    close = context.signals["close"]["SBER"]
    if math.isnan(close):
        return
    held = await context.portfolio.get_asset_positions_amount(context.sber)
    if close < context.signals["lower"]["SBER"] and not held:
        await context.order_target_percent(context.sber, 0.95, style=MarketOrder())
    elif close > context.signals["middle"]["SBER"] and held:
        await context.order_target_percent(context.sber, 0.0, style=MarketOrder())
