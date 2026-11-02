"""Sweep parity: short targets, order_percent, order_value, a comparison with the loop's name."""
import math

import polars as pl

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 30


class AlgorithmConfig(BaseAlgorithmConfig):
    lookback: int = 10
    size: float = 0.2
    symbols: list[str] = ["JNJ@XNYS", "KO@XNYS", "PG@XNYS"]


async def initialize(context):
    config = context.algorithm.config
    context.universe = {s.split("@")[0]: await context.symbol(s) for s in config.symbols}
    context.i = 0
    context.indicators(momentum=pl.col("close").forward_fill().pct_change(config.lookback))


async def handle_data(context, data):
    bar = context.i
    context.i += 1
    config = context.algorithm.config
    if bar % 5 != 0:
        return
    for name, asset in context.universe.items():
        momentum = context.signals["momentum"][name]
        if math.isnan(momentum):
            continue
        if name == "JNJ" and bar % 10 == 0:
            await context.order_value(asset, 2_000.0, style=MarketOrder())
        elif momentum > 0:
            await context.order_target_percent(asset, config.size, style=MarketOrder())
        else:
            await context.order_target_percent(asset, -config.size, style=MarketOrder())
    if bar % 20 == 0:
        await context.order_percent(context.universe["KO"], 0.05, style=MarketOrder())
