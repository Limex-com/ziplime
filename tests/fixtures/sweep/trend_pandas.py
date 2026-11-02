"""Sweep parity: indicators, a loop over the universe, order_target_percent, a counter."""
import math

import pandas as pd
import polars as pl

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 40


class AlgorithmConfig(BaseAlgorithmConfig):
    fast: int = 5
    slow: int = 20
    rebalance_every: int = 3
    invested: float = 0.9
    symbols: list[str] = ["JNJ@XNYS", "KO@XNYS", "PG@XNYS"]


async def initialize(context):
    config = context.algorithm.config
    context.universe = {s.split("@")[0]: await context.symbol(s) for s in config.symbols}
    context.i = 0
    price = pl.col("close").forward_fill()
    context.indicators(close=pl.col("close"), fast=price.rolling_mean(config.fast),
                       slow=price.rolling_mean(config.slow))


async def handle_data(context, data):
    bar = context.i
    context.i += 1
    config = context.algorithm.config
    if not context.signals.is_ready("slow") or bar % config.rebalance_every != 0:
        return
    weight = config.invested / len(context.universe)
    for name, asset in context.universe.items():
        if pd.isna(context.signals["close"][name]):
            continue
        if context.signals["fast"][name] > context.signals["slow"][name]:
            await context.order_target_percent(asset, weight, style=MarketOrder())
        elif await context.portfolio.get_asset_positions_amount(asset) != 0:
            await context.order_target_percent(asset, 0.0, style=MarketOrder())
