"""Sweep parity: per-instrument state on the context -- entry prices, days held, a stop."""
import polars as pl

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 30


class AlgorithmConfig(BaseAlgorithmConfig):
    fast: int = 5
    slow: int = 15
    stop: float = 0.03
    hold: int = 10
    shares: int = 100
    symbols: list[str] = ["JNJ@XNYS", "KO@XNYS", "PG@XNYS"]


async def initialize(context):
    config = context.algorithm.config
    context.universe = {s.split("@")[0]: await context.symbol(s) for s in config.symbols}
    context.entry = {}
    context.age = {}
    context.trades = 0
    price = pl.col("close").forward_fill()
    context.indicators(fast=price.rolling_mean(config.fast), slow=price.rolling_mean(config.slow))


async def handle_data(context, data):
    config = context.algorithm.config
    if not context.signals.is_ready("slow"):
        return
    for name, asset in context.universe.items():
        spot = await data.current(assets=[asset], fields=["close"])
        price = spot["close"][0]
        if name not in context.entry:
            if context.signals["fast"][name] > context.signals["slow"][name]:
                await context.order(asset, config.shares, style=MarketOrder())
                context.entry[name] = price
                context.age[name] = 0
        else:
            context.age[name] = context.age.get(name, 0) + 1
            if price < context.entry[name] * (1 - config.stop) or context.age[name] >= config.hold:
                await context.order_target(asset, 0, style=MarketOrder())
                del context.entry[name]
                context.age.pop(name)
    for name, opened_at in context.entry.items():
        if opened_at > 1_000_000:
            context.entry[name] = 0.0
