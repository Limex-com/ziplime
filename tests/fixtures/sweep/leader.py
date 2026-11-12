"""Sweep parity: max(..., key=...) over instruments, a dict of weights, context.rebalance."""
import polars as pl

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 40


class AlgorithmConfig(BaseAlgorithmConfig):
    lookback: int = 20
    weight: float = 0.8
    symbols: list[str] = ["JNJ@XNYS", "KO@XNYS", "PG@XNYS"]


async def initialize(context):
    config = context.algorithm.config
    context.universe = {s.split("@")[0]: await context.symbol(s) for s in config.symbols}
    context.i = 0
    context.indicators(momentum=pl.col("close").forward_fill().pct_change(config.lookback))


async def handle_data(context, data):
    config = context.algorithm.config
    bar = context.i
    context.i += 1
    if bar % 5 != 0 or not context.signals.is_ready("momentum"):
        return
    scores = {name: context.signals["momentum"][name] for name in context.universe}
    leader = max(scores, key=lambda name: scores[name])
    weights = {}
    for name, asset in context.universe.items():
        if name == leader:
            weights[asset] = config.weight
    await context.rebalance(weights, style=MarketOrder())
