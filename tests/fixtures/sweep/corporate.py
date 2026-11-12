"""Sweep parity: holding through a split (with cash in lieu) and a cash dividend."""
from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 0


class AlgorithmConfig(BaseAlgorithmConfig):
    shares: int = 100
    weight: float = 0.4


async def initialize(context):
    context.universe = {"JNJ": await context.symbol("JNJ@XNYS"),
                        "KO": await context.symbol("KO@XNYS")}
    context.i = 0


async def handle_data(context, data):
    config = context.algorithm.config
    if context.i == 0:
        await context.order(context.universe["JNJ"], config.shares, style=MarketOrder())
        await context.order_target_percent(context.universe["KO"], config.weight, style=MarketOrder())
    context.i += 1
