"""Hold one name at a fixed weight from the first bar; counts its bars for the lock-step test."""
from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder


class AlgorithmConfig(BaseAlgorithmConfig):
    symbol: str = "JNJ"
    weight: float = 1.0


async def initialize(context):
    context.asset = await context.symbol(context.config.symbol, mic="XNYS")
    context.bars = 0


async def handle_data(context, data):
    context.bars += 1
    await context.order_target_percent(context.asset, context.config.weight, style=MarketOrder())
