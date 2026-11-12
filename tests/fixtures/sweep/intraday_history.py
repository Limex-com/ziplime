"""Sweep parity, intraday: days built from the run's bars, the bars themselves, and the previous
close read in before_trading_start."""
import datetime

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 0


class AlgorithmConfig(BaseAlgorithmConfig):
    days: int = 3
    bars: int = 12
    shares: int = 50
    symbols: list[str] = ["JNJ@XNYS", "KO@XNYS"]


async def initialize(context):
    context.universe = {s.split("@")[0]: await context.symbol(s)
                        for s in context.algorithm.config.symbols}
    context.previous = 0.0


async def before_trading_start(context, data):
    spot = await data.current(assets=[context.universe["JNJ"]], fields=["close"])
    context.previous = spot["close"][0]


async def handle_data(context, data):
    config = context.algorithm.config
    for name, asset in context.universe.items():
        daily = await data.history(assets=[asset], bar_count=config.days, fields=["close", "high"])
        recent = await data.history(assets=[asset], bar_count=config.bars, fields=["close"],
                                    frequency=datetime.timedelta(minutes=5))
        spot = await data.current(assets=[asset], fields=["close"])
        price = spot["close"][0]
        held = await context.portfolio.get_asset_positions_amount(asset)
        if held == 0 and price > daily["close"].mean() and price > recent["close"].mean() \
                and context.previous > 0:
            await context.order(asset, config.shares, style=MarketOrder())
        elif held > 0 and (price < recent["close"].min() * 1.001 or price > daily["high"].max()):
            await context.order_target(asset, 0, style=MarketOrder())
