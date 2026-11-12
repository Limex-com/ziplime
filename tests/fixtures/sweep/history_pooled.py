"""A frame of several instruments read as one: the engine pools their rows, so a sweep refuses."""
from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 0


class AlgorithmConfig(BaseAlgorithmConfig):
    window: int = 10
    band: float = 1.0
    shares: int = 100
    symbols: list[str] = ["JNJ@XNYS", "KO@XNYS", "PG@XNYS"]


async def initialize(context):
    context.universe = {s.split("@")[0]: await context.symbol(s)
                        for s in context.algorithm.config.symbols}


async def handle_data(context, data):
    config = context.algorithm.config
    pooled = await data.history(assets=list(context.universe.values()), bar_count=config.window,
                                fields=["close"])
    for name, asset in context.universe.items():
        window = await data.history(assets=[asset], bar_count=config.window, fields=["close"])
        mean = pooled["close"].mean()
        spread = window["close"].std()
        spot = await data.current(assets=[asset], fields=["close"])
        price = spot["close"][0]
        held = await context.portfolio.get_asset_positions_amount(asset)
        if price < mean - config.band * spread and held == 0:
            await context.order_target(asset, config.shares, style=MarketOrder())
        elif price > mean and held > 0:
            await context.order_target_value(asset, 0.0, style=MarketOrder())
