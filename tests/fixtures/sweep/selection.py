"""Sweep parity: data.returns, a dict of scores, sorted(...)[:k] and membership."""
from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 0


class AlgorithmConfig(BaseAlgorithmConfig):
    lookback: int = 20
    top_k: int = 2
    rebalance_every: int = 5
    symbols: list[str] = ["JNJ@XNYS", "KO@XNYS", "PG@XNYS", "XOM@XNYS"]


async def initialize(context):
    context.universe = {s.split("@")[0]: await context.symbol(s)
                        for s in context.algorithm.config.symbols}
    context.i = 0


async def handle_data(context, data):
    config = context.algorithm.config
    bar = context.i
    context.i += 1
    if bar % config.rebalance_every != 0:
        return
    momentum = await data.returns(assets=list(context.universe.values()), bar_count=config.lookback)
    scores = {}
    for name, asset in context.universe.items():
        if asset in momentum:
            scores[name] = momentum[asset]
    best = sorted(scores, key=scores.get, reverse=True)[:config.top_k]
    weight = 0.9 / config.top_k
    for name, asset in context.universe.items():
        if name in best and scores[name] > 0:
            await context.order_target_percent(asset, weight, style=MarketOrder())
        elif await context.portfolio.get_asset_positions_amount(asset) != 0:
            await context.order_target_percent(asset, 0.0, style=MarketOrder())
