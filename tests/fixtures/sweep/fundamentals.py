"""Sweep parity: a dataset declared in initialize, read through an indicator."""
import polars as pl

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 0


class AlgorithmConfig(BaseAlgorithmConfig):
    size: int = 100
    margin: float = 0.0


async def initialize(context):
    context.universe = {"JNJ": await context.symbol("JNJ", mic="XNYS"),
                        "KO": await context.symbol("KO", mic="XNYS")}
    context.datasets = {"fundamentals": "fundamentals"}
    context.indicators(
        quality=pl.col("fundamentals.gross_profit") / pl.col("fundamentals.total_assets"))


async def handle_data(context, data):
    config = context.algorithm.config
    scores = {name: float(context.signals["quality"][name]) for name in context.universe}
    if any(score != score for score in scores.values()):
        return
    best = max(scores, key=lambda name: scores[name])
    for name, listing in context.universe.items():
        held = await context.portfolio.get_asset_positions_amount(listing)
        if name == best and held == 0 and scores[name] > config.margin:
            await context.order(asset=listing, amount=config.size, style=MarketOrder())
        elif name != best and held > 0:
            await context.order(asset=listing, amount=-held, style=MarketOrder())
