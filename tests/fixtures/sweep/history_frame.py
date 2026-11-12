"""Sweep parity: data.history over several instruments -- grouped by sid, and filtered by sid."""
import polars as pl

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
    frame = await data.history(assets=list(context.universe.values()), bar_count=config.lookback,
                               fields=["close"])
    if frame.is_empty():
        return
    stats = frame.group_by("sid").agg(
        (pl.col("close").last() / pl.col("close").first() - 1).alias("momentum"),
        pl.col("close").std().alias("volatility"))
    scores = {row["sid"]: row["momentum"] / row["volatility"]
              for row in stats.iter_rows(named=True) if row["volatility"] > 0}
    best = sorted(scores, key=scores.get, reverse=True)[:config.top_k]
    weight = 0.9 / config.top_k
    for name, asset in context.universe.items():
        mean = frame.filter(pl.col("sid") == asset.sid)["close"].mean()
        price = (await data.current(assets=[asset], fields=["close"]))["close"][0]
        if asset.sid in best and price > mean:
            await context.order_target_percent(asset, weight, style=MarketOrder())
        elif await context.portfolio.get_asset_positions_amount(asset) != 0:
            await context.order_target_percent(asset, 0.0, style=MarketOrder())
