"""Sweep parity: a computation on history whose slice changes during the run (with the portfolio).
A compiled step looks its numbers up from what the first bar saw, notices the change and the batch
runs again without compiling."""
import numpy as np

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 0


class AlgorithmConfig(BaseAlgorithmConfig):
    window: int = 10
    symbols: list[str] = ["JNJ@XNYS", "KO@XNYS", "PG@XNYS"]


async def initialize(context):
    context.universe = {s.split("@")[0]: await context.symbol(s)
                        for s in context.algorithm.config.symbols}


async def handle_data(context, data):
    config = context.algorithm.config
    # A longer average once something is held: a slice bound that changes during the run
    n = config.window + 5 * (context.portfolio.positions_value > 0)
    for name, asset in context.universe.items():
        p = (await data.history(assets=[asset], bar_count=40, fields=["close"]))["close"].to_numpy()
        held = await context.portfolio.get_asset_positions_amount(asset)
        if p[-1] > np.mean(p[-n:]) and held == 0:
            await context.order_target_percent(asset, 0.3, style=MarketOrder())
        elif p[-1] < np.mean(p[-n:]) and held > 0:
            await context.order_target_percent(asset, 0.0, style=MarketOrder())
