"""Sweep parity: decisions as entries/exits signals, so the sweep runs the vector kernel.

Every other fixture here is swept through its handle_data. This one returns "entries" and "exits"
from compute_signals, which sends the sweep to `simulate_signals_batch` instead -- the path with
its own GPU kernel. handle_data only executes the signals, by the kernel's rule: an exit wins over
an entry, an entry only from flat, an exit closes the whole position.
"""
from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 40


class AlgorithmConfig(BaseAlgorithmConfig):
    fast: int = 5
    slow: int = 20
    size: int = 50
    symbols: list[str] = ["JNJ@XNYS", "KO@XNYS", "PG@XNYS"]


async def initialize(context):
    config = context.algorithm.config
    context.universe = {s.split("@")[0]: await context.symbol(s) for s in config.symbols}


def compute_signals(context, prices):
    config = context.algorithm.config
    fast = prices.close.rolling(config.fast).mean()
    slow = prices.close.rolling(config.slow).mean()
    return {"slow": slow, "entries": fast > slow, "exits": fast < slow}


async def handle_data(context, data):
    if not context.signals.is_ready("slow"):
        return
    size = context.algorithm.config.size
    for name, asset in context.universe.items():
        held = await context.portfolio.get_asset_positions_amount(asset)
        if context.signals["exits"][name]:
            if held > 0:
                await context.order(asset=asset, amount=-held, style=MarketOrder())
        elif context.signals["entries"][name] and held == 0:
            await context.order(asset=asset, amount=size, style=MarketOrder())
