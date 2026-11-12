"""Sweep parity: limit and stop orders, open orders, EODCancel chosen by the config."""
from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.cancel_policy import EODCancel
from ziplime.finance.execution import LimitOrder, StopOrder

WARMUP = 0


class AlgorithmConfig(BaseAlgorithmConfig):
    discount: float = 0.01
    stop: float = 0.03
    eod: bool = False
    shares: int = 50
    symbols: list[str] = ["JNJ@XNYS", "KO@XNYS"]


async def initialize(context):
    config = context.algorithm.config
    context.universe = {s.split("@")[0]: await context.symbol(s) for s in config.symbols}
    if config.eod:
        context.set_cancel_policy(EODCancel())


async def handle_data(context, data):
    config = context.algorithm.config
    for name, asset in context.universe.items():
        if context.get_open_orders(asset):
            continue
        held = await context.portfolio.get_asset_positions_amount(asset)
        spot = await data.current(assets=[asset], fields=["close"])
        price = spot["close"][0]
        if held == 0:
            await context.order(asset, config.shares, style=LimitOrder(price * (1 - config.discount)))
        else:
            await context.order(asset, -held, style=StopOrder(price * (1 - config.stop)))
