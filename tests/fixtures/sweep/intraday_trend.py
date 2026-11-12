"""Sweep parity, intraday: indicators on 5-minute bars, scheduled entries and a flat close."""
import polars as pl

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.cancel_policy import EODCancel
from ziplime.finance.execution import MarketOrder
from ziplime.utils.events import date_rules, time_rules

WARMUP = 40


class AlgorithmConfig(BaseAlgorithmConfig):
    fast: int = 6
    slow: int = 24
    wait: int = 30
    weight: float = 0.4
    symbols: list[str] = ["JNJ@XNYS", "KO@XNYS"]


async def initialize(context):
    config = context.algorithm.config
    context.universe = {s.split("@")[0]: await context.symbol(s) for s in config.symbols}
    context.set_cancel_policy(EODCancel())
    context.indicators(fast=pl.col("close").rolling_mean(config.fast),
                       slow=pl.col("close").rolling_mean(config.slow))
    context.schedule_function(enter, date_rules.every_day(),
                              time_rules.market_open(minutes=config.wait))
    context.schedule_function(flatten, date_rules.every_day(), time_rules.market_close(minutes=30))


async def handle_data(context, data):
    for name, asset in context.universe.items():
        held = await context.portfolio.get_asset_positions_amount(asset)
        if held > 0 and context.signals["fast"][name] < context.signals["slow"][name]:
            await context.order_target(asset, 0, style=MarketOrder())


async def enter(context, data):
    config = context.algorithm.config
    for name, asset in context.universe.items():
        if context.signals["fast"][name] > context.signals["slow"][name]:
            await context.order_target_percent(asset, config.weight, style=MarketOrder())


async def flatten(context, data):
    for name, asset in context.universe.items():
        if await context.portfolio.get_asset_positions_amount(asset) != 0:
            await context.order_target(asset, 0, style=MarketOrder())
