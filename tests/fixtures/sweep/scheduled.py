"""Sweep parity: schedule_function with a config-dependent rule, before_trading_start."""
from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder
from ziplime.utils.events import date_rules

WARMUP = 0


class AlgorithmConfig(BaseAlgorithmConfig):
    offset: int = 0
    weight: float = 0.5


async def initialize(context):
    context.universe = {"JNJ": await context.symbol("JNJ@XNYS")}
    context.reference = 0.0
    context.schedule_function(rebalance,
                              date_rule=date_rules.week_start(days_offset=context.algorithm.config.offset))


async def before_trading_start(context, data):
    spot = await data.current(assets=[context.universe["JNJ"]], fields=["close"])
    context.reference = spot["close"][0]


async def handle_data(context, data):
    pass


async def rebalance(context, data):
    config = context.algorithm.config
    spot = await data.current(assets=[context.universe["JNJ"]], fields=["close"])
    target = config.weight if spot["close"][0] > context.reference else 0.0
    await context.order_target_percent(context.universe["JNJ"], target, style=MarketOrder())
