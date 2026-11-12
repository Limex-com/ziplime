"""Sweep parity: a futures contract -- multiplier, daily settlement, per-contract commission."""
import polars as pl

from ziplime.assets.domain.asset_type import AssetType
from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 20


class AlgorithmConfig(BaseAlgorithmConfig):
    fast: int = 3
    slow: int = 10
    contracts: int = 2


async def initialize(context):
    config = context.algorithm.config
    context.universe = {"CL": await context.symbol("CLZ23@XCME",
                                                   asset_type=AssetType.FUTURES_CONTRACT)}
    context.indicators(fast=pl.col("close").rolling_mean(config.fast),
                       slow=pl.col("close").rolling_mean(config.slow))


async def handle_data(context, data):
    config = context.algorithm.config
    contract = context.universe["CL"]
    if not context.signals.is_ready("slow"):
        return
    if context.signals["fast"]["CL"] > context.signals["slow"]["CL"]:
        await context.order_target(contract, config.contracts, style=MarketOrder())
    else:
        await context.order_target(contract, -config.contracts, style=MarketOrder())
