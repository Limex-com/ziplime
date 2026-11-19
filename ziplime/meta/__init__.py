"""Metastrategies: algorithms that run other algorithms and trade them as assets.

    async def initialize(context):
        context.s1 = await context.strategy("s1")
        context.bb = await context.strategy("bollinger", grid={"bb_period": [10, 20, 30],
                                                               "bb_std": [1.5, 2.0, 2.5]})

    async def handle_data(context, data):
        if context.s1.performance.last(60).sharpe > 1:
            await context.order_target_percent(context.s1, 0.5, style=MarketOrder())
        await context.order_target_percent(context.bb.best("sharpe", window=60), 0.5,
                                           style=MarketOrder())

Run it with run_simulation like any algorithm; each strategy's own result is in
``result.strategies``. See docs/backtesting/metastrategies.md.
"""
from ziplime.meta.exchange import STRATEGY_VENUE, StrategyExchange, StrategyUnit
from ziplime.meta.handle import Performance, StrategyCluster, StrategyHandle

__all__ = ["STRATEGY_VENUE", "Performance", "StrategyCluster", "StrategyExchange", "StrategyHandle",
           "StrategyUnit"]
