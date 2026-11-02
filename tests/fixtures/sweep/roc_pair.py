"""Sweep parity: a pair switched by TA-Lib's ROC on each instrument's history array -- a function
computed once on the whole history (lazy.WINDOW_LOCAL) -- with its period from initialize."""
import talib

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 0


class AlgorithmConfig(BaseAlgorithmConfig):
    lookback: int = 90
    gap: float = 0.0


async def initialize(context):
    context.first = await context.symbol("JNJ")
    context.second = await context.symbol("KO")
    context.lookback = context.algorithm.config.lookback


async def handle_data(context, data):
    first, second, lookback = context.first, context.second, context.lookback
    close_first = (await data.history(assets=[first], fields=["close"],
                                      bar_count=lookback + 1))["close"].to_numpy()
    close_second = (await data.history(assets=[second], fields=["close"],
                                       bar_count=lookback + 1))["close"].to_numpy()
    roc_first = talib.ROC(close_first, timeperiod=lookback)[-1]
    roc_second = talib.ROC(close_second, timeperiod=lookback)[-1]
    if roc_first < roc_second - context.algorithm.config.gap:
        await context.order_target_percent(asset=first, target=1.0, style=MarketOrder())
        await context.order_target_percent(asset=second, target=0.0, style=MarketOrder())
    else:
        await context.order_target_percent(asset=first, target=0.0, style=MarketOrder())
        await context.order_target_percent(asset=second, target=1.0, style=MarketOrder())
