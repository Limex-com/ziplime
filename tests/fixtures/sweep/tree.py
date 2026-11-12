"""Sweep parity: a decision tree over named instruments looked up in handle_data, computed from
history arrays in the strategy's own nested functions (NumPy, TA-Lib), ranked by sort(key=...),
and positions read through portfolio.positions."""
import numpy as np
import talib

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 0


class AlgorithmConfig(BaseAlgorithmConfig):
    drawdown_limit: float = 8.0
    vol_limit: float = 1.5
    rsi_period: int = 14
    momentum_window: int = 10


async def initialize(context):
    pass


async def handle_data(context, data):
    config = context.algorithm.config
    assets_list = ["JNJ", "KO", "PG", "XOM"]
    assets = {s: await context.symbol(s, mic="XNYS") for s in assets_list}

    price_history = {}
    for s in assets_list:
        df = await data.history(assets=[assets[s]], fields=["close"], bar_count=60)
        price_history[s] = df["close"].to_numpy()

    def cum_return(symbol, window):
        p = price_history[symbol][-window:]
        return (p[-1] / p[0] - 1) * 100

    def std_return(symbol, window):
        p = price_history[symbol][-window:]
        rets = np.diff(p) / p[:-1]
        return np.std(rets) * 100

    def max_drawdown(symbol, window):
        p = price_history[symbol][-window:]
        cummax = np.maximum.accumulate(p)
        return np.max((cummax - p) / cummax * 100)

    def rsi(symbol, period):
        return talib.RSI(price_history[symbol], timeperiod=period)[-1]

    selected = []
    if max_drawdown("KO", 40) >= config.drawdown_limit:
        if std_return("JNJ", 14) <= config.vol_limit:
            cands = ["JNJ", "KO", "PG"]
            cands.sort(key=lambda x: cum_return(x, config.momentum_window), reverse=True)
            selected = [assets[s] for s in cands[:2]]
        else:
            selected = [assets["XOM"]]
    else:
        if rsi("PG", config.rsi_period) >= 50:
            selected = [assets["PG"]]
        else:
            cands = ["XOM", "KO", "JNJ", "PG"]
            cands.sort(key=lambda x: cum_return(x, 5))
            selected = [assets[s] for s in cands[:3]]

    weight = 0.95 / len(selected) if selected else 0.0
    for s, asset in assets.items():
        held = getattr(context.portfolio.positions.get(asset, 0), "amount", 0)
        if asset in selected:
            if weight > 0:
                await context.order_target_percent(asset=asset, target=weight, style=MarketOrder())
        elif held != 0:
            await context.order_target_percent(asset=asset, target=0, style=MarketOrder())
