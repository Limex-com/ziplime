"""Scale the position so that its risk, not its size, is constant.

Realised volatility is measured on the adjusted chain, then turned into a contract count through
the notional helper. When the market gets noisier the position shrinks.
"""
import statistics

from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si"], "description": "Volatility-targeted position size"}
WINDOW = 20
TARGET_ANNUAL_RISK = 60_000.0      # roubles of annualised standard deviation
SESSIONS_PER_YEAR = 250
#: Re-size only on a meaningful change in the target.
REBALANCE_BAND = 2


async def initialize(context: TradingAlgorithm):
    context.chain = await context.continuous_future("Si", offset=0, roll="volume", adjustment="mul")
    context.held = None
    context.held_amount = 0


async def handle_data(context: TradingAlgorithm, data: BarData):
    contract = await data.current_contract(context.chain)
    if contract is None:
        return
    history = await data.history(assets=[context.chain], bar_count=WINDOW + 1, fields=["close"],
                                 frequency="1d")
    closes = history["close"].to_list() if len(history) else []
    if len(closes) < WINDOW + 1 or any(not c for c in closes):
        return

    returns = [b / a - 1 for a, b in zip(closes[:-1], closes[1:])]
    daily_vol = statistics.pstdev(returns)
    if daily_vol <= 0:
        return
    annual_vol_per_contract = context.notional_exposure(
        asset=contract, amount=1, price=closes[-1]) * daily_vol * (SESSIONS_PER_YEAR ** 0.5)
    wanted = int(TARGET_ANNUAL_RISK / annual_vol_per_contract) if annual_vol_per_contract else 0

    if context.held is not None and context.held.sid != contract.sid:
        await context.order(asset=context.held, amount=-context.held_amount, style=MarketOrder())
        context.held, context.held_amount = None, 0
    if context.held is None:
        if wanted:
            await context.order(asset=contract, amount=wanted, style=MarketOrder())
            context.held, context.held_amount = contract, wanted
    elif abs(wanted - context.held_amount) >= REBALANCE_BAND:
        await context.order(asset=contract, amount=wanted - context.held_amount,
                            style=MarketOrder())
        context.held_amount = wanted
