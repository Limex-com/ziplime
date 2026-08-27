"""Print what the simulation does and does not model, alongside the margin it posts.

A futures backtest that runs cleanly is not automatically realistic. ``context.realism_warnings()``
lists the effects that are not reproduced, and ``context.futures_margin_requirement()`` shows what
the open book ties up under the configured model.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.finance.realism import format_realism_warnings
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si"], "margin": "fixed",
                 "description": "Reports realism gaps and posted margin while it trades"}


async def initialize(context: TradingAlgorithm):
    context.chain = await context.continuous_future("Si", offset=0, roll="volume", adjustment="mul")
    context.held = None
    context.reported = False
    context.peak_margin = 0.0


async def handle_data(context: TradingAlgorithm, data: BarData):
    contract = await data.current_contract(context.chain)
    if contract is None:
        return
    if context.held is not None and context.held.sid != contract.sid:
        await context.order(asset=context.held, amount=-1, style=MarketOrder())
        context.held = None
    if context.held is None:
        await context.order(asset=contract, amount=2, style=MarketOrder())
        context.held = contract

    posted = context.futures_margin_requirement()
    context.peak_margin = max(context.peak_margin, posted)

    if not context.reported and posted:
        context.reported = True
        print()
        print(f"  margin modelled: {context.models_futures_margin()}")
        print(f"  posted margin on the first filled session: {posted:,.2f} "
              f"({posted / context.portfolio.portfolio_value:.1%} of equity)")
        print(format_realism_warnings(context.realism_warnings()))
        print()
