"""Size against margin rather than against cash.

A futures position costs nothing to open, so "how much cash do I have" is not a position limit.
What limits it is margin: the exchange takes a deposit per contract and marks it daily.

This targets a notional on the front WTI contract, then checks the margin that results against a
budget and cuts the position if it is over. Rebalanced monthly, because margin moves with the price
and a daily rebalance would churn commission for no reason.

Run it without a margin model and `models_futures_margin()` is False -- the check below has nothing
to bite on and sizing is unconstrained. That is what the realism warnings exist to make visible.

The result is not the point and is not a recommendation: this is a directional position, so it
makes or loses roughly leverage times the move in crude. What it demonstrates is that the margin
check binds and cuts the position, which is visible in the printed line.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "roots": ["CL"],
    "description": "Hold crude sized so its initial margin stays inside a budget",
    "margin": "fixed",
}

#: Notional to aim for, as a multiple of the portfolio. Futures let you exceed 1.0 easily, which
#: is the point of checking margin rather than cash.
TARGET_LEVERAGE = 1.5
#: The share of the portfolio the initial margin may occupy. With the 10% initial rate the harness
#: configures, this permits a notional of 1.0x -- so the 1.5x target above is over budget and the
#: check below has to cut it. That is the whole demonstration.
MARGIN_BUDGET = 0.10


async def initialize(context: TradingAlgorithm):
    chain = await context.futures_chain("CL")
    context.contract = chain[0]
    context.last_rebalance = None
    context.reported = False


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    if context.last_rebalance is not None and (today - context.last_rebalance).days < 30:
        return

    quotes = await data.current(assets=[context.contract], fields=["price"])
    prices = dict(zip(quotes["sid"].to_list(), quotes["price"].to_list()))
    price = prices.get(context.contract.sid)
    # A contract that had not listed yet when the window opened is forward-filled with zeros
    # until its first real bar, and zero is not a price.
    if not price or price <= 0:
        return

    value = context.portfolio.portfolio_value
    target = context.contracts_for_notional(
        asset=context.contract, notional=value * TARGET_LEVERAGE, price=price)
    held = await context.portfolio.get_asset_positions_amount(context.contract)
    if target != held:
        await context.order(asset=context.contract, amount=target - held, style=MarketOrder())
    context.last_rebalance = today

    if not context.models_futures_margin():
        if not context.reported:
            print("No margin model configured - position sizing is unconstrained.")
            context.reported = True
        return

    # Margin is known only once the position exists and has been marked.
    posted = context.futures_margin_by_currency().get("USD", 0.0)
    if posted > value * MARGIN_BUDGET and target > 0:
        # Over budget: cut back proportionally, using the margin actually posted per contract.
        per_contract = posted / target
        affordable = int(value * MARGIN_BUDGET / per_contract) if per_contract else 0
        if affordable < target:
            await context.order(asset=context.contract, amount=affordable - target,
                                style=MarketOrder())
            if not context.reported:
                print(f"{today} margin {posted:,.0f} exceeded the "
                      f"{value * MARGIN_BUDGET:,.0f} budget; cut {target} -> {affordable}")
                context.reported = True
