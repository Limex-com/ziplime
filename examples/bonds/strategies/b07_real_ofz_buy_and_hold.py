"""Hold a real OFZ while its price falls, and let the coupons carry it.

The other examples run on generated prices so that they work without a token. This one runs on the
real thing: ОФЗ 26238, a long bullet with a 7.1% coupon, bought at 66.5% of par in January 2024.

What actually happened is the point. The clean price did not recover to par -- it fell to 48.5 by
October 2024 and was still only 54.3 in July 2026, an 18% decline over the holding period. The
position nonetheless ends up ahead, because five coupons of 35.40 per bond more than covered the
loss on the price. That is the whole argument for owning a bond rather than trading one, and it is
invisible in any backtest that does not pay the schedule.

It also shows why the quotation matters: the quote is a percentage of a 1000-rouble nominal, so
the fall from 66.5 to 54.3 is 122 roubles per bond, not 12.20. Reading the tape as money would
report a tenth of the loss, a tenth of the coupon income and a tenth of the exposure.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "tickers": ["SU26238RMFS4"],
    "description": "Hold a real OFZ through a rate cycle, on ingested MOEX prices",
    "real_data": True,
}


async def initialize(context: TradingAlgorithm):
    context.bond = await context.bond_symbol("SU26238RMFS4@MISX")
    context.bought = False
    context.reported = False


async def handle_data(context: TradingAlgorithm, data: BarData):
    quoted = (await data.current(assets=[context.bond], fields=["price"]))["price"][0]
    if quoted is None:
        return

    if not context.bought:
        per_bond = await context.bond_dirty_price(context.bond, quoted)
        accrued = await context.accrued_interest(context.bond)
        ytm = await context.bond_yield_to_maturity(context.bond, quoted)
        amount = int(context.portfolio.cash * 0.9 / per_bond)
        if amount <= 0:
            return
        print(f"{context.simulation_dt.date()} buying {amount} at {quoted:.3f}% "
              f"= {per_bond:,.2f} RUB each (accrued {accrued:.2f}, "
              f"simple yield to maturity {ytm:.2%})")
        await context.order(asset=context.bond, amount=amount, style=MarketOrder())
        context.bought = True
        return

    if not context.reported and context.simulation_dt.date().year == 2026:
        held = await context.portfolio.get_asset_positions_amount(context.bond)
        per_bond = await context.bond_dirty_price(context.bond, quoted)
        print(f"{context.simulation_dt.date()} holding {held} at {quoted:.3f}% "
              f"= {per_bond:,.2f} RUB each; portfolio {context.portfolio.portfolio_value:,.2f}")
        context.reported = True
