"""Buy before the record date, sell after it, and see what the coupon is really worth.

The trade that looks free and is not. Holding across a record date earns the coupon, but the clean
price is not the whole story on either side: the buyer pays the accrued interest going in and gets
it back going out, so the coupon does not arrive as a windfall -- it replaces the accrual that was
paid for.

Run it against b01. The buy-and-hold run keeps the coupons; this one keeps the coupons minus the
accrued interest it paid on every entry, minus commission on every round trip. The gap is the
answer to whether coupon capture is a strategy or an illusion.
"""
import datetime

from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "tickers": ["ZLB26"],
    "description": "Buy into each coupon and sell out after it, paying the accrued interest each time",
}

#: Enter this many days before the record date, leave this many days after the payment.
ENTRY_LEAD = datetime.timedelta(days=10)
EXIT_LAG = datetime.timedelta(days=5)


async def initialize(context: TradingAlgorithm):
    context.bond = await context.bond_symbol("ZLB26@MISX")
    # The schedule is reference data, so it can be read once rather than every bar.
    schedule = await context.bond_schedule(context.bond)
    context.coupons = [event for event in schedule if event.event_type.value == "COUPON"]


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    held = await context.portfolio.get_asset_positions_amount(context.bond)

    entering = any(event.entitlement_date - ENTRY_LEAD <= today <= event.entitlement_date
                   for event in context.coupons)
    leaving = any(event.date < today <= event.date + EXIT_LAG for event in context.coupons)

    if held and leaving:
        await context.order(asset=context.bond, amount=-held, style=MarketOrder())
        return
    if held or not entering:
        return

    price = await data.current(assets=[context.bond], fields=["price"])
    quoted = price["price"][0]
    if quoted is None:
        return
    per_bond = await context.bond_dirty_price(context.bond, quoted)
    amount = int(context.portfolio.cash * 0.9 / per_bond)
    if amount > 0:
        await context.order(asset=context.bond, amount=amount, style=MarketOrder())
