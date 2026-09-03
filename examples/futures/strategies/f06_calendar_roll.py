"""Hold the front WTI contract and roll it before delivery.

A dated contract dies. Holding one to the end of a backtest is not a long position in crude oil, it
is a long position in one specific month that stops existing -- and for a **physically delivered**
contract like WTI, holding it past the notice date is an obligation to take delivery of a thousand
barrels per contract at Cushing, Oklahoma. Staying long the market therefore means rolling: closing
the expiring contract and opening the next one out.

The roll is not free, and its cost is the shape of the curve. WTI is backwardated over this window
-- the next contract out is *cheaper* -- so each roll closes a position at a higher price than it
reopens at, and the difference is recorded below on every roll.

This runs on the recent window rather than the full history, because that is where the front of the
stored chain really is the market's front month. See ``futures_config`` for why: Yahoo removes
expired contracts, so the chain runs forward from today rather than back through the past.

``ROLL_OFFSET_DAYS`` is deliberately wide. Rolling on the notice date itself works on a cash-settled
contract and is reckless on a deliverable one, where liquidity has already left and the delivery
window is days away.
"""
import datetime

from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "roots": ["CL"],
    "window": "roll",
    "description": "Stay long crude by rolling the front contract before it goes to delivery",
    "margin": "fixed",
}

#: Calendar days before a contract's last tradable session to move to the next one.
ROLL_OFFSET_DAYS = 21
CONTRACTS = 3


def as_date(value) -> datetime.date:
    return value.date() if isinstance(value, datetime.datetime) else value


async def initialize(context: TradingAlgorithm):
    context.chain = await context.futures_chain("CL")
    context.held = None
    context.rolls = 0


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    # The first contract still tradable ROLL_OFFSET_DAYS from now. Once the front contract fails
    # that test the answer changes to the next one out, and that is the roll.
    threshold = today + datetime.timedelta(days=ROLL_OFFSET_DAYS)
    target = next((c for c in context.chain
                   if as_date(c.asset.auto_close_date) > threshold), None)
    if target is None or (context.held is not None and target.sid == context.held.sid):
        return

    assets = [target] if context.held is None else [context.held, target]
    quotes = await data.current(assets=assets, fields=["price"])
    prices = dict(zip(quotes["sid"].to_list(), quotes["price"].to_list()))
    target_price = prices.get(target.sid)
    if not target_price or target_price <= 0:
        return

    if context.held is None:
        await context.order(asset=target, amount=CONTRACTS, style=MarketOrder())
        print(f"{today} open   {CONTRACTS} x {target.symbol} @ {target_price:>7,.2f}  "
              f"(last tradable {as_date(target.asset.auto_close_date)})")
    else:
        old_price = prices.get(context.held.sid)
        if not old_price or old_price <= 0:
            return
        held_amount = await context.portfolio.get_asset_positions_amount(context.held)
        await context.order(asset=context.held, amount=-held_amount, style=MarketOrder())
        await context.order(asset=target, amount=held_amount, style=MarketOrder())
        context.rolls += 1

        new_price = target_price
        multiplier = target.asset.multiplier
        # Selling the near contract dearer than the far one is bought is a gain on the roll; that
        # is what backwardation pays a long position, and it reverses in contango.
        carry = (old_price - new_price) * multiplier * held_amount
        print(f"{today} roll #{context.rolls}  {context.held.symbol} @ {old_price:>7,.2f} "
              f"-> {target.symbol} @ {new_price:>7,.2f}   "
              f"spread {old_price - new_price:>+6,.2f}  = {carry:>+11,.0f} on {held_amount} "
              f"contracts")
    context.held = target
