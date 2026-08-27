"""Buy a fixed money amount of the bond named in ``TICKER``, using ``order_value``.

Exercises the value-sizing path rather than the quantity one: ``order_value`` has to convert
through the dirty price, because a bond quote is a percentage of face value. Sizing off the quote
would buy about ten times too many.
"""
import os

from ziplime.domain.bar_data import BarData
from ziplime.trading.trading_algorithm import TradingAlgorithm

TICKER = os.environ.get("ZIPLIME_TEST_BOND_TICKER", "ZLB26")
VALUE = float(os.environ.get("ZIPLIME_TEST_BOND_VALUE", "100000"))


async def initialize(context: TradingAlgorithm):
    context.bond = await context.bond_symbol(f"{TICKER}@MISX")
    context.bought = False
    context.realism = []


async def handle_data(context: TradingAlgorithm, data: BarData):
    if context.bought:
        # Reported after the fill, not alongside the order: the bond-specific warnings depend on
        # the book actually holding a bond, and the order has not filled yet on the bar that
        # places it.
        context.realism = [str(w) for w in context.realism_warnings()]
        return
    price = await data.current(assets=[context.bond], fields=["price"])
    if price["price"][0] is None:
        return
    await context.order_value(asset=context.bond, value=VALUE)
    context.bought = True
