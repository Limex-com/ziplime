"""Buy the bond named in ``TICKER`` on the first session and hold it.

Used by the end-to-end bond tests. Deliberately minimal: a fixed number of bonds rather than a
share of the portfolio, so two runs over different issues buy the same quantity and the difference
between their results is attributable to the schedule alone.
"""
import os

from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

TICKER = os.environ.get("ZIPLIME_TEST_BOND_TICKER", "ZLB26")
QUANTITY = int(os.environ.get("ZIPLIME_TEST_BOND_QUANTITY", "100"))


async def initialize(context: TradingAlgorithm):
    context.bond = await context.bond_symbol(f"{TICKER}@MISX")
    context.bought = False


async def handle_data(context: TradingAlgorithm, data: BarData):
    if context.bought:
        return
    price = await data.current(assets=[context.bond], fields=["price"])
    if price["price"][0] is None:
        return
    await context.order(asset=context.bond, amount=QUANTITY, style=MarketOrder())
    context.bought = True
