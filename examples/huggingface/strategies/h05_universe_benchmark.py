"""Hold the same fourteen names, equally weighted, and never look at a disclosure.

The control. `h03` and `h04` both trade a universe of large-capitalisation American shares over
2016-2026, a decade in which that universe roughly quintupled on its own. Any strategy confined to
those names will return a great deal, and a number with nothing to compare it against says nothing
about whether the disclosures contributed anything.

So this buys all fourteen on the first session, rebalances to equal weight once a quarter, and
reads no data at all. Whatever it returns is what the universe gave away for free. Read the other
two against it, not against zero.

This is the cheapest honest thing an example suite can include, and its absence is how a
backtest on an alternative dataset ends up looking like a discovery.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from hf_config import CONGRESS_UNIVERSE  # noqa: E402

from ziplime.api import date_rules  # noqa: E402
from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.finance.execution import MarketOrder  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {
    "window": "congress",
    "description": "Equal-weight the same universe, ignoring every disclosure -- the control",
}



async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(ticker, mic=mic)
                        for ticker, mic in CONGRESS_UNIVERSE]
    context.schedule_function(rebalance, date_rules.quarter_start())


async def rebalance(context: TradingAlgorithm, data: BarData):

    # Only names with a quote today: several of these listed part-way through the window, and a
    # target on a name with no price cannot be filled.
    quotes = await data.current(assets=context.universe, fields=["price"])
    priced = {sid for sid, price in zip(quotes["sid"].to_list(), quotes["price"].to_list())
              if price and price > 0}
    if not priced:
        return

    weight = 1.0 / len(priced)
    for asset in context.universe:
        await context.order_target_percent(
            asset=asset, target=weight if asset.sid in priced else 0.0, style=MarketOrder())
