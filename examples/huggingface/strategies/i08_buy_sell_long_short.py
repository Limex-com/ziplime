"""Long the buying, short the selling. The test that removes the market.

Every long-only rule in this suite competes with `i00`, and `i00` returns a great deal because
these 125 names survived a decade. Holding a subset of survivors captures most of that regardless
of how the subset was chosen.

Taking the sellers short removes it. Half the capital long names with recent insider buying, half
short names with recent insider selling, so market direction largely cancels and what is left is
the spread between the two lists. For the congressional data the answer was clearly negative. Here
the disclosures are two days old rather than a month, which is the reason to ask again.

The short book is modelled as a negative position and nothing else: no borrow cost, no recall risk,
no hard-to-borrow constraint. On micro caps -- which is what this universe is -- that understates
the cost of being short by a wide margin, and the result should be read as an upper bound.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from hf_config import INSIDER10_UNIVERSE  # noqa: E402
from insider import mount_features, rebalance  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "insider10",
                 "description": "Long insider buying, short insider selling -- market removed"}
LOOKBACK = 30
SIDE_WEIGHT = 0.5
MAX_WEIGHT = 0.05
REBALANCE_EVERY_DAYS = 7


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in INSIDER10_UNIVERSE]
    context.source = await mount_features(
        context, fields=["n_open_market_buys", "n_open_market_sells"])
    context.last_rebalance = None


def side(sids: set[int], budget: float) -> dict[int, float]:
    """Equal weights within one side, capped absolutely and never renormalised past the cap."""
    if not sids:
        return {}
    return {sid: min(budget / len(sids), MAX_WEIGHT) for sid in sids}


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    if context.last_rebalance and (today - context.last_rebalance).days < REBALANCE_EVERY_DAYS:
        return
    context.last_rebalance = today

    window = await data.history(
        assets=context.universe, bar_count=LOOKBACK,
        fields=["n_open_market_buys", "n_open_market_sells"], data_source=context.source)
    if window.is_empty():
        return

    bought, sold = set(), set()
    for row in window.iter_rows(named=True):
        if (row["n_open_market_buys"] or 0) > 0:
            bought.add(row["sid"])
        if (row["n_open_market_sells"] or 0) > 0:
            sold.add(row["sid"])
    # A name with both is a disagreement among insiders, not a signal either way.
    both = bought & sold
    bought -= both
    sold -= both

    targets = side(bought, SIDE_WEIGHT)
    targets.update({sid: -w for sid, w in side(sold, SIDE_WEIGHT).items()})
    if targets:
        await rebalance(context, data, targets, tolerance=0.01)
