"""Gross profitability again, with the ranking computed over the whole history in one pass.

The counterpart of `f02_gross_profitability`: same measure, same universe, same quarterly
rebalance, written the other way. Not a bit-exact twin, and the numbers do differ -- two
hand-written filters over thirteen years of two hundred names diverge on one eligibility call and
never converge again. What *is* exact is the thing that matters: the statements this reads as of
each bar are the same ones `data.current` returns there, checked value by value against the real
dataset and in `tests/test_vectorized_fundamentals.py` against every bar of a run.

What moves into `compute_signals` is the arithmetic: the statements resolved as of every bar, the
ratio, and the cross-sectional rank. What stays in the bar loop is everything that reads the
book -- which names are priced today, what is already held, what the rebalance has to trade.

The one new line is `prices.dataset("fundamentals")`. A company files four times a year, so the
statements cannot be pivoted onto the bar grid the way prices can; what it returns is the as-of
view, which is the same thing `data.current` returns at a single bar, computed for every bar at
once. The engine reproduces the source's own resolution -- these statements coalesce by column,
because a later filing restating a period repeats fewer line items than the original.

`rank(axis=1)` ranks across the row, so each bar's ranking uses only that bar's values and the
causality check passes. A z-score against the whole period's mean would not, and would be refused
before the first order rather than quietly inflating the result.
"""
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from fundamentals import mount  # noqa: E402
from hf_config import FUNDAMENTALS_UNIVERSE  # noqa: E402
from portfolio import priced, rebalance_to  # noqa: E402

from ziplime.api import date_rules  # noqa: E402
from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "fundamentals",
                 "description": "Gross profitability, ranked vectorised over the whole history"}

KEEP = 40
#: Ignore a company whose freshest statement is older than this -- annual filings, so a year and a
#: half allows for a late filer without letting one that stopped reporting in 2015 sit in the book.
MAX_STALENESS = datetime.timedelta(days=550)


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in FUNDAMENTALS_UNIVERSE]
    context.datasets = {"fundamentals": await mount(context)}
    context.schedule_function(rebalance, date_rules.quarter_start())


def compute_signals(context, prices):
    """One pass over every bar and every company: the ratio, and how stale each filing is.

    The *ranking* is deliberately not here. Which companies are eligible today depends on which
    ones have a price on this bar, and that is bar-local state -- rank across the whole universe
    in advance and the top forty includes names that cannot be bought, so the book ends up holding
    thirty. Rank across the row only when the row is the whole choice.
    """
    f = prices.dataset("fundamentals")
    # Assets of zero or less is not a business with infinite profitability, it is a filing to
    # ignore -- and dividing by it would put that company at the top of the ranking forever.
    assets = f.total_assets.where(f.total_assets > 0)
    return {"quality": f.gross_profit / assets, "age_days": f.age_days}


async def rebalance(context: TradingAlgorithm, data: BarData):
    if not context.signals.is_ready("quality"):
        return

    prices_now = await priced(context, data)
    by_symbol = {listing.symbol: listing for listing in context.universe}
    quality, ages = context.signals["quality"], context.signals["age_days"]

    scores = {}
    for symbol, score in quality:
        listing = by_symbol.get(symbol)
        if listing is None or listing.sid not in prices_now or score != score:
            continue
        age = float(ages[symbol])
        if age != age or age > MAX_STALENESS.days:
            continue
        scores[listing.sid] = float(score)

    ranked = sorted(scores, key=lambda sid: -scores[sid])[:KEEP]
    if not ranked:
        return
    share = 1.0 / len(ranked)
    await rebalance_to(context, data, {sid: share for sid in ranked}, tolerance=share * 0.25)
