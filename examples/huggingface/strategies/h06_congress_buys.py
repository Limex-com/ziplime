"""Hold what Congress bought, weighted by how much they said they bought.

Modelled on the published "Congress Buys" strategy at quiverquant.com/strategies: the universe is
names members of Congress or their families were disclosed purchasing, positions are weighted by
the reported size of those purchases, and the book is rebalanced weekly.

Two details of that design are worth copying and are implemented here:

* **The window stretches until the book is diversified enough.** A fixed lookback produces a
  one-name portfolio in a quiet fortnight and a forty-name one after a busy month. Instead the
  window grows until it holds at least ``MINIMUM_NAMES`` distinct stocks, so the book has a floor
  on its diversification without a ceiling on its responsiveness.
* **A cap on any single name.** Disclosed amounts are bands running to "over $50,000,000", and one
  such row would otherwise take the whole portfolio.

What is *not* copied is the start date. The published backtest begins on 1 April 2020, eight
trading days after the COVID low; any long equity book started there quintuples, and a strategy's
own contribution is invisible inside that. This one starts in 2016 and is reported against
`h05_universe_benchmark`, which holds the same sixty names and reads nothing.
"""
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from congress import load_disclosures, mount_disclosures  # noqa: E402
from hf_config import CONGRESS_REVISION, CONGRESS_UNIVERSE  # noqa: E402

from ziplime.api import date_rules  # noqa: E402
from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.finance.execution import MarketOrder  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {
    "window": "congress",
    "description": "Hold what Congress bought, weighted by the disclosed size, rebalanced weekly",
}

#: Filings read per name. The effective window is set by MINIMUM_NAMES, not by this.
LOOKBACK = 80
#: Grow the window until the book holds at least this many distinct names.
MINIMUM_NAMES = 10
#: Most recent filings to consider at the tightest. Extended when too few names qualify.
BASE_WINDOW_DAYS = 30
MAX_WINDOW_DAYS = 365
#: No single name may exceed this share of the book.
MAX_WEIGHT = 0.20
#: Leave a position alone while it is within this much of its target. Without a band, a weekly
#: rebalance re-trades all sixty names every week against nothing but price drift: an earlier
#: version of this did exactly that and paid commission on 20 719 trades, against the 3 287 the
#: published strategy reports over a shorter window.
TOLERANCE = 0.02


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(ticker, mic=mic)
                        for ticker, mic in CONGRESS_UNIVERSE]
    purchases = await load_disclosures(
        revision=CONGRESS_REVISION,
        row_filter=(pl.col("transaction_type") == "purchase")
                   & (~pl.col("is_option").fill_null(False)))
    context.buys = await mount_disclosures(
        purchases, name="congress:buys", asset_service=context.asset_service,
        start_date=context.clock.start_session, end_date=context.clock.end_session,
        session_timezone=str(context.clock.trading_calendar.tz),
        fields=["amount_usd", "ticker"])
    context.reported = False
    context.schedule_function(rebalance, date_rules.week_start())


def weights_from(filed: pl.DataFrame, today) -> dict[int, float]:
    """Disclosed dollars per name over the shortest window holding enough names."""
    window = BASE_WINDOW_DAYS
    while window <= MAX_WINDOW_DAYS:
        cutoff = today - __import__("datetime").timedelta(days=window)
        recent = filed.filter(pl.col("date").dt.date() >= cutoff)
        if recent["sid"].n_unique() >= MINIMUM_NAMES:
            break
        window *= 2
    else:
        recent = filed

    totals = {row["sid"]: row["amount_usd"] or 0.0
              for row in recent.group_by("sid").sum().iter_rows(named=True)}
    totals = {sid: value for sid, value in totals.items() if value > 0}
    if not totals:
        return {}

    # Cap, then give the excess to the names still under the cap, repeatedly. Capping and
    # renormalising in one pass would inflate every remaining name past the cap it just enforced.
    weights = {sid: value / sum(totals.values()) for sid, value in totals.items()}
    for _ in range(len(weights)):
        over = {sid for sid, weight in weights.items() if weight > MAX_WEIGHT + 1e-12}
        if not over:
            break
        spare = sum(weights[sid] - MAX_WEIGHT for sid in over)
        under = [sid for sid in weights if sid not in over]
        if not under:
            return {sid: 1.0 / len(weights) for sid in weights}
        room = sum(weights[sid] for sid in under)
        for sid in over:
            weights[sid] = MAX_WEIGHT
        for sid in under:
            weights[sid] += spare * (weights[sid] / room if room else 1.0 / len(under))
    return weights


async def rebalance(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()

    filed = await data.history(assets=context.universe, bar_count=LOOKBACK,
                               fields=["amount_usd"], data_source=context.buys)
    if filed.is_empty():
        return

    weights = weights_from(filed, today)
    if not weights:
        return

    if not context.reported:
        context.reported = True
        names = {asset.sid: asset.symbol for asset in context.universe}
        top = sorted(weights.items(), key=lambda kv: -kv[1])[:8]
        print(f"{today} first book, {len(weights)} names, largest:")
        for sid, weight in top:
            print(f"    {names.get(sid, sid):6s} {weight:6.1%}")

    value = context.portfolio.portfolio_value
    for asset in context.universe:
        target = weights.get(asset.sid, 0.0)
        held = await context.portfolio.get_asset_positions_value(asset)
        current = (held / value) if value else 0.0
        if abs(target - current) < TOLERANCE:
            continue
        await context.order_target_percent(asset=asset, target=target, style=MarketOrder())
