"""Long what Congress bought, short what Congress sold -- the version that answers the question.

Modelled on the published "Congress Long-Short" strategy, and here for a specific reason. Every
long-only rule in this directory returns several hundred percent, and `h05_universe_benchmark`
shows why: the sixty names rose roughly tenfold over the window, and holding a subset of them at
any weighting captures most of that. A long-only backtest on this data mostly measures the market.

Taking the sells short removes it. The book is built to be roughly dollar-neutral -- half the
capital long the names disclosed as purchases, half short the names disclosed as sales -- so market
direction largely cancels and what is left is the difference between the two lists. If
congressional disclosures carry information about *which* stocks do better, this is where it shows
up. If they do not, this is where that shows up too, and it is worth knowing either way.

Read the result next to `h06_congress_buys`, which trades the same purchases with no short book.
The gap between them is what the market contributed.

Two caveats the numbers cannot fix:

* **Shorting is modelled as a negative position and nothing else.** No borrow fee, no recall, no
  hard-to-borrow constraint. Real short books pay for the privilege and occasionally lose it, and
  over ten years that cost is not small.
* **A sale is not a bearish view.** Members sell to raise cash, to diversify, to satisfy an ethics
  agreement, or because a fund manager they never speak to decided to. Treating every disclosed
  sale as a short signal takes a position on that interpretation, and it is a strong one.
"""
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from congress import SALES, load_disclosures, mount_disclosures  # noqa: E402
from hf_config import CONGRESS_REVISION, CONGRESS_UNIVERSE  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.finance.execution import MarketOrder  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {
    "window": "congress",
    "description": "Long the disclosed buys, short the disclosed sells -- market direction removed",
}

WINDOW = __import__("datetime").timedelta(days=60)
#: Share of capital on each side. Half and half, so the book is close to dollar-neutral.
SIDE_WEIGHT = 0.5
#: No single name on either side may exceed this share of the book.
MAX_WEIGHT = 0.10
REBALANCE_EVERY_DAYS = 7
TOLERANCE = 0.02


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(ticker, mic=mic)
                        for ticker, mic in CONGRESS_UNIVERSE]
    disclosures = await load_disclosures(
        revision=CONGRESS_REVISION,
        row_filter=pl.col("transaction_type").is_in(["purchase", *SALES])
                   & (~pl.col("is_option").fill_null(False)))
    context.flow = await mount_disclosures(
        disclosures, name="congress:flow", asset_service=context.asset_service,
        start_date=context.clock.start_session, end_date=context.clock.end_session,
        session_timezone=str(context.clock.trading_calendar.tz),
        fields=["amount_usd", "direction", "ticker"])
    context.last_rebalance = None
    context.reported = False


def side_weights(rows: list[dict], budget: float) -> dict[int, float]:
    """Weights within one side, in proportion to disclosed dollars and capped per name.

    The cap is absolute. When too few names qualify to fill the budget at that cap, the side simply
    holds less and the rest stays in cash -- it is **not** renormalised back up to the budget.
    Renormalising after capping defeats the cap: with two names on a side and a 10% ceiling, it
    scales both to 25% and the ceiling never bound at all. An earlier version of this did that and
    opened the backtest with a 25% short in Microsoft.
    """
    totals: dict[int, float] = {}
    for row in rows:
        totals[row["sid"]] = totals.get(row["sid"], 0.0) + abs(row["amount_usd"] or 0.0)
    totals = {sid: value for sid, value in totals.items() if value > 0}
    if not totals:
        return {}
    gross = sum(totals.values())
    return {sid: min(budget * value / gross, MAX_WEIGHT) for sid, value in totals.items()}


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    if context.last_rebalance and (today - context.last_rebalance).days < REBALANCE_EVERY_DAYS:
        return

    # `since=` rather than a row count: these are disclosures, not bars, so "the last sixty rows"
    # would mean sixty filings -- which for a rarely-traded name reaches back years.
    filed = await data.history(assets=context.universe, since=WINDOW,
                               fields=["amount_usd", "direction"], data_source=context.flow)
    if filed.is_empty():
        return
    context.last_rebalance = today

    recent = filed.to_dicts()
    bought = side_weights([r for r in recent if (r["direction"] or 0) > 0], SIDE_WEIGHT)
    sold = side_weights([r for r in recent if (r["direction"] or 0) < 0], SIDE_WEIGHT)
    if not bought and not sold:
        return

    # A name on both lists nets out rather than being held twice. Members disagree, and some of
    # them buy a stock in the same month others sell it.
    targets: dict[int, float] = {}
    for sid, weight in bought.items():
        targets[sid] = targets.get(sid, 0.0) + weight
    for sid, weight in sold.items():
        targets[sid] = targets.get(sid, 0.0) - weight

    if not context.reported:
        context.reported = True
        names = {asset.sid: asset.symbol for asset in context.universe}
        longs = sorted((w, s) for s, w in targets.items() if w > 0)[-3:]
        shorts = sorted((w, s) for s, w in targets.items() if w < 0)[:3]
        print(f"{today} first book: {sum(1 for w in targets.values() if w > 0)} long, "
              f"{sum(1 for w in targets.values() if w < 0)} short")
        for weight, sid in reversed(longs):
            print(f"    long  {names.get(sid, sid):6s} {weight:+6.1%}")
        for weight, sid in shorts:
            print(f"    short {names.get(sid, sid):6s} {weight:+6.1%}")

    value = context.portfolio.portfolio_value
    for asset in context.universe:
        target = targets.get(asset.sid, 0.0)
        held = await context.portfolio.get_asset_positions_value(asset)
        current = (held / value) if value else 0.0
        if abs(target - current) < TOLERANCE:
            continue
        await context.order_target_percent(asset=asset, target=target, style=MarketOrder())
