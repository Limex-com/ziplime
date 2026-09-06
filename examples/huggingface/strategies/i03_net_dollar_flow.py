"""Buy where insiders spent more than they took out, by dollars rather than by headcount.

`i01` and `i02` count people. This weighs money: the dataset's own `net_notional_usd`, purchases
minus sales in dollars, summed over the trailing window. One officer putting a million dollars in
is a different statement from five putting in five thousand each, and counting treats them alike.

The weakness is that dollars are missing on 31% of transaction rows -- a grant or a gift often
carries no price -- so this reads a smaller slice of the data than the count-based rules do.

The window is read with ``since=`` rather than ``bar_count=``, which on this source is the
difference between "the last thirty days" and "the last thirty days on which this issuer had a
filing". For a company whose insiders file weekly those are the same; for a quiet one the row count
reaches back a year.
"""
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from hf_config import INSIDER10_UNIVERSE  # noqa: E402
from insider import mount_features, rebalance  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "insider10", "description": "Hold names where insiders were net buyers in dollars over the trailing window"}
WINDOW = datetime.timedelta(days=30)
HOLD_DAYS = 90
REBALANCE_EVERY_DAYS = 7


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in INSIDER10_UNIVERSE]
    context.source = await mount_features(context, fields=['net_notional_usd'])
    context.opened_on = {}
    context.held = frozenset()
    context.last_rebalance = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    if context.last_rebalance and (today - context.last_rebalance).days < REBALANCE_EVERY_DAYS:
        return
    context.last_rebalance = today

    window = await data.history(assets=context.universe, since=WINDOW,
                                fields=['net_notional_usd'], data_source=context.source)
    firing = set()
    if not window.is_empty():
        totals = {}
        for r in window.iter_rows(named=True):
            totals[r["sid"]] = totals.get(r["sid"], 0.0) + float(r["net_notional_usd"] or 0.0)
        firing = {sid for sid, v in totals.items() if v > 0}

    for sid in firing:
        context.opened_on[sid] = today
    context.opened_on = {s: d for s, d in context.opened_on.items()
                         if (today - d).days < HOLD_DAYS}
    held = frozenset(context.opened_on)
    if held == context.held:
        return
    context.held = held
    weight = 1.0 / len(held) if held else 0.0
    await rebalance(context, data, {sid: weight for sid in held})
