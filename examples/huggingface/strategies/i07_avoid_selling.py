"""Hold everything except what insiders have been selling.

The control with one subtraction. `i00` holds all 125 names; this holds all 125 *minus* those whose
insiders sold on the open market recently. If insider selling carries information, removing those
names should beat holding them, and the difference from `i00` is the whole measurement.

It is the cheapest possible test of the sell side, and a fairer one than shorting: it asks whether
selling is worth *avoiding*, which is a much lower bar than whether it is worth betting against.
`i08` takes the harder version.

Insiders sell for reasons that have nothing to do with the company -- a house, a divorce, a
diversification rule, a scheduled plan. The dataset cannot separate those from the informed kind,
because the 10b5-1 flag that would identify pre-scheduled trades is never populated (see the README
notes). So this necessarily treats all selling alike.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from hf_config import INSIDER10_UNIVERSE  # noqa: E402
from insider import mount_features, priced, rebalance  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "insider10",
                 "description": "Hold the whole universe except names insiders have been selling"}
LOOKBACK = 30
EXCLUDE_DAYS = 60
REBALANCE_EVERY_DAYS = 7


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in INSIDER10_UNIVERSE]
    context.source = await mount_features(context, fields=["n_open_market_sells"])
    context.excluded_on = {}
    context.held = frozenset()
    context.last_rebalance = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    if context.last_rebalance and (today - context.last_rebalance).days < REBALANCE_EVERY_DAYS:
        return
    context.last_rebalance = today

    window = await data.history(assets=context.universe, bar_count=LOOKBACK,
                                fields=["n_open_market_sells"], data_source=context.source)
    if not window.is_empty():
        for row in window.iter_rows(named=True):
            if (row["n_open_market_sells"] or 0) > 0:
                context.excluded_on[row["sid"]] = today
    context.excluded_on = {s: d for s, d in context.excluded_on.items()
                           if (today - d).days < EXCLUDE_DAYS}

    live = await priced(context, data)
    held = frozenset(sid for sid in live if sid not in context.excluded_on)
    if not held or held == context.held:
        return
    context.held = held
    weight = 1.0 / len(held)
    await rebalance(context, data, {sid: weight for sid in held})
