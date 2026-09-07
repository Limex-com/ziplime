"""Buy where the chief executive or chief financial officer bought on the open market.

Not every insider knows the same things. A director attends eight board meetings a year; the CEO
and CFO see the numbers as they form. The literature has long held that purchases by the two
officers closest to the accounts are the most informative subset of Form 4 filings, and this tests
exactly that against `i01`, which counts any insider.

The filter is narrow -- 3 875 of 297 066 issuer-days in 2022 carry a CEO or CFO purchase -- so the
book is small and turns over slowly.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from hf_config import INSIDER10_UNIVERSE  # noqa: E402
from insider import mount_features
from portfolio import rebalance  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "insider10", "description": "Hold names the CEO or CFO bought on the open market"}
LOOKBACK = 30
HOLD_DAYS = 90
REBALANCE_EVERY_DAYS = 7


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in INSIDER10_UNIVERSE]
    context.source = await mount_features(context, fields=['n_ceo_buys', 'n_cfo_buys'])
    context.opened_on = {}
    context.held = frozenset()
    context.last_rebalance = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    if context.last_rebalance and (today - context.last_rebalance).days < REBALANCE_EVERY_DAYS:
        return
    context.last_rebalance = today

    window = await data.history(assets=context.universe, bar_count=LOOKBACK,
                                fields=['n_ceo_buys', 'n_cfo_buys'], data_source=context.source)
    firing = set()
    if not window.is_empty():
        firing = {r["sid"] for r in window.iter_rows(named=True)
                  if (r["n_ceo_buys"] or 0) + (r["n_cfo_buys"] or 0) > 0}

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
