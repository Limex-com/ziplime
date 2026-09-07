"""Buy where an outside director bought, and nobody in management did.

The mirror of `i02`. Directors are not employees: they sit on the board, see the same reports as
the auditors, and have no salary tied to the share price. Whether that makes their purchases more
informative than an officer's, or less, is an open question that this data can answer directly --
`n_director_buys` and `n_ceo_buys`/`n_cfo_buys` are separate columns.

Management purchases are excluded rather than ignored, so the two books do not overlap and the
comparison against `i02` is clean.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from hf_config import INSIDER10_UNIVERSE  # noqa: E402
from insider import mount_features
from portfolio import rebalance  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "insider10", "description": "Hold names an outside director bought while management did not"}
LOOKBACK = 30
HOLD_DAYS = 90
REBALANCE_EVERY_DAYS = 7


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in INSIDER10_UNIVERSE]
    context.source = await mount_features(context, fields=['n_director_buys', 'n_ceo_buys', 'n_cfo_buys'])
    context.opened_on = {}
    context.held = frozenset()
    context.last_rebalance = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    if context.last_rebalance and (today - context.last_rebalance).days < REBALANCE_EVERY_DAYS:
        return
    context.last_rebalance = today

    window = await data.history(assets=context.universe, bar_count=LOOKBACK,
                                fields=['n_director_buys', 'n_ceo_buys', 'n_cfo_buys'], data_source=context.source)
    firing = set()
    if not window.is_empty():
        firing = {r["sid"] for r in window.iter_rows(named=True)
                  if (r["n_director_buys"] or 0) > 0
                  and (r["n_ceo_buys"] or 0) + (r["n_cfo_buys"] or 0) == 0}

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
