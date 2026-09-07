"""Buy where the purchase is large *for this company*, not large in dollars.

A hundred thousand dollars is a rounding error at one issuer and the largest insider purchase in a
decade at another. Ranking raw dollars therefore ranks company size, which is a factor this suite
already has three ways of expressing. Normalising by the name's own history asks a different
question: is this buy unusual for the people who buy here?

The comparison is the trailing 30-day purchase total against the median of that same series over
the window. A name whose insiders bought several times its usual amount qualifies; a name where a
large buy is ordinary does not.

This is the only rule in the suite that standardises before ranking, which is the normal first step
in building a cross-sectional factor and is worth having one example of.
"""
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from hf_config import INSIDER10_UNIVERSE  # noqa: E402
from insider import mount_features
from portfolio import rebalance_to  # noqa: E402

from ziplime.api import date_rules  # noqa: E402
from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "insider10",
                 "description": "Hold names whose insider buying is large against their own history"}
LOOKBACK = 120
HOLD_DAYS = 90
#: How many times its own typical level the trailing purchase total must reach.
MULTIPLE = 3.0


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in INSIDER10_UNIVERSE]
    context.source = await mount_features(context, fields=["buy_notional_usd_30d"])
    context.opened_on = {}
    context.held = frozenset()
    context.schedule_function(rebalance, date_rules.week_start())


async def rebalance(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()

    window = await data.history(assets=context.universe, bar_count=LOOKBACK,
                                fields=["buy_notional_usd_30d"], data_source=context.source)
    firing = set()
    if not window.is_empty():
        series: dict[int, list[float]] = {}
        for row in window.sort("date").iter_rows(named=True):
            series.setdefault(row["sid"], []).append(float(row["buy_notional_usd_30d"] or 0.0))
        for sid, values in series.items():
            positive = [v for v in values if v > 0]
            # A name needs a history to be unusual against; a handful of points is not one.
            if len(positive) < 5 or values[-1] <= 0:
                continue
            typical = statistics.median(positive)
            if typical > 0 and values[-1] >= MULTIPLE * typical:
                firing.add(sid)

    for sid in firing:
        context.opened_on[sid] = today
    context.opened_on = {s: d for s, d in context.opened_on.items()
                         if (today - d).days < HOLD_DAYS}
    held = frozenset(context.opened_on)
    if held == context.held:
        return
    context.held = held
    weight = 1.0 / len(held) if held else 0.0
    await rebalance_to(context, data, {sid: weight for sid in held})
