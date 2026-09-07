"""Buy on gross profitability: gross profit over total assets.

Novy-Marx's measure, and the argument behind it is about where accounting noise lives. Net income
has been through depreciation policy, tax planning, restructuring charges and every other place a
management team can express a preference. Gross profit -- revenue less the cost of the goods --
has been through almost none of that, so as a measure of how good a business is it is cleaner even
though it is further from what shareholders receive.

Scaling by assets rather than by equity avoids ranking leverage: a company can raise return on
equity by borrowing without becoming better at anything.

This is the strategy most exposed to the dataset's biggest reading trap. `total_assets` is the
field the naive as-of read destroys -- 78% of its values disappear if you keep the newest row per
report instead of the newest value per column, because a later filing repeats the income statement
and not the balance sheet.
"""
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from fundamentals import FIELDS, as_of, mount, rank_and_hold  # noqa: E402
from hf_config import FUNDAMENTALS_UNIVERSE  # noqa: E402
from insider import priced, rebalance  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "fundamentals", "description": "Hold the most gross-profitable: gross profit over total assets"}

#: How far back to look for the freshest filing. Annual statements, so a year and a half covers a
#: company that files late without reaching back to the one before.
WINDOW = datetime.timedelta(days=550)
KEEP = 40
REBALANCE_EVERY_DAYS = 90
NEEDS = ['gross_profit', 'total_assets']


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in FUNDAMENTALS_UNIVERSE]
    context.source = await mount(context)
    context.last_rebalance = None
    context.reported = False


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    if context.last_rebalance and (today - context.last_rebalance).days < REBALANCE_EVERY_DAYS:
        return
    context.last_rebalance = today

    window = await data.history(assets=context.universe, since=WINDOW,
                                fields=FIELDS, data_source=context.source)
    known = as_of(window, FIELDS)
    if not known:
        return
    prices = await priced(context, data)

    scores = {}
    for sid, f in known.items():
        if sid not in prices or any(f.get(n) is None for n in NEEDS):
            continue
        if f["total_assets"] <= 0:
            continue
        scores[sid] = f["gross_profit"] / f["total_assets"]

    if not context.reported and scores:
        context.reported = True
        names = {a.sid: a.symbol for a in context.universe}
        top = sorted(scores.items(), key=lambda kv: -kv[1])[:5]
        print(f"{today} {len(known)} companies with a filing, {len(scores)} scored; top:")
        for sid, s in top:
            print(f"    {names.get(sid, sid):6s} {s:>10.4f}")

    await rebalance(context, data, rank_and_hold(scores, KEEP), tolerance=0.01)
