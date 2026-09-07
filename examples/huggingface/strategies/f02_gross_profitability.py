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

import polars as pl
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from fundamentals import FIELDS, mount, rank_and_hold  # noqa: E402
from hf_config import FUNDAMENTALS_UNIVERSE  # noqa: E402
from portfolio import priced, rebalance_to  # noqa: E402

from ziplime.api import date_rules  # noqa: E402
from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "fundamentals", "description": "Hold the most gross-profitable: gross profit over total assets"}

#: Ignore a company whose freshest statement is older than this. Annual filings, so a year and a
#: half allows for a late filer without letting a company that stopped reporting in 2015 sit in
#: the book forever.
MAX_STALENESS = datetime.timedelta(days=550)
KEEP = 40
NEEDS = ['gross_profit', 'total_assets']


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in FUNDAMENTALS_UNIVERSE]
    context.source = await mount(context)
    context.reported = False
    context.schedule_function(rebalance, date_rules.quarter_start())


async def rebalance(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()

    # The same call every strategy in this directory makes, against every dataset. The source
    # was mounted knowing that its rows are revisions, so `current` means the newest known value
    # per column rather than the newest row.
    statements = await data.current(assets=context.universe, fields=FIELDS,
                                    data_source=context.source)
    if statements.is_empty():
        return
    fresh = statements.filter(pl.col("date") >= context.simulation_dt - MAX_STALENESS)
    known = {r["sid"]: r for r in fresh.iter_rows(named=True)}
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

    await rebalance_to(context, data, rank_and_hold(scores, KEEP), tolerance=0.01)
