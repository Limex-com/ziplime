"""Avoid companies whose earnings are not cash: the accruals anomaly.

Sloan's result. Earnings are the sum of cash the business collected and accruals -- revenue booked
but not yet received, costs deferred, inventory capitalised. The cash half persists into next
year; the accrual half tends not to, and the market prices earnings as though the two were alike.

So this ranks by *low* accruals: net income minus operating cash flow, over total assets, held
with the sign inverted. A company earning what it collects ranks above one earning what it has
invoiced.

It needs all three of net income, operating cash flow and total assets from the same statement,
which makes it the strictest test in the suite of whether the as-of read is right. With the row-
picking read most companies drop out for want of a balance sheet.
"""
import datetime
import sys

import polars as pl
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from fundamentals import FIELDS, mount, rank_and_hold  # noqa: E402
from hf_config import FUNDAMENTALS_UNIVERSE  # noqa: E402
from portfolio import priced, rebalance  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "fundamentals", "description": "Hold companies whose earnings are cash rather than accruals"}

#: Ignore a company whose freshest statement is older than this. Annual filings, so a year and a
#: half allows for a late filer without letting a company that stopped reporting in 2015 sit in
#: the book forever.
MAX_STALENESS = datetime.timedelta(days=550)
KEEP = 40
REBALANCE_EVERY_DAYS = 90
NEEDS = ['net_income', 'operating_cash_flow', 'total_assets']


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
        accruals = (f["net_income"] - f["operating_cash_flow"]) / f["total_assets"]
        scores[sid] = -accruals

    if not context.reported and scores:
        context.reported = True
        names = {a.sid: a.symbol for a in context.universe}
        top = sorted(scores.items(), key=lambda kv: -kv[1])[:5]
        print(f"{today} {len(known)} companies with a filing, {len(scores)} scored; top:")
        for sid, s in top:
            print(f"    {names.get(sid, sid):6s} {s:>10.4f}")

    await rebalance(context, data, rank_and_hold(scores, KEEP), tolerance=0.01)
