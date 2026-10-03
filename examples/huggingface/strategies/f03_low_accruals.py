"""Avoid companies whose earnings are not cash: the accruals anomaly.

Sloan's result. Earnings are the sum of cash the business collected and accruals -- revenue booked
but not yet received, costs deferred, inventory capitalised. The cash half persists into next
year; the accrual half tends not to, and the market prices earnings as though the two were alike.

So this ranks by *low* accruals: net income minus operating cash flow, over total assets, with the
sign inverted. A company earning what it collects ranks above one earning what it has invoiced.

It needs all three figures from the same statement, which makes it the strictest test in the suite
of whether the point-in-time read is right. `data.current` on this source returns the newest known
value per column rather than the newest row -- without that, most companies drop out for want of a
balance sheet, because a filing that restates a period repeats the income statement and not the
balance sheet.
"""
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from fundamentals import FIELDS, mount  # noqa: E402
from hf_config import FUNDAMENTALS_UNIVERSE  # noqa: E402
from playbook import equities, factor, fresh, hold_top, show_once  # noqa: E402

from ziplime.api import date_rules  # noqa: E402
from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "fundamentals",
                 "description": "Hold companies whose earnings are cash rather than accruals"}

#: Ignore a company whose freshest statement is older than this. Annual filings, so eighteen
#: months allows for a late filer without letting one that stopped reporting sit in the book.
MAX_AGE = datetime.timedelta(days=550)
KEEP = 40


async def initialize(context: TradingAlgorithm):
    context.universe = await equities(context, FUNDAMENTALS_UNIVERSE)
    context.source = await mount(context)
    context.schedule_function(rebalance, date_rules.quarter_start())


async def rebalance(context: TradingAlgorithm, data: BarData):
    rows = await data.current(assets=context.universe, fields=FIELDS,
                              data_source=context.source)
    scores = factor(
        fresh(rows, context, MAX_AGE),
        lambda r: -(r["net_income"] - r["operating_cash_flow"]) / r["total_assets"])

    held = await hold_top(context, data, scores, keep=KEEP)
    names = {a.sid: a.symbol for a in context.universe}
    show_once(context, f"{len(scores)} companies scored, holding {len(held)}:",
              (f"{names.get(sid, sid):6s} {scores[sid]:>8.4f}"
               for sid in sorted(held, key=lambda s: -scores[s])[:5]))
