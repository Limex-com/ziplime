"""Buy the cheapest companies by earnings yield -- net income over market value.

The oldest anomaly there is, and the one every fundamentals dataset gets tested on first. Earnings
yield inverts the price-to-earnings ratio so that a company with no earnings ranks last rather than
producing an infinity, which is why it is the form used in practice.

Market value is computed here rather than looked up: `shares_outstanding` from the filing times
today's price. That is deliberate. A vendor's market-cap field is restated when share counts are
corrected, so it carries information the date it claims to describe did not have; a share count
taken from a filing that was public, multiplied by a price that was quoted, does not.

It also has to be corrected for splits, and nothing in the dataset says so. A filing reports the
shares that existed then; the price series is back-adjusted so that every historical price is in
today's share basis. Multiply the two directly and any company that has split since is valued wrong
by exactly the split factor -- Deckers came out at an earnings yield of 41.7%, a P/E of 1.8, before
this was fixed. Ratios built only from filings do not have this problem; only the ones that meet a
price do.

The earnings are annual and as reported, never restated -- which for this corpus is the difference
between a backtest and a description of the present.
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

STRATEGY_INFO = {"window": "fundamentals", "description": "Hold the highest earnings yield: annual net income over market value"}

#: Ignore a company whose freshest statement is older than this. Annual filings, so a year and a
#: half allows for a late filer without letting a company that stopped reporting in 2015 sit in
#: the book forever.
MAX_STALENESS = datetime.timedelta(days=550)
KEEP = 40
NEEDS = ['net_income', 'shares_outstanding', 'shares_adjustment']


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
        # The share count as filed, moved onto the price series' own basis -- see
        # fundamentals._split_factors for why that is not optional.
        shares = f["shares_outstanding"] * f["shares_adjustment"]
        market_value = shares * prices[sid]
        if market_value <= 0:
            continue
        scores[sid] = f["net_income"] / market_value

    if not context.reported and scores:
        context.reported = True
        names = {a.sid: a.symbol for a in context.universe}
        top = sorted(scores.items(), key=lambda kv: -kv[1])[:5]
        print(f"{today} {len(known)} companies with a filing, {len(scores)} scored; top:")
        for sid, s in top:
            print(f"    {names.get(sid, sid):6s} {s:>10.4f}")

    await rebalance_to(context, data, rank_and_hold(scores, KEEP), tolerance=0.01)
