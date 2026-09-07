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
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from fundamentals import FIELDS, as_of, mount, rank_and_hold  # noqa: E402
from hf_config import FUNDAMENTALS_UNIVERSE  # noqa: E402
from insider import priced, rebalance  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "fundamentals", "description": "Hold the highest earnings yield: annual net income over market value"}

#: How far back to look for the freshest filing. Annual statements, so a year and a half covers a
#: company that files late without reaching back to the one before.
WINDOW = datetime.timedelta(days=550)
KEEP = 40
REBALANCE_EVERY_DAYS = 90
NEEDS = ['net_income', 'shares_outstanding', 'shares_adjustment']


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

    await rebalance(context, data, rank_and_hold(scores, KEEP), tolerance=0.01)
