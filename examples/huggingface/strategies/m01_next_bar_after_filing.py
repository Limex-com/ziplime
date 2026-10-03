"""React to a Form 4 in the first bar its acceptance timestamp allows, on one-minute data.

Every other strategy here runs on daily bars, and every one of them reads a dataset whose
knowledge times are recorded to the second. That precision buys nothing at daily resolution: a
filing accepted at 16:54 on Tuesday and one accepted at 09:31 are the same row to a simulation
that has one bar per session.

This one has 390 bars a session, so the question becomes answerable. The rule is deliberately
plain -- hold the universe, drop a name the first bar after an insider sale of it becomes public,
put it back a session later -- because the rule is not the point. The point is *when* the order
can be placed, and the answer is not the one a daily backtest implies.

**Of the 38 distinct instants at which a Form 4 on these names became public over the three
sessions this runs on, none fell inside market hours.** Not one. Across all 309 011 Form 4
point-in-time rows filed in 2026, 83.6% arrive after the close and the median is 16:54; only 12.8%
land while the market is trading. So for five filings in six the earliest honest fill is the next
session's open -- which is what this prints, filing instant beside fill bar, and what a daily
simulation has no way to express.

Three sessions of one-minute bars is not a backtest and the return means nothing; Yahoo serves
about a week of one-minute history and deletes the rest, so the window moves and the number will
not reproduce. What reproduces is the timing.
"""
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from intraday import INTRADAY_UNIVERSE  # noqa: E402
from playbook import equities, hold  # noqa: E402

from ziplime.api import date_rules, time_rules  # noqa: E402
from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "intraday-1m",
                 "description": "Drop a name the first minute an insider sale of it is public"}

DATASET = "ZipLime/insider-trading"
#: How far back to look on the very first bar, before there is a previous look to measure from.
FIRST_LOOK = datetime.timedelta(days=1)
#: How long a name stays out of the book after a disclosed sale.
BLOCK_FOR = datetime.timedelta(days=1)
#: Form 4 code for an open-market sale. `A` is a grant and `F` is tax withholding -- neither is
#: somebody choosing to sell, and treating them alike is how this signal gets diluted.
SALE = "S"
#: Filings to print before falling silent. Enough to read the pattern, not enough to bury it.
SHOW = 12


async def initialize(context: TradingAlgorithm):
    context.universe = await equities(context, INTRADAY_UNIVERSE)
    context.source = await context.huggingface_dataset(
        DATASET, config="pit", fields=["ticker", "transaction_code"], name="insider:pit")
    context.blocked = {}
    context.shown = 0
    context.reactions = 0
    context.seen = set()
    context.last_look = None
    context.schedule_function(react, date_rules.every_day(), time_rules.every_minute())


async def react(context: TradingAlgorithm, data: BarData):
    now = context.simulation_dt
    # Everything published since the algorithm last looked -- **not** one bar back. A fixed
    # one-minute window looks sound and silently discards five filings in six: 16:00 to 09:31 is
    # eighteen hours with no bar in it, and that is where most Form 4s arrive. At the first bar of
    # a session this window reaches back over the whole overnight gap, which is the point.
    since = (now - context.last_look) if context.last_look else FIRST_LOOK
    context.last_look = now
    fresh = await data.history(assets=context.universe, since=since,
                               fields=["transaction_code"], data_source=context.source)
    names = {asset.sid: asset.symbol for asset in context.universe}

    for row in fresh.iter_rows(named=True):
        if row["transaction_code"] != SALE:
            continue
        context.blocked[row["sid"]] = now
        # One Form 4 is many rows -- a filing reports each transaction separately, and ten sales
        # by one officer arrive as ten rows sharing an acceptance timestamp. The report is about
        # filings, so it counts instants rather than rows.
        filing = (row["sid"], row["date"])
        if filing in context.seen:
            continue
        context.seen.add(filing)
        context.reactions += 1
        if context.shown < SHOW:
            context.shown += 1
            lag = now - row["date"]
            print(f"    {names.get(row['sid'], row['sid']):6s} filed {row['date']:%m-%d %H:%M:%S}"
                  f" -> acted {now:%m-%d %H:%M}  ({lag.total_seconds() / 3600:5.1f}h later)")

    context.blocked = {sid: at for sid, at in context.blocked.items() if now - at < BLOCK_FOR}
    await hold(context, data, [sid for sid in names if sid not in context.blocked])
