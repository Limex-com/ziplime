"""Follow a committee, but test the claim the data actually supports.

The folk story about congressional trading is jurisdictional: a member of the committee that
oversees an industry knows things about it first, so watch what they buy in their own sector. It is
a good story. This dataset does not support it.

Sort the purchases disclosed by members of the House Armed Services Committee, and the names at the
top are Amazon, Pfizer, Alphabet, AT&T, Microsoft, Bank of America and Disney -- not a defence
contractor among them. Energy and Commerce members buy Adobe and VMware; Financial Services members
buy Microsoft and Nvidia. Every committee's list is the same list of large-cap technology shares
that every retail brokerage account holds. Whatever these filings show, it is not members trading
their own jurisdiction.

So the strategy tests something the data can actually answer: **consensus**. When several distinct
members of the same committee are disclosed buying the same name inside a short window, that is a
different and much rarer event than one member doing it -- 58 such days at a three-member threshold
against a thousand at two, on this committee. It is the same reasoning behind cluster-buying in
insider filings: one person buys for many reasons, several people buying at once is harder to
explain away.

What weakens it, stated plainly
-------------------------------

* **The roster is a snapshot.** ``committee_members`` says who sits on the committee *now*, not who
  sat on it in 2016. Membership turns over, so the early years judge past trades by present
  membership. This is look-ahead of a kind the point-in-time machinery cannot fix, because the
  dataset does not carry a membership history.
* **Consensus may just be the market.** If five members buy Microsoft in the same month, the
  simplest explanation is that Microsoft is what everyone was buying that month, committee or not.
  Nothing here controls for that, and a serious study would compare against non-members.
* **Everything arrives late.** As in `h03`, positions are opened on the day the report was filed --
  a median of 28 days after the trade.

The result is a demonstration of the mechanism and of the join, not evidence about the signal.
"""
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from congress import committee_roster, committees, load_disclosures, mount_disclosures  # noqa: E402
from hf_config import COMMITTEE_ID, CONGRESS_REVISION, CONGRESS_UNIVERSE  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.finance.execution import MarketOrder  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {
    "window": "congress",
    "description": "Hold names several members of one committee were disclosed buying at once",
}

#: Distinct committee members who must be disclosed buying a name before it is held.
CONSENSUS = 2
#: Trailing filings read per name. Consensus is judged inside this window.
LOOKBACK = 40
#: How long a name is held after the consensus that put it there, in calendar days.
HOLD_DAYS = 90


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(ticker, mic=mic)
                        for ticker, mic in CONGRESS_UNIVERSE]

    roster = await committee_roster(COMMITTEE_ID, revision=CONGRESS_REVISION)
    members = [m for m in roster["bioguide_id"].to_list() if m]
    all_committees = await committees(revision=CONGRESS_REVISION)
    named = all_committees.filter(pl.col("committee_id") == COMMITTEE_ID)
    context.committee_name = named["name"][0] if len(named) else COMMITTEE_ID

    purchases = await load_disclosures(
        revision=CONGRESS_REVISION,
        row_filter=pl.col("bioguide_id").is_in(members)
                   & (pl.col("transaction_type") == "purchase")
                   & (~pl.col("is_option").fill_null(False)))
    context.committee = await mount_disclosures(
        purchases, name=f"congress:committee:{COMMITTEE_ID}",
        asset_service=context.asset_service,
        start_date=context.clock.start_session, end_date=context.clock.end_session,
        session_timezone=str(context.clock.trading_calendar.tz),
        fields=["bioguide_id", "member_name", "ticker", "amount_usd"])

    print(f"Following {context.committee_name} -- {len(members)} members on the current roster, "
          f"{len(purchases):,} disclosed share purchases")
    context.opened_on: dict[int, object] = {}
    context.currently_held: frozenset[int] = frozenset()
    context.reported = False


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    filed = await data.history(assets=context.universe, bar_count=LOOKBACK,
                               fields=["bioguide_id", "member_name", "amount_usd"],
                               data_source=context.committee)

    agreed: set[int] = set()
    if not filed.is_empty():
        # Distinct members per name inside the window. Counting rows instead would let one member
        # filing three purchases of the same stock look like three people agreeing.
        by_name = filed.group_by("sid").agg(pl.col("bioguide_id").n_unique().alias("members"))
        agreed = {row["sid"] for row in by_name.iter_rows(named=True)
                  if row["members"] >= CONSENSUS}

    for sid in agreed:
        context.opened_on.setdefault(sid, today)
    # Let a name go once its holding period is up, rather than holding while the window still
    # happens to contain the old filings.
    context.opened_on = {sid: opened for sid, opened in context.opened_on.items()
                         if (today - opened).days < HOLD_DAYS or sid in agreed}

    held = set(context.opened_on)
    if not context.reported and held:
        context.reported = True
        names = {asset.sid: asset.symbol for asset in context.universe}
        detail = filed.filter(pl.col("sid").is_in(list(held)))
        print(f"{today} first consensus in {context.committee_name}:")
        for sid in sorted(held):
            who = detail.filter(pl.col("sid") == sid)["member_name"].unique().to_list()
            print(f"    {names.get(sid, sid):6s} bought by {len(who)}: {', '.join(sorted(who))}")

    # Trade only when the set of names changes. Re-issuing targets every session would rebalance
    # against nothing but the portfolio's own drift: an earlier version of this did exactly that
    # and paid commission on 18 061 trades over the same 2 680 sessions.
    if frozenset(held) == context.currently_held:
        return
    context.currently_held = frozenset(held)

    weight = 1.0 / len(held) if held else 0.0
    for asset in context.universe:
        await context.order_target_percent(
            asset=asset, target=weight if asset.sid in held else 0.0, style=MarketOrder())
