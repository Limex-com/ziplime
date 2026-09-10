"""Replicate a single legislator's disclosed book -- Nancy Pelosi's -- as it became public.

The trades are real and so is the delay. A Periodic Transaction Report reaches the public a median
of **28 days** after the transaction it describes, and this strategy buys on the day the report was
filed, not the day the trade happened. That lag is the whole story of copying Congress: whatever
edge the original trade had, a copier gets what is left of it four weeks later.

Getting the date right took work, and the obvious choice is wrong. The dataset keys its ``trades``
table on ``notification_date``, but on a House report that is the day the **filer was notified** of
a transaction -- which, for the managed and spousal accounts most of these trades sit in, is often
the day of the trade itself. For Pelosi it equals the transaction date on 221 of 487 rows. Keying
on it would have this strategy buying a month before the disclosure existed. So ``congress.py``
joins the ``filings`` table and keys on ``filing_date``, the day the document reached the Clerk.
See that module for the numbers.

What is being replicated, and what is not
-----------------------------------------

* **Options are excluded.** 96 of her 487 rows are options, and holding shares instead of a call is
  a different position with different leverage and different risk. Silently converting one into the
  other would misstate the book rather than replicate it.
* **Sizes are bands, not numbers.** A disclosure says "$1,000,001 - $5,000,000"; the midpoint is
  used, because that is the most a band supports. There is no exact position size in this data and
  there never will be.
* **Most of it is not hers.** 410 of 487 rows are her spouse's account. "The Pelosi portfolio" is a
  convenient name for something that is mostly someone else's trading.
* **A full sale exits the name.** ``sale_full`` means the position is gone; ``sale_partial`` means
  some of it is, and how much is not disclosed, so it is treated as a proportional trim.
* **The book is concentrated, and stays that way.** Weights follow the disclosed dollars with no
  cap. A cap is a risk rule that is not in the filings, and imposing one distorts what is: capping
  a two-name book at 25% and renormalising turns a 2:1 split into 50/50.

The result is not a recommendation and not evidence. It is one book over ten years, with no control
for the fact that the names in it are large-cap technology shares that rose a great deal anyway --
which `h04` exists partly to put in perspective.
"""
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from congress import load_disclosures, mount_disclosures  # noqa: E402
from hf_config import CONGRESS_REVISION, CONGRESS_UNIVERSE  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.finance.execution import MarketOrder  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {
    "window": "congress",
    "description": "Replicate Nancy Pelosi's disclosed book on the day each report was filed",
}

LEGISLATOR = "Pelosi"
#: Trailing filings to read per name. Her book is small, so this reaches back years.
LOOKBACK = 60


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(ticker, mic=mic)
                        for ticker, mic in CONGRESS_UNIVERSE]

    disclosures = await load_disclosures(
        revision=CONGRESS_REVISION,
        row_filter=(pl.col("member_last_name").str.to_lowercase() == LEGISLATOR.lower())
                   # Shares only. A call option is not the same position as the stock.
                   & (~pl.col("is_option").fill_null(False)))
    context.pelosi = await mount_disclosures(
        disclosures, name=f"congress:{LEGISLATOR.lower()}",
        asset_service=context.asset_service,
        start_date=context.clock.start_session, end_date=context.clock.end_session,
        session_timezone=str(context.clock.trading_calendar.tz),
        fields=["amount_usd", "direction", "transaction_type", "ticker", "transaction_date"],
        revision=None)

    context.last_seen = None
    context.reported = False


async def handle_data(context: TradingAlgorithm, data: BarData):
    filed = await data.history(assets=context.universe, bar_count=LOOKBACK,
                               fields=["amount_usd", "direction", "transaction_type"],
                               data_source=context.pelosi)
    if filed.is_empty():
        return

    # Rebalance only when something new has been filed. Her book changes a handful of times a
    # year; reading it daily and re-ordering would churn commission against no new information.
    latest = filed["date"].max()
    if context.last_seen == latest:
        return
    context.last_seen = latest

    # Net disclosed dollars per name, with a full sale wiping the name out rather than netting
    # against it -- "sold all of it" is a statement about the position, not about a dollar amount.
    book: dict[int, float] = {}
    for row in filed.sort("date").iter_rows(named=True):
        sid, amount = row["sid"], row["amount_usd"] or 0.0
        if row["transaction_type"] == "sale_full":
            book[sid] = 0.0
        else:
            book[sid] = max(0.0, book.get(sid, 0.0) + amount * (row["direction"] or 0))

    held = {sid: value for sid, value in book.items() if value > 0}
    total = sum(held.values())
    if not total:
        for asset in context.universe:
            await context.order_target_percent(asset=asset, target=0.0, style=MarketOrder())
        return

    # Weights in proportion to the disclosed dollars, with no cap. A cap would be a risk overlay
    # that is not in the data, and capping then renormalising actively misstates it: with a book
    # of two names disclosed at 750k and 375k, a 25% cap sends both to the ceiling and renormalises
    # them to 50/50, promoting the smaller position and erasing the 2:1 the filings recorded. The
    # replicated book is concentrated because the original is, and the drawdown says so.
    weights = {sid: value / total for sid, value in held.items()}

    if not context.reported:
        context.reported = True
        names = {asset.sid: asset.symbol for asset in context.universe}
        print(f"{context.simulation_dt.date()} first replicated book from filings up to "
              f"{latest.date()}:")
        for sid, weight in sorted(weights.items(), key=lambda kv: -kv[1]):
            print(f"    {names.get(sid, sid):6s} {weight:6.1%}  "
                  f"({held[sid]:>12,.0f} USD disclosed, net)")

    for asset in context.universe:
        await context.order_target_percent(asset=asset, target=weights.get(asset.sid, 0.0),
                                           style=MarketOrder())
