"""Building a point-in-time view of congressional trades that is actually point-in-time.

The dataset's ``trades`` table is keyed on ``notification_date``, and its README says to use that
as the point-in-time key. For a large minority of rows that is too generous, and the reason is on
the form itself: on a House Periodic Transaction Report, the notification date is the day the
**filer was told** about a transaction -- which for a managed or spousal account can be the day of
the trade. It is not the day the report reached the public.

The numbers say how much that matters. Across the whole table the filing date is later than the
notification date in **116 603 of 275 936 rows**; for Nancy Pelosi, whose trades are mostly in a
spousal account, notification equals the transaction date on 221 of 487 rows. A strategy keyed on
notification would be buying a month before the disclosure existed, and would look very good doing
it.

So this module joins ``filings``, which carries ``filing_date`` -- the day the document was filed
with the Clerk and became public -- and mounts on that. Filing lag then lands where the STOCK Act
puts it: a median of 28 days and a 90th percentile of 54.

Three filters are applied for reasons the dataset documents:

* ``source_form == "ptr"`` -- periodic transaction reports only. Annual Schedule B disclosures are
  in the same table and are filed the *following year*, so their lag is measured in hundreds of
  days and they are a different thing entirely.
* ``superseded_by IS NULL`` -- amendments resubmit whole reports, so originals and amendments
  otherwise both appear and every amended trade is counted twice. 32 274 rows are affected.
* ``ticker IS NOT NULL`` -- rows the extractor could not tie to a ticker, mostly bonds identified
  by CUSIP.
"""
import datetime

import polars as pl

from ziplime.data.data_sources.huggingface.huggingface_data_source import (
    HuggingFaceDataSource, load_table,
)

DATASET = "ZipLime/congress-trading"

#: Rows a strategy should see: a real transaction report, not superseded, tied to a ticker.
CLEAN = (
    (pl.col("source_form") == "ptr")
    & pl.col("superseded_by").is_null()
    & pl.col("ticker").is_not_null()
)

#: The dataset's transaction types that mean "bought" and "sold".
PURCHASES = ("purchase",)
SALES = ("sale_full", "sale_partial")


async def load_disclosures(revision: str | None = None,
                           row_filter: pl.Expr | None = None) -> pl.DataFrame:
    """Congressional transaction reports, keyed on the day each report was filed.

    Args:
        revision: Dataset commit to read. Pinned by the caller for a reproducible backtest.
        row_filter: An extra condition on the trades, applied on top of :data:`CLEAN`.

    Returns:
        The trades with a ``filing_date`` column joined on, and an ``amount_usd`` midpoint.
    """
    trades = await load_table(DATASET, "trades", revision=revision,
                              row_filter=CLEAN & row_filter if row_filter is not None else CLEAN)
    filings = await load_table(DATASET, "filings", revision=revision)

    joined = trades.join(filings.select(["filing_id", "filing_date"]), on="filing_id", how="left")
    return joined.filter(pl.col("filing_date").is_not_null()).with_columns(
        # The disclosed amount is a band, never a number: "$1,000,001 - $5,000,000". The midpoint
        # is the honest summary of a band, and it is the best this data will ever support --
        # there is no exact position size in a congressional disclosure and there never will be.
        ((pl.col("min_amount_usd") + pl.col("max_amount_usd")) / 2.0).alias("amount_usd"),
        pl.when(pl.col("transaction_type").is_in(PURCHASES)).then(1)
        .when(pl.col("transaction_type").is_in(SALES)).then(-1)
        .otherwise(0).alias("direction"),
    )


async def mount_disclosures(frame: pl.DataFrame, name: str, asset_service,
                            start_date: datetime.date, end_date: datetime.date,
                            session_timezone: str, fields: list[str] | None = None,
                            revision=None) -> HuggingFaceDataSource:
    """Mount a prepared disclosure frame on its **filing** date.

    The event column is named so the knowledge date is floored against it: a report cannot have
    been filed before the trade it describes, and a handful of rows in this dataset say otherwise
    because of a misread year digit.
    """
    return HuggingFaceDataSource.from_frame(
        frame=frame, name=name, knowledge_column="filing_date", entity_column="ticker",
        event_column="transaction_date", asset_service=asset_service,
        start_date=start_date, end_date=end_date, session_timezone=session_timezone,
        fields=fields, revision=revision)


async def committee_roster(committee_id: str, revision: str | None = None) -> pl.DataFrame:
    """The members of one committee, with their names.

    The roster is a snapshot, not a history: it says who sits on the committee now, not who sat on
    it in 2015. Membership does change, so a backtest that leans on it is using today's roster to
    judge the past -- worth knowing before reading anything into the result.
    """
    members = await load_table(DATASET, "committee_members", revision=revision,
                               row_filter=pl.col("committee_id") == committee_id)
    legislators = await load_table(DATASET, "legislators", revision=revision)
    return members.join(legislators, on="bioguide_id", how="left")


async def committees(revision: str | None = None) -> pl.DataFrame:
    """Every committee, so a strategy can name one and print what it is."""
    return await load_table(DATASET, "committees", revision=revision)
