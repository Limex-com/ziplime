"""Mounting the earnings calendar: an announcement, timed to the second, and what it said.

`ZipLime/earnings-calendar` answers a question none of the other datasets here can: **at what
instant did the market learn this company's results.** Every timestamp is read from the filing's
own SGML header, where `ACCEPTANCE-DATETIME` is unambiguous US Eastern, rather than from EDGAR's
bulk feed, whose `acceptanceDateTime` carries a `Z` and is Eastern local time for much of the
corpus -- 94% of it before 2021. Nothing in the value distinguishes the two, so getting it right
cost one request per release. On the 12 540 releases these examples read, 253 426 of the 258 482
rows in the dataset carry a non-zero seconds component and none is flagged estimated.

**Read the name carefully: this is not a forward calendar.** A row becomes visible when the 8-K
was accepted, which is when the announcement happened. Nothing here says that a company *will*
report next Tuesday, so a strategy that wants to be out of a name before its results has to
predict the date, and `e03` does exactly that and says so. What the dataset gives is the past,
placed on the clock to the second.

## What is actually checkable here

* **89.2% of releases in this universe land outside market hours** -- 6 914 after the close,
  4 567 pre-market, 1 059 during the session. That distribution *is* the evidence the timestamps
  are right; a broken clock would smear releases uniformly across the day.
* **`event_date` is the period end, not the announcement.** Median 34 days before the release,
  never after the knowledge date in any of the 258 482 rows. So the adapter's habit of flooring a
  knowledge date against its own event date -- which exists because congress `trades` contains
  rows dated a thousand years out -- moves nothing here.
* **A year-on-year change divides by the absolute value of the base.** A company going from
  -0.67 to +0.20 a share reads **+1.30**, not -1.30. That is the correct handling and it is not
  the obvious one: the naive formula flips the sign on exactly the 25.6% of rows whose year-ago
  figure is negative, and would have this suite systematically shorting every recovery.

## The one thing to guard against

18 rows carry `|eps_yoy_change| > 1000`, and the cause is not a small denominator -- only 8 rows
in the whole dataset have a year-ago EPS under two cents. It is the numerator: `eps_diluted` of
3 550 000, of 2 920 000, of -590 000. Those are not per-share figures, they are the totals, tagged
into the EPS field by the filer and carried through the concept map in `company-fundamentals`.

So the strategies here rank on **revenue** growth rather than EPS growth by default, and every one
that touches a per-share figure discards absurd values explicitly. Ranking alone is not protection:
a rank puts the wrong number at the top of the book rather than in the middle of it.
"""
import datetime
import sys
from pathlib import Path

import polars as pl
from huggingface_hub import HfApi, snapshot_download

sys.path.insert(0, str(Path(__file__).parent))
from hf_config import CIK_TO_TICKER, EARNINGS_DATASET  # noqa: E402

from ziplime.data.data_sources.huggingface import hub  # noqa: E402
from ziplime.data.data_sources.huggingface.huggingface_data_source import (  # noqa: E402
    HuggingFaceDataSource, Resolution,
)
from ziplime.data.data_sources.huggingface.manifest import parse_manifest  # noqa: E402

#: Columns a strategy may read. `session` says where in the trading day the release landed, which
#: is the column this dataset exists for.
FIELDS = ["session", "fiscal_period", "eps_diluted", "eps_yoy_change",
          "revenue", "revenue_yoy_change", "net_income"]

#: A diluted EPS beyond this is a tagging error, not a company. See the module docstring: the
#: largest in the corpus is 3 550 000 a share. Applied to the figure, not to the growth rate, so
#: a genuine tenfold recovery from a small base survives and a mis-tagged total does not.
MAX_PLAUSIBLE_EPS = 1_000.0


def load_releases(revision: str | None = None) -> pl.DataFrame:
    """Every earnings release for the mapped universe, one row per announcement.

    Paths come from ``manifest.json`` rather than a glob written here -- the sibling datasets have
    re-partitioned three times while these examples were being written, and each time a hard-coded
    path stopped finding anything. The manifest is the only part of the layout that is promised.
    """
    sha = HfApi().dataset_info(EARNINGS_DATASET, revision=revision).sha
    pinned = hub.resolve_revision(EARNINGS_DATASET, revision=sha)
    manifest = parse_manifest(
        repo_id=EARNINGS_DATASET, revision=sha,
        manifest_json=hub.read_text(pinned, "manifest.json"),
        readme=hub.read_text(pinned, "README.md"))
    spec = manifest.config("pit")

    prefix = spec.paths[0].split("**")[0].split("*")[0].rstrip("/")
    local = snapshot_download(EARNINGS_DATASET, repo_type="dataset", revision=sha,
                              allow_patterns=[f"{prefix}/**"])
    # `spec.select` rather than a glob: this config is published as a Delta table, whose
    # transaction log is Parquet too and is not data.
    parts = [Path(local) / path for path in
             spec.select(str(p.relative_to(local)) for p in Path(local).rglob("*.parquet"))]
    if not parts:
        raise SystemExit(f"No Parquet under {prefix} at {sha[:8]}; the layout changed again.")

    return (
        pl.scan_parquet(parts)
        .select(["entity_id", "knowledge_date", "event_date", *FIELDS])
        .collect()
        .with_columns(pl.col("entity_id")
                      .replace_strict(CIK_TO_TICKER, default=None)
                      .alias("ticker"))
        .filter(pl.col("ticker").is_not_null())
        # A mis-tagged total in the EPS field, dropped rather than ranked. Both the figure and the
        # growth computed from it go, because the growth inherits the error.
        .with_columns(
            pl.when(pl.col("eps_diluted").abs() <= MAX_PLAUSIBLE_EPS)
              .then(pl.col("eps_diluted")).otherwise(None).alias("eps_diluted"),
            pl.when(pl.col("eps_diluted").abs() <= MAX_PLAUSIBLE_EPS)
              .then(pl.col("eps_yoy_change")).otherwise(None).alias("eps_yoy_change"))
        .sort("knowledge_date")
    )


async def mount(context, releases: pl.DataFrame | None = None,
                name: str = "earnings") -> HuggingFaceDataSource:
    """Mount the releases on their acceptance instant.

    ``Resolution.LATEST_ROW``, not ``COALESCE``: each row is a separate announcement rather than a
    revision of one, so the newest release is the answer and carrying a figure forward from the
    previous quarter would invent a report that was never made.
    """
    frame = releases if releases is not None else load_releases()
    return HuggingFaceDataSource.from_frame(
        frame=frame, name=name, knowledge_column="knowledge_date", entity_column="ticker",
        event_column=None, asset_service=context.asset_service,
        start_date=context.clock.start_session, end_date=context.clock.end_session,
        session_timezone=str(context.clock.trading_calendar.tz),
        fields=FIELDS, resolution=Resolution.LATEST_ROW)


def next_expected(last_release: datetime.date, quarter_days: int = 91) -> datetime.date:
    """When a company that last reported on ``last_release`` is likely to report again.

    A prediction, not a fact. The dataset records announcements as they happened and says nothing
    about scheduled dates, so a strategy that wants to be flat into results has to guess. US
    issuers report on a quarterly cycle and the median gap in this universe is 91 days, which is
    what this uses -- and a guess with a median error of about a week is why `e03` steps out for a
    window rather than for a day.
    """
    return last_release + datetime.timedelta(days=quarter_days)


async def growers(context, data, session: str | None = None) -> set[int]:
    """Instruments whose results, published since the last look, grew revenue on the year.

    Reads from the previous call rather than a fixed window, so a release is acted on once and
    none falls between two checks. ``session`` restricts to releases that landed in one part of
    the trading day -- ``pre_market``, ``after_close`` or ``market_hours`` -- which is the split
    this dataset exists to make possible.
    """
    now = context.simulation_dt
    since = ((now - context.last_look) if getattr(context, "last_look", None)
             else datetime.timedelta(days=7))
    context.last_look = now

    rows = await data.history(assets=context.universe, since=since,
                              fields=["revenue_yoy_change", "session"],
                              data_source=context.source)
    return {row["sid"] for row in rows.iter_rows(named=True)
            if (row["revenue_yoy_change"] or 0.0) > 0.0
            and (session is None or row["session"] == session)}
