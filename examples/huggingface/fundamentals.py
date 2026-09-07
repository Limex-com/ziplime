"""Mounting SEC XBRL fundamentals, and reading them as of a date without losing half the numbers.

`ZipLime/company-fundamentals` is the strongest of the ZipLime datasets on point-in-time
discipline: it keeps what a filing *said* rather than what the figure was later restated to, and
its own README measures why that matters -- 86% of companies have restated at least one figure by
more than 1%. Backtesting on today's restated numbers trades on information that did not exist.

Two things have to be handled before a strategy can read it, and both are structural rather than
defects.

**There is no ticker.** The identifier is the issuer CIK, and the README is explicit that a
CIK-to-ticker map is a separate problem with its own look-ahead trap. `hf_config.CIK_TO_TICKER`
builds one from the insider-trading dataset, which carries both keys; see the note there for the
bias that introduces.

**A later revision is not a better row.** This is the part that costs data. A period is reported
more than once -- the original filing, then a comparative column in next year's filing -- and the
later report carries *fewer* line items, because a 10-K states three years of income statement and
only two balance sheets. So the obvious as-of read, keeping the newest row per report, throws away
everything the newest row happens not to repeat.

Measured on `period_year=2022` as of 2026-01-01, against the README's own recipe:

    total_assets          10 376 values kept, 47 314 available   (+356%)
    eps_diluted           27 472 kept                            (+20.5%)
    revenue               28 689 kept                            (+19.6%)
    operating_cash_flow   24 184 kept                            (+17.1%)

:func:`as_of` therefore coalesces **field by field** across the revisions visible at the simulation
time -- newest non-null wins per column, not per row. It is still strictly point-in-time: only rows
the engine has already filtered by ``knowledge_date`` reach it.
"""
import datetime
import sys
from pathlib import Path

import polars as pl
from huggingface_hub import HfApi, snapshot_download

sys.path.insert(0, str(Path(__file__).parent))
from hf_config import (  # noqa: E402
    CIK_TO_TICKER, FUNDAMENTALS_DATASET, FUNDAMENTALS_END, FUNDAMENTALS_START,
)

from ziplime.data.data_sources.huggingface.huggingface_data_source import (  # noqa: E402
    HuggingFaceDataSource,
)

#: Statement kinds a strategy may mix. Annual flows and the balance sheets that go with them.
#:
#: `quarter`, `half_year` and `nine_months` are deliberately excluded: one filing emits all of
#: them, so a read that does not filter gets four rows for the same period at four different
#: scales, and summing or averaging across them is nonsense. `unusual` covers transition periods of
#: five to fourteen quarters and is excluded for the same reason.
ANNUAL_KINDS = ("year", "instant")

#: Columns read from the dataset itself. Named so the mount keeps seven of seventy.
SOURCE_FIELDS = [
    "revenue", "gross_profit", "net_income", "total_assets", "total_equity",
    "operating_cash_flow", "shares_outstanding",
]

#: What a strategy sees: the source columns plus the split factor computed below, which is not in
#: the dataset and has to be derived.
FIELDS = [*SOURCE_FIELDS, "shares_adjustment"]


def _split_factors(tickers: list[str], start: datetime.date,
                   end: datetime.date) -> dict[str, list[tuple[datetime.date, float]]]:
    """Split events per ticker, cached, so a share count can be put on the price's basis.

    The reason this is needed at all: a filing reports the shares that existed when it was filed,
    and a price series is back-adjusted so that every historical price is quoted in *today's*
    share basis. Multiply one by the other and a company that has split since is valued wrong by
    the split factor. Deckers filed 34.4m shares for 2012 and split six-for-one in 2024, so a naive
    market value came out at $0.31bn against the real $1.86bn -- an earnings yield of 41.7% where
    the truth was 6.9%.

    Note that this reads splits that happen *after* the simulation date. That is a unit conversion,
    not a forecast: the resulting ratio is identical to one computed from the unadjusted price and
    the as-filed share count, both of which were knowable at the time. Nothing about the company's
    prospects enters.
    """
    import yfinance as yf
    from ziplime.data.services import frame_cache

    key = frame_cache.cache_key("yahoo-splits", sorted(tickers), start, end)
    cached = frame_cache.load(key, max_age=datetime.timedelta(days=7))
    if cached is None:
        raw = yf.download(tickers, start=start, end=end, progress=False, auto_adjust=True,
                          actions=True, group_by="Ticker", threads=True)
        rows = []
        for ticker in tickers:
            try:
                series = raw[ticker]["Stock Splits"]
            except Exception:
                continue
            for when, ratio in series[series > 0].items():
                rows.append({"ticker": ticker, "split_date": when.date(),
                             "ratio": float(ratio)})
        cached = pl.DataFrame(rows, schema={"ticker": pl.Utf8, "split_date": pl.Date,
                                            "ratio": pl.Float64})
        frame_cache.store(key, cached)

    factors: dict[str, list[tuple[datetime.date, float]]] = {}
    for row in cached.iter_rows(named=True):
        factors.setdefault(row["ticker"], []).append((row["split_date"], row["ratio"]))
    return factors


def _period_years(start: datetime.date, end: datetime.date) -> list[int]:
    """Period years whose filings can be knowable inside the window.

    The config partitions by **event** year, not knowledge year, so a window cannot be pruned the
    way the insider dataset's can. A filing about 2013 can be accepted in 2015, and a comparative
    restatement of it later still -- so the read starts three years before the window and runs to
    its end.
    """
    return list(range(start.year - 3, end.year + 1))


def load_statements(revision: str | None = None,
                    start: datetime.date | None = None,
                    end: datetime.date | None = None) -> pl.DataFrame:
    """Annual statements with a ticker attached, one row per revision.

    Every revision is kept as its own row with its own ``knowledge_date``. Collapsing them here
    would decide, at mount time, what a strategy is allowed to know later -- which is the whole
    thing this dataset exists to avoid.
    """
    start = start or FUNDAMENTALS_START
    end = end or FUNDAMENTALS_END
    sha = HfApi().dataset_info(FUNDAMENTALS_DATASET, revision=revision).sha

    frames = []
    for year in _period_years(start, end):
        local = snapshot_download(FUNDAMENTALS_DATASET, repo_type="dataset", revision=sha,
                                  allow_patterns=[f"data/pit/period_year={year}/**"])
        parts = list(Path(local).glob(f"data/pit/period_year={year}/**/*.parquet"))
        if parts:
            frames.append(pl.read_parquet(parts, columns=[
                "entity_id", "knowledge_date", "event_date", "period_kind", "revision",
                "logical_report_id", *SOURCE_FIELDS]))
    if not frames:
        raise SystemExit(f"No statements found for {start}..{end}.")

    statements = (pl.concat(frames, how="diagonal")
                  .filter(pl.col("period_kind").is_in(ANNUAL_KINDS))
                  .with_columns(pl.col("entity_id")
                                .replace_strict(CIK_TO_TICKER, default=None)
                                .alias("ticker"))
                  .filter(pl.col("ticker").is_not_null()))

    # Put every as-filed share count on the same basis as the back-adjusted price series.
    splits = _split_factors(sorted(set(statements["ticker"].to_list())), start, end)
    if splits:
        def factor(ticker: str, known: datetime.datetime) -> float:
            product = 1.0
            for split_date, ratio in splits.get(ticker, ()):
                if split_date > known.date():
                    product *= ratio
            return product

        statements = statements.with_columns(
            pl.struct(["ticker", "knowledge_date"]).map_elements(
                lambda r: factor(r["ticker"], r["knowledge_date"]),
                return_dtype=pl.Float64).alias("shares_adjustment"))
    else:
        statements = statements.with_columns(pl.lit(1.0).alias("shares_adjustment"))
    return statements


async def mount(context, statements: pl.DataFrame | None = None,
                name: str = "fundamentals") -> HuggingFaceDataSource:
    """Mount the statements on their acceptance timestamp.

    ``knowledge_date`` is when EDGAR accepted the filing, which is when it could be read. The
    engine's own window filter does the rest, so nothing here can see a filing before it existed.
    """
    frame = statements if statements is not None else load_statements()
    return HuggingFaceDataSource.from_frame(
        frame=frame, name=name, knowledge_column="knowledge_date", entity_column="ticker",
        event_column=None, asset_service=context.asset_service,
        start_date=context.clock.start_session, end_date=context.clock.end_session,
        session_timezone=str(context.clock.trading_calendar.tz),
        fields=FIELDS)


def as_of(window: pl.DataFrame, fields: list[str]) -> dict[int, dict[str, float]]:
    """The freshest known value of each field per instrument, coalesced across revisions.

    Not the freshest *row*. A later filing repeats a period with fewer line items, so picking the
    newest row silently drops whatever it omitted -- 78% of ``total_assets``, measured. Taking the
    newest non-null per column keeps what was known without inventing anything: every value
    returned appeared in some filing already visible at this moment.

    Args:
        window: What ``data.history`` returned, already filtered by knowledge date.
        fields: Columns to resolve.

    Returns:
        ``{sid: {field: value}}``, omitting instruments with nothing known yet.
    """
    if window.is_empty():
        return {}
    resolved = (window.sort("date")
                .group_by("sid")
                .agg([pl.col(field).drop_nulls().last().alias(field) for field in fields]))
    out: dict[int, dict[str, float]] = {}
    for row in resolved.iter_rows(named=True):
        values = {field: row[field] for field in fields if row[field] is not None}
        if values:
            out[row["sid"]] = values
    return out


def rank_and_hold(scores: dict[int, float], keep: int) -> dict[int, float]:
    """Equal weights on the ``keep`` highest scores, or nothing if too few qualify."""
    ranked = sorted(scores.items(), key=lambda kv: -kv[1])[:keep]
    if not ranked:
        return {}
    weight = 1.0 / len(ranked)
    return {sid: weight for sid, _ in ranked}
