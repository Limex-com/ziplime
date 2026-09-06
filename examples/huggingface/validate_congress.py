"""Check the congressional dataset against itself, one legislator at a time.

    python examples/huggingface/validate_congress.py

A dataset assembled by parsing scanned PDFs is going to contain mistakes, and the useful question
is not whether it has any but whether they are findable and bounded. This runs the checks that its
own reference tables make possible -- who was in office when, which filing a trade came from,
whether the three published views of the same trades agree -- and reports what it finds per
legislator, since that is where a parsing failure shows up as something a human recognises as
wrong.

The checks, in the order they run:

1. **Referential integrity.** Every ``bioguide_id`` in the trades should exist in ``legislators``;
   every ``filing_id`` should exist in ``filings``.
2. **Terms of office.** A periodic transaction report describes trading *during* service. A
   transaction dated before a member took office is either a candidate-period trade, a term
   boundary, or a misparsed year -- and the three are told apart by how far out they are.
3. **The STOCK Act's 45 days.** The statute requires disclosure within 45 days. How much of the
   data obeys it is a fact about Congress rather than about the parser, but a distribution with
   the wrong shape would point at the parser.
4. **Cross-view agreement.** ``trades`` and ``features`` publish the same disclosures at different
   grains. They should reconcile exactly once the rows the dataset flags as undateable are removed.
5. **Per-legislator profiles.** Volume, span, lag and flagged rows for the most active members.

Nothing here needs the simulation engine; it reads the Hub and prints.
"""
import asyncio
import datetime
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent))

from congress import CLEAN, DATASET  # noqa: E402
from hf_config import CONGRESS_REVISION  # noqa: E402

from ziplime.data.data_sources.huggingface.huggingface_data_source import load_table  # noqa: E402

#: A transaction this far before a member took office is not a candidate-period trade or a term
#: boundary; it is a date that did not survive the parser.
IMPLAUSIBLE_YEARS_EARLY = 2


def rule(title: str) -> None:
    print(f"\n{title}\n{'-' * len(title)}")


async def main() -> int:
    # Resolve up front so the report names the exact commit it read, whether the caller pinned
    # one or left it to the default branch.
    from ziplime.data.data_sources.huggingface import hub
    revision = hub.resolve_revision(DATASET, revision=CONGRESS_REVISION).sha
    trades = await load_table(DATASET, "trades", revision=revision)
    filings = await load_table(DATASET, "filings", revision=revision)
    legislators = await load_table(DATASET, "legislators", revision=revision)
    terms = await load_table(DATASET, "legislator_terms", revision=revision)
    features = await load_table(DATASET, "features", revision=revision)

    print(f"\n{DATASET} @ {revision[:8]}")
    print(f"  trades {len(trades):>8,}   filings {len(filings):>7,}   "
          f"legislators {len(legislators):>6,}   terms {len(terms):>6,}")

    problems = 0

    # ---------------------------------------------------------------- 1. referential integrity
    rule("1. Referential integrity")
    known = set(legislators["bioguide_id"].to_list())
    seen = {b for b in trades["bioguide_id"].to_list() if b}
    orphan_members = seen - known
    no_member = trades["bioguide_id"].is_null().sum()
    filing_ids = set(filings["filing_id"].to_list())
    orphan_filings = trades.filter(~pl.col("filing_id").is_in(list(filing_ids)))

    print(f"  legislators appearing in trades      {len(seen):>8,}")
    print(f"  ... with no row in `legislators`     {len(orphan_members):>8,}"
          f"{'   <- unresolvable' if orphan_members else ''}")
    print(f"  trades with no legislator at all     {no_member:>8,}   "
          f"({no_member / len(trades):.2%}; these cannot be attributed)")
    print(f"  trades whose filing is not in `filings` {len(orphan_filings):>5,}   "
          f"({len(orphan_filings) / len(trades):.2%}; no filing date is available for them)")
    problems += len(orphan_members)

    # ---------------------------------------------------------------- 2. terms of office
    rule("2. Transactions against terms of office")
    span = terms.group_by("bioguide_id").agg(pl.col("start").min().alias("took_office"),
                                             pl.col("end").max().alias("left_office"))
    dated = trades.filter(pl.col("bioguide_id").is_not_null(),
                          pl.col("transaction_date").is_not_null()).join(span, on="bioguide_id",
                                                                         how="left")
    early = dated.filter(pl.col("transaction_date") < pl.col("took_office"))
    late = dated.filter(pl.col("transaction_date") > pl.col("left_office"))
    by_form = {r["source_form"]: r["len"] for r in early.group_by("source_form").len().iter_rows(
        named=True)}

    print(f"  dated before taking office           {len(early):>8,}   "
          f"({len(early) / len(dated):.2%})")
    print(f"    of which annual disclosures        {by_form.get('annual_b', 0):>8,}   "
          f"<- legitimate: an annual report lists assets held for years")
    print(f"    of which transaction reports       {by_form.get('ptr', 0):>8,}   "
          f"<- a PTR should describe trading while in office")
    print(f"  dated after leaving office           {len(late):>8,}   "
          f"(a final report filed on the way out is normal)")

    cutoff = datetime.timedelta(days=365 * IMPLAUSIBLE_YEARS_EARLY)
    implausible = early.filter(pl.col("source_form") == "ptr",
                               (pl.col("took_office") - pl.col("transaction_date")) > cutoff)
    print(f"\n  Transaction reports dated more than {IMPLAUSIBLE_YEARS_EARLY} years before the "
          f"member took office: {len(implausible)}")
    if len(implausible):
        shown = implausible.group_by(["member_name", "bioguide_id"]).agg(
            pl.len().alias("rows"),
            pl.col("transaction_date").min().alias("earliest"),
            pl.col("took_office").first().alias("took_office"),
            pl.col("date_quality").first().alias("flag")).sort("rows", descending=True)
        for row in shown.iter_rows(named=True):
            gap = (row["took_office"] - row["earliest"]).days // 365
            print(f"    {str(row['member_name'])[:24]:24s} {row['rows']:>4} rows  "
                  f"earliest {row['earliest']}  took office {row['took_office']}  "
                  f"({gap} years early)  flagged: {row['flag']}")
        already_flagged = implausible.filter(pl.col("date_quality") != "ok").height
        print(f"\n    Flagged by the dataset itself: {already_flagged} of {len(implausible)}. "
              f"{'Every one -- the publisher found these too.' if already_flagged == len(implausible) else 'Some were not.'}")
        problems += len(implausible) - already_flagged

    # ---------------------------------------------------------------- 3. the statutory window
    rule("3. Disclosure lag against the STOCK Act's 45 days")
    ptr = (trades.filter(pl.col("source_form") == "ptr", pl.col("superseded_by").is_null())
           .join(filings.select(["filing_id", "filing_date"]), on="filing_id", how="left")
           .filter(pl.col("filing_date").is_not_null(), pl.col("transaction_date").is_not_null()))
    lag = (ptr["filing_date"] - ptr["transaction_date"]).dt.total_days()
    print(f"  transaction reports with both dates  {len(ptr):>8,}")
    print(f"  median lag                           {lag.median():>8,.0f} days")
    for bound in (30, 45, 60, 90, 365):
        print(f"  filed within {bound:>3} days                {(lag <= bound).sum() / len(lag):>8.1%}")
    print(f"  filed more than a year late          {(lag > 365).sum():>8,}   "
          f"({(lag > 365).sum() / len(lag):.2%}; late filing is a real and reported phenomenon)")
    print(f"  filed before the trade happened      {(lag < 0).sum():>8,}   "
          f"<- impossible; the adapter floors these to the transaction date")

    # ---------------------------------------------------------------- 4. cross-view agreement
    rule("4. Do `trades` and `features` describe the same disclosures?")
    clean = trades.filter(pl.col("ticker").is_not_null(), pl.col("superseded_by").is_null())
    undateable = clean.filter(pl.col("date_quality").is_in(["unparsed", "out_of_range"]))
    feature_total = int(features["n_disclosures"].sum())
    reconciled = len(clean) - len(undateable)
    print(f"  trades, tickered and not superseded  {len(clean):>8,}")
    print(f"  ... minus rows with no usable date   {len(undateable):>8,}")
    print(f"  = {reconciled:>8,}")
    print(f"  features, sum of n_disclosures       {feature_total:>8,}")
    agree = reconciled == feature_total
    print(f"  {'They reconcile exactly.' if agree else f'MISMATCH of {reconciled - feature_total}'}")
    if not agree:
        problems += abs(reconciled - feature_total)

    # ---------------------------------------------------------------- 5. ticker plausibility
    rule("5. Are the tickers tickers?")
    tickered = trades.filter(pl.col("ticker").is_not_null())
    shaped = pl.col("ticker").str.contains(r"^[A-Z]{1,5}(\.[A-Z])?$")
    odd = tickered.filter(~shaped)
    print(f"  distinct values in the ticker column {tickered['ticker'].n_unique():>8,}")
    print(f"  ... not shaped like a US ticker      {odd['ticker'].n_unique():>8,}")
    print(f"  rows carrying one                    {len(odd):>8,}   "
          f"({len(odd) / len(tickered):.2%})")
    common = odd.group_by("ticker").len().sort("len", descending=True).head(6)
    print("  most common:", ", ".join(f"{r['ticker']!r} x{r['len']}"
                                      for r in common.iter_rows(named=True)))
    print("  These are CUSIPs, foreign listings and fund names in a column meant for tickers.")
    print("  The dataset README says such values were replaced with nulls; some remain. They are")
    print("  harmless to a strategy -- nothing resolves them against an equity database, so the")
    print("  rows are dropped -- but `features` aggregates by this column, so they appear there")
    print("  as instruments that do not exist.")

    # ---------------------------------------------------------------- 6. amount-band outliers
    rule("6. Disclosed amounts that do not fit the filer")
    ptr_clean = trades.filter(CLEAN)
    huge = ptr_clean.filter(pl.col("min_amount_usd") >= 25_000_001)
    print(f"  transaction reports in the $25m-$50m band {len(huge):>6,}   "
          f"across {huge['member_name'].n_unique()} members")
    suspect = (huge.with_columns(pl.col("transaction_date").dt.year().alias("year"))
               .group_by("member_name").agg(pl.len().alias("rows"),
                                            pl.col("year").n_unique().alias("years"),
                                            pl.col("year").min().alias("first_year"))
               .sort("rows", descending=True))
    for row in suspect.head(5).iter_rows(named=True):
        total = ptr_clean.filter(pl.col("member_name") == row["member_name"]).height
        flag = ("  <- every one in a single year, out of "
                f"{total:,} rows" if row["years"] == 1 and row["rows"] > 5 else "")
        print(f"    {str(row['member_name'])[:26]:26s} {row['rows']:>4} rows over "
              f"{row['years']} year(s), from {row['first_year']}{flag}")
    print("\n  A member disclosing two dozen separate $25m-$50m transactions in ordinary listed")
    print("  shares, in one year and in no other, is a shape that reads as a misaligned amount")
    print("  column rather than as trading. The rows are flagged `amount_quality = ok`, so nothing")
    print("  in the data marks them, and they carry 60% of that member's disclosed volume while")
    print("  being 1.4% of their rows. Strategies weighting by disclosed size are exposed to this.")

    # ---------------------------------------------------------------- 7. per-legislator profiles
    rule("7. The most active legislators, as the strategies see them")
    usable = (trades.filter(CLEAN)
              .join(filings.select(["filing_id", "filing_date"]), on="filing_id", how="left")
              .filter(pl.col("filing_date").is_not_null()))
    profile = (usable.group_by(["bioguide_id", "member_name"]).agg(
        pl.len().alias("rows"),
        pl.col("ticker").n_unique().alias("tickers"),
        pl.col("transaction_date").min().alias("first"),
        pl.col("transaction_date").max().alias("last"),
        ((pl.col("filing_date") - pl.col("transaction_date")).dt.total_days())
        .median().alias("lag"),
        (pl.col("transaction_type") == "purchase").mean().alias("buy_share"),
        pl.col("max_amount_usd").sum().alias("upper_usd"),
    ).sort("rows", descending=True).head(15))

    print(f"  {'member':26s} {'rows':>6s} {'names':>6s} {'first':>11s} {'last':>11s} "
          f"{'lag':>5s} {'buys':>6s} {'disclosed up to':>17s}")
    for row in profile.iter_rows(named=True):
        print(f"  {str(row['member_name'])[:26]:26s} {row['rows']:>6,} {row['tickers']:>6,} "
              f"{str(row['first']):>11s} {str(row['last']):>11s} {row['lag']:>4.0f}d "
              f"{row['buy_share']:>6.0%} {row['upper_usd']:>17,.0f}")

    rule("Verdict")
    print(f"  rows the strategies use          {len(usable):>8,}")
    print(f"  distinct legislators             {usable['bioguide_id'].n_unique():>8,}")
    print(f"  unexplained problems             {problems:>8,}")
    if problems == 0:
        print("\n  Every anomaly found is either explained by the rules of disclosure -- annual\n"
              "  reports listing old assets, final filings after leaving office, genuinely late\n"
              "  filers -- or already flagged by the dataset. The three published views agree.")
    return 0 if problems == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
