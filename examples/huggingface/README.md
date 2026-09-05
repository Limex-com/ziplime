# Point-in-time datasets from the Hugging Face Hub

Mount a dataset published on the Hub and read it inside a strategy. No ingest step, no bundle to
build, no credentials:

```python
df = await data.history(assets=context.universe, bar_count=40,
                        fields=["net_notional_usd"],
                        data_source="hf://ZipLime/congress-trading/features")
```

The first read resolves the dataset to a commit, downloads only the Parquet parts the simulation
window can reach, maps its tickers onto the asset database, and keeps the result in memory. Every
later bar is served from there, and a second run of the same backtest starts from the Hugging Face
cache.

## Running them

```bash
cd examples/huggingface && python run_all.py
```

The equities are priced from Yahoo Finance. The datasets come from the Hub.

| strategy | dataset | shows |
| --- | --- | --- |
| `h01_congress_flow` | `ZipLime/congress-trading` | the one-line form — an address inside `data.history` |
| `h02_insider_cluster_buys` | `ZipLime/insider-trading` | the explicit form, mounted in `initialize` and **pinned to a commit** |
| `h03_pelosi_portfolio` | congress `trades` + `filings` | replicating one legislator's book, on the day each report was filed |
| `h04_committee_consensus` | + `committee_members` | several members of one committee buying the same name at once |
| `h05_universe_benchmark` | none | the control: the same universe, equally weighted, ignoring every disclosure |

## The result, before anything else

```
strategy                   sess trades    final value    return   max dd
h03_pelosi_portfolio       2680    167   4,177,758.91 +317.78%  -45.70%
h04_committee_consensus    2680     63   6,227,405.27 +522.74%  -38.75%
h05_universe_benchmark     2680    601  10,734,032.51 +973.40%  -36.62%
```

**The control wins, and it is not close.** Holding the same fourteen names equally weighted and
never reading a single disclosure returned +973%, against +523% for following a committee and
+318% for replicating Nancy Pelosi's book — with a *smaller* drawdown than either. On this universe
and this decade, acting on congressional disclosures destroyed more than half the return available
from simply owning the same shares.

That is why `h05` is here. Both disclosure strategies return several hundred percent, and either
number looks like a discovery until something honest sits next to it. What they are mostly
measuring is that large-capitalisation American technology shares rose a great deal between 2016
and 2026; the disclosures subtract from that by holding fewer names, and by being weeks late.

None of this settles whether congressional trades carry information. It says that *these* rules,
on *this* universe, over *this* window, did not extract any — which is the most a single backtest
can say, and more than most report.

## The thing that makes it point-in-time

These datasets carry two dates, and they are not interchangeable:

* the **event date** — when the trade happened, when the quarter ended;
* the **knowledge date** — when it became observable to someone watching the source.

A member of Congress reports a transaction up to 45 days after making it. A 13F reports a quarter
that ended 45 days earlier and is amended for months afterwards — the latest arrival in that
dataset came **452 days** late. Index a backtest on the event date and it trades on information
nobody had; the equity curve looks wonderful, and nothing in the output says why.

So the mount indexes on the knowledge date, and ziplime's own window filter — `date < simulation
time`, the same one every data source goes through — does the rest. The column is chosen by
`ziplime/data/data_sources/huggingface/manifest.py`, which **refuses to mount a dataset that
offers only event dates** rather than quietly using one.

The order within the knowledge dates matters too. The insider `features` table carries both
`knowledge_day` (the New York day of the underlying filings) and `feature_available_at` (midnight
at the start of the *following* day, when the aggregated row could first be read). They differ by
up to a day, always in the direction that flatters a backtest, so the later one wins.

`tests/test_huggingface_datasets.py::PointInTimeTests` pins this down on a hand-made dataset: one
disclosure executed on the 1st and published on the 20th, invisible for nineteen days and visible
on the twentieth.

## How a dataset qualifies

Nothing here is specific to these three datasets, which is the point. A dataset is mountable when
it publishes either of two files, both of which it would publish anyway:

* **`manifest.json`** — ziplime's own bundle metadata, the format
  `ziplime/data/services/file_system_bundle_registry.py` writes: `name`, `data_type`,
  `entity_domain`, per-config paths, coverage;
* **the README's YAML front matter** — the Hub's standard `configs:` block.

Plus two column conventions:

| needs | accepted names, best first |
| --- | --- |
| a knowledge date | `feature_available_at`, `knowledge_date`, `knowledge_day`, `available_at`, `disclosure_date`, `notification_date`, `date` |
| an instrument | `ticker`, `asset_ticker`, `symbol`, `entity_id` |

Publish those and the dataset mounts, with no code written for it.

## Pinning, and why it is the default

These datasets are append-only and grow — the insider corpus gains rows every day the SEC accepts
a Form 4. Left on `main`, the same backtest run a month apart reads different data and returns a
different number, with nothing to say why.

So a branch is resolved to a commit once, up front, and reported:

```
Pinned a Hugging Face dataset to the commit it is at now
  dataset=ZipLime/congress-trading revision=67c335f5207d5190ada5f89803615848dbf52ee0
```

Pass that back to repeat the run exactly:

```python
context.insiders = await context.huggingface_dataset(
    "ZipLime/insider-trading", config="features",
    revision="ba0785efcede0b3a13af48dc658a1d39bc87ad1e",
    fields=["is_cluster_buy", "n_unique_buyers_30d"])
```

## What the mount reports, and why you should read it

```
Materialised a Hugging Face dataset
  parquet_parts_available=143  parquet_parts_read=11
  rows_in_window=321496  rows_mounted=181307  instruments=2446
Dropped rows whose ticker matches no listing
  dropped=140189  tickers_resolved=2446/6385
```

Two numbers deserve attention. **`parts_read` versus `parts_available`** is the window doing its
job: a 2012 backtest reads 11 of 143 partitions instead of downloading a decade. And
**`tickers_resolved`** is how much of the dataset your asset database can actually see — these
datasets carry CUSIP-identified bonds, delisted names and foreign listings that an equity database
does not hold, and a mount that silently kept 60% of its rows without saying so would be worse
than one that says so.

## Three things worth copying

**Name the exchange.** `context.symbol("META")` is ambiguous — META lists on two venues — and
resolving by ticker alone picks one silently, not necessarily the one your price bundle holds.
Both examples pass `mic=`.

**Check tradability *and* a price.** `data.can_trade` answers from the listing and the calendar; it
does not read prices, and a listing's recorded start date is only as good as the vendor that
supplied it — in this asset database these equities all claim `1900-01-01`. A dataset can carry
rows for a name years before it lists, so `h02` checks both.

**Read the window, not the day.** These datasets publish a row only on days something was
disclosed, so `bar_count=40` is forty *disclosure* days, which is a much longer stretch of calendar
than forty sessions. The trailing-window columns (`net_notional_usd_30d` and friends) are the ones
to read at any frequency coarser than daily, since ziplime downsamples with `.last()`.

## Getting the disclosure date right

The congressional strategies key on **`filing_date`**, joined from the `filings` table — not on
`notification_date`, which is what the `trades` table is keyed on and what its README recommends.

On a House Periodic Transaction Report the notification date is the day the **filer was told** about
a transaction. For the managed and spousal accounts most of these trades sit in, that is frequently
the day of the trade. Across the table, `filing_date` is later than `notification_date` in **116 603
of 275 936 rows**; for Pelosi, notification equals the transaction date on **221 of 487**. A
strategy keyed on notification would open positions a month before the disclosure existed — and
would look considerably better for it.

With `filing_date` the lag lands where the STOCK Act puts it: a median of **28 days**, 90th
percentile 54. `examples/huggingface/congress.py` does the join and documents the three filters it
applies — periodic reports only, no superseded amendments, tickered rows only.

The adapter also floors every knowledge date against the event date it belongs to. Congressional
`trades` contains **319 rows** whose disclosure is dated before the trade it describes, one of them
by a misread year digit that puts it a thousand years early. Left alone, that row is visible from
the first bar of every backtest.

## Results are not recommendations

`h01` returns +157% over 2023–2026 on five names that rose a great deal regardless of what Congress
disclosed. `h03` and `h04` lose decisively to their own control. Nothing here is evidence about a
signal; it is a demonstration of the plumbing, and of what the plumbing has to get right before any
evidence is possible.
