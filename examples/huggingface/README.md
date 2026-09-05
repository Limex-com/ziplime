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
| `h06_congress_buys` | congress `trades` + `filings` | the published "Congress Buys" method — size-weighted, weekly, window stretched for diversification |
| `h07_congress_long_short` | congress `trades` + `filings` | long the buys, short the sells: the same signal with market direction removed |

## The results, before anything else

Seven strategies, one universe of sixty names, 2016-2026. `h05` is the control: it holds all sixty
equally weighted and reads no data at all.

```
strategy                   sess trades    final value    return   max dd
h06_congress_buys          2680   3913   7,728,267.35 +672.83%  -33.49%
h05_universe_benchmark     2680   2525   6,294,918.97 +529.49%  -32.25%   <- control
h03_pelosi_portfolio       2680    252   4,025,601.95 +302.56%  -49.44%
h04_committee_consensus    2680    843   2,293,146.43 +129.31%  -47.44%
h07_congress_long_short    2680   1798     846,034.61  -15.40%  -19.98%
```

Three things fall out of that table, and the third is the one that matters.

**Only one rule beat the control.** `h06`, which follows the published Congress Buys method --
weight by the disclosed purchase size, rebalance weekly, stretch the lookback until the book holds
at least ten names -- returned 143 percentage points more than equal-weighting the same universe,
at the same drawdown. The two rules written from scratch here, replicating one legislator and
following one committee, lost to the control decisively and with far deeper drawdowns.

**Almost all of the return is the market, in every case.** The control returns +529% because these
sixty shares rose roughly sixfold. Against zero, `h03` at +303% reads as a triumph; against the
control it is a 226-point shortfall. A long-only backtest on an alternative dataset without a
matching control is close to uninterpretable, which is the cheapest and most-skipped honesty in
this whole area.

**With market direction removed, the signal is negative.** `h07` runs the same disclosures
dollar-neutral -- long the purchases, short the sales -- and loses 15% over ten years. That is
consistent with the rest: the *buy* list carries something, since `h06` beat equal weight; the
*sell* list is not a short signal, since members sell for liquidity and diversification as readily
as from conviction; and shorting through a decade-long advance is expensive. It is one path with no
significance test behind it, so it settles nothing on its own -- but it is the experiment that
separates a signal from a rising market, and it is the one usually missing.

### On comparing this to a published number

The Congress Buys page reports a 37.88% CAGR with a -22.80% drawdown and a beta of 1.14. The beta
is the interesting figure: a book that moves 1.14 times the market is mostly a market position, so
the published return needs the same control this table has.

The published backtest also starts on **1 April 2020**, eight trading days after the COVID low. Any
long equity strategy begun there is flattered enormously, and the strategy's own contribution is
invisible inside the recovery. `h06` uses the same method over 2016-2026 and is reported against
`h05` for exactly that reason.


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
