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

## What gets cached, and what expires

There is no ingest step, so everything is fetched on demand -- and a research loop that refetches
on every process start is unusable. Four things are remembered, at different levels:

| what | where | expires |
| --- | --- | --- |
| Dataset Parquet files | `huggingface_hub`'s own cache, keyed by commit | never — a commit is immutable |
| The resolved commit of a branch | in the process | at process exit, so `main` can move |
| The mounted frame (windowed, sids resolved) | rebuilt each mount | — it takes 0.95s |
| Daily price bars | `~/.cache/ziplime/frames`, as Parquet | **one day** |

The last row is the one with a trap in it. **Price history is not immutable**: a split or a
dividend restates every bar before it once the source adjusts, so a frame cached in June is wrong
after a July split. A day is short enough to catch a restatement on the next session and long
enough that an afternoon of editing a strategy pays the download once. Point-in-time datasets are
different — pinned to a commit, they can be cached forever, which is what `frame_cache.FOREVER` is
for.

Set `ZIPLIME_CACHE_DIR` to move it, or delete the directory to force a refetch.

```
build bundle, cold           17.87s
build bundle, fresh process   0.13s
```

The mounted frame is not cached at all, and does not need to be. It used to take 57 seconds, of
which 0.04 was reading Parquet and the rest was resolving twelve thousand tickers to sids one
database query at a time. Batching that query made it 0.95 seconds — which is the better answer
than caching a slow computation.

## A strategy should be its idea

Zipline algorithms are short because two things are handed to them: `schedule_function` decides
when the logic runs, and a pipeline turns a column of numbers into a ranked selection. Neither
exists in this fork, so the first version of these strategies hand-rolled both — the same
five-line *has it been ninety days yet* gate in **twenty files out of twenty**, the same loop
skipping rows with missing inputs, the same rank-and-equal-weight.

`playbook.py` takes that out of the way. A whole strategy, all of its logic:

```python
async def initialize(context):
    context.universe = await equities(context, FUNDAMENTALS_UNIVERSE)
    context.source = await mount(context)

@every(days=90)
async def handle_data(context, data):
    rows = await data.current(assets=context.universe, fields=FIELDS,
                              data_source=context.source)
    scores = factor(fresh(rows, context, MAX_AGE),
                    lambda r: -(r["net_income"] - r["operating_cash_flow"]) / r["total_assets"])
    held = await hold_top(context, data, scores, keep=KEEP)
```

| | |
| --- | --- |
| `@every(days=N)` | run at most this often — `schedule_function` wearing a smaller hat |
| `equities(context, pairs)` | resolve `(ticker, MIC)` pairs to listings |
| `fresh(rows, context, max_age)` | drop values too old to act on |
| `factor(rows, fn)` | score, skipping rows whose inputs are missing |
| `any_of(rows, predicate)` | the instruments an event fired on |
| `hold_top(...)` / `hold(...)` | rank-and-weight, or hold a set |
| `show_once(...)` | report the first book and then be quiet |

`factor` catches exactly `TypeError` and `ZeroDivisionError` — arithmetic on `None`, and a zero
denominator. On sparse fundamentals those two *mean* "this company did not report it", which is
the normal case rather than an error; anything else still raises. That also retires the `NEEDS`
constant every scoring strategy used to carry.

Measured across the seven strategies rewritten on it:

```
                            before   after
i01_cluster_buys                24      14   -41%
i02_executive_buys              25      14   -44%
i06_directors_only              26      15   -42%
f03_low_accruals                33      12   -63%
TOTAL                          186      99   -46%
```

### Two things this nearly got wrong

**A refactor must not move the numbers, and this one appeared to.** `i01` fell from +319.58% to
+262.07%. Running it once with the old `bar_count=30` reproduced +319.58% and 12 128 trades
exactly — so the plumbing was sound and the difference came entirely from switching that read to
`since=30 days`, which is a deliberate change of window semantics made in the same pass. The
`since` version is kept because it is the correct one; the number moved for a reason worth naming.

**`hold` trades when the set changes, not when weights drift.** A signal saying *hold these names*
is answered by the membership of the set. Rebalancing to equal weight in between quietly turns the
rule into "hold these names and also sell whichever of them went up" — a different strategy, and
on this data a worse one.

### Where these belong

`every` is `schedule_function`; `factor` with `hold_top` is what a pipeline does. They sit in an
examples directory because that is where they could be written today, not because that is where
they should live. If one moves into the engine first, make it `every` — it was duplicated in every
strategy here without exception.

## One way to read, whatever the dataset

Every strategy here reads through the same two calls, against prices, congressional
disclosures, insider filings and SEC financial statements alike:

```python
window  = await data.history(assets=universe, since=..., fields=[...], data_source=source)
current = await data.current(assets=universe, fields=[...], data_source=source)
```

That was not true for a while, and the way it broke is worth knowing.

`data.current` asks a source for its state now, and the base implementation takes the newest row.
For prices and for daily aggregates that is right. For SEC statements it is wrong: the same period
is republished as it is revised, and a later filing restating it reports **fewer** line items — so
the newest row is missing whatever that filing did not repeat, which is 78% of `total_assets`.

The first version of these examples fixed that in the strategy: `fundamentals.as_of(window, FIELDS)`
coalesced the columns after reading. It worked, and it was wrong in design — the caller had to know
which dataset needed which helper, and a fundamentals strategy ended up importing
`from insider import rebalance` because that is where the shared code happened to live.

Now the source declares it. `HuggingFaceDataSource` takes a `Resolution`:

| | what `data.current` returns |
| --- | --- |
| `LATEST_ROW` (default) | the newest row per instrument — prices, daily aggregates |
| `COALESCE` | the newest non-null value per **column**, across the revisions visible then |

`fundamentals.mount()` passes `COALESCE`; nothing else needs to. The helper is gone, and reading a
financial statement looks exactly like reading a price.

What legitimately stays per dataset is **preparation**, not access: which config to mount, which
columns, which rows to exclude. `congress.load_disclosures()` joins `filings` to get a real
publication date; `fundamentals.load_statements()` filters `period_kind` and derives a split
factor. That is knowledge about a dataset and it has to live somewhere. The reading does not.

Portfolio helpers live in `portfolio.py` for the same reason — turning target weights into orders
has nothing to do with what the strategy read.

## Counting rows or counting time

`data.history` takes **either** `bar_count` or `since`, and on event data the choice matters more
than it looks.

```python
# bar data: thirty rows is thirty sessions, which is what you meant
await data.history(assets=universe, bar_count=30, fields=["close"])

# event data: thirty *days*, however many filings that turns out to be
await data.history(assets=universe, since=datetime.timedelta(days=30),
                   fields=["net_notional_usd"], data_source=context.source)
```

A bar source emits one row per session, so a row count and a time span are the same question. A
disclosure source does not. Thirty rows of congressional filings for a rarely-traded name reach
back four years; thirty rows for Nvidia reach back four months. `bar_count=30` there is a question
about how often that company's insiders file, not about the last month — and the strategy silently
gets a different window per instrument.

Both forms return the same shape, so they are interchangeable at the call site. Passing both, or
neither, raises rather than picking one.

Before this existed every strategy here over-fetched and re-filtered by hand:

```python
filed = await data.history(assets=universe, bar_count=80, ...)      # hope 80 is enough
cutoff = today - datetime.timedelta(days=60)
recent = filed.filter(pl.col("date").dt.date() >= cutoff)           # then throw most of it away
```

That is three lines of boilerplate per strategy, a guess at `bar_count` that is wrong for some
instrument in every universe, and a silent truncation when the guess is too small.

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

## Checking the data before trusting it

```bash
python examples/huggingface/validate_congress.py
```

A dataset built by parsing scanned PDFs will contain mistakes. The useful question is whether they
are findable and bounded, and this dataset publishes the reference tables that make them findable:
who was in office when, which filing a trade came from, and three views of the same disclosures
that ought to reconcile.

What the checks return, on revision `67c335f5`:

| check | result |
| --- | --- |
| every `bioguide_id` resolves in `legislators` | yes, 698 of 698 |
| `trades` and `features` reconcile | **exactly** — 178 841 clean rows minus 340 undateable = 178 501, the sum `features` publishes |
| transactions dated before the member took office | 539, of which 498 are annual reports legitimately listing old assets |
| disclosure lag | median 28 days, 86.3% inside the STOCK Act's 45 |
| filed *before* the trade happened | 100 rows — impossible; the adapter floors them |

Per-legislator profiles are where a parsing failure becomes something a human recognises as wrong,
and three findings came out of them:

**Rows the dataset flags, and rows it does not.** Fifteen transaction reports are dated more than
two years before their filer entered Congress. Twelve carry `date_quality = "out_of_range"` — the
publisher found them too, and Ro Khanna's eleven are a 2025 misread as 2005. Three are flagged
`ok`: two of Jefferson Shreve's dated nine years early, and one of Tony Wied's dated four. Those
are unflagged errors, and the `CLEAN` filter in `congress.py` does not catch them because nothing
in the row says anything is wrong.

**Two per cent of tickers are not tickers.** 1 954 distinct values in that column are CUSIPs
(`011798LP8`), foreign listings (`1066.HK`, `3288.T`), fund names (`JOVE EQUITY FUND I, LP`) and
placeholders (`US TREASURY`, `CD-EX`). The dataset README says these were replaced with nulls;
some survived. Strategies are unaffected — nothing resolves them against an equity database — but
`features` aggregates by this column and so publishes instruments that do not exist.

**One member's disclosed volume is mostly an artifact.** Diana Harshbarger accounts for 27 of the
43 transaction reports in the `$25m–$50m` band, every one of them in 2022 and none before or
after, in ordinary listed shares like EA, Align and Southwest. Those 27 rows are 1.4% of her
filings and 60% of her disclosed volume — $1.96bn, against $763m without them. Nothing in the data
marks them: `amount_quality` reads `ok`. A strategy weighting positions by disclosed size, which
`h06` does and which the published Congress Buys method does, is exposed to exactly this.

### What the checks changed

Running them was not academic. `congress.py` now also requires `date_quality == "ok"` and
`amount_quality != "invalid"`, because without those the strategies were reading 500 rows with
broken dates — including transactions dated 2005 — and 604 rows whose disclosed amount the
extractor had itself marked unusable and which fed straight into position sizing. Adding the
filter took 31 percentage points off `h06`'s return. That return was partly an artifact.

`amount_quality == "snapped"` is kept: it means a figure was rounded to the nearest published
band, which is a repair rather than a defect, and it covers 21 032 rows.

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
