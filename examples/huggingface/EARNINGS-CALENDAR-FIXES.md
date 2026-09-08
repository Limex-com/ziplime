# ZipLime/earnings-calendar — improvements, in order of value

Every number below was measured against the published data at `sha 6512dc9c`, not taken from the
README. Where the README and the data disagree, the data is quoted.

The dataset is already the most careful in the family on the thing it is about: all 454 611
acceptance timestamps come from the filing's own SGML header (`acceptance_source =
filing_header`, with no fallback rows), 80.6% of releases land outside market hours exactly as
claimed, and a year-on-year change correctly divides by the **absolute value** of the base, so a
company going from -0.67 to +0.20 a share reads +1.30 rather than -1.30. The naive formula flips
the sign on the 25.6% of rows whose year-ago figure is negative; this one does not. The steps
below are about the edges.

---

## Step 1 — Put a point-in-time `ticker` in `pit`

`pit` identifies a company only by `entity_id`, the issuer CIK. Every consumer therefore builds
their own CIK-to-ticker map before they can join a price series, and a hand-built one covers a few
hundred names at best.

`ZipLime/security-master` is in the same family and exists for this: 143 932 dated identifier
spans, 68.8% of CUSIP pairs resolving, and a documented 90.5% CIK coverage.

Add `ticker` and `ticker_confidence`, resolved **as of `knowledge_date`** — the span that was
current when the announcement was made, not the span that is current now. Resolving with today's
map is the look-ahead the security master was built to prevent: a company that changed symbol in
2020 would be labelled with the new symbol for its entire history, and the price series fetched
under that symbol is the post-change one.

This is the single change that most improves usability, and it applies equally to
`company-fundamentals`, `institutional-portfolio-13f` and `fund-holdings-nport`.

## Step 2 — Compute `session` against the exchange calendar, not against 09:30 and 16:00

The boundaries are currently fixed clock times. Measured on the published labels:

```
pre_market     06:00:12 .. 09:29:59
market_hours   09:30:00 .. 15:59:59
after_close    16:00:00 .. 23:53:10
```

On the 136 NYSE half days in this window the exchange closes at 13:00, and **37 releases that
arrived after that close are labelled `market_hours`** — for instance:

```
2018-11-23  14:19:14   (the day after Thanksgiving)
2014-12-24  15:14:30   (Christmas Eve)
2008-07-03  14:48:55   (the day before Independence Day)
2025-07-03  14:24:06
```

`session` is the column this dataset exists for. Derive it from the session's own open and close.

## Step 3 — `is_trading_day` is `True` on every row, including days the exchange was shut

All 454 611 rows carry `is_trading_day = true`. But **599 releases fall on 25 days that are not
NYSE sessions**:

```
2012-10-29   109 releases   Hurricane Sandy -- exchange closed, EDGAR open
2012-10-30   194 releases   Hurricane Sandy
2004-04-09, 2006-04-14, 2007-04-06, 2011-04-22, 2013-03-29, 2024-03-29   Good Fridays
```

The column appears to test for a business day rather than a trading session. As published it is
wrong in exactly the cases a consumer would consult it for.

## Step 4 — Replace it with `first_tradeable_session`

What a backtest needs is not a boolean but a date: the first session whose **close falls after the
acceptance instant**. One column, and it dissolves three separate pieces of reasoning every
consumer currently redoes — was this before or after the close, was the exchange open at all, and
what does a 16:30 Friday release mean.

It is also where the cases above stop mattering: for a release accepted on 29 October 2012 the
answer is simply 31 October.

## Step 5 — Reject implausible per-share figures with the columns already in the row

18 releases carry `|eps_yoy_change| > 1000`. The cause is not a small denominator — only 8 rows in
the corpus have a year-ago EPS under two cents. It is the numerator:

```
cik 0000045012  eps_diluted 2 920 000   net_income 2 638 000 000   (Q4 2023)
cik 0001163199  eps_diluted 3 550 000   net_income        —
cik 0000020212  eps_diluted  -590 000   net_income   -23 400 000
```

Those are totals tagged into the per-share field. The check needs nothing new:
`net_income / eps_diluted` is the implied share count, and on the 182 536 rows where both are
present it is well behaved —

```
p1      2 068 750
median 56 715 252
p99  2 290 677 966
```

— while **138 rows (0.076%) imply fewer than 10 000 shares**, and those 138 contain every one of
the 18 absurd growth rates. Halliburton's Q4 2023 row implies 903 shares.

Null `eps_diluted` and `eps_yoy_change` when the implied count falls below a floor (50 000 is
comfortably clear of the p1), and record `eps_quality = "implausible"` rather than dropping the
row, so the announcement and its timestamp survive.

Ranking is not protection against this: a rank puts a wrong number at the **top** of a book, not
in the middle of it. The upstream cause is in `company-fundamentals`' concept map and should be
fixed there as well, but this check is local and free.

## Step 6 — Give `events` a knowledge column

`events` is 4 382 182 rows across 46 item codes, including **315 589 director-or-officer changes**
under item 5.02 — the most interesting alternative data in the repository. It cannot be mounted at
all: the config carries `filed_date` and nothing else, and the ziplime convention refuses a config
with no knowledge-date column rather than risk a look-ahead. Confirmed against the adapter:
`NoKnowledgeDateError`.

The cheap correct fix is the one `fund-holdings-nport` already uses: `knowledge_date = filed_date`
at 23:59:59 Eastern. It errs late, so it can never leak.

Then improve it for free: 454 611 exact acceptance instants already exist in `announcements`. Join
on `accession_number`, use the real timestamp where there is one, and set
`knowledge_estimated = true` only for the remainder.

## Step 7 — Carry `form` and `accession_number` into `pit`

4 730 of the 454 611 announcements are `8-K/A` amendments. In `pit` an amendment is
indistinguishable from an original — neither `form` nor `accession_number` is present — so a
consumer cannot tell a corrected announcement from a first one, and cannot join back to
`announcements` or `earnings` to find out.

## Step 8 — Add standardised unexpected earnings

The README is right that consensus is licensed and unavailable. But the post-announcement drift
literature does not use consensus: it uses **SUE** — the residual against the same quarter a year
earlier, divided by the standard deviation of that residual over the previous eight quarters.

Everything needed is already here. And it fixes a real problem with what is here now: a raw
year-on-year change is not comparable across companies. A utility growing revenue 3% and a biotech
growing 300% cannot be ranked against each other meaningfully, so a cross-sectional strategy built
on `revenue_yoy_change` is ranking volatility as much as surprise. SUE is scale-free, and it is
the column that would make this dataset directly usable for the event study it was built for.

## Step 9 — Refresh two README numbers

* The 2011-onwards pairing rate is **87.2%** on the published data (252 681 of 289 707 by
  `announced_date_et`), not 88.4%.
* The session counts are **227 742 / 138 647 / 88 222**, against 227 741 / 138 647 / 88 219 in the
  README.

Both are trivial and both are the kind of drift that makes a reader stop trusting the numbers that
matter.

## Step 10 — State in the README that this is not a forward calendar

The name invites the opposite reading, and the opposite reading is what most people want when they
search for it. A row becomes visible when the 8-K was accepted, which is when the announcement
happened; nothing here schedules a future release.

Worth quantifying in the same breath, because a consumer will try: predicting "the previous
release plus 91 days" over the 12 311 consecutive gaps in a 230-name sample gives a **median
absolute error of 7 days**, and lands within a week only **56.1%** of the time. The gap
distribution is median 91, p10 57, p25 79, p75 96, p90 111.

Saying this plainly costs a paragraph and saves everyone who wants to be flat into results from
discovering it themselves.
