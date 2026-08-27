# Changes: bond trading and cross-asset portfolios

> **Start with [`changes-overview.md`](changes-overview.md)** — the map of the whole body of work,
> covering both this and the futures work in [`changes.md`](changes.md). This document is the detail
> behind the bond and cross-asset half: evidence, live-API findings and open questions.

Written for a ziplime maintainer. Every entry says **what changed, why, and how confident we are**.

Test suite: **342 passed, 4 xfailed** (up from 143 passed / 4 xfailed; the xfails pre-date this
work and are unchanged). 179 of the new tests are bonds, 28 of them against recorded live payloads.

Verified against the **live Finam API**: reference data and bars ingested for six real MOEX issues,
and a backtest on ОФЗ 26238 reconciled by hand against the raw coupon schedule and price series
(see §6). Equities, bonds and futures also verified together in one portfolio (see §7).

---

## 1. What a bond needed that nothing in ziplime had

Three properties of a bond break assumptions the rest of the backtester makes. All three are now
handled by the engine, so a strategy orders a bond the same way it orders anything else.

### 1.1 The quote is not the price

MOEX quotes a bond as a **percentage of face value**. `98.42` on a 1000-rouble nominal is 984.20
roubles per bond. Read as money — which is what `amount * price` does everywhere in the engine —
every bond position is understated by `face_value / 100`, a factor of ten on a standard nominal.

`Bond.price_quotation` (`PERCENT_OF_FACE` | `MONEY`) records the convention;
`ziplime/finance/bonds.py` does the conversion, and `Ledger.process_transaction` and
`calculate_position_tracker_stats` both go through it.

### 1.2 The buyer pays accrued interest

Between coupons the buyer owes the seller the accrued part of the coupon (НКД). The tape carries
the *clean* price; settlement is at the *dirty* one. On an 8% coupon this averages about 4% of face
value per trade — not a rounding error over a year of round trips.

Accrual comes from the coupon schedule when one is stored: elapsed days over the length of the
coupon period, which is exactly how an exchange publishes it, and needs no day-count convention.
`DayCount` (ACT/365, ACT/360, ACT/ACT, 30/360) is the fallback for a bond whose schedule was never
ingested.

### 1.3 The principal is not constant

An amortizing issue repays face value in instalments, so the nominal a quote is a percentage *of*
shrinks, and so does the coupon computed on it. `Bond.face_value` is the nominal at issue;
`BondBook.face_value(bond, dt)` reports what is outstanding.

---

## 2. New code

| area | what |
| --- | --- |
| domain | `Bond`, `BondEvent`, `BondEventType`, `DayCount`, `PriceQuotation` |
| storage | `bonds` and `bond_events` tables, migration `e5c8b13a7f42`, repository + service methods |
| pricing | `ziplime/finance/bonds.py` — `BondBook`, `coupon_schedule`, `current_yield`, `simple_yield_to_maturity` |
| engine | dirty-price settlement, coupon/amortization processing, redemption at par, bond position value |
| costs | `PerBondTurnover` (a fraction of money transacted — how bond desks actually bill) |
| connector | `finam_bonds.py`, `FinamClient.bonds_past`, `FinamAssetDataSource.get_bonds` |
| API | `bond_symbol`, `bond_dirty_price`, `accrued_interest`, `bond_face_value`, `bond_schedule`, `bond_current_yield`, `bond_yield_to_maturity` |
| data | five demo issues seeded into `data/assets.sqlite` |
| examples | `examples/bonds/` — six strategies, a seeder, a runner, a README |
| docs | `docs/backtesting/bonds.md` |

### Design decisions worth flagging

**One flat `bond_events` table with a type discriminator**, rather than a table per event kind.
That is the shape the vendors send (Finam returns one list with a `type` field and a detail object
per type), and the simulation asks one question per session — "what is payable today" — which one
indexed table answers in a single scan.

**Schedules are loaded ahead of time into a `BondBook`.** The ledger settles a transaction inside a
synchronous call and cannot reach the database there, so schedules are fetched when a bond is first
ordered and once per session for every bond held.

**Redemption is booked explicitly, not left to auto-close.** A matured bond does not trade; the
issuer repays the outstanding principal. Closing the position at the last printed bar would book a
gain or loss that never happened. `Ledger.redeem_matured_bonds` runs at session start, after
coupons, so a final coupon dated to the maturity date is still earned.

**Terms are inferred from the calendar.** Finam's `AllAssets` gives a bond's identity and nothing
else — no nominal, no coupon, no maturity. `/v1/bonds/past` and `/v1/bonds/future` give the
calendar, and the terms are read off it: nominal from `amortization_details.initial_face_value`,
frequency from the median spacing between payments (median, so one holiday-shifted payment cannot
move it), rate from the last published percentage, maturity from the final event.
`BondTerms.inferred` names every derived field so it can be told apart from one the vendor stated.

**Redemption is recognised, not assumed.** Finam never sends `MATURITY`; it sends a terminal
`AMORTIZATION`. The connector re-types it, and `MATURITY` deliberately does **not** pay cash — the
ledger repays the outstanding principal on that date, so the principal has exactly one path.

---

## 3. Bugs found and fixed

Each was found by a test, not by inspection.

### 3.1 Amortization applied twice at redemption — **found by test, fixed**

`redeem_matured_bonds` quoted par against the nominal *at issue* while settlement converted it back
using the *outstanding* nominal, so an amortizing bond repaid a fraction of a fraction: a
1000-nominal issue with 400 outstanding repaid 160 instead of 400.

**Evidence.** `test_an_amortized_bond_repays_only_what_is_left` returned 1600 for 10 bonds where
4000 was due. Fixed by quoting par against the same outstanding nominal the conversion uses.

### 3.2 Redemption settled at zero when maturity fell on a non-trading day — **found by test, fixed**

`BondBook.face_value` reports nothing outstanding after maturity, which is true of the issuer's
remaining obligation but the wrong basis for valuing the redemption trade itself — and that trade
books on the next *session*, which can be days after a weekend maturity. The result was a
redemption worth 0.

**Evidence.** `b04_hold_to_maturity` (maturity Saturday 2025-04-05, redeemed Monday 2025-04-07)
returned −81.92%. Fixed with `BondBook.valuation_date`, which never runs a valuation past maturity.

### 3.3 Payments whose record date fell on a non-trading day were dropped — **found by test, fixed**

Entitlement was matched on `record_date == session`. A record date is a calendar date and the
simulation only wakes on sessions, so a coupon whose record date landed on a Saturday was never
earned by anyone. On a semi-annual bond that silently drops roughly two coupons in seven.

**Evidence.** `b01_buy_and_hold_coupon` returned +8.39% where the schedule implies about +23%.
Fixed by querying the half-open interval since the previous session, and by paying everything due
on *or before* the current session.

### 3.4 `NoSlippage.process_order` had a stale signature — **pre-existing, fixed**

It took no `price` argument (the caller passes one) and referred to a `data` name that does not
exist in that scope, so every order routed through it raised `TypeError` instead of filling. Not a
bond bug — the model was unusable for any asset class. Fixed because the bond tests need a
frictionless fill and `FixedBasisPointsSlippage` rejects `basis_points=0`.

### 3.5 `order_value` passed arguments `order` does not accept — **pre-existing, fixed**

`order_value` forwarded `limit_price=`/`stop_price=` to `TradingAlgorithm.order`, which takes an
execution style instead, so `order_value` raised `TypeError` unless the caller happened to pass a
style. Again not bond-specific: `order_value` was broken for every asset class. Fixed with
`make_execution_style`, which resolves the shorthand the docstring already documented.

---

## 4. What the live API actually returns

The first version of this connector was written from the endpoint documentation. Probing the live
API with a real token showed that **six** of its assumptions were wrong. Each is now covered by a
test against a recorded payload (`tests/test_finam_bonds_recorded.py`).

| assumption | reality | consequence if missed |
| --- | --- | --- |
| `AllAssets` type is `BOND` | it is **`BONDS`** | matched 0 of 297 947 instruments |
| redemption is a `MATURITY` event | it is a terminal `AMORTIZATION` at 100% | no maturity date; principal counted twice |
| `new_face_value` is the nominal after the instalment | it is **today's** outstanding, identical on every event | face value frozen for the rest of the bond's life |
| `coupon_details.face_value` is the coupon's basis | today's outstanding again | amortizing issues understated by whatever was repaid |
| `lot_size` is a usable nominal | it is the **trading lot** | 1.0 for an OFZ with a 1000 nominal, 1000.0 for RUS-30 with a nominal of 1.0 — wrong in both directions |
| `pagination.has_next` drives paging | never populated; `limit` is required and the window defaults to ~1 year | a bond issued in 2021 returns its last two coupons |

Two further behaviours, both now reported rather than silently absorbed:

* **A coupon of zero means "rate not fixed yet".** Floaters publish future coupon dates with a
  value and percentage of zero. Reading the chronologically last coupon reported every floating-rate
  bond as paying nothing; the rate is now taken from the last coupon that was actually fixed, and
  the count of unfixed ones is logged per issue.
* **`sort_direction` is accepted and ignored.** Results are sorted client-side.

### The one hard limitation

**A redeemed issue cannot be ingested.** Every archived listing sampled returned nothing from both
calendar endpoints — no schedule, no nominal, no maturity. Backtests are therefore limited to bonds
still listed; their history is complete, but a universe free of survivorship bias is not obtainable
from this API. `include_matured` now defaults to False because asking costs two requests per bond
and returns nothing.

---

## 5. The data in `data/assets.sqlite`

The database carries **eleven bonds and 380 schedule events**:

**Six real MOEX issues**, ingested from the live API and chosen to span the shapes that behave
differently — two OFZ bullets of different duration, an amortizing OFZ, two amortizing corporate
floaters, and a dollar eurobond with a 1.00 nominal:

| ticker | nominal | maturity | rate | notes |
| --- | --- | --- | --- | --- |
| `SU26212RMFS9` | 1000 RUB | 2028-01-19 | 7.05% | bullet |
| `RU000A0JR4U9` | 1000 RUB | 2028-10-30 | 10.74% | amortizing floater |
| `XS0114288789` | 1.00 USD | 2030-03-31 | 7.50% | dollar eurobond, 46 instalments |
| `RU000A0JRU20` | 1000 RUB | 2031-09-26 | 8.91% | amortizing floater |
| `SU46020RMFS2` | 1000 RUB | 2036-02-06 | 6.90% | amortizing OFZ |
| `SU26238RMFS4` | 1000 RUB | 2041-05-15 | 7.10% | long bullet |

Daily bars for all six are in the `finam_bonds_daily` bundle (2024-01 to 2026-08).

**Five synthetic demo issues** (`ZL` tickers, `DEMOBOND` identifiers) remain so the examples and
the end-to-end tests run with no token: a plain coupon bond, a zero-coupon control, an amortizing
issue, one with a put window, and one that matures inside the example window.

---

## 6. Verification against real data

`examples/bonds/run_real.py` buys ОФЗ 26238 on 2024-01-03 and holds to 2026-08-01. Reconciled by
hand against the raw schedule and price series:

```
bought 1342 bonds at 66.500% = 670.45 RUB each (665.00 clean + 5.45 accrued)
invested                899,743.90
coupons   1342 x 177.00     237,534.00     (five coupons of 35.40, from the ingested schedule)
price     1342 x -122.27   -164,086.34     (66.500% -> 54.273%, on a 1000 nominal)
accrued returned at exit     15,139.53
expected final            1,081,273.29   (+8.13%)
backtest final            1,080,560.53   (+8.06%)
```

The 713-rouble gap is exactly the modelled friction: 270 of commission (3bp of turnover) plus 450
of slippage (5bp on the entry). The engine reproduces the instrument.

It is also the case worth reading: the clean price **fell 18%** over the holding period and the
position still ends ahead, because the coupons more than covered it. A backtest that does not pay
the schedule reports this position as a loss.

## 7. Cross-asset: equities, bonds and futures in one portfolio

Three classes settle by rules that contradict each other while sharing one cash balance:

| class | cash paid on opening | position value | exposure |
| --- | --- | --- | --- |
| equity | `price × amount` | `price × amount` | same as value |
| bond | `dirty price × amount` | dirty value | same as value |
| futures | **nothing** — margin only | **zero** | `price × multiplier × qty` |

`examples/cross_asset/` holds three strategies on real MOEX data (SBER, ОФЗ 26238, Si futures) —
equity+bond, bond+futures, and all three — and `tests/test_cross_asset.py` pins the invariants:
each class pays for itself by its own rule, a futures mark settles as cash while an equity's does
not, a bond mark is read against its face value, margin is charged only to the futures leg, and a
coupon and a dividend falling on the same session are both paid.

```
x01_equity_and_bond     523 sessions   2 trades   +7.86%   Bond, Equity
x02_bond_and_futures    523 sessions  11 trades  +32.96%   Bond, FuturesContract
x03_all_three           523 sessions  83 trades  +34.56%   Bond, Equity, FuturesContract
```

Checked on the real `x03` run: `cash + positions_value == portfolio_value` on **all 523 sessions**,
cash never went negative, and all three classes were held simultaneously throughout.

### Three bugs this exposed

**A ticker is unique only within an asset class — resolving by name alone returns the wrong
instrument.** The shipped database holds 168 tickers that exist under two classes: `SiH5@RTSX` is a
futures contract and also an equity, because an equity vendor listed it that way. A bundle ingest
that resolves symbols by name tagged the futures bars with the *equity's* sid, and the strategy
then found no prices for the contract it was holding and silently traded nothing.

Fixed on two levels: `ingest_market_data` now accepts the resolved listings (`assets=...`), which
removes the guess entirely; and asking the database to choose between candidate classes raises
`AmbiguousSymbol` instead of taking the first match.

**One bundle could not hold more than one asset class.** `ingest_market_data_bundle` resolved every
symbol under a single `asset_type` and raised on the rest, so a cross-asset strategy — which reads
from one market data source — had no way to get its instruments into one bundle. It now accepts a
sequence of types, and the listings themselves.

**Expired positions were never liquidated — pre-existing, affects futures generally.**
`PositionTracker.maybe_create_close_position_transaction` looked the position up as
`self.positions.get(asset)`, but `positions` is nested `{exchange: {account: {asset: Position}}}`,
so an asset key never matched and the method always returned `None`. Auto-close silently did
nothing: an expired contract stayed on the books at a stale mark for the rest of the run, still
counted in exposure and leverage. (The same method also read a `self.data_bundle` that does not
exist on that object, which would have raised had the lookup ever succeeded.)

Found by checking the real cross-asset run: `x03` ended holding three contracts that had expired
months earlier. After the fix its final book is the equity and the bond alone, and futures position
rows over the run drop from 892 to 497. Portfolio value is unchanged — a cash-settled contract is
closed at its last mark, so the correction is to the bookkeeping, not to P&L. Covered by four tests
in `tests/test_futures_acceptance.py`.

---

## 8. Open questions and known gaps

- **Floaters.** Coupons whose rate the vendor has not fixed are ingested as zero-value events and
  pay nothing. Fixed-coupon issues are complete; a floating-rate study needs a rate source ziplime
  does not have.
- **Survivorship.** Only listed issues can be ingested — see §4.
- **Nominal when the calendar never states one.** Falls back to 1000 and logs a warning.
- **`RUS-30` does not reconcile.** Its 47 published instalments sum to 1.08 against a nominal it
  reports as 1.00. The redemption date is taken from the end of the schedule (correct), but the
  outstanding principal derived from the instalments carries that 8% error. The ingest warns.
- **No credit risk, and offers are not exercised.** Both reported by `context.realism_warnings()`.
- **Single-currency cash.** Unchanged from the futures work: the database now holds both rouble and
  dollar bonds, and a portfolio mixing them would add P&L in two currencies. The cross-asset
  examples stay in roubles throughout for this reason.
- **`data.current` is not positional.** It takes a set of assets and omits any with no bar today,
  so reading its rows by position can pair a price with the wrong instrument. Every example keys on
  `sid`; nothing in the API enforces it.
