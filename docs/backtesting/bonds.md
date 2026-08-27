# Trading bonds

Ziplime trades bonds alongside equities and futures. A bond is not a share with a coupon bolted
on, though: three of its properties break assumptions the rest of a backtester makes, and all three
are handled by the engine so that a strategy can order a bond the same way it orders anything else.

## What makes a bond different

### The quote is not the price

MOEX — like most exchanges — quotes a bond as a **percentage of its face value**. A quote of
`98.42` on a 1000-rouble nominal means 984.20 roubles per bond, not 98.42. Reading the quote as
money understates every bond position by roughly `face_value / 100`: a factor of ten on a standard
nominal.

`Bond.price_quotation` records which convention an issue uses, and the ledger converts on every
transaction and every mark.

```python
from ziplime.assets.domain.price_quotation import PriceQuotation

PriceQuotation.PERCENT_OF_FACE.money_price(98.42, 1000.0)   # 984.20
PriceQuotation.MONEY.money_price(984.20, 1000.0)            # 984.20
```

### The buyer pays accrued interest

Between coupons the buyer owes the seller the part of the coupon that has already accrued — НКД on
MOEX, accrued interest elsewhere. The tape carries the **clean** price; settlement happens at the
**dirty** one:

```
dirty price = clean price × face value / 100 + accrued interest
```

Ignoring it makes every purchase look cheaper and every sale poorer than it was. Over a year of
round trips on an 8% coupon that averages about 4% of face value per trade.

Accrual is computed from the coupon schedule when one is stored — elapsed days over the length of
the coupon period, which is exactly how an exchange publishes it — and from the bond's own rate and
day-count convention when one is not.

### The principal is not constant

An amortizing issue repays face value in instalments before maturity. Two things follow: the
coupon shrinks, because it is computed on what is still outstanding, and the same *quote* is worth
less money, because it is a percentage of a smaller nominal.

`Bond.face_value` is the nominal **at issue**; what is outstanding today comes from the schedule.

## Getting bonds into the asset database

### From the Finam Trade API

```bash
export FINAM_API_SECRET=tapi_sk_...
# reference data: terms and the coupon schedule
python examples/ingest_assets_data_finam_bonds.py SU26238RMFS4 SU26212RMFS9
# then the bars, which are keyed by the sids the step above assigned
python examples/ingest_data_finam_bonds.py SU26238RMFS4 SU26212RMFS9
```

Three endpoints carry everything, and none carries all of it:

- `GET /v1/assets/all` enumerates every instrument, archived ones included, with `type: BONDS` and
  little else — ticker, MIC, ISIN, name.
- `GET /v1/bonds/past` returns the calendar already paid.
- `GET /v1/bonds/future` returns the calendar still to come, ending in the redemption.

So the **terms are read off the calendar**: the nominal from
`amortization_details.initial_face_value`, the frequency from the spacing between payments, the
rate from the last published percentage, maturity from the final event. `BondTerms.inferred` names
every field that was derived rather than stated.

#### What the payloads actually mean

Four fields do not mean what their names suggest. Each was established against the live API, and
each silently corrupts a bond if taken at face value:

| field | looks like | actually is |
| --- | --- | --- |
| `type` in `AllAssets` | `BOND` | **`BONDS`** — the singular matches nothing |
| a terminal `AMORTIZATION` | an instalment | **the redemption**; `MATURITY` is never sent |
| `new_face_value` | the nominal after the instalment | **today's** outstanding nominal, identical on every event |
| `coupon_details.face_value` | the basis the coupon was computed on | today's outstanding nominal again |
| `lot_size` in `GetAsset` | the nominal | the **trading lot** — 1.0 for an OFZ with a 1000 nominal, 1000.0 for RUS-30 with a nominal of 1.0 |

Two more behaviours worth knowing:

- **A coupon of zero means "not fixed yet".** Floating-rate issues publish their future coupon
  dates with a value and a percentage of zero. They are ingested as dated events that pay nothing,
  and the ingest warns per issue.
- **`limit` is required and the default window is about a year.** Omitting `limit` returns no
  events and no pagination block at all, and without date bounds a bond issued in 2021 returns its
  last two coupons. `pagination.has_next` is never populated. The client handles all three.

#### The one hard limitation

**A redeemed issue cannot be ingested.** Both calendar endpoints answer with nothing for an
archived listing, so there is no schedule, no nominal and no maturity to derive. Backtests are
therefore limited to bonds that are still listed — their *history* is complete, which is what
matters for a study of the past, but a survivorship-free universe of already-matured bonds is not
obtainable from this API.

### The demo universe

For the examples and the tests, `data/assets.sqlite` ships with five synthetic issues — `ZL`
tickers, `DEMOBOND` identifiers — covering a plain coupon bond, a zero-coupon bond, an amortizing
bond, one with a put window, and one that matures inside the example window. Re-seed with:

```bash
python examples/bonds/seed_demo_bonds.py
```

## Writing a bond strategy

```python
from ziplime.finance.execution import MarketOrder


async def initialize(context):
    context.bond = await context.bond_symbol("ZLB26@MISX")
    context.bought = False


async def handle_data(context, data):
    if context.bought:
        return
    quote = (await data.current(assets=[context.bond], fields=["price"]))["price"][0]
    if quote is None:
        return

    # What one bond actually costs: neither the quote nor the nominal.
    per_bond = await context.bond_dirty_price(context.bond, quote)
    amount = int(context.portfolio.cash * 0.9 / per_bond)

    await context.order(asset=context.bond, amount=amount, style=MarketOrder())
    context.bought = True
```

### The API

| call                                            | returns                                              |
| ----------------------------------------------- | ---------------------------------------------------- |
| `bond_symbol(symbol, mic=None)`                 | the bond listing for a ticker                         |
| `bond_dirty_price(asset, quote, dt=None)`       | money one bond changes hands for                      |
| `accrued_interest(asset, dt=None)`              | coupon accrued per bond (НКД)                         |
| `bond_face_value(asset, dt=None)`               | principal outstanding, after amortization             |
| `bond_schedule(asset)`                          | every coupon, instalment, offer and redemption        |
| `bond_current_yield(asset, quote, dt=None)`     | annual coupons over the dirty price                   |
| `bond_yield_to_maturity(asset, quote, dt=None)` | simple annualised return of holding to maturity       |

`order_value`, `order_percent` and `order_target_value` all size bonds correctly on their own —
they convert through the dirty price rather than the quote.

## What the engine does for you

- **Coupons and amortization instalments** are earned on the record date and paid on the payment
  date, so selling in between does not forfeit the payment and a short position is charged for it.
  A record date that falls on a weekend or an exchange holiday is still picked up on the next
  session.
- **Redemption at maturity** books as a closing trade at par against the principal still
  outstanding, not at whatever the last bar printed. A bond that last traded at 95 still repays
  100% of face.
- **Position value** is the dirty value of the holding, so exposure and leverage figures are in
  money rather than in quote points.
- **Commission** defaults to `PerBondTurnover`, a fraction of the money transacted — which is how
  bond desks bill, and the only model that charges the right amount on a percent-of-face quote.

## What is not modelled

`context.realism_warnings()` reports these at runtime:

- **No credit risk.** An issuer always pays; default and restructuring are not simulated.
- **Offers are not exercised.** A put or call window is recorded and left there; a strategy can
  read it and trade around it, but nothing exercises it automatically.
- **A floater's unfixed coupons pay nothing.** Where the vendor has published a coupon date but
  not its rate, the event is ingested with a value of zero and pays zero. That understates a
  floating-rate bond over any window reaching past its last fixed coupon. Fixed-coupon issues are
  unaffected — their whole schedule is published.
- **Survivorship.** Only listed bonds can be ingested, so a universe built from this connector
  contains no issues that have already been redeemed.

## See also

- `examples/bonds/README.md` — six worked strategies
- `ziplime.finance.bonds` — the pricing arithmetic, and `BondBook`, which holds the schedules
- `ziplime.data.data_sources.finam.finam_bonds` — how the Finam payloads become bonds
