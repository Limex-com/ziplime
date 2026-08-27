# Bond examples

Six short strategies on MOEX-style bonds, each showing one thing about trading debt in ziplime
rather than trying to make money.

## Running them

The repository ships a small **demo bond universe** already seeded into `data/assets.sqlite`, so
the examples run with no vendor token and no network:

```bash
cd examples/bonds

# only if you have cleared the database: re-seed the demo issues
python seed_demo_bonds.py

# all six, with a summary table
python run_all.py

# or a few by name
python run_all.py --only b01 b02
```

For **real MOEX bonds**, ingest them from the Finam Trade API into the same database and run
`b07`, which trades a real OFZ on ingested prices:

```bash
export FINAM_API_SECRET=tapi_sk_...
python examples/ingest_assets_data_finam_bonds.py SU26238RMFS4     # terms + coupon schedule
python examples/ingest_data_finam_bonds.py       SU26238RMFS4     # daily bars
python examples/bonds/run_real.py
```

`run_all.py` skips real-data strategies, so the demo set keeps working with no token. Only issues
that are **still listed** can be ingested: a redeemed bond has no calendar at either endpoint.

## The demo issues

Synthetic and marked as such — `ZL` tickers, `DEMOBOND` identifiers. They exist so a whole bond
life fits in a test: a coupon every six months, an instalment every year, a redemption at the end.

| ticker  | what it is                                   | why it is here                             |
| ------- | -------------------------------------------- | ------------------------------------------ |
| `ZLB26` | 8.5% semi-annual, repaid in full at maturity | the paying case                            |
| `ZLZ26` | zero-coupon, same dates as `ZLB26`           | the **non**-paying control                 |
| `ZLA27` | 11% quarterly, principal in four instalments | amortization                               |
| `ZLO27` | 12% semi-annual with a put window            | offers (оферта)                            |
| `ZLS25` | 9% semi-annual, matures inside the window    | redemption at par                          |

## What each one shows

| strategy                    | shows                                                                 |
| --------------------------- | --------------------------------------------------------------------- |
| `b01_buy_and_hold_coupon`   | the baseline: buy once, collect every coupon from the schedule         |
| `b02_zero_coupon_discount`  | the same trade with **no payouts at all** — the control for `b01`      |
| `b03_amortized_bond`        | principal returning in instalments; coupons and position value shrink  |
| `b04_hold_to_maturity`      | redemption at par, which is not a trade at the last printed quote      |
| `b05_bond_ladder`           | staggered maturities; rungs redeem one by one with no sell orders      |
| `b06_coupon_capture`        | why buying into a coupon is not free: you pay the accrued interest     |
| `b07_real_ofz_buy_and_hold` | **real MOEX data**: coupons carrying a position through a price fall   |

Run `b01` and `b02` together: they share an issue date, a maturity and a holding period, so the
difference between their results is exactly what the coupon schedule contributes.

`b07` makes the same point on real data. ОФЗ 26238 bought at 66.5% of par in January 2024 was
still only 54.3% in July 2026 — an 18% fall in the clean price — and the position is nonetheless
ahead, because five coupons of 35.40 per bond more than covered it.

## Three things a bond backtest has to get right

Everything in these examples turns on them, and all three are handled by the engine rather than by
the strategies:

1. **The quote is not the price.** A MOEX bond quotes as a percentage of face value, so `98.42`
   on a 1000-rouble nominal is 984.20 roubles. Read as money it understates the position tenfold.
2. **The buyer pays accrued interest (НКД).** The tape carries the *clean* price; settlement
   happens at the *dirty* one. Over a year of round trips this averages half a coupon per trade.
   Ask `await context.bond_dirty_price(bond, quote)` rather than computing it yourself.
3. **The principal is not constant.** An amortizing issue repays face value in instalments, so the
   nominal a quote is a percentage *of* shrinks. `await context.bond_face_value(bond)` reports
   what is outstanding today.

## The bond API in a strategy

```python
bond   = await context.bond_symbol("ZLB26@MISX")     # look up a bond listing
money  = await context.bond_dirty_price(bond, quote) # what one bond costs, in cash
accrued= await context.accrued_interest(bond)        # НКД per bond today
face   = await context.bond_face_value(bond)         # principal outstanding today
events = await context.bond_schedule(bond)           # coupons, amortizations, offers
cy     = await context.bond_current_yield(bond, quote)
ytm    = await context.bond_yield_to_maturity(bond, quote)
```

## What is not modelled

`context.realism_warnings()` reports these at runtime too:

- **No credit risk.** An issuer always pays. A high-yield issue shows its yield without the risk
  that earns it.
- **Offers are not exercised.** A put or call window is in the schedule and a strategy can trade
  around it, but nothing exercises it.
- **Forward coupons are assumed fixed.** Coupons already paid come from the vendor's calendar and
  are exact; coupons still ahead are generated from the last known rate — right for a fixed-coupon
  bond, wrong for a floater.
