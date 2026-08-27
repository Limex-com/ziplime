# Futures strategy examples

Twenty short strategies on MOEX FORTS futures, each showing one thing about trading futures in
ziplime rather than trying to make money.

## Running them

```bash
# once: instruments and bars for the roots the examples use (Si, SR, GZ, MX, GL)
export FINAM_API_SECRET=tapi_sk_...
cd examples
python ingest_assets_data_finam.py
python ingest_data_finam_futures.py

# and, for the cross-venue example, natural gas from MOEX and NYMEX in its own bundle
python ingest_natgas_arbitrage_data.py

# then: all twenty, with a summary table
cd futures_strategies
python run_all.py

# or a few by name
python run_all.py --only s06 s16
```

`run_all.py` fails if any strategy errors, places no trades, or returns exactly zero — an example
that silently does nothing is not an example.

## What each one shows

| | strategy | shows |
|---|---|---|
| 01 | buy and roll | the basic shape: a continuous future says *which* contract, you trade that contract |
| 02 | short and roll | futures P&L is symmetric; no borrow, same margin either way |
| 03 | calendar roll style | rolling on a schedule instead of on liquidity, via `roll_finder_settings` |
| 04 | signal on adjusted series | signal from the adjusted chain, fill on the raw contract — the separation that matters |
| 05 | back month | `offset=1` walks down the chain |
| 06 | calendar spread | two offsets held against each other; nearly flat outright, exposed to the curve |
| 07 | momentum crossover | a trend rule that a roll gap cannot trigger falsely |
| 08 | notional sizing | `contracts_for_notional` — price × multiplier, rounded to whole contracts |
| 09 | volatility target | constant risk rather than constant size |
| 10 | margin-aware sizing | `FixedRateFuturesMarginModel` plus `futures_margin_requirement()` |
| 11 | margin per root | `PerRootFuturesMarginModel`, the way an exchange quotes margin |
| 12 | expiry-aware flat | standing aside in the thin sessions before expiry |
| 13 | chain liquidity | `data.current_chain` — trade where the volume actually is |
| 14 | stop loss | a stop in points, converted to money by the multiplier |
| 15 | multi-root basket | four chains, equal *notional* — not equal contract counts |
| 16 | cross-root spread | a notional-matched pair across two roots |
| 17 | mean reversion | a z-score that needs a continuous history to mean anything |
| 18 | Donchian breakout | channel breakout on the adjusted chain |
| 19 | roll cost aware | measuring what carrying the position forward costs |
| 20 | realism report | `realism_warnings()` — what the simulation does *not* model |
| 21 | natgas cross-venue basis | MOEX against NYMEX: paired by delivery month, matched by notional, exited before the deliverable leg's notice date, margin reported per currency |

## Helpers these examples use

Added for futures work, available on `context` inside an algorithm:

- `continuous_future(root, offset, roll, adjustment)` — a chain specifier
- `data.current_contract(chain)` / `data.current_chain(chain)` — the tradable contract(s) today
- `contracts_for_notional(asset, notional, price)` — whole contracts for a target exposure
- `notional_exposure(asset, amount, price)` — `amount × price × multiplier`
- `futures_margin_requirement(maintenance=False, currency=None)` and `futures_margin_by_currency()`
  — MOEX collects roubles even for its dollar-quoted contracts, so a cross-venue book has two
  requirements and they are not added together
- `contract.asset.is_deliverable` — a physically delivered contract has to leave the book before
  its notice date
- `realism_warnings()` — effects this configuration does not reproduce

## Reading the numbers

The returns are what these rules did on Si, SBRF, GAZR, MIX and GOLD between June 2021 and June
2026 (and, for `s21`, on natural gas from September 2023), after commission and slippage. They are illustrations of mechanics, not strategy
recommendations; several lose money, which is the honest outcome for a rule this simple. Every run
is subject to the realism gaps `s20` prints — most importantly that same-bar execution is on by
default and margin is not enforced.
