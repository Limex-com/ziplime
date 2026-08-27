# Cross-asset examples

Three strategies holding more than one asset class in one portfolio, on real MOEX data: Sberbank
shares, a long OFZ and USD/RUB futures.

## Running them

```bash
export FINAM_API_SECRET=tapi_sk_...

# reference data, once per class
python examples/ingest_assets_data_finam_bonds.py SU26238RMFS4
python examples/ingest_assets_data_finam.py                    # futures chains

# then one bundle holding all three classes
python examples/cross_asset/ingest_cross_asset_data.py

cd examples/cross_asset && python run_all.py
```

`run_all.py` fails a strategy that declares a class and never trades it — a cross-asset example
that quietly trades one class is not a cross-asset example.

## What each one shows

| strategy                | classes                 | shows                                              |
| ----------------------- | ----------------------- | -------------------------------------------------- |
| `x01_equity_and_bond`   | equity + bond           | 60/40 split **by money**, not by count              |
| `x02_bond_and_futures`  | bond + futures          | a position with value next to one with none         |
| `x03_all_three`         | equity + bond + futures | monthly rebalance across all three sizing rules     |

## Why this needs its own examples

The three classes settle by rules that contradict each other, and they share one cash balance:

| class   | cash paid on opening      | position value | exposure                    |
| ------- | ------------------------- | -------------- | --------------------------- |
| equity  | `price × amount`          | `price × amount` | same as value             |
| bond    | `dirty price × amount`    | dirty value    | same as value               |
| futures | **nothing** — margin only | **zero**       | `price × multiplier × qty`  |

Sizing by count instead of by money puts ten times too much into a bond quoted as a percentage of
a 1000-rouble nominal, and an unbounded amount into a future that costs nothing to open.

## Two traps these examples exist to document

**A ticker is unique only within an asset class.** The shipped database has 168 tickers that exist
under two — `SiH5@RTSX` is a futures contract *and*, because an equity vendor listed it that way,
an equity. Resolving a symbol by name alone can return the wrong instrument, and the failure is
silent: the bars get tagged with the other instrument's sid and the strategy finds no prices for
what it is holding. Pass resolved listings to the ingest (`assets=...`), as
`ingest_cross_asset_data.py` does. Asking the database to choose between classes raises
`AmbiguousSymbol` rather than guessing.

**`data.current` is not positional.** It takes a set of assets, so the order of its rows is not the
order of the request, and an asset with no bar today is simply absent. Key on `sid`:

```python
quotes = await data.current(assets=[equity, bond, contract], fields=["price"])
prices = dict(zip(quotes["sid"].to_list(), quotes["price"].to_list()))
equity_quote = prices.get(equity.sid)
```

## Verified

On the real `x03` run: `cash + positions_value == portfolio_value` on all 523 sessions, cash never
negative, all three classes held throughout, no errors. `tests/test_cross_asset.py` pins the same
invariants on synthetic fixtures, where the numbers can be asserted exactly.

## One bundle, not three

A simulation reads from a single market data source, so every instrument a strategy touches has to
be in the same bundle. `ingest_market_data` accepts a sequence of asset types and, better, the
resolved listings themselves.
