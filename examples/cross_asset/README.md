# Cross-asset examples

Holding more than one asset class in one portfolio: equities from Yahoo Finance alongside a bond.

## Running them

```bash
# once: the equity reference data, and the synthetic bond universe
python examples/ingest_assets_data_yahoo_finance.py
python examples/bonds/seed_demo_bonds.py

cd examples/cross_asset && python run_all.py
```

No credentials are needed. Equity bars come from Yahoo Finance, which is free; the bonds are
synthetic, because no free source publishes a coupon schedule — see
`ziplime.data.data_sources.demo_bonds`.

`run_all.py` fails a strategy that declares an asset class and never trades it — a cross-asset
example that quietly trades one class is not a cross-asset example.

## What it shows

| strategy | classes | shows |
| --- | --- | --- |
| `x01_equity_and_bond` | equity + bond | 60/40 split **by money**, not by count |

## Why this needs its own example

The two classes settle by rules that contradict each other, and they share one cash balance:

| class | cash paid on opening | position value | exposure |
| --- | --- | --- | --- |
| equity | `price × amount` | `price × amount` | same as value |
| bond | `dirty price × amount` | dirty value | same as value |
| futures | **nothing** — margin only | **zero** | `price × multiplier × qty` |

Futures are listed for completeness: the engine supports them and the acceptance suite covers
them, but a futures example needs a chain with real expirations and volumes, which no free source
provides.

Sizing by count instead of by money puts ten times too much into a bond quoted as a percentage of
a 1000-unit nominal.

## Two traps these examples exist to document

**A ticker is unique only within an asset class.** A database carrying more than one class will
contain tickers that exist under two of them — an equity vendor listing a futures ticker as an
equity is the usual way it happens. Resolving a symbol by name alone can return the wrong
instrument, and the failure is silent: the bars get tagged with the other instrument's sid and the
strategy finds no prices for what it is holding. Pass resolved listings to the ingest
(`assets=...`). Asking the database to choose between classes raises `AmbiguousSymbol` rather than
guessing.

**`data.current` is not positional.** It takes a set of assets, so the order of its rows is not the
order of the request, and an asset with no bar today is simply absent. Key on `sid`:

```python
quotes = await data.current(assets=[equity, bond], fields=["price"])
prices = dict(zip(quotes["sid"].to_list(), quotes["price"].to_list()))
equity_quote = prices.get(equity.sid)
```

## One bundle, not two

A simulation reads from a single market data source, so every instrument a strategy touches has to
be in the same bundle. `_harness.py` builds one by fetching the equity bars from Yahoo and
generating the bond bars, then concatenating them — that part is worth copying if you point these
at real data.
