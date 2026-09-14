# Options

Four runners live here, and they differ in where the option prices come from rather than in what
the engine does with them.

| | chain | prices | reach | needs |
|---|---|---|---|---|
| `run_all.py` (o01, o02) | **synthetic**, generated over real SPY bars | modelled | any window | nothing |
| `run_0dte_straddle.py` (o03, o04) | **real** OPRA, over gRPC | real | days | `GRPC_TOKEN` |
| `run_yahoo_spread.py` (o05) | **real** OPRA, over Yahoo | real | ~a month daily, a session intraday | nothing |
| `run_structures.py` (o05–o09) | **real** OPRA, over Yahoo, built once and shared | real | ~a month daily | nothing |

```
python examples/options/run_all.py              # synthetic 0DTE chain
python examples/options/run_all.py --only o01
python examples/options/run_0dte_straddle.py    # real 0DTE, needs the gRPC feed
python examples/options/run_yahoo_spread.py     # real chain, free, no credentials
python examples/options/run_structures.py       # five structures over one chain, side by side
python examples/options/run_structures.py --only o06
```

The synthetic source marks itself `is_real_market_data = False`, and
`refuse_performance_claims` stops anything reporting a return on it. The two real ones do not, and
mean it.

**Both real sources read the chain as it stands now.** A contract that has already expired is gone
from it, so a window in the past sees only the contracts that outlived it. The gRPC source can
reach expired contracts through a second lookup (`ExpiredAwareFeed`); Yahoo cannot, which is why
`run_yahoo_spread.py` trades an expiry that has not yet passed.

---

## The option prices are not a market

They are generated from the underlying's bars and a volatility model. They exist so that option
machinery — chain selection, multi-leg structures, Greeks, expiry settlement, order plumbing — can
be built and tested against something with the right *shape*.

**No number produced from them describes what a strategy would have earned.** Not the return, not
the Sharpe ratio, not the drawdown. A premium-selling strategy will look profitable on this data
because the model prices options with a variance risk premium and then delivers exactly the
model's own volatility. That is a tautology, not a result.

This is enforced rather than requested: `SyntheticOptionChainSource.is_real_market_data` is
`False`, `refuse_performance_claims()` raises on it, and `run_all.py` has no return column.

What the generator *does* reproduce, because option code has to be correct against all of it:

| Property | Why it is in the model |
|---|---|
| A chain listed each session, expiring the same session | Contracts appear, trade, expire and settle every day — the whole 0DTE lifecycle, exercised five times in a five-session run |
| Prices decaying to exactly intrinsic at the closing bar | The settlement value and the last mark have to agree, or the expiry sweep silently disagrees with the data |
| Time to expiry measured in minutes | An option with five minutes left is worth an eighth of what a whole-day model says |
| A put-skewed smile whose wings turn up | Without skew a put spread and a call spread are mirror images; without curvature a condor's long wings come out free, turning defined risk into naked risk in the arithmetic |
| An at-the-money level tracking the underlying's realised volatility | Otherwise every regime prices identically, which is the one thing a development harness must not do |
| A put's high taken from the underlying's **low** | Wrong in every hand-rolled option fixture, and it silently inverts stop logic on half the chain |
| Volume and open interest peaking at the money and building through the session | So liquidity-aware selection has something to select on |

What it does not reproduce: the term structure (there is one expiry), the intraday path of implied
volatility, jumps around scheduled events, market makers widening or pulling quotes, and any
relationship between the skew and the underlying's next move.

---

## Plugging in real data

`ziplime.data.data_sources.options.source.OptionChainSource` is the seam. It has two methods:

```python
async def contracts(underlying_symbol, mic, sessions) -> list[ContractSpec]   # what existed
async def bars(contracts, timestamps) -> pl.DataFrame                          # what it was worth
```

Three implementations exist. `SyntheticOptionChainSource` generates a chain over real underlying
bars; `GrpcOptionChainSource` reads a paid feed; `YahooOptionChainSource` reads Yahoo Finance and
needs no credentials at all. Everything downstream is unchanged between them:
`build_option_bundle` writes the contracts, mints the sids, assembles the bundle, and a strategy
does not know which source it is running on — `o03` and `o05` differ in their structure, not in
their plumbing. Set `is_real_market_data = True` and the performance guard stops firing.

`tests/test_options_end_to_end.py::ChainSourceSeamTests` is a second implementation of the
interface, run through the same ingest and the same simulation, so the seam is demonstrated rather
than asserted.

Three things a real feed does that the interface deliberately allows for:

- **Strikes are listed, not computed.** A real chain is wider than anyone trades and asymmetric.
  `contracts()` returns what a source knows about, not a rule.
- **Bars have gaps.** A far wing stops being quoted for minutes at a time; `bars()` may return no
  row, and the engine already handles that on the same path as a delisting.
- **`implied_volatility` is a quote, not a model output.** A real source fills it from its own
  quotes; strategies read the column either way.

---

## The strategies

| File | Structure | What it demonstrates |
|---|---|---|
| `o01_iron_condor.py` | iron condor | The chain listed and reached each session, strike selection by offset from the money, a four-leg structure sized on its own known maximum loss, held to settlement |
| `o02_delta_selected_strangle.py` | strangle | Selection by delta, reading a contract's implied volatility from the data, tracking the book's Greeks bar by bar, and flattening before the close rather than settling |
| `o03_vectorised_0dte_straddle.py` | straddle | Real 0DTE contracts, entered on a signal computed across the whole window before the run starts |
| `o04_event_0dte_straddle.py` | straddle | The same trade decided bar by bar. `o03` and `o04` fill identically, which is the point of the pair |
| `o05_yahoo_call_spread.py` | vertical spread | Two legs of opposite sign on free data, held with a stop measured on the underlying |
| `o06_long_butterfly.py` | butterfly | A pin bet paid for up front: three strikes, a 1/−2/1 body, risk capped at the debit |
| `o07_short_iron_butterfly.py` | iron butterfly | The same view taken as a credit, with wings that make the worst case finite |
| `o08_risk_reversal.py` | risk reversal | A structure that costs nothing and risks everything — and refuses itself when `max_loss` breaks a stated budget |
| `o09_call_ratio_spread.py` | ratio spread | `max_loss` of `-inf`, printed rather than hidden, plus the leg-rounding check that stops a ratio spread becoming naked shorts |

**o06 and o07 are the pair worth reading together.** Same opinion — "it finishes right about here"
— opposite cash flow, and the difference shows up in what happens when the opinion is wrong.
**o08 and o09 are the pair worth reading after them**, because both look nearly free at entry and
both are where the money actually is at stake. `run_structures.py` prints all five over one chain
so those columns sit next to each other:

```
     structure                          legs      net   max gain    max loss       P&L
o05  bull call spread 310/320              2      482        518        -482       335
o06  long butterfly 320/330/340            3      157        843        -157         5
o07  iron butterfly 300/310/320            4     -885        885        -115       -55
o08  -                                     0        -          -           -         0
o09  1x2 call ratio spread 310/325         2      -50      1,550   UNBOUNDED      -398
```

`net` is positive for a debit and negative for a credit. o08's row is empty because the structure
**refused itself**: `max_loss` came to −31,669 against a `LOSS_BUDGET` of 25,000. That is the
example working, not failing.

None of these is a trading idea, and sixteen sessions of one underlying is not evidence about any
of them. The last two columns are what the table is for.

---

## Things worth knowing before writing your own

**The chain has to be fetched every session.** `SPY240614C00523000` exists for one day. There is
nothing to resolve in `initialize` and hold onto:

```python
chain = await context.option_chain(context.underlying)   # defaults to today's expiry
chain = await context.option_chain(context.underlying, expiration_date=EXPIRY)   # or a later one
```

**Build structures with the constructors rather than by hand.** `ziplime.finance.options.strategies`
has `straddle`, `strangle`, `vertical_spread`, `butterfly`, `iron_condor`, `iron_butterfly`,
`risk_reversal`, `ratio_spread` and `condor_around`. Each validates its own shape — an iron
butterfly whose short legs sit at different strikes is an iron condor and is refused as one — and
each returns an `OptionStrategy` that prices, sizes and reports itself:

```python
structure = butterfly(lower=low, body=mid, upper=high)
net = structure.net_premium(premiums)             # per unit; positive a debit, negative a credit
structure.max_loss(net), structure.max_profit(net), structure.breakevens(net)
for listing, amount in structure.orders(quantity):
    await context.order(asset=listing, amount=amount, style=MarketOrder())
```

**`orders()` drops a leg that rounds to zero and keeps the rest.** At whole quantities nothing
rounds away, but a fractional size can hand you the short legs of a structure without its long
one. Check the shape against what you asked for — `o09` does.

**Time to expiry is minutes, not days.** `context.time_to_expiry(listing)` measures to the
expiration session's close. At 09:31 it is about 0.0018 years; at 15:55 it is a tenth of that.

**Greeks are `vollib`'s conventions**, which are the practitioner ones: theta per calendar day,
vega per volatility point, rho per percentage point. On a 0DTE contract a one-day theta can exceed
the option's own price — it is a rate, not a prediction.

**Every cash amount goes through the multiplier.** A contract quoted at 1.20 costs 120.
`OptionStrategy` already reports premium, maximum profit and maximum loss in money, so size on
those rather than on the quote.

**Expiry settles at intrinsic value, not at the last quote**, and the engine does it without being
asked. A wing that stopped being quoted at lunchtime settles at zero; a strike that finishes a dime
in the money pays ten dollars a contract.

**Commissions dominate.** `PerOptionContract` charges 0.65 plus 0.05 of fees per contract, so a
four-leg structure costs about 2.80 to open and the same to close. Against a credit of 100 that is
five percent of the edge, per side, every session.

## Installing

The pricing needs `vollib`, which ziplime does not install by default:

```
poetry install --with options
```
