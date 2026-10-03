# Futures examples

Seven strategies on **real Yahoo Finance futures chains** — individual dated contracts, with real
expiration dates and a real term structure. No credentials.

## Running them

```bash
python examples/futures/ingest_futures_data_yahoo.py   # discover the chains, then the bars
cd examples/futures && python run_all.py
```

## What Yahoo gives, and what it does not

Yahoo carries **individual dated contracts** under `ROOT + month code + 2-digit year + exchange
suffix` — `ESZ26.CME`, `CLX26.NYM`. Each has its own price history, its own expiration date and its
own place on the curve, so rolls, calendar spreads and term structure all work against real data.

Ingesting the four roots below gives 64 contracts: a quarterly S&P chain 5 deep and monthly energy
and grain chains up to 24 deep, each contract with several years of daily bars.

Three things are missing or unreliable, and each one shapes what the examples can show.

**There is no endpoint that lists a chain.** The contracts have to be *discovered*: candidate
tickers are generated from each root's listing cycle and probed, and the ones that answer with
prices are the live chain. That is what `get_futures` does.

**Expired contracts are removed.** `ESZ24.CME` returns 404, and so does everything that has already
settled. So a chain runs *forward* from today, not back through history — and that splits the
examples into two windows:

| window | used by | why |
| --- | --- | --- |
| `START`…`END` (2023-09 … 2026-09) | curve work — term structure, spreads, baskets | every contract in the chain is real and their prices are simultaneous, so the curve is sound over the whole history |
| `ROLL_START`…`ROLL_END` (2026-06 … 2026-09) | roll work | only here is the front of the stored chain the market's *real* front month; in 2023 the real front month was a contract Yahoo has since deleted |

Conflating those two produces a backtest that looks right and is not: a strategy holding "the front
contract" over the long window would have held a 2026 contract throughout 2023, which no trader did.

**Volume is thin or absent.** The simulation will not fill an order on a bar with no volume — no
trade happened, so none could have — and Yahoo reports none on many sessions:

| root | sessions with volume | consequence |
| --- | --- | --- |
| ES | 0.4 % – 26 % | effectively untradable; an order sits unfilled for months |
| CL | 26 % – 99 % | tradable, with fills arriving over several sessions |
| NG | 51 % – 96 % | tradable |
| ZC | 10 % – 96 % | tradable |

So `f04` works its order to target over 48 days instead of sending it once, and the S&P chain is
read but never traded. It also means a **volume-based roll finder is unusable here** — worse than
unusable, since on WTI the reported volume does not track the front month, so it would roll to the
wrong contract. `CalendarRollFinder` is the one to use with this data.

**And the contract specification is absent entirely.** The multiplier — the number that turns a
quote into money — is not in the payload, so it comes from a table in
`ziplime/data/data_sources/yahoo/yahoo_futures.py`, transcribed from the exchanges' published terms
and checked in `tests/test_yahoo_futures.py` against the notional and tick value each implies. An
E-mini S&P contract is about $296 000 of exposure at 5 916 index points and its tick is $12.50 —
figures anyone who trades it recognises. That check matters: a wrong multiplier passes every type
check and misstates every position by a factor of a hundred.

## What each one shows

| strategy | root | shows |
| --- | --- | --- |
| `f01_notional_sizing` | CL | opening a future moves no cash and creates no position value — only exposure |
| `f02_margin_aware_sizing` | CL | margin, not cash, is what limits a futures position; the budget binds and cuts it |
| `f03_variation_margin` | NG | P&L settles into cash every day rather than sitting in the position |
| `f04_cross_market_basket` | CL, NG, ZC | equal exposure across multipliers of 1 000, 10 000 and 50 |
| `f05_term_structure` | CL, ES | reading the real curve — and why contango means opposite things in crude and in an index |
| `f06_calendar_roll` | CL | rolling the front contract before delivery, and what the roll costs |
| `f07_calendar_spread` | CL | trading the shape of the curve rather than its level |

`f04` is the one that makes the multiplier concrete: equal exposure means 4 contracts of one market
and 13 of another. Sizing by count instead would put two hundred times more risk in gas than in
corn.

`f06` performs a genuine roll: WTI's October contract stops being the front month on 2026-08-27,
and the position moves to November at a $1.56 gain per barrel — because the curve is backwardated,
so the long rolls *down* into a cheaper contract.

Results are not recommendations. `f02` in particular is a leveraged directional position; what it
demonstrates is the margin check binding and cutting the size.

## Four things worth copying

**Read the position, never the order.** These examples print what the ledger holds, not what they
asked for. On thin contracts those differ for a long time: `f01` orders 7 contracts and is filled 5.
An example that printed its intent would have misreported the portfolio for as long as the order
took to work.

**Check `get_open_orders` before topping up.** An unfilled order is not in the position, so
re-sending the shortfall each session stacks order on order and buys a multiple of the target.
`f04` shows the guard.

**Zero is not a price.** A contract that had not listed yet when the window opened is forward-filled
with zeros until its first real bar, so guard on `price > 0` and not merely on `price is not None`.
The chain is ordered by expiration, so `chain[0]` is the front contract — but not always the front
contract that is *trading*.

**`data.current` is not positional.** It takes a set of assets, so the order of its rows is not the
order of the request. Key on `sid`:

```python
quotes = await data.current(assets=context.contracts, fields=["price"])
prices = dict(zip(quotes["sid"].to_list(), quotes["price"].to_list()))
```
