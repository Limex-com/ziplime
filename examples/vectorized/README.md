# Vectorised computation in ziplime

Two different mechanisms for two different jobs. They are easy to confuse, and they are wanted in
opposite situations.

| | What is vectorised | Execution | Costs | Futures and options | What it is for |
|---|---|---|---|---|---|
| **Signals inside a run** | the strategy's arithmetic | **ziplime**, bar by bar | ziplime's models | yes | one strategy that has to run faster |
| **Sweep, then report** | the whole run | **ziplime's kernel** | ziplime's models | **no, refused** | hundreds of parameter combinations |

```
python examples/vectorized/signals_in_run.py      # the first
python examples/vectorized/sweep_then_report.py   # the second
```

Both are ziplime all the way down. Neither needs an outside engine: the sweep is computed by
`ziplime.vectorized.kernel`, and `numba` only compiles its inner loop — without it the same loop
runs under the interpreter and produces the same numbers about five times more slowly.

---

# 1. Signals inside a run

A strategy is rarely uniform. Indicators are array work; decisions are not. Position size is
computed from the portfolio value after every preceding trade, an exposure limit depends on what is
already in the book, a futures roll depends on the contract's calendar.

So what is cut here is **the strategy, not the run**. `compute_signals` is evaluated once before the
first bar; `handle_data` reads one row per bar and trades through the ordinary blotter, with the
ordinary slippage and commissions.

```python
WARMUP = 60                                   # sessions of history the indicators need

async def initialize(context):
    context.universe = {"SPY": await context.symbol("SPY", mic="ARCX")}

def compute_signals(context, prices):         # <- one pass over the whole history
    return {
        "fast": prices.close.rolling(20).mean(),
        "slow": prices.close.rolling(60).mean(),
    }

async def handle_data(context, data):         # <- bar by bar, as usual
    if not context.signals.is_ready("slow"):
        return
    if float(context.signals["fast"]["SPY"]) > float(context.signals["slow"]["SPY"]):
        room = 0.6 * context.portfolio.portfolio_value       # this does not vectorise
        ...
```

### What was measured

On `signals_in_run.py`: 1429 sessions, two instruments, four indicators.

```
Time inside the strategy itself (handle_data):
   vectorised     0.73s
   bar by bar     1.70s        2.3x

Time for the whole run:
   vectorised     4.29s
   bar by bar     5.18s        1.2x
```

**The gap between those two numbers matters more than either of them.** The mechanism removes the
cost of the signals, not the cost of the simulation: every session the engine still runs the
ledger, processes dividends and computes metrics, and none of that is touched here. Measured
separately with an empty `handle_data`, the engine's own overhead is 0.32s over 1194 sessions.

Which tells you when this pays off: when the strategy spends appreciably more time in `handle_data`
than the engine spends per bar. Heavy indicators, a wide universe, long windows — yes. One moving
average on two tickers — barely.

### Two guards, without which this is dangerous

Handing user code the whole price history and asking it to be careful is the standard way to get
look-ahead. A backtest that peeks at the future does not crash; it reports a beautiful return that
has nothing to do with reality. Both guards are therefore structural rather than documented.

**The future is unreachable on a bar.** `context.signals` is cut to the simulation clock. On bar `t`
there is no method, property or accessor that returns row `t+1`. Not "protected" — absent.

**The formula's causality is checked.** `compute_signals` is recomputed on truncated history at
several points and compared against the full pass. A signal at bar `t` is honest if and only if
computing it from the first `t` bars alone gives the same number.

```
Signal 'momentum' looks ahead. At bar 240 (2024-03-15) the whole-history computation
gives 1.0, but computing from the first 240 bars alone -- all that was known then --
gives 2.0.
```

Accepted: `rolling`, `expanding`, `shift(+n)`, elementwise arithmetic, `rank(axis=1)` across a row.
Rejected: whole-sample statistics (`mean`, `std`, `quantile`, `rank` down a column), `shift(-n)`,
dividing by `iloc[-1]`.

The check is **sufficient but not complete**: it compares at sampled points. Any accidental leak
shifts nearly every row and is caught; a leak deliberately hidden between the sample points is not.
It is a smoke alarm, not a proof.

### Three ways to trip

**A boolean signal hides the warm-up.** `rolling(20).mean()` is NaN until the window fills, but
`fast > slow` is `False`, because a comparison against NaN is False rather than NaN. Return numbers
and compare on the bar, or `is_ready` has nothing to read and the strategy quietly does not trade
for its first month.

**`WARMUP` is set by the longest window,** not the most prominent one. Declare 60 with a 63-bar
indicator and it starts the run under-counted. If the bundle cannot supply the warm-up asked for,
the run warns.

**The index has to stay panel-shaped.** `rolling`, `shift` and comparisons preserve it; `dropna`,
`reset_index` and `groupby` do not. A signal with a different index is rejected immediately, rather
than on the bar that trips over it.

### Commissions and slippage

They work exactly as usual — which is the main difference from the second mechanism. Orders go
through the same blotter, and `PerShare`, `VolumeShareSlippage` and `FixedBasisPointsSlippage`
apply in full. The vector touches only the signal arithmetic, never the execution.

Covered by tests: the same strategy with `PerShare(0.005)` and 5 bp of slippage produces an account
identical to the cent in both versions — same trade price, same commission on the order.

### Futures and options

Fully supported. Execution stays entirely with ziplime, so the multiplier, variation margin and
auto-close at expiry apply as in any ordinary run. The vector sees only prices, and a future's price
is the same series of numbers as a share's.

Covered by an end-to-end test on a CL contract with a multiplier of 1000: exposure comes out as
`price × 1000 × quantity`, no cash is debited for the notional, and the result matches the bar-by-bar
version to 1e-9.

### What it does not do

It does not speed up the engine and it does not vectorise decisions. If a strategy reads the
portfolio on every bar — and those are precisely the strategies an event-driven engine exists for —
that part stays bar by bar, and rightly so.

---

# 2. Sweep, then report

A different job: not running one strategy faster, but running forty-eight variants.
`sweep_then_report.py` sweeps 48 moving-average combinations in 0.2s — 48 full runs for an
event-driven engine — and then **replays the winner** through ziplime's ledger.

The sweep is computed by `ziplime.vectorized.kernel`: signals in, trades out. Everything after that
is ordinary ziplime — the ledger carries cash and positions, `MetricsTracker` assembles the result.

It replays rather than translating statistics. The obvious alternative — copying a finished equity
curve into a table with ziplime's column names — is ten times shorter and defeats the whole point:
two engines, each with its own Sharpe, are not comparable, and the difference between runs would
become indistinguishable from the difference between strategies.

```python
result = simulate_signals(
    prices=prices, entries=entries, exits=exits,
    size=SIZE, initial_cash=CASH,
    execution=ExecutionTiming.next_close(),   # the moment of execution is stated explicitly
    commission=PerShare(cost=0.005, min_trade_cost=1.0),
    slippage=FixedBasisPointsSlippage(basis_points=5.0))

report = await to_execution_result(
    portfolio=result,           # the kernel's result is accepted directly
    listings=listings,          # {column: ziplime listing}
    prices=prices,
    trading_calendar=calendar,
    exchange=exchange)          # cash_balance has to match initial_cash
```

Reconciliation runs after every bar; a discrepancy beyond tolerance is a `ReconciliationError`,
not a warning.

**What is not here:** orders in ziplime's sense. Trades arrive already priced, not one order passes
through the blotter, and `orders` in the packets is empty. Logic that reads the book — sizing off
portfolio value, an exposure limit, a roll — belongs to the first mechanism or to an ordinary run:
a boolean array of signals cannot express it.

### Commissions and slippage

Costs are given to the **kernel**, and they are ziplime's models — the same classes as in an
event-driven run:

```python
result = simulate_signals(
    ...,
    commission=PerShare(cost=0.005, min_trade_cost=1.0),
    slippage=FixedBasisPointsSlippage(basis_points=5.0))
```

They have to be set **before the sweep**, not after: costs change which parameters win. A sweep
without them ranks a different problem from the one being solved.

Models taken from the exchange are not applied on this path — trades arrive already priced — and
the adapter refuses to swallow them silently, because unpaid costs do not show up as an error, they
show up as a higher return:

```
The exchange carries cost models a vectorised replay cannot run, so the result
would report a backtest that paid nothing:
  SPY commission = PerShare
```

Re-pricing the fills after the fact is impossible in principle: the replayed book would stop
matching the run being replayed, and reconciliation — the thing this path's credibility rests on —
would fail on every bar. Whoever executes sets the costs.

**What the kernel will not take:** `VolumeShareSlippage` prices a fill against a book this path does
not have; `PerContract`, `PerOptionContract` and `PerBondTurnover` belong to instruments this path
refuses anyway. All of them are rejected by name rather than ignored. The bar-volume participation
cap *is* supported — the part of `VolumeShareSlippage` that a bar's volume can express.

If the full execution model is needed, that is the first mechanism.

### Futures and options — unsupported, and refused

The vectorised path knows exactly one thing: a quantity of something, bought with cash, worth
`size × price`. A future is neither half of that. It is worth `price × multiplier × quantity` — a CL
contract at 75 with a multiplier of 1000 is a position of 75,000, not of 75 — and it is not bought
with cash: margin is posted, and variation moves daily.

Reconciliation caught this before — the discrepancy came out exactly as the multiplier — but
reported it as a mismatch whose three named causes were all the wrong ones. The multiplier is known
**before** the first bar, so the refusal now comes immediately:

```
A vectorbt portfolio cannot stand for these instruments, so replaying it would
report positions of the wrong size:
  CL (CLZ23): futures contract, multiplier 1000, margined rather than paid for in cash
```

There is no setting that fixes it: feeding in `price × multiplier` lies about the cash, feeding in
the margin as starting capital lies about the value. Options are rejected for the same reason
(contract size 100), and margined options doubly so.

Futures and options are the first mechanism.

**About the example's numbers:** the winner is picked by final value over the whole window — pure
overfitting. "+61%" describes the sweep, not a strategy. The example is about the machinery.

`ReconciliationError`, in order of likelihood:

1. the exchange's starting capital is not the one the trades were computed against;
2. `prices` is not the series the book was valued with;
3. the source holds several books (a sweep, or ungrouped columns) — caught separately and before
   the replay, or the error would read as "no listing" for columns like `(3, 10, 'JNJ')`.

---

## vectorbt

It is absent at runtime: nothing under `ziplime/` imports it, and the whole test suite passes with
the package uninstalled. It remains in exactly one role — the reference engine for
`tests/reference/vectorbt/`, where the same set of signals is run through both engines and the
trades are required to agree. `to_execution_result` still accepts its portfolio: replaying someone
else's trades through this ledger is the only way to compare them with ziplime in the same terms.

Where the two engines differ deliberately, that is pinned by tests rather than papered over:

| | ziplime | vectorbt |
|---|---|---|
| An order that cannot be paid for | rejected entirely, with a reason | filled with whatever cash is left |
| Entry and exit on the same bar | the exit wins | netted by its own rules |
| Shorts | separate `short_entries` / `short_exits` arrays | `exits` overloaded through `direction` |
