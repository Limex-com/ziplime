# Changes: futures support, data connectors, correctness fixes

> **Start with [`changes-overview.md`](changes-overview.md)** — the map of the whole body of work,
> covering both this and the bond/cross-asset work in [`changes-bonds.md`](changes-bonds.md). This
> document is the detail behind the futures half: evidence, judgement calls and open questions.

Written for a ziplime maintainer. Every entry says **what changed, why, and how confident we are**
it is right — several items are bug fixes with a demonstrated failure, a few are judgement calls,
and the open questions are listed rather than buried.

Test suite **at the end of this workstream**: 143 passed, 4 xfailed (the 4 xfails pre-date this
work). Of those, 89 are futures acceptance tests and 12 cover the connector registry. The bond and
cross-asset work later took the suite to 342 passed / 4 xfailed; see
[`changes-overview.md`](changes-overview.md).

---

## 1. Bug fixes in existing ziplime code

These were found by writing tests against an acceptance checklist, not by inspection. Each is
listed with the evidence.

### 1.1 Variation margin was applied in one direction only — **P0**

`Ledger.update_portfolio` did:

```python
payout_total = self._get_payout_total()
if payout_total > 0:            # <-- losses discarded
    self._cash_flow(payout_total)
```

`_get_payout_total()` advances every position's mark to the current price *as a side effect*, so a
discarded negative payout was gone for good. A long futures position could not lose money.

**Evidence.** A long Si position over 2025-01..2026-06, while the price fell from 105 700 to
about 80 000, reported **+10.02%**. After the fix: **−4.16%**.
**Confidence: certain.** Regression test `test_a_losing_day_reduces_cash` fails on the old code.

### 1.2 `PositionTracker.positions` is nested; three call sites treated it as flat — **P0**

The structure is `{exchange: {account: {asset: Position}}}`, but `Ledger.process_transaction`,
`Ledger._get_payout_total` and `PositionTracker.handle_commission` all indexed it by asset.

- The ledger ones raised `KeyError` on the first futures roll.
- `handle_commission` silently never matched, so **commission never reached a cost basis for any
  asset class** — equities included.

Added `PositionTracker.get_position(asset)` and routed all three through it.
**Confidence: certain** for the crash; **high** for the commission path (cash was always charged
correctly, so only the reported cost basis and realised-PnL figures move).

### 1.3 Futures branches tested the wrong object — **P0**

`Position.asset` and `Transaction.asset` are `ExchangeAsset`; the instrument is `.asset`. Code in
`ledger.py`, `position_tracker.py`, `finance_ext.py`, `per_contract.py`,
`volatility_volume_share.py` and `trading_algorithm.py` tested `isinstance(position.asset,
FuturesContract)` — always False — and read `asset.price_multiplier` and `asset.root_symbol`, which
do not exist on either type (the field is `multiplier`).

Futures P&L was therefore computed as if they were equities. `SimulationExchange` already did this
correctly (`type(asset.asset)`), which is why commission/slippage model selection worked.
**Confidence: certain.**

### 1.4 The volume roll used the current session's full volume — **P0 look-ahead**

`VolumeRollFinder.get_contract_center(root, dt)` compared volumes *on `dt`*. A session's volume is
not known until it closes, so the roll was timed on information the market had not produced.

Now the decision uses the last **completed** session (`_last_completed_session`, bisect over the
bundle's session list).
**Confidence: certain** that it was look-ahead. On MOEX Si the roll dates did not move, because the
auto-close safety net binds first there — so **no result changed**, but the mechanism is fixed.

### 1.5 `run_simulation` ignored the slippage models it was given

It computed `equity_slippage`/`future_slippage` and then hardcoded fresh instances when building
`SimulationExchange`. The parameters did nothing.
**Confidence: certain.**

### 1.6 `MarketImpactBase.process_order` could never run

The base of `VolatilityVolumeShare` — the **default futures slippage model** — was written against
zipline's synchronous `BarData` and referenced an undefined name `data`. Any futures backtest using
the default slippage raised immediately. Rewritten against the async exchange API;
`get_txn_volume(volume, order)` replaces `get_txn_volume(data, order)`.
**Confidence: certain** it was broken. The rewrite preserves the original formula (η·σ·√ψ) but is
**not** output-compatible with any prior behaviour, because there was none.

### 1.7 The recorded past mutated as the simulation ran — **P0 for reporting**

`perf["positions"]` stored live `Position` objects. Closing a contract in September rewrote what
June's row showed. Cash and P&L were always correct; the record was not.
`Ledger.positions` now returns `dataclasses.replace` snapshots.

**Evidence.** Backtest to T versus to T+1 year, compared over 876 sessions: everything matched
except the positions column, in 8 rows. After the fix, identical.
**Confidence: certain.** Regression test included.

### 1.8 The bundle load window dropped the first session east of UTC

`file_system_delta_lake_bundle_storage` filtered `pl.col("date") >= start_date.date()` — a
timezone-aware UTC column against a bare date, i.e. midnight **UTC**. A Moscow session stamped
00:00 MSK is 21:00 UTC the day before, so it fell outside.

Now compares against the timezone-aware bound.
**Confidence: high.** Exchanges west of UTC are unaffected either way (verified for NYSE); this
only ever added a session for MOEX.

### 1.9 Smaller ones

| What | Effect |
|---|---|
| `validate_benchmark` ran even with no benchmark | `AttributeError` on any benchmark-free run |
| `save_exchanges` was insert-only | second market ingest hit a UNIQUE violation |
| slippage "No volume" warning used `asset_name` on an `ExchangeAsset` | warning path raised `AttributeError` |
| `AssetRepository.get_exchange_asset_by_symbol` futures branch referenced an undefined `exchange_name` | `NameError` |
| `get_exchange_equities_by_symbols` did not filter by asset type | `KeyError` when a ticker exists on the same MIC as two asset types |

---

## 2. Futures support that did not exist

The schema had a `futures_contracts` table and the finance layer had `PerContract` and
`VolatilityVolumeShare`, but nothing wrote contracts and the continuous-future layer was dead code:
`get_ordered_contracts` queried `self.futures_contracts` and `self.futures_root_symbols`, sync
SQLAlchemy `Table`s that were never assigned; no `RollFinder` class existed anywhere;
`DataBundle._roll_finders` was read but never set; `TradingAlgorithm.continuous_future()` reached
through `self.data_portal`, which is set to `None`.

### 2.1 Schema

Migration `b60ffbf8f072`:

- new table **`futures_root_symbols`** (`root_symbol` PK, `description`, `mic`, `root_asset_id`,
  `multiplier`, `tick_size`, `quote_currency`) — the annotated type `FuturesRootSymbolFK` already
  existed and pointed at a table that did not;
- **`futures_contracts.root_symbol`** column + FK.

Hand-written rather than left as autogenerated: SQLite has no `ALTER TABLE ADD CONSTRAINT` and
refuses `ADD COLUMN NOT NULL` without a default, so both go through `batch_alter_table`. Downgrade
tested.

Where a contract lives: `asset_router` (`asset_type='FUTURES_CONTRACT'`) → `futures_contracts` →
`exchange_assets` (this row owns the `sid` that bundles key bars by). A commodity row stands in as
the underlying, because `futures_contracts.root_asset_id` is `NOT NULL` and FORTS roots have no
tradeable underlying in this database.

### 2.2 Repository and service

Added `save_futures_roots`, `save_futures_contracts`, `get_futures_roots`,
`get_exchange_futures_contracts_by_root`, `get_exchange_futures_contract_by_symbol`, a working
`get_futures_contract_by_symbol`, and filled the futures and commodity branches of `get_all_assets`
(both were `pass`). Removed the dead sync `Table`-based `lookup_future_symbol`,
`_get_contract_sids`, `_get_root_symbol_exchange`.

**Idempotency.** `save_equities`/`save_currencies`/`save_exchange_assets` inserted blindly;
re-ingest duplicated rows. Now upsert. Listings are keyed by **`(mic, symbol, asset)`**, not
`(mic, symbol)` — the same ticker on the same exchange can belong to two assets (a Yahoo equity row
for `SiZ6@RTSX` and the FORTS futures contract of that name both exist in our test database), and
the coarser key silently dropped all 14 futures listings.

**Cache invalidation.** `get_all_assets` is memoised twice (`aiocache` + `self._cached_assets`) and
`save_exchange_assets` resolves ids through it; added `_invalidate_asset_cache`.

### 2.3 Continuous futures

New `ziplime/assets/domain/roll_finder.py`: `RollFinder`, `CalendarRollFinder`, `VolumeRollFinder`
(async, `ExchangeAsset`-based). `OrderedContracts` rewritten over a sorted list of listings —
the old one assumed `pd.Timestamp` and called `.value` on `datetime.date`. `ContinuousFuture`
rewritten as a frozen dataclass; `_encode_continuous_future_sid` kept so sids stay stable.

`DataBundle` now resolves a `ContinuousFuture` in `get_data_by_limit`: rolls are turned into
segments, each segment is read from the contract that was actually held, and older segments are
shifted onto the newest one's level (`mul`/`add`/`None`).

`CalendarRollFinder` takes either `roll_offset_days` (calendar days, the old implicit meaning) or
`roll_offset_sessions` + `trading_calendar`. Three calendar days across a weekend is one trading
session; the distinction is now explicit rather than assumed.

**Default roll timing changed.** Both styles now roll one session *before* auto close
(`DataBundle.roll_finder_settings`). Rolling exactly on auto close meant `_cleanup_expired_assets`
had already liquidated the position, and that liquidation goes through
`maybe_create_close_position_transaction` — **no commission, no slippage**. Visible as a trade
count: a calendar-roll strategy booked 22 trades where a volume-roll one booked 43.

**Ordering a `ContinuousFuture` now raises.** It is a data specifier; `data.current_contract(cf)`
returns the tradable contract.

### 2.4 Lifecycle dates

`auto_close_date` used to equal `expiration_date`, so the engine liquidated a position on the last
trading day, before the strategy could trade or roll on it — while `_can_order_asset` still allowed
orders that same day. Contradictory. Now `end_date` is the last tradable session and `auto_close` is
strictly after it. **This moved a real result: −2.97% → −3.42%** on the Si example.

### 2.5 Settlement type and delivery — new

`SettlementType` (`CASH` / `PHYSICAL`) on `FuturesContract` and `FuturesRoot`, plus a
`settlement_type` column on both tables (migration `c1a4f7d2e9b3`).

A cash-settled contract can be held to the last session; a deliverable one turns into an obligation
to deliver or receive the underlying. The lifecycle dates now differ accordingly:

| | `notice_date` | `auto_close_date` | effect |
|---|---|---|---|
| cash-settled | expiration | expiration + 1 | held to the last session, liquidated after |
| **deliverable** | 3 business days before expiration | **= notice_date** | force-closed **while still tradable**, before the delivery window |

`Ledger.close_position` logs explicitly when it is closing a deliverable position, so a
delivery-avoidance close does not read as ordinary housekeeping.

Which roots are deliverable: **MOEX single-stock futures** (SBRF, GAZR, LKOH, ROSN, VTBR) deliver
the shares; **NYMEX Henry Hub gas** delivers physical gas three business days after it stops
trading. MOEX's index, currency and commodity futures are cash-settled.
**Confidence: high** on the classification (domain knowledge; Finam's API does not expose a
settlement-type field, so it is not machine-verified). **The three-session notice offset is a
convention, not an exchange rule** — it is a configurable stand-in for "get out before delivery",
and it steps over weekends but not holidays.

**This moved results.** Marking SBRF and GAZR deliverable pulls their auto-close forward, so the
examples that trade them roll earlier: the cross-root pair went from +20.10% to +27.39%, the
four-root basket from −23.01% to −17.79%.

### 2.6 Margin — new

`ziplime/finance/margin.py`: `NoFuturesMarginModel` (default; prints `NO FUTURES MARGIN MODEL` once
when futures are first traded), `FixedRateFuturesMarginModel`, `PerRootFuturesMarginModel`. Threaded
through `run_simulation(futures_margin_model=...)`.

**Margin currency is not quote currency.** Verified against `GetAssetParams`, where
`long_initial_margin.currency_code` is returned per contract:

| contract | quote | **margin** |
|---|---|---|
| `NGU6@RTSX`, `BRZ6@RTSX`, `RIZ6@RTSX`, `GDZ6@RTSX` | USD | **RUB** |
| `NGV26@XNYM` | USD | **USD** |
| `SiZ6@RTSX` | RUB | RUB |

MOEX collects roubles for everything it lists, including the contracts it quotes in dollars. So:

- `margin_currency` is stored per contract and per root, read from the API at ingest where the
  endpoint answers (it refuses archived contracts) and falling back to the root spec, logging a
  warning if the two disagree;
- `Ledger.futures_margin_by_currency()` returns `{"RUB": ..., "USD": ...}`;
- `futures_margin_requirement()` takes an optional `currency` and **raises** if the book spans
  several and none was named, rather than adding roubles to dollars;
- `FixedRateFuturesMarginModel` computes a share of notional, which is in the *quote* currency. If
  margin is collected in another, it needs `fx_rates={("USD", "RUB"): rate}` and raises
  `MissingFxRate` otherwise. `PerRootFuturesMarginModel` quotes an absolute amount per contract in
  the collected currency and needs no rate — the right default for a cross-venue book.

**Margin is modelled, not enforced** — no margin calls, no buying-power constraint. And the
portfolio still holds **one untyped cash balance**: requirements are reported per currency, but
P&L and cash are added up as a single number. A genuinely cross-currency book needs an FX series
the ledger does not carry. `ziplime/finance/realism.py` states both.

### 2.7 Algorithm API additions

`futures_margin_requirement(maintenance, currency)`, `futures_margin_by_currency`,
`models_futures_margin`, `realism_warnings`, `notional_exposure`, `contracts_for_notional`.
`FuturesContract.is_deliverable` lets a strategy see the obligation coming. `run_simulation` gained `print_algo` and `futures_margin_model`;
`load_bundle` gained `asset_service` and `roll_finder_settings`.

### 2.8 MOEX fee/eta tables

`FUTURE_EXCHANGE_FEES_BY_SYMBOL` and `ROOT_SYMBOL_TO_ETA` held CME roots only, and `PerContract`
indexed the fee dict directly — a MOEX root raised `KeyError` mid-simulation. Added a MOEX section
and made the lookup a `defaultdict`. **The MOEX fee values are order-of-magnitude placeholders**;
replace them with your broker's schedule.

---

## 3. Data connectors

### 3.1 Registry

`ziplime/data/data_sources/registry.py`. Connectors register themselves; `bundle_utils` and the CLI
resolve them by name. A connector whose package is absent is skipped silently — that is how a build
ships without one — while a connector that is present but fails to import is logged, because that
is a missing dependency rather than a deliberate omission. Third parties can register through a
`ziplime.data_providers` entry point.

**Verified by deleting `ziplime/data/data_sources/finam/`**: every module still imports,
`list_providers()` returns only `yahoo`, asking for `finam` gives
`UnknownDataProvider: ... Registered providers: ['yahoo']`, the Yahoo ingest still runs, and the
suite reports 18 passed / 16 skipped. `ConnectorIndependenceTests` keeps it that way by asserting
no core module mentions a connector.

Yahoo moved to `ziplime/data/data_sources/yahoo/`; the old module paths remain as shims that emit
`DeprecationWarning`.

### 3.2 Finam connector — new

REST over `httpx`. Four things about the API shape the code:

1. **HTTP/2 is required, not an optimisation.** Finam fronts gRPC with envoy; over HTTP/1.1 envoy
   buffers a whole transcoded response to produce `Content-Length`, and `/v1/assets` (3.4 MB)
   returns `500 Response not transcoded because the transcoder's internal buffer size exceeds the
   configured limit`. Over HTTP/2 the same request succeeds. This added `h2` via `httpx[http2]`.
2. **`Bars` intermittently returns an empty array** for a request that has data — it alternated
   request-to-request in our testing. An empty array is also the honest answer for a contract that
   never traded, so the client retries an empty chunk before believing it. Without this, ingestion
   silently lost months of history and mis-dated expiries.
3. **Range limits**: about a year per request for `TIME_FRAME_D`, about a week for `TIME_FRAME_M1`.
   `bars()` chunks and de-duplicates.
4. **Rate limiting**: 429s appear well before a browser would see them. Exponential backoff with
   `Retry-After`, 8 attempts capped at 30 s, default concurrency 3.

`AllAssets` uses **cursor pagination** — 3000 rows per page. Following `next_cursor` yields 298 452
instruments including 5 539 archived RTSX futures; not following it looks exactly like a hard
truncation at 3000. `GetAsset` answers for archived contracts too, which is where accurate
expiration dates come from.

**`contract_size` is not a multiplier.** It is the size of the underlying — Si's is 1000 (i.e.
$1000) while one price point is one rouble. Mapping it onto `multiplier` would have inflated Si
exposure a thousandfold. Multipliers live in a curated table and are cross-checked at ingest time
against `GetAssetParams` margin (`long_initial_margin / long_risk_rate / price`); all 18 checkable
roots agree.
**Confidence: high** for RUB-quoted roots. **Lower for USD-quoted ones** (`RI`, `BR`, `NG`, `GD`,
`SV`): MOEX margins them in roubles at a rate that moves, so the automatic check is skipped and
their multiplier is the quote-currency figure.

### 3.3 Cross-venue support

`cme_futures.py` adds the NYMEX contracts Finam mirrors. Two conventions differ from FORTS: the
ticker carries a **two-digit** year (`NGZ25` vs `NGZ5`) and the name ends `MonYY`.

**Root symbols are not unique across venues** — `NG` is natural gas on both MOEX and NYMEX, and
`futures_root_symbols` keys a chain by root symbol alone. Non-MOEX roots are therefore namespaced
`CODE.MIC` (`NG.XNYM`).

> **Judgement call, flagged for review.** The modelling-correct fix is a composite
> `(root_symbol, mic)` key, which would require adding `mic` to `futures_contracts` and changing the
> FK — a wider migration touching `get_ordered_contracts`, `create_continuous_future` and the
> continuous-future sid encoding. The namespace achieves uniqueness with no schema change and keeps
> existing data valid, at the cost of an asymmetry: MOEX roots keep bare codes, others do not.

---

## 4. What we did not do

- **`handle_commission` cost basis** is now fixed (1.2), which shifts reported cost-basis and
  realised-PnL figures for existing equity backtests. Cash was always right.
- **Overnight sessions** (CME-style, a session beginning the previous evening) are untested. Daily
  bars are stamped at the session start in exchange time, which would be wrong for that shape.
  Neither MOEX nor NYSE data exercises it.
- **`close` semantics** — Finam does not document whether it is settlement or last trade. Taken as
  delivered.
- **Price limits / locked markets** are not modelled, only disclosed.
- **Delivery is avoided, not simulated.** A deliverable position is force-closed at its notice
  date; assignment, delivery cost and the squeeze risk of being caught long into a delivery window
  are not modelled.
- **Cash has no currency.** Margin requirements are reported per currency, but the portfolio still
  adds P&L across currencies as one number.
- **Settlement type is not machine-verified.** Finam exposes no such field, so the CASH/PHYSICAL
  classification comes from domain knowledge and should be reviewed against contract specs.
- **Stale prices**: forward-filled bars are identifiable by zero volume, but `can_trade()` does not
  consult them.
- **Connector dependencies are still mandatory.** `yfinance` is only used by Yahoo and `h2` only by
  Finam, but both are non-optional in `pyproject.toml`. Making them extras changes what a plain
  `poetry install` produces, so it was left for the maintainer to decide.
- **Bundle size**: `forward_fill_missing_ohlcv_data=True` materialises a full session grid per
  contract, so a 6-year futures bundle is roughly 4× larger than the traded bars it contains
  (178 959 rows against 31 755 traded). Harmless for correctness — the padding carries zero volume —
  but worth knowing.

---

## 5. Verification

| Check | Result |
|---|---|
| Test suite | 134 passed, 4 xfailed |
| Adjustment independence of fills (`mul`/`add`/`None`) | byte-identical fills, identical final value 963 360.6965 |
| Time travel (backtest to T vs T+1 year, 876 sessions) | session index, fills, positions, portfolio value, cash, exposure, P&L, returns all identical |
| Expirations vs the third-Thursday rule | 14/14 Si contracts match, and match the API |
| Back-adjustment | raw roll jumps up to +9.96% reduced to ≈0; the one large residual (+7.31%, 2020-03-18) is a real ruble move — SiM0's own return that day was +7.306% |
| Connector separability | Finam package deleted: 0 import failures, Yahoo ingest works, 18 passed / 16 skipped |
| Margin currency | API-confirmed per contract: MOEX returns `RUB` for four dollar-quoted contracts, NYMEX returns `USD` |
| Cross-venue book | reports `{'RUB': 1 934 020, 'USD': 10 500}`; asking for one number raises |
| Deliverable lifecycle | NYMEX gas: notice = auto close = 3 business days before expiration, strictly before it; MOEX gas: auto close after expiration |
| Strategy examples | 21/21 run, trade, and produce a non-zero result |
