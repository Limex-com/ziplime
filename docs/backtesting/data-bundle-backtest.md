## Reporting metrics in different currencies

Pass a `CurrencyReporting` configuration to `run_simulation`, `run_simulation_iter`, or
`ziplime.vectorized.to_execution_result`. Trading and reconciliation run once in the book's native
currency. Each requested reporting currency gets independent metric instances, PNL, return history,
and benchmark returns; metric names such as `pnl`, `returns`, and `sharpe` do not change.

```python
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.data.services.currency_conversion_service import CurrencyConversionService
from ziplime.finance.domain.currency_reporting import CurrencyReporting

# Select a specific currency listing from the assets database, not a parsed ticker.
eur_usd = await asset_service.get_exchange_currency_by_symbol(
    AssetSymbol(symbol="EURUSD", mic="YOUR_FX_MIC")
)
if eur_usd is None:
    raise ValueError("The requested FX listing is not stored.")
fx_bundle, fx_storage = await bundle_service.load_bundle_definition("currency-bars")
conversion = await CurrencyConversionService.from_bundle(
    storage=fx_storage,
    bundle=fx_bundle,
    listings=[eur_usd],
    start=start_date,
    end=end_date,
    max_rate_age=datetime.timedelta(days=7),
)
reporting = CurrencyReporting(
    base_currency="EUR",
    currencies=("EUR", "USD"),
    conversion=conversion,
)
# Add currency_reporting=reporting to the simulation/replay arguments.
# result.perf_by_currency["USD"] has the same metric columns as result.perf.
```

FX bars must express **quote-currency units per one unit of the listing's currency asset**.
Direct and inverse conversions are supported; cross-rates are not inferred. `date` must be an aware
timestamp at which the selected price was known. For a frame with a separate availability column,
pass `timestamp_column="available_at"` when preparing the conversion service. Never use a daily
bar's midnight label as the availability time of its closing price. The loader includes a lookback
to seed the initial valuation; rates are selected as-of, without future observations. Missing, stale,
nonpositive, or nonfinite rates raise errors.

Initial capital is valued at the first session's open. Balances/exposures use valuation-time rates,
whereas cash flows and external deposits/withdrawals use event-time rates. Deposits/withdrawals are
excluded from PNL. FX changes affect returns even when the native portfolio is unchanged.

The existing `result.perf` and `result.risk_report` remain native/default reports. Currency-specific
reports are in `result.perf_by_currency` and `result.risk_report_by_currency`, in the requested order.
Streaming packets expose `metrics_by_currency`, with `currency` and `native_currency` labels.
Orders, transactions, position records (including their prices/cost bases), and arbitrary
`recorded_vars` retain their native units; only book-level monetary metrics are translated.
Custom metric objects are deep-copied before initialization for each currency.

By default this requires a **single-base-currency execution book**. Holdings quoted or settling margin
in another currency are rejected. Enable investment accounting below for mixed-currency cash and
historical security cost accounting. Benchmarks are
translated using their listing's quote currency. For supplied session-aligned `benchmark_returns`,
also set `benchmark_currency` on `CurrencyReporting`. A no-benchmark run remains a zero-return
benchmark in every reporting currency.

### Investment PNL and mixed-currency balances

For a runnable, credentials-free example using a temporary assets database and Delta Lake FX bars:

```sh
python -m examples.currencies.run_investment_pnl
```

See `examples/currencies/run_investment_pnl.py` for setup and result/history handling, and
`examples/currencies/strategy.py` for same-bar order comparisons, historical investment PNL,
external funding and executed FX cash legs. The synthetic example uses one XNYS calendar for
both currencies; it is not a model of real SBER trading hours or execution.

Enable `investment_pnl` to keep native cash balances and historical FIFO investment lots:

```python
reporting = CurrencyReporting(
    base_currency="EUR",                 # Execution portfolio/account totals use this currency.
    currencies=("EUR", "USD"),
    conversion=conversion,               # Select a USD/EUR listing and its historical bars.
    investment_pnl=True,
    opening_cash={
        ("LIME", "simulation_account", "EUR"): 10_000.0,
        ("LIME", "simulation_account", "USD"): 150.0,
    },
)
```

Each cash key is `(exchange_name, trading_account_id, currency)`. Explicit balances replace the
scalar starting balance for investment accounting. Without `opening_cash`, a single-exchange run
seeds its account with `total_cash` in `base_currency`; multi-exchange runs require explicit balances.
Trades debit/credit their actual quote-currency balance. Display conversion never exchanges cash.
Before an order is sent to `exchange.submit_order`, native consideration plus
`commission.calculate_for_asset(...)` is checked against cash in that exchange/account/currency.
Buy limits use their limit-price budget. The check reads the posted ledger balance; there is no
separate order cash-reservation tracker. Insufficient cash raises `ValueError` before the broker
call by default. Already-executed trades and fees are always recorded at their actual amounts.

Set `context.automatic_currency_exchange = True` in `initialize` to acquire missing quote-currency
cash automatically in simulation. The algorithm resolves a selected FX listing through the asset
service, includes its commission in the funding check, and submits `[FX order, asset order]` to
`SimulationExchange.submit_orders()` in one call. Single orders use `submit_order()`.
Both are normal open orders in the existing blotter. Currency
orders are processed first; each order uses the configured bar and slippage model for its fills.
There is no separate waiting-order tracker or automatic resubmission in the algorithm. An
unavailable pair or insufficient source cash raises before either order is submitted. Automatic
FX batches currently require `SimulationExchange`; live orders require quote-currency cash first.
Currency-pair fills change both native cash balances instead of opening security positions.
No cash is credited from an FX quote or submission acknowledgment. Explicit withdrawals, income
liabilities and FX sales also require available native cash.

Currency execution uses its own `currency_slippage` and `currency_commission` models, with
independent `NoSlippage()` and `NoCommission()` defaults. Pass these parameters to
`SimulationExchange`, `run_simulation`, `run_simulation_iter`, `run_algorithm` or
`run_algorithm_iter`. Currency costs are independent of equity settings. Algorithm runners
preserve each supplied simulation exchange's models when these parameters are omitted.

For each reporting currency, acquisition cost is converted at each fill's timestamp, sale proceeds at
sale time, and remaining holdings at the current market price and FX rate. Lots are independent for
every `(exchange, account, listing)`. Partial closes use FIFO; shorts and direction reversals are
supported. Fill-linked fees are incorporated once in basis/realized PNL at their posting-time FX rate.
Cash income uses payment-time FX and preserves the account's record-date entitlement after a sale.
The position tracker owns the unpaid dividend and coupon entitlements. Each dividend payment
retains its original payout, exchange, account and listing. The ledger posts native cash and
cash-flow metrics once, and clears the entitlement only after posting succeeds. The ledger retains
the original fills, linked fees and native cash postings. One FIFO calculation supplies historical
costs for every reporting currency. Each currency's `MetricsTracker` owns its performance baselines,
return history and risk metrics, sharing the one ledger and position tracker.

In a deliberately exaggerated synthetic example, buying 100 SBER at EUR 100 with USD/EUR 82
costs USD 121.9512195. At EUR 110 and
USD/EUR 200 the holding is worth USD 55: unrealized PNL is **+EUR 1,000 but -USD 66.9512195**.
Converting the EUR gain at today's rate would incorrectly report +USD 5.

The strategy can request valuations before metrics run:

```python
from ziplime.finance.domain.order_price_basis import OrderPriceBasis
from ziplime.finance.domain.order_quantity_basis import OrderQuantityBasis

# Both EUR-quoted and USD-quoted orders, in the supplied order.
values = await context.value_orders(orders=orders, currency="USD")
gross_submitted_usd = values.gross_value
net_submitted_usd = values.net_value

# Limit valuation requires a limit price; market valuation uses this bar's available mark.
remaining_at_limit = await context.value_orders(
    orders=orders, currency="USD",
    quantity_basis=OrderQuantityBasis.REMAINING,
    price_basis=OrderPriceBasis.LIMIT,
)
investment = await context.investment_pnl(currency="USD")
positions = await context.value_positions(currency="USD")
```

Pass enum members for both bases; the defaults are `OrderPriceBasis.MARKET` and
`OrderQuantityBasis.SUBMITTED`. Returned snapshots retain the selected enum members.

`Order.amount` remains a quantity. Each immutable monetary snapshot includes its native amount,
converted amount, price/timestamp and FX observation/timestamp/listing SID. Gross order value sums
absolute values; net value preserves buy/sell signs. These are estimates, not fills, fee estimates,
cash reservations or buying-power guarantees. Investment-mode submissions also retain immutable
`order.submitted_valuations[currency]` for each configured reporting currency. Order valuations can
be used independently of reporting by passing `currency_conversion=conversion` to the simulation.
The algorithm initializes one monetary valuation service and reuses it for order estimates and
converted value sizing, sharing the FX converter and the ledger's live bond schedules.

Currency reports expose cumulative `investment_realized_pnl`, `investment_unrealized_pnl`,
`investment_income`, `investment_fees`, `investment_cost` and `investment_pnl`. Investment PNL is
realized plus unrealized plus income; fees are already included and must not be subtracted again.
These are lifetime security-accounting totals, even in daily/minute packets. Existing `pnl` remains
simulation-period portfolio PNL, including cash FX. `cash_fx_pnl` is the residual after subtracting
the simulation-period change in investment PNL, so a sale freezes security realized PNL while the
retained foreign cash continues to gain/lose with FX. Streaming currency packets include the
immutable per-position `investment` snapshot. Raw execution cost bases remain native average
bases, not the reporting FIFO cost; use investment snapshots for investment PNL.

Post actual executed FX amounts with
`await context.exchange_currency(source_currency="EUR", currency="USD", sold=8200, received=100, execution_id="fx-1")`.
This records two cash legs, not a broker order. Use
`await context.fund_account(amount=100, currency="USD", exchange_name="LIME", trading_account_id="simulation_account",
event_id="deposit-1")` for external funding excluded from PNL. Repeated identical event/fill IDs in
the same account/listing scope are idempotent; conflicting duplicates raise.

`context.investment_history()` exports immutable native events, cash events and unpaid income entitlements for caller-managed
storage. `CurrencyReporting(investment_history=..., ...)` restores them; opening execution positions
must already exist with timestamped marks and reconcile to the historical lots. An average cost alone
cannot reconstruct historical FX. There is no automatic database/file serialization of this history.

Supported investment values include equities, currencies, premium-paid options (contract multiplier)
and bonds (dirty monetary value, including accrued interest). Margined futures/options, amortization
principal allocation and fractional-split cash-in-lieu raise explicit errors rather than invent PNL.
Foreign-quote percentage sizing is also rejected; submit quantities or use converted value sizing.
Vectorized replay shares the accounting path, but its source portfolio must still reconcile in the
declared base currency: the single-currency signal kernel does not model mixed-currency buying power.
Configured currencies update historical costs and PNL incrementally as postings arrive. Requesting
an additional currency converts the dated native amounts without matching FIFO again. Explicit
historical queries and history restoration use the same ledger accounting on the requested history.

## Define symbols, start/end date and data frequency
```python
symbols = ["META", "AAPL", "AMZN", "NFLX", "GOOGL",]
start_date = datetime.datetime(year=2025, month=1, day=1, tzinfo=datetime.timezone.utc)
end_date = datetime.datetime(year=2025, month=2, day=27, tzinfo=datetime.timezone.utc)
data_frequency = datetime.timedelta(minutes=1)
```

## Initialize client

### Initialize market data source instance
`LimexHubDataSource` instance can be created by providing parameters directly to constructor

```python
market_data_bundle_source = LimexHubDataSource(limex_api_key="YOUR_API_KEY",
                                            maximum_threads=NUMBER_OF_THREDS_YOU_WANT_TO_USE)
```


```python
import asyncio
import datetime
import logging
import pathlib

import polars as pl
import structlog

from ziplime.utils.bundle_utils import get_bundle_service
from ziplime.utils.logging_utils import configure_logging

from pathlib import Path

import pytz

from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.data.services.bundle_service import BundleService
from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry
from ziplime.finance.commission import PerShare, DEFAULT_PER_SHARE_COST, DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE, \
    PerContract, DEFAULT_PER_CONTRACT_COST, EquityCommissionModel
logger = structlog.get_logger(__name__)


async def _run_simulation():
    symbols = ["META", "AAPL", "AMZN", "NFLX", "GOOGL"]
    start_date = datetime.datetime(year=2025, month=1, day=3, tzinfo=pytz.timezone("America/New_York"))
    end_date = datetime.datetime(year=2025, month=2, day=1, tzinfo=pytz.timezone("America/New_York"))
    bundle_data_frequency = datetime.timedelta(days=1)
    emission_rate = datetime.timedelta(days=1)
    
    bundle_service = get_bundle_service()
    asset_service = get_asset_service(
        clear_asset_db=False,
        db_path=str(pathlib.Path(__file__).parent.parent.resolve().joinpath("data", "assets.sqlite"))
    )
    # Use aggregations if you ingested data of frequency less than 1 day
    aggregations = [
        pl.col("open").first(),
        pl.col("high").max(),
        pl.col("low").min(),
        pl.col("close").last(),
        pl.col("volume").sum(),
        pl.col("symbol").last()
    ]
    market_data_bundle = await bundle_service.load_bundle(bundle_name="limex_us_minute_data",
                                                          bundle_version=None,
                                                          frequency=bundle_data_frequency,
                                                          start_date=start_date,
                                                          end_date=end_date,
                                                          symbols=symbols,
                                                          start_auction_delta=datetime.timedelta(minutes=15),
                                                          end_auction_delta=datetime.timedelta(minutes=15),
                                                          aggregations=aggregations,
                                                          )

    custom_data_sources = []
    custom_data_sources.append(
        await bundle_service.load_bundle(bundle_name="limex_us_fundamental_data", bundle_version=None))

    equity_commission = PerShare(
        cost=DEFAULT_PER_SHARE_COST,
        min_trade_cost=DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE,

    )

    # run daily simulation
    result = await run_simulation(
        start_date=start_date,
        end_date=end_date,
        trading_calendar="NYSE",
        algorithm_file=str(Path("algorithms/test_algo/test_algo.py").absolute()),
        total_cash=100000.0,
        market_data_source=market_data_bundle,
        custom_data_sources=custom_data_sources,
        config_file=str(Path("algorithms/test_algo/test_algo_config.json").absolute()),
        emission_rate=emission_rate,
        benchmark_asset_symbol="AAPL",
        benchmark_returns=None,
        stop_on_error=False,
        asset_service=asset_service,
        equity_commission=equity_commission
    )

    if result.errors:
        logger.error(result.errors)
    print(result.perf.head(n=10).to_markdown())


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(_run_simulation())

```
