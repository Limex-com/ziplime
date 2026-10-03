"""Turning a set of fills into a ziplime execution result.

The fills normally come from :func:`~ziplime.vectorized.kernel.simulate_signals`, and this module
replays them: they are fed through ziplime's own
:class:`~ziplime.finance.domain.ledger.Ledger` and
:class:`~ziplime.finance.metrics_tracker.MetricsTracker`, and the performance table falls out of
the same code an event-driven run uses.

That is a **replay, not a translation**, and the alternative is worth naming because it is the
obvious one: copying an equity curve into a frame with ziplime's column names would be a tenth of
the code and would quietly make the hybrid useless. Two engines that each compute their own Sharpe
ratio cannot be compared -- one annualises from a returns series, another from its own frequency
setting, and a disagreement between two runs would be indistinguishable from a disagreement
between two strategies. Replaying means a difference in the output is a difference in the trades,
which is the only question worth asking of a hybrid.

What it costs: the replay is O(sessions x positions) rather than free. That is small against the
vectorised part -- the sweep over parameters is where the time goes -- and it buys numbers that
mean the same thing on both sides.

What it cannot do: a vectorised run has no orders in the ziplime sense. The fills arrive already
priced, so no order passes the blotter and ``orders`` in the resulting packets is empty. The
kernel's commission and slippage models were applied when the fills were decided, not here.

**A ``vectorbt`` portfolio is also accepted, and that is on purpose.** Anything exposing
``.wrapper.index``, ``.orders.records_readable`` and ``.value()`` can be replayed, which is how a
second engine's trades are made comparable with ziplime's own -- same ledger, same metric set, so
the only thing left to differ is the trading. ``tests/reference/vectorbt/`` uses it exactly that
way. Nothing in ziplime imports vectorbt; this is a door, not a dependency.

For an input that carries its own value series -- a vectorbt portfolio does --
:func:`to_execution_result` reconciles the replayed book against it and refuses a result that does
not match, so a divergence surfaces as an error rather than as a plausible number.
"""
import dataclasses
import datetime
import math
import uuid

import pandas as pd
import polars as pl
import structlog

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.assets.entities.option_contract import OptionContract
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.domain.commission import Commission
from ziplime.finance.domain.ledger import Ledger
from ziplime.finance.domain.transaction import Transaction
from ziplime.finance.metrics import default_metrics
from ziplime.finance.metrics_tracker import MetricsTracker
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.sources.benchmark_source import BenchmarkSource
from ziplime.trading.trading_algorithm_execution_result import TradingAlgorithmExecutionResult

_logger = structlog.get_logger(__name__)

#: How far the replayed portfolio value may sit from the source's own before it is refused, as a
#: fraction of starting capital. A cent on a hundred thousand. Anything looser hides a systematic
#: difference -- a fee convention, a missing trade -- inside what looks like rounding.
DEFAULT_TOLERANCE = 1e-7


class ReconciliationError(ValueError):
    """The replayed ledger and the source's own value series disagree about the same trades.

    One of the two is wrong, and which one is not knowable from here. Raised rather than warned:
    a hybrid backtester whose two halves disagree silently is worse than one that only has a
    single half, because it invites comparisons that mean nothing.
    """

    def __init__(self, session, replayed: float, reported: float, tolerance: float):
        super().__init__(
            f"On {session} the replayed ledger holds {replayed:,.6f} but the source reports "
            f"{reported:,.6f} -- a difference of {abs(replayed - reported):,.6f}, beyond the "
            f"{tolerance:g} tolerance.\n"
            f"Three things cause this, in the order they are worth checking: the exchange's "
            f"starting cash differs from the starting cash the fills were decided under; "
            f"`prices` is not the series the source valued the book with, so the positions are "
            f"marked at something else; or the source holds more than one book and the wrong one "
            f"is being compared."
        )
        self.session = session
        self.replayed = replayed
        self.reported = reported


@dataclasses.dataclass
class VectorizedRun:
    """Stands where a :class:`TradingAlgorithm` would, for a run that never had one.

    A vectorised run has no algorithm object -- no ``initialize``, no ``handle_data``, no context.
    What it does have is the ledger the replay built and the portfolio vectorbt produced, and this
    carries both so that code reading ``result.trading_algorithm.portfolio`` keeps working.
    """

    ledger: Ledger
    #: The ``vectorbt.Portfolio`` this was replayed from, kept so the vectorised side stays
    #: reachable: its parameter sweep, its trade records, its plots.
    source: object = None

    @property
    def portfolio(self):
        return self.ledger.portfolio

    @property
    def _ledger(self) -> Ledger:
        """The ledger, under the name :class:`TradingAlgorithm` exposes it by."""
        return self.ledger


def _orders_frame(portfolio) -> pd.DataFrame:
    """vectorbt's order records, with the columns this module needs.

    ``records_readable`` is used rather than the raw record array because the raw one identifies
    columns by integer position, and a portfolio built with ``group_by`` reorders them.
    """
    orders = portfolio.orders.records_readable
    if orders.empty:
        return orders
    missing = {"Column", "Timestamp", "Size", "Price", "Side"} - set(orders.columns)
    if missing:
        raise ValueError(
            f"vectorbt order records are missing {sorted(missing)}. This adapter was written "
            f"against vectorbt 0.28's `records_readable`; a different version may name them "
            f"differently.")
    return orders


def replay_transactions(portfolio, listings: dict[str, ExchangeAsset],
                        exchange_name: str, trading_account_id: str,
                        include_fees: bool = True) -> list[Transaction]:
    """Turn vectorbt's orders into ziplime transactions.

    Args:
        portfolio: A ``vectorbt.Portfolio``.
        listings: Maps each of the portfolio's columns to the ziplime listing it stands for. The
            mapping is explicit because a vectorbt column is a bare string and a ziplime listing
            is an instrument on an exchange -- "T" is two different companies and the adapter
            cannot guess which one was traded.
        exchange_name: Exchange the transactions are booked on.
        trading_account_id: Account they are booked to.
        include_fees: Carry vectorbt's per-order fees onto the transaction. They are already
            inside its cash flow, so dropping them here makes the two sides disagree by exactly
            the commission.

    Returns:
        Transactions in timestamp order.

    Raises:
        KeyError: if the portfolio holds a column no listing was given for. Skipping it silently
            would produce a result that is short some trades and looks fine.
    """
    orders = _orders_frame(portfolio)
    if orders.empty:
        return []

    unknown = set(orders["Column"].unique()) - set(listings)
    if unknown:
        raise KeyError(
            f"No listing given for {sorted(unknown)}. Every column the portfolio traded has to "
            f"map to an instrument, or the replayed book is missing those trades entirely.")

    transactions = []
    for row in orders.sort_values("Timestamp").to_dict("records"):
        listing = listings[row["Column"]]
        # vectorbt records size as a magnitude and the direction in `Side`.
        side = str(row["Side"]).lower()
        if side.startswith("buy"):
            amount = float(row["Size"])
        elif side.startswith("sell"):
            amount = -float(row["Size"])
        else:
            raise ValueError(f"Unrecognised order side {row['Side']!r} in vectorbt records.")
        transactions.append(Transaction(
            id=uuid.uuid4().hex,
            asset=listing,
            amount=amount,
            dt=row["Timestamp"].to_pydatetime(),
            price=float(row["Price"]),
            order_id=None,
            exchange_name=exchange_name,
            trading_account_id=trading_account_id,
            commission=float(row.get("Fees", 0.0)) if include_fees else 0.0,
        ))
    return transactions


def _reject_unrepresentable_instruments(listings: dict[str, ExchangeAsset]) -> None:
    """Refuse instruments a vectorbt portfolio cannot stand for.

    vectorbt models one thing: a quantity of something bought with cash, worth ``size x price``.
    A futures contract is neither half of that. It is worth ``price x multiplier x amount`` -- a CL
    contract at 75 with a multiplier of 1000 is a position of 75,000, not 75 -- and it is not
    bought with cash at all: it posts margin and settles variation daily, so the cash line moves
    with the price rather than dropping by the notional at entry. An option is the same story with
    a contract size of 100, and a margined option adds the futures-style settlement on top.

    The reconciliation does catch it: replaying a futures trade produces a portfolio value that
    disagrees with vectorbt's by exactly the multiplier, and the result is refused. But it is
    refused with a message about starting cash and price series, none of which is the reason, and
    a multiplier is knowable before a single bar is replayed. So it is checked here instead.

    There is no setting that repairs this. Feeding vectorbt ``price x multiplier`` would misstate
    the cash side; feeding it margin as ``init_cash`` would misstate the value side. Futures
    belong on the event-driven path, where the ledger already knows what a contract is worth and
    what it ties up -- and :mod:`ziplime.vectorized.signals` vectorises the signals of such a
    strategy without touching any of that accounting.
    """
    problems = []
    for column, listing in sorted(listings.items(), key=lambda item: str(item[0])):
        asset = listing.asset
        multiplier = getattr(asset, "multiplier", 1.0) or 1.0
        if isinstance(asset, FuturesContract):
            problems.append(f"{column} ({listing.symbol}): futures contract, multiplier "
                            f"{multiplier:g}, margined rather than paid for in cash")
        elif isinstance(asset, OptionContract):
            problems.append(f"{column} ({listing.symbol}): option contract, contract size "
                            f"{multiplier:g}")
        elif multiplier != 1.0:
            problems.append(f"{column} ({listing.symbol}): multiplier {multiplier:g}")
    if not problems:
        return

    raise ValueError(
        "A vectorbt portfolio cannot stand for these instruments, so replaying it would report "
        "positions of the wrong size:\n  " + "\n  ".join(problems) + "\n"
        "vectorbt holds a quantity bought with cash and worth size x price. A contract is worth "
        "price x multiplier x amount, and a futures position posts margin instead of spending the "
        "notional -- both the value and the cash line differ, and no fee or price setting "
        "reconciles them.\n"
        "Run these event-driven and vectorise the signals instead "
        "(ziplime.vectorized.signals): the ledger applies the multiplier and the margin model, "
        "and only the indicator arithmetic moves out of the bar loop."
    )


def _reject_ignored_cost_models(exchange, listings: dict[str, ExchangeAsset]) -> None:
    """Refuse an exchange whose cost models this path cannot run.

    vectorbt decides its own fills: it applies ``fees``, ``fixed_fees`` and ``slippage`` inside
    ``from_signals``/``from_orders``, and what arrives here is the finished price and fee per
    order. ziplime's commission and slippage models run in the blotter, which no order passes
    through on this path, so an exchange configured with them would report a backtest costing
    nothing while the author believed otherwise -- and unpaid costs do not show up as an error,
    they show up as a better return.

    Re-pricing the fills here instead is not an option that exists: the replayed book would stop
    matching the run it is replaying, and the reconciliation that makes this path trustworthy
    would fail on every bar. Costs have to be decided once, by whatever decided the fills.

    So pass them where the fills are decided -- ``simulate_signals(..., commission=PerShare(...),
    slippage=FixedBasisPointsSlippage(...))``. The kernel takes ziplime's own models, charges them
    per fill, and they are carried through and booked in the ledger here. Set them before the
    sweep rather than after it: costs change which parameters win, so a sweep run without them
    ranks a different problem from the one being solved. (Replaying another engine's portfolio,
    the same rule applies to that engine's own fee settings.)

    One model still cannot be applied: ``VolumeShareSlippage`` prices a fill from an order book
    the vector path does not have. The kernel's volume cap covers the part of it that a bar's
    traded volume can express; the rest needs the event-driven path. A sweep can therefore crown
    a size that could never have been filled, and only checking the winner's order sizes against
    traded volume repairs that.
    """
    # Only the instruments this portfolio actually holds. An exchange carries a model for every
    # asset class it could trade and fills the ones nobody set with defaults, so checking all of
    # them would refuse an equities replay over an option commission the author never chose.
    ignored = set()
    for listing in listings.values():
        commission = exchange.get_commission_model(listing)
        if commission is not None and not isinstance(commission, NoCommission):
            ignored.add(f"{listing.symbol} commission = {type(commission).__name__}")
        slippage = exchange.get_slippage_model(listing)
        if slippage is not None and not isinstance(slippage, NoSlippage):
            ignored.add(f"{listing.symbol} slippage = {type(slippage).__name__}")
    if not ignored:
        return

    raise ValueError(
        "The exchange carries cost models a vectorised replay cannot run, so the result would "
        "report a backtest that paid nothing:\n  " + "\n  ".join(sorted(ignored)) + "\n"
        "The fills arrive already priced on this path, so costs have to be applied where they "
        "were decided: simulate_signals(commission=..., slippage=...), which takes ziplime's own "
        "models and books them here. Set them before the sweep rather than after it -- costs "
        "change which parameters win.\n"
        "The kernel takes ziplime's own models directly, so most of these move across unchanged. "
        "The ones it cannot reduce to numbers -- VolumeShareSlippage, the per-contract and bond "
        "commissions -- need the event-driven path, where the blotter fills every order "
        "(ziplime.vectorized.signals vectorises such a strategy's arithmetic without touching "
        "its execution).\n"
        "Pass ignore_exchange_costs=True to replay with these models ignored deliberately."
    )


async def to_execution_result(
        portfolio,
        listings: dict[str, ExchangeAsset],
        prices: pd.DataFrame,
        trading_calendar,
        exchange,
        asset_service=None,
        benchmark_source=None,
        metrics=None,
        emission_rate: datetime.timedelta = datetime.timedelta(days=1),
        trading_account_id: str = "vectorized_account",
        tolerance: float = DEFAULT_TOLERANCE,
        reconcile: bool = True,
        ignore_exchange_costs: bool = False,
) -> TradingAlgorithmExecutionResult:
    """Replay a vectorbt portfolio through ziplime's ledger and return its execution result.

    Args:
        portfolio: A ``vectorbt.Portfolio``.
        listings: ``{portfolio column: ziplime listing}``. See :func:`replay_transactions`.
        prices: Close prices the positions are marked at, indexed by the same timestamps the
            portfolio uses, one column per portfolio column. This is what vectorbt valued the
            book with, and marking against anything else guarantees the reconciliation fails.
        trading_calendar: Calendar the sessions belong to.
        exchange: Exchange the transactions are booked on. Its starting cash has to match the
            portfolio's ``init_cash``, since that is the capital both sides are measuring.
        asset_service: Passed through to the metrics tracker. Optional; nothing in the default
            metric set reaches it.
        benchmark_source: Benchmark to measure against. ``None`` gives zero benchmark returns,
            which is a statement about the run rather than a missing value.
        metrics: Metric set. Defaults to :func:`~ziplime.finance.metrics.default_metrics`, which
            is what makes the result comparable to an event-driven one.
        emission_rate: Bar interval of the portfolio.
        trading_account_id: Account the transactions are booked to.
        tolerance: See :data:`DEFAULT_TOLERANCE`.
        reconcile: Check the replayed value against vectorbt's. Turning it off is for debugging a
            divergence, not for living with one.
        ignore_exchange_costs: Proceed even though the exchange carries commission or slippage
            models that this path cannot run. See :func:`_reject_ignored_cost_models`.

    Returns:
        The same result type an event-driven run produces, with ``trading_algorithm`` set to a
        :class:`VectorizedRun`.

    Raises:
        ReconciliationError: if the replayed book and vectorbt disagree by more than ``tolerance``.
        ValueError: if the exchange carries cost models this path would silently ignore, unless
            ``ignore_exchange_costs`` says to proceed without them.
    """
    # The kernel's own result is the native input; a vectorbt portfolio is the one this module
    # was written for. Converting here rather than asking every caller to wrap means the kernel
    # never has to speak vectorbt's vocabulary -- see `VectorPortfolio`, which is the scaffolding
    # this line hides.
    from ziplime.vectorized.kernel.models import VectorSimulationResult

    if isinstance(portfolio, VectorSimulationResult):
        portfolio = portfolio.as_portfolio()

    index = portfolio.wrapper.index
    if len(index) == 0:
        raise ValueError("The portfolio has no bars to replay.")

    _reject_unrepresentable_instruments(listings)
    if not ignore_exchange_costs:
        _reject_ignored_cost_models(exchange, listings)

    # Bars are not sessions. At a daily rate they coincide and every index below is the identity,
    # which is why this went unnoticed; at a minute rate a session is 390 bars, and treating each
    # one as a session gives the ledger a returns array 390 times too long and the metrics a
    # benchmark that cannot align with it.
    opening, closing = _session_boundaries(index)
    opens_at, closes_at = set(opening), set(closing)
    session_closes = pd.DatetimeIndex([index[ix] for ix in closing])
    sessions = pl.Series([stamp.date() for stamp in session_closes])
    intraday = emission_rate < datetime.timedelta(days=1)
    ledger = Ledger(trading_sessions=session_closes, data_frequency=emission_rate)
    starting_cash = exchange.get_start_cash_balance()
    ledger._portfolio.cash = starting_cash
    ledger._portfolio.starting_cash = starting_cash
    ledger._portfolio.portfolio_value = starting_cash

    reported_value = portfolio.value()
    _require_single_book(reported_value)

    transactions = replay_transactions(
        portfolio, listings=listings, exchange_name=exchange.name,
        trading_account_id=trading_account_id)
    by_stamp: dict[datetime.datetime, list[Transaction]] = {}
    for transaction in transactions:
        by_stamp.setdefault(transaction.dt, []).append(transaction)

    if benchmark_source is None:
        benchmark_source = _zero_benchmark(
            asset_service=asset_service, trading_calendar=trading_calendar, sessions=sessions,
            exchange=exchange, emission_rate=emission_rate, index=index)

    tracker = MetricsTracker(
        sessions=sessions,
        asset_service=asset_service,
        exchanges={exchange.name: exchange},
        trading_calendar=trading_calendar,
        first_session=index[0].date(),
        last_session=index[-1].date(),
        emission_rate=emission_rate,
        ledger=ledger,
        metrics=metrics if metrics is not None else default_metrics(),
        benchmark_source=benchmark_source,
    )
    await tracker.handle_start_of_simulation()

    # Bars the ledger actually has to be brought up to date on: every session close, because that
    # is what the result reports, and every bar carrying a trade, because that is where cash moves.
    # At a daily rate this is every bar, so the loop below behaves exactly as it did.
    material = set(closes_at)
    material.update(position for position, stamp in enumerate(index) if stamp in by_stamp)

    packets = []
    session_ix = 0
    for bar_ix, stamp in enumerate(index):
        # The event-driven path opens each session with this and so must the replay. It clears the
        # session's transaction list and, more consequentially, snapshots the running return so
        # that `todays_returns` is the day's rather than the whole run's. Skipping it leaves the
        # portfolio value correct -- reconciliation still passes -- while every metric computed
        # from daily returns is silently wrong: Sharpe, volatility, sortino, drawdown.
        if bar_ix in opens_at:
            ledger.start_of_session(session_label=stamp.to_pydatetime())
            if intraday:
                # Sets the session the minute packets belong to and runs the start-of-session
                # metrics. At a daily rate the existing path reaches the same state without it,
                # and is left alone rather than changed to match a rate it never runs at.
                await tracker.handle_market_open(session_label=stamp.date())

        for transaction in by_stamp.get(stamp, ()):
            ledger.process_transaction(transaction)
            # The fee has to be booked separately. `Transaction.commission` is a record of what was
            # charged, not an instruction to charge it -- the ledger moves cash in
            # `process_commission`, and setting only the field leaves the two sides apart by
            # exactly the fees, which is what reconciliation then reports.
            if transaction.commission:
                ledger.process_commission(
                    commission=Commission(asset=transaction.asset, order=None,
                                          amount=transaction.commission),
                    tr=None)

        # Revaluing the book is the expensive part -- it re-derives every position's statistics --
        # and on intraday bars almost all of it is thrown away. A mark taken at 10:04 changes
        # nothing that survives to 10:05: the session's row is valued at the session's close, and
        # the returns that feed the metrics are differences between closes. So the ledger is
        # walked only where the result is read -- each session's close -- and wherever cash moves,
        # since a trade has to be reconciled at the bar it was priced on.
        #
        # This holds because nothing on this path accumulates between marks. An instrument that
        # settled variation margin against its previous mark would make every intermediate bar
        # material; futures and options are refused here for other reasons, and if that ever
        # changes this is the first thing to revisit.
        if bar_ix in material:
            marks = {}
            for column, listing in listings.items():
                price = prices[column].iloc[bar_ix] if column in prices.columns else None
                if price is not None and not (isinstance(price, float) and math.isnan(price)):
                    marks[(listing.sid, exchange)] = float(price)
            if marks:
                ledger.sync_last_sale_prices(dt=stamp.to_pydatetime(), prices=marks)
            await ledger.update_portfolio()
            ledger.update_account()

        if reconcile and bar_ix in material:
            replayed = float(ledger.portfolio.portfolio_value)
            expected = float(_value_at(reported_value, bar_ix))
            if abs(replayed - expected) > tolerance * max(abs(starting_cash), 1.0):
                raise ReconciliationError(session=stamp.date(), replayed=replayed,
                                          reported=expected, tolerance=tolerance)

        # Order matters, and it is the opposite of the obvious one. `end_of_session` writes this
        # session's return into the ledger's daily-returns series, and the risk metrics read that
        # series rather than the packet -- so closing the session after asking for the packet
        # leaves every metric computing on a series one element short. The portfolio value, the
        # cash and the `returns` column all still agree to machine precision while the Sharpe
        # ratio quietly differs in the second decimal. The event-driven loop closes first.
        # The engine calls `handle_minute_close` on every intraday bar; the replay deliberately
        # does not. Its packet is the minute packet, which a replay has no use for and discards,
        # and its one ledger effect -- `end_of_bar` writing the partial return into
        # `daily_returns_array[session_ix]` -- lands in the same slot `end_of_session` overwrites a
        # moment later with the settled value. What it costs is not small: it runs the full
        # cumulative metric set, empyrical's alpha/beta included, once per minute, which was 59% of
        # the replay on minute bars and made the vectorised path slower than simply running the
        # strategy event-driven. Skipping it changes no column of the result; `MinuteBarTests`
        # holds that.

        if bar_ix in closes_at:
            ledger.end_of_session(session_ix=session_ix)
            session_ix += 1
            packet = tracker.handle_market_close(dt=stamp.to_pydatetime())
            packet["daily_perf"]["recorded_vars"] = {}
            packet["daily_perf"].update(packet["cumulative_risk_metrics"])
            packets.append(packet["daily_perf"])

    risk_report = tracker.handle_simulation_end()
    perf = pd.DataFrame(packets, index=pd.DatetimeIndex([p["period_close"] for p in packets]))
    _logger.info("Replayed a vectorbt portfolio", sessions=len(perf),
                 transactions=len(transactions), instruments=len(listings))
    return TradingAlgorithmExecutionResult(
        trading_algorithm=VectorizedRun(ledger=ledger, source=portfolio),
        perf=perf,
        risk_report=risk_report,
        errors=[],
    )


def _session_boundaries(index) -> tuple[list[int], list[int]]:
    """Positions of the first and last bar of each trading session, in chronological order.

    Ordered lists rather than sets, and that is not a detail. Built as sets, the session closes
    came back out in whatever order the set chose to iterate, which for positions 0..1000 is
    ascending -- so the daily path and the short intraday runs looked correct -- and for positions
    spread over 23,400 minute bars is not. The sessions then reached the metrics shuffled, the
    benchmark's date range ran from a later date to an earlier one, matched nothing, and the run
    died several frames away in a metric holding a None it had quietly assigned itself.

    At a daily rate every bar opens and closes its own session, so both lists are simply the bar
    positions and everything built from them collapses to the bar index itself.
    """
    opening, closing = [], []
    previous = None
    for position, stamp in enumerate(index):
        session = stamp.date()
        if session != previous:
            opening.append(position)
            if position:
                closing.append(position - 1)
            previous = session
    if len(index):
        closing.append(len(index) - 1)
    return opening, closing


def _zero_benchmark(asset_service, trading_calendar, sessions, exchange,
                    emission_rate: datetime.timedelta, index) -> BenchmarkSource:
    """A benchmark of zero returns, for a run measured against nothing.

    The default metric set always asks for a benchmark -- alpha and beta are in it -- so there is
    no "no benchmark" to pass. Zero returns is the honest stand-in and is what the event-driven
    path builds for the same case, which matters here: a vectorised run and an event-driven run of
    the same strategy have to be measured against the same thing to be comparable at all.
    """
    zeros = pl.DataFrame({
        "date": pl.Series(list(index)).cast(pl.Datetime(time_unit="us", time_zone=str(index.tz))),
        "close": [0.0] * len(index),
    })
    return BenchmarkSource(
        asset_service=asset_service, trading_calendar=trading_calendar, sessions=sessions,
        exchange=exchange, emission_rate=emission_rate,
        benchmark_fields=frozenset({"close"}), precalculated_series=zeros,
        benchmark_asset=None, benchmark_returns=None)


def _require_single_book(value) -> None:
    """Refuse a portfolio that is really several.

    ``from_signals`` over a parameter grid returns one book per combination, and so does a
    portfolio whose columns were never grouped. Converting one of those has no answer -- which
    book's ledger? -- and the failure it would otherwise produce is a `KeyError` about missing
    listings, because the columns are parameter tuples rather than tickers. Checked here, before
    anything is replayed, so the message is about the shape rather than about the symptom.
    """
    if isinstance(value, pd.DataFrame) and value.shape[1] != 1:
        raise ValueError(
            f"The portfolio reports {value.shape[1]} value series, so it holds more than one book "
            f"-- a parameter sweep, or columns that were not grouped. Converting needs one: "
            f"select a combination with `portfolio[key]`, or rebuild it with group_by and "
            f"cash_sharing.")


def _value_at(value, index: int) -> float:
    """One bar's portfolio value, whether vectorbt returned a Series or a one-column frame."""
    if isinstance(value, pd.DataFrame):
        return value.iloc[index, 0]
    return value.iloc[index]
