import datetime
from copy import copy, deepcopy
from dataclasses import replace
from types import SimpleNamespace

import numpy as np
import pandas as pd
from typing import Any
import polars as pl
import structlog
from exchange_calendars import ExchangeCalendar

from ziplime.assets.services.asset_service import AssetService
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.ledger import Ledger
from ziplime.finance.domain.currency_reporting import CurrencyReporting
from ziplime.sources.currency_benchmark_source import CurrencyBenchmarkSource
from ziplime.domain.data_frequency import DataFrequency
from ziplime.sources.benchmark_source import BenchmarkSource


class MetricsTracker:
    """The algorithm's interface to the registered risk and performance
    metrics.

    Parameters
    ----------
    trading_calendar : TradingCalendar
        The trading calendar used in the simulation.
    first_session : datetime.datetime
        The label of the first trading session in the simulation.
    last_session : datetime.datetime
        The label of the last trading session in the simulation.
    capital_base : float
        The starting capital for the simulation.
    emission_rate : {'daily', 'minute'}
        How frequently should a performance packet be generated?
    data_frequency : {'daily', 'minute'}
        The data frequency of the data portal.
    metrics : list[Metric]
        The metrics to track.
    currency_reporting : CurrencyReporting or None, optional
        Independent currency metric histories, deep-copied metric sets, and translated
        benchmarks. Investment mode additionally initializes native opening cash
        and historical FIFO accounting before any metric start hooks.

    Attributes
    ----------
    _currency_reporting : CurrencyReporting or None
        Optional configuration shared by the native and currency trackers.
        Only the native tracker creates currency-specific children.
    _currency_trackers : dict[str, MetricsTracker]
        Independently initialized metric histories in configured currency order.
    currency : str or None
        Reporting currency; child trackers own performance state and share the execution ledger.
    """

    def __init__(
            self,
            sessions: pl.Series,
            asset_service: AssetService,
            exchanges: dict[str, Exchange],
            trading_calendar: ExchangeCalendar,
            first_session: datetime.datetime,
            last_session: datetime.datetime,
            emission_rate: datetime.timedelta,
            ledger: Ledger,
            metrics,
            benchmark_source: BenchmarkSource,
            intraday_metrics: bool = False,
            currency_reporting: CurrencyReporting | None = None,
            currency: str | None = None,
    ):
        self.emission_rate = emission_rate
        self._benchmark_source = benchmark_source
        self._logger = structlog.getLogger(__name__)

        self._trading_calendar = trading_calendar
        self._first_session = first_session
        self._last_session = last_session

        self._current_session = first_session
        self._market_open = trading_calendar.session_first_minute(first_session)
        self.exchanges = exchanges
        self.asset_service = asset_service
        # self.data_bundle = data_bundle
        self._session_count = 0

        self._sessions = sessions
        self._total_session_count = len(sessions)
        self._ledger = ledger
        self._metrics = metrics
        self._currency_reporting = currency_reporting
        self._currency_trackers: dict[str, MetricsTracker] = {}
        self.currency = currency
        self._portfolio = self._account = None
        self.position_tracker = None
        self.investment = None
        self.initial_investment_pnl = 0.0
        self._initial_rate = self._initial_value = self._initial_external = None
        self._event_ix = 0
        self._external_capital = self._cash_flow_total = 0.0
        self._previous_total_returns = 0.0
        if currency is not None:
            self.daily_returns_series = pd.Series(np.nan, index=ledger.daily_returns_series.index)
            self.daily_returns_array = self.daily_returns_series.values

        self._start_of_simulation_metrics = [
            metric for metric in self._metrics if getattr(metric, "start_of_simulation", None)
        ]
        self._end_of_simulation_metrics = [
            metric for metric in self._metrics if getattr(metric, "end_of_simulation", None)
        ]
        self._start_of_session_metrics = [
            metric for metric in self._metrics if getattr(metric, "start_of_session", None)
        ]
        self._end_of_session_metrics = [
            metric for metric in self._metrics if getattr(metric, "end_of_session", None)
        ]
        self._end_of_bar_metrics = [
            metric for metric in self._metrics if getattr(metric, "end_of_bar", None)
        ]
        # At an intraday rate `end_of_bar` fires on every bar of the run -- 390 times a session at
        # one minute -- and in the default set all but one of those metrics do nothing except fill
        # the minute packet, which `TradingAlgorithmExecutor` discards: the performance table it
        # builds has one row per session. Computing a cumulative Sharpe ratio, alpha and beta on
        # every minute to throw all but the last away was 86% of an intraday run.
        #
        # So unless intraday packets were asked for, only the metrics that carry something from
        # one bar to the next run there -- `MaxLeverage` and its running maximum, which would
        # otherwise miss every intraday peak. A metric claims to be skippable by setting
        # `packet_only`; anything that does not is assumed to keep state and keeps running, so an
        # unfamiliar metric is slow rather than silently wrong.
        self.intraday_metrics = intraday_metrics
        self._every_bar_metrics = (
            self._end_of_bar_metrics if intraday_metrics
            else [metric for metric in self._end_of_bar_metrics
                  if not getattr(metric, "packet_only", False)]
        )

        if emission_rate == DataFrequency.MINUTE:

            def progress(self):
                return 1.0  # a fake value

        else:

            def progress(self):
                return self._session_count / self._total_session_count

        # don't compare these strings over and over again!
        self._progress = progress

    @property
    def portfolio(self):
        return self._portfolio if self.currency is not None else self._ledger.portfolio

    @property
    def account(self):
        return self._account if self.currency is not None else self._ledger.account

    @property
    def positions(self):
        return self._ledger.positions

    def transactions(self, dt=None):
        return [replace(transaction) for transaction in self._ledger.transactions(dt=dt)]

    def orders(self, dt=None):
        return [copy(order) for order in self._ledger.orders(dt=dt)]

    @property
    def todays_returns(self):
        return (1 + self.portfolio.returns) / (1 + self._previous_total_returns) - 1

    @property
    def _metric_ledger(self):
        # Existing metric hooks receive the same fields for native and currency reports.
        return self if self.currency is not None else self._ledger

    def initialize_currency(self, dt: datetime.datetime) -> None:
        reporting = self._currency_reporting
        self._initial_rate = reporting.conversion.rate(base=reporting.base_currency, quote=self.currency, dt=dt)
        portfolio, _, _, investment = self._ledger.currency_snapshot(self.currency, dt)
        self._initial_value = portfolio.portfolio_value
        self._cash_flow_total = portfolio.cash_flow
        if self._ledger.native_currency_accounting:
            self._initial_external = self._ledger.external_value(self.currency, dt)
        else:
            self._initial_external = 0.0
        if investment is not None:
            self.initial_investment_pnl = investment.total_pnl
        self.refresh(dt)

    def refresh(self, dt: datetime.datetime) -> None:
        """Refresh this currency's data; retain independent performance history here."""
        if self.currency is None:
            raise ValueError("Only currency-specific metric trackers have reporting snapshots.")
        reporting = self._currency_reporting
        portfolio, account, stats, investment = self._ledger.currency_snapshot(self.currency, dt)
        rate = reporting.conversion.rate(base=reporting.base_currency, quote=self.currency, dt=dt)
        if self._ledger.native_currency_accounting:
            external = self._ledger.external_value(self.currency, dt) - self._initial_external
            cash_flow = portfolio.cash_flow
        else:
            external = self._external_capital
            cash_flow = self._cash_flow_total
            for stamp, amount, is_external in self._ledger.reporting_events[self._event_ix:]:
                converted = amount * reporting.conversion.rate(base=reporting.base_currency, quote=self.currency, dt=stamp)
                if is_external:
                    external += converted
                else:
                    cash_flow += converted
            self._event_ix = len(self._ledger.reporting_events)
            self._external_capital, self._cash_flow_total = external, cash_flow
        self._portfolio = replace(
            portfolio, starting_cash=self._initial_value, cash_flow=cash_flow,
            pnl=portfolio.portfolio_value - self._initial_value - external,
            returns=(1 + self._ledger.portfolio.returns) * rate / self._initial_rate - 1)
        self._account = account
        self.position_tracker = SimpleNamespace(stats=stats)
        self.investment = investment

    async def handle_start_of_simulation(self):
        if self._currency_reporting is not None:
            reporting = self._currency_reporting
            initial_dt = self._trading_calendar.session_open(session=self._first_session).to_pydatetime()
            if self.currency is not None:
                self.initialize_currency(initial_dt)
            else:
                self._ledger.enable_currency_reporting(
                    base_currency=reporting.base_currency, dt=initial_dt, conversion=reporting.conversion)
                if reporting.investment_pnl:
                    if reporting.opening_cash is None:
                        if len(self.exchanges) != 1:
                            raise ValueError("Multi-exchange accounting requires explicit opening_cash by account.")
                        exchange = next(iter(self.exchanges.values()))
                        opening_cash = {
                            (exchange.name, exchange.account_id, reporting.base_currency): self._ledger.portfolio.cash}
                    else:
                        opening_cash = dict(reporting.opening_cash)
                    self._ledger.enable_investment_accounting(reporting=reporting, dt=initial_dt, opening_cash=opening_cash)
                for currency in reporting.currencies:
                    tracker = MetricsTracker(
                        sessions=self._sessions, asset_service=self.asset_service,
                        exchanges=self.exchanges, trading_calendar=self._trading_calendar,
                        first_session=self._first_session, last_session=self._last_session,
                        emission_rate=self.emission_rate, ledger=self._ledger,
                        metrics=deepcopy(self._metrics), currency_reporting=reporting, currency=currency,
                        benchmark_source=CurrencyBenchmarkSource(
                            source=self._benchmark_source, reporting=reporting, currency=currency, initial_dt=initial_dt),
                        intraday_metrics=self.intraday_metrics)
                    self._currency_trackers[currency] = tracker
                    await tracker.handle_start_of_simulation()
        for metric in self._start_of_simulation_metrics:
            metric.start_of_simulation(
                ledger=self._metric_ledger, emission_rate=self.emission_rate,
                trading_calendar=self._trading_calendar, sessions=self._sessions,
                benchmark_source=self._benchmark_source)

    def handle_minute_close(self, dt: datetime.datetime):
        """Handles the close of the given minute in minute emission.

        Parameters
        ----------
        dt : Timestamp
            The minute that is ending

        Returns
        -------
        A minute perf packet.
        """
        packet = {
            "period_start": self._first_session,
            "period_end": self._last_session,
            # TODO: add support for multiple exchanges
            "capital_base": list(self.exchanges.values())[0].get_start_cash_balance(),
            "minute_perf": {
                "period_open": self._market_open,
                "period_close": dt,
            },
            "cumulative_perf": {
                "period_open": self._first_session,
                "period_close": self._last_session,
            },
            "progress": self._progress(self),
            "cumulative_risk_metrics": {},
        }
        ledger = self._metric_ledger
        if self.currency is None:
            ledger.end_of_bar(session_ix=self._session_count)
        else:
            self.daily_returns_array[self._session_count] = self.todays_returns
        for metric in self._every_bar_metrics:
            metric.end_of_bar(
                packet=packet,
                ledger=ledger,
                session=dt,
                session_ix=self._session_count,
                exchanges=self.exchanges,
            )
        self._add_currency_packets(packet=packet, dt=dt, intraday=True)
        return packet

    async def handle_market_open(self, session_label: datetime.datetime) -> None:
        """Handles the start of each session.

        Parameters
        ----------
        session_label : Timestamp
            The label of the session that is about to begin.
        """

        self._current_session = session_label
        self._market_open = self._trading_calendar.session_first_minute(session_label)

        if self.currency is not None:
            self._previous_total_returns = self.portfolio.returns
        for tracker in self._currency_trackers.values():
            await tracker.handle_market_open(session_label=session_label)

        for metric in self._start_of_session_metrics:
            metric.start_of_session(ledger=self._metric_ledger, session=session_label, exchanges=self.exchanges)
        # self.start_of_session(ledger=self._metric_ledger, session=session_label, data_bundle=data_bundle)

    def handle_market_close(self, dt: datetime.datetime):
        """Handles the close of the given day.

        Parameters
        ----------
        dt : Timestamp
            The most recently completed simulation datetime.
        Returns
        -------
        A daily perf packet.
        """

        session_ix = self._session_count
        # increment the day counter before we move markers forward.
        self._session_count += 1

        packet = {
            "period_start": self._first_session,
            "period_end": self._last_session,
            # TODO: add support for multiple exchanges
            "capital_base": list(self.exchanges.values())[0].get_start_cash_balance(),
            "daily_perf": {
                "period_open": self._market_open,
                "period_close": dt,
            },
            "cumulative_perf": {
                "period_open": self._first_session,
                "period_close": self._last_session,
            },
            "progress": self._progress(self),
            "cumulative_risk_metrics": {},
        }
        if self.currency is not None:
            self.daily_returns_array[session_ix] = self.todays_returns

        for metric in self._end_of_session_metrics:
            metric.end_of_session(
                packet=packet,
                ledger=self._metric_ledger,
                session=self._current_session,
                session_ix=session_ix,
                exchanges=self.exchanges,
            )
        self._add_currency_packets(packet=packet, dt=dt, intraday=False)
        return packet

    def _add_currency_packets(self, packet: dict[str, Any], dt: datetime.datetime, intraday: bool) -> None:
        """Attach independent currency metrics and optional investment snapshots in place.

        Parameters
        ----------
        packet : dict[str, Any]
            Native packet to augment with ``metrics_by_currency``. Unchanged when
            no reporting trackers are configured.
        dt : datetime.datetime
            Aware bar/session close used for current balances, marks, and FX.
        intraday : bool
            Select minute-close hooks/``minute_perf`` rather than daily-close
            hooks/``daily_perf``.

        Notes
        -----
        Each currency packet carries ``currency`` and ``native_currency`` labels.
        Investment mode adds an immutable ``investment`` snapshot plus lifetime
        realized/unrealized PNL, income, fees, remaining cost, and total security
        PNL. Fees are already included in PNL. ``cash_fx_pnl`` is cumulative
        portfolio PNL less the simulation-period change in lifetime security PNL;
        it includes cash FX/execution differences, not external contributions.
        """
        if not self._currency_trackers:
            return
        packets = {}
        for currency, tracker in self._currency_trackers.items():
            tracker.refresh(dt=dt)
            if intraday:
                converted = tracker.handle_minute_close(dt=dt)
            else:
                converted = tracker.handle_market_close(dt=dt)
            converted["capital_base"] = tracker.portfolio.starting_cash
            converted["currency"] = currency
            converted["native_currency"] = self._currency_reporting.base_currency
            if tracker.investment is not None:
                snapshot = tracker.investment
                converted["investment"] = snapshot
                fields = {
                    "investment_realized_pnl": snapshot.realized_pnl,
                    "investment_unrealized_pnl": snapshot.unrealized_pnl,
                    "investment_income": snapshot.income,
                    "investment_fees": snapshot.fees,
                    "investment_pnl": snapshot.total_pnl,
                    "investment_cost": snapshot.historical_cost,
                    "cash_fx_pnl": tracker.portfolio.pnl - (snapshot.total_pnl - tracker.initial_investment_pnl),
                }
                converted["cumulative_perf"].update(fields)
                converted["minute_perf" if intraday else "daily_perf"].update(fields)
            packets[currency] = converted
        packet["metrics_by_currency"] = packets

    def handle_simulation_end(self):
        """When the simulation is complete, run the full period risk report
        and send it out on the results socket.
        """
        self._logger.info(
            f"Simulated {self._session_count} trading days\n first open: "
            f"{self._trading_calendar.session_open(self._first_session).astimezone(tz=self._trading_calendar.tz)}\n "
            "last close: "
            f"{self._trading_calendar.session_close(self._last_session).astimezone(tz=self._trading_calendar.tz)}",
        )

        packet = {}
        for metric in self._end_of_simulation_metrics:
            metric.end_of_simulation(
                packet=packet,
                ledger=self._metric_ledger,
                trading_calendar=self._trading_calendar,
                sessions=self._sessions,
                data_bundle=self.data_bundle,
                benchmark_source=self._benchmark_source,
            )
        if self._currency_trackers:
            packet["risk_report_by_currency"] = {
                currency: tracker.handle_simulation_end()
                for currency, tracker in self._currency_trackers.items()
            }
        return packet
