import datetime
from copy import deepcopy
from typing import Any
import polars as pl
import structlog
from exchange_calendars import ExchangeCalendar

from ziplime.assets.services.asset_service import AssetService
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.ledger import Ledger
from ziplime.finance.domain.currency_reporting import CurrencyReporting
from ziplime.finance.domain.reporting_ledger import ReportingLedger
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
        Independent currency ledgers, deep-copied metric sets, and translated
        benchmarks. Investment mode additionally initializes native opening cash
        and historical FIFO accounting before any metric start hooks.

    Attributes
    ----------
    _currency_reporting : CurrencyReporting or None
        Optional configuration; child currency trackers leave this unset to
        avoid recursively creating further currency trackers.
    _currency_trackers : dict[str, MetricsTracker]
        Independently initialized metric histories in configured currency order.
    _reporting_ledgers : dict[str, ReportingLedger]
        Currency-specific monetary views sharing the one native execution ledger.
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
        self._reporting_ledgers: dict[str, ReportingLedger] = {}

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

    async def handle_start_of_simulation(self):
        if self._currency_reporting is not None:
            reporting = self._currency_reporting
            initial_dt = self._trading_calendar.session_open(session=self._first_session).to_pydatetime()
            self._ledger.enable_currency_reporting(base_currency=reporting.base_currency, dt=initial_dt)
            if reporting.investment_pnl:
                if reporting.opening_cash is None:
                    if len(self.exchanges) != 1:
                        raise ValueError("Multi-exchange investment books require explicit opening_cash by account.")
                    exchange = next(iter(self.exchanges.values()))
                    opening_cash = {
                        (exchange.name, exchange.account_id, reporting.base_currency): self._ledger.portfolio.cash}
                else:
                    opening_cash = dict(reporting.opening_cash)
                self._ledger.enable_investment_accounting(reporting=reporting, dt=initial_dt, opening_cash=opening_cash)
            for currency in reporting.currencies:
                ledger = ReportingLedger(
                    source=self._ledger,
                    reporting=reporting,
                    currency=currency,
                    initial_dt=initial_dt,
                )
                tracker = MetricsTracker(
                    sessions=self._sessions, asset_service=self.asset_service,
                    exchanges=self.exchanges, trading_calendar=self._trading_calendar,
                    first_session=self._first_session, last_session=self._last_session,
                    emission_rate=self.emission_rate, ledger=ledger,
                    metrics=deepcopy(x=self._metrics),
                    benchmark_source=CurrencyBenchmarkSource(
                        source=self._benchmark_source, reporting=reporting, currency=currency, initial_dt=initial_dt),
                    intraday_metrics=self.intraday_metrics,
                )
                self._currency_trackers[currency] = tracker
                self._reporting_ledgers[currency] = ledger
                await tracker.handle_start_of_simulation()
        for metric in self._start_of_simulation_metrics:
            metric.start_of_simulation(
                ledger=self._ledger,
                emission_rate=self.emission_rate,
                trading_calendar=self._trading_calendar,
                sessions=self._sessions,
                benchmark_source=self._benchmark_source,
            )

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
        ledger = self._ledger
        ledger.end_of_bar(session_ix=self._session_count)
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

        for currency, tracker in self._currency_trackers.items():
            self._reporting_ledgers[currency].start_of_session(session_label=session_label)
            await tracker.handle_market_open(session_label=session_label)

        for metric in self._start_of_session_metrics:
            metric.start_of_session(ledger=self._ledger, session=session_label, exchanges=self.exchanges)
        # self.start_of_session(ledger=self._ledger, session=session_label, data_bundle=data_bundle)

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
        # self._ledger.end_of_session(session_ix=session_ix)

        for metric in self._end_of_session_metrics:
            metric.end_of_session(
                packet=packet,
                ledger=self._ledger,
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
            ledger = self._reporting_ledgers[currency]
            ledger.refresh(dt=dt)
            if intraday:
                converted = tracker.handle_minute_close(dt=dt)
            else:
                ledger.end_of_session(session_ix=tracker._session_count)
                converted = tracker.handle_market_close(dt=dt)
            converted["capital_base"] = ledger.portfolio.starting_cash
            converted["currency"] = currency
            converted["native_currency"] = ledger.reporting.base_currency
            if ledger.investment is not None:
                snapshot = ledger.investment
                converted["investment"] = snapshot
                fields = {
                    "investment_realized_pnl": snapshot.realized_pnl,
                    "investment_unrealized_pnl": snapshot.unrealized_pnl,
                    "investment_income": snapshot.income,
                    "investment_fees": snapshot.fees,
                    "investment_pnl": snapshot.total_pnl,
                    "investment_cost": snapshot.historical_cost,
                    "cash_fx_pnl": ledger.portfolio.pnl - (snapshot.total_pnl - ledger.initial_investment_pnl),
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
                ledger=self._ledger,
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
