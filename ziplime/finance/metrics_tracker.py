import datetime
import polars as pl
import structlog
from exchange_calendars import ExchangeCalendar

from ziplime.assets.services.asset_service import AssetService
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.ledger import Ledger
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

        if emission_rate == DataFrequency.MINUTE:

            def progress(self):
                return 1.0  # a fake value

        else:

            def progress(self):
                return self._session_count / self._total_session_count

        # don't compare these strings over and over again!
        self._progress = progress

    def handle_start_of_simulation(self):
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
        self._ledger.sync_last_sale_prices(dt=dt, handle_non_market_minutes=False)
        packet = {
            "period_start": self._first_session,
            "period_end": self._last_session,
            "capital_base": list(self.exchanges.values())[0].get_start_cash_balance(), # TODO: add support for multiple exchanges
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
        for metric in self._end_of_bar_metrics:
            metric.end_of_bar(
                packet=packet,
                ledger=ledger,
                session=dt,
                session_ix=self._session_count,
                exchanges=self.exchanges,
            )
        return packet

    async def handle_market_open(self, session_label: datetime.datetime) -> None:
        """Handles the start of each session.

        Parameters
        ----------
        session_label : Timestamp
            The label of the session that is about to begin.
        """
        self._ledger.start_of_session(session_label=session_label)

        # TODO: handle ajustments repository
        adjustment_reader = self.asset_service._adjustments_repository
        if adjustment_reader is not None:
            # this is None when running with a dataframe source
            await self._ledger.process_dividends(
                next_session=session_label,
                adjustment_reader=adjustment_reader,
            )

        self._current_session = session_label
        self._market_open = self._trading_calendar.session_first_minute(session_label)

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

        if self.emission_rate == datetime.timedelta(days=1):
            # this method is called for both minutely and daily emissions, but
            # this chunk of code here only applies for daily emissions. (since
            # it's done every minute, elsewhere, for minutely emission).
            self._ledger.sync_last_sale_prices(dt=dt, handle_non_market_minutes=False)

        session_ix = self._session_count
        # increment the day counter before we move markers forward.
        self._session_count += 1

        packet = {
            "period_start": self._first_session,
            "period_end": self._last_session,
            "capital_base": list(self.exchanges.values())[0].get_start_cash_balance(), # TODO: add support for multiple exchanges
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
        self._ledger.end_of_session(session_ix=session_ix)

        for metric in self._end_of_session_metrics:
            metric.end_of_session(
                packet=packet,
                ledger=self._ledger,
                session=self._current_session,
                session_ix=session_ix,
                exchanges=self.exchanges,
            )
        return packet

    def handle_simulation_end(self):
        """When the simulation is complete, run the full period risk report
        and send it out on the results socket.
        """
        self._logger.info(
            f"Simulated {self._session_count} trading days\n first open: "
            f"{self._trading_calendar.session_open(self._first_session).astimezone(tz=self._trading_calendar.tz)}\n "
            f"last close: {self._trading_calendar.session_close(self._last_session).astimezone(tz=self._trading_calendar.tz)}",
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
        return packet
