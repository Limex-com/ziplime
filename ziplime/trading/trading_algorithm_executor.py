from typing import AsyncIterator

import pandas as pd
import structlog
from logging import Logger

from ziplime.trading.trading_algorithm import TradingAlgorithm
from ziplime.trading.trading_algorithm_execution_result import TradingAlgorithmExecutionResult
from ziplime.trading.trading_algorithm_execution_status import TradingAlgorithmExecutionStatus
from ziplime.utils.date_utils import make_utc_aware
from ziplime.utils.api_support import ZiplineAPI


class TradingAlgorithmExecutor:

    def __init__(self, logger: Logger = structlog.get_logger(__name__)):
        self._logger = logger

    def _create_daily_stats(self, perfs):
        # create daily and cumulative stats dataframe
        daily_perfs = []
        # TODO: the loop here could overwrite expected properties
        # of daily_perf. Could potentially raise or log a
        # warning.
        for perf in perfs:
            if "daily_perf" in perf:
                perf["daily_perf"].update(perf["daily_perf"].pop("recorded_vars"))
                perf["daily_perf"].update(perf["cumulative_risk_metrics"])
                daily_perfs.append(perf["daily_perf"])
            else:
                risk_report = perf

        daily_dts = pd.DatetimeIndex([p["period_close"] for p in daily_perfs])
        # daily_dts = make_utc_aware(daily_dts)
        daily_stats = pd.DataFrame(daily_perfs, index=daily_dts)
        return daily_stats, risk_report

    async def run_algorithm(self, trading_algorithm: TradingAlgorithm) -> TradingAlgorithmExecutionResult:
        """Run the algorithm."""
        self._logger.info("Running algorithm")

        # Create ziplime and loop through simulated_trading.
        # Each iteration returns a perf dictionary
        try:
            perfs = []
            errors = []
            async for perf, errors in await trading_algorithm.get_generator():
                perfs.append(perf)

            # convert perf dict to pandas dataframe
            daily_stats, risk_report = self._create_daily_stats(perfs)

            self.analyze(trading_algorithm=trading_algorithm, perf=daily_stats)
        finally:
            trading_algorithm.data_portal = None
            trading_algorithm.metrics_tracker = None

        return TradingAlgorithmExecutionResult(
            trading_algorithm=trading_algorithm,
            perf=daily_stats,
            risk_report=risk_report,
            errors=errors
        )

    async def run_algorithm_iter(self, trading_algorithm: TradingAlgorithm) -> AsyncIterator[
        TradingAlgorithmExecutionStatus]:
        """Run the algorithm."""
        self._logger.info("Running algorithm")

        # Create ziplime and loop through simulated_trading.
        # Each iteration returns a perf dictionary
        try:
            perfs = []
            errors = []
            status = None
            async for perf, errors in await trading_algorithm.get_generator():
                perfs.append(perf)
                if "progress" not in perf:
                    continue
                print("progress", perf["progress"])
                status = TradingAlgorithmExecutionStatus(
                    perf=perf,
                    cumulative_perf=perf["cumulative_perf"],
                    daily_perf=perf["daily_perf"],
                    cumulative_risk_metrics=perf["cumulative_risk_metrics"],
                    progress=perf["progress"],
                    trading_algorithm=trading_algorithm,
                    errors=errors,
                    result=None
                )
                if perf["progress"] < 1:
                    yield status


            # convert perf dict to pandas dataframe
            daily_stats, risk_report = self._create_daily_stats(perfs)

            self.analyze(trading_algorithm=trading_algorithm, perf=daily_stats)
        finally:
            trading_algorithm.data_portal = None
            trading_algorithm.metrics_tracker = None
        status.result = TradingAlgorithmExecutionResult(
                trading_algorithm=trading_algorithm,
                perf=daily_stats,
                risk_report=risk_report,
                errors=errors
            )
        yield status
        # yield TradingAlgorithmExecutionStatus(
        #     trading_algorithm=trading_algorithm,
        #     perf=daily_stats,
        #     risk_report=risk_report,
        #     errors=errors,
        #     result=TradingAlgorithmExecutionResult(
        #         trading_algorithm=trading_algorithm,
        #         perf=daily_stats,
        #         risk_report=risk_report,
        #         errors=errors
        #     )
        # )

    def analyze(self, trading_algorithm: TradingAlgorithm, perf):
        if trading_algorithm._analyze is None:
            return

        with ZiplineAPI(trading_algorithm):
            trading_algorithm._analyze(trading_algorithm, perf)
