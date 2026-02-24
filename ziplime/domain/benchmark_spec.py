import datetime

import pandas as pd
import structlog

from ziplime.data.benchmarks import get_benchmark_returns_from_file

from ziplime.errors import SymbolNotFound

from ziplime.assets.repositories.sqlalchemy_asset_repository import SqlAlchemyAssetRepository

class BenchmarkSpec:
    """
    Helper for different ways we can get benchmark data for the Zipline CLI and
    ziplime.utils.run_algo.run_algorithm.

    Parameters
    ----------
    benchmark_returns : pd.Series, optional
        Series of returns to use as the benchmark.
    benchmark_file : str or file
        File containing a csv with `date` and `return` columns, to be read as
        the benchmark.
    benchmark_sid : int, optional
        Sid of the asset to use as a benchmark.
    benchmark_symbol : str, optional
        Symbol of the asset to use as a benchmark. Symbol will be looked up as
        of the end date of the backtest.
    no_benchmark : bool
        Flag indicating that no benchmark is configured. Benchmark-dependent
        metrics will be calculated using a dummy benchmark of all-zero returns.
    """

    def __init__(
            self,
            benchmark_returns: pd.Series,
            benchmark_file: str | None,
            benchmark_sid: int | None,
            benchmark_symbol: str | None,
            no_benchmark: bool,
    ):
        self._logger = structlog.get_logger(__name__)

        self.benchmark_returns = benchmark_returns
        self.benchmark_file = benchmark_file
        self.benchmark_sid = benchmark_sid
        self.benchmark_symbol = benchmark_symbol
        self.no_benchmark = no_benchmark

    def resolve(self, asset_repository: SqlAlchemyAssetRepository, start_date: datetime.date, end_date: datetime.date):
        """
        Resolve inputs into values to be passed to TradingAlgorithm.

        Returns a pair of ``(benchmark_sid, benchmark_returns)`` with at most
        one non-None value. Both values may be None if no benchmark source has
        been configured.

        Parameters
        ----------
        asset_repository : ziplime.assets.AssetFinder
            Asset finder for the algorithm to be run.
        start_date : datetime.datetime
            Start date of the algorithm to be run.
        end_date : datetime.datetime
            End date of the algorithm to be run.

        Returns
        -------
        benchmark_sid : int
            Sid to use as benchmark.
        benchmark_returns : pd.Series
            Series of returns to use as benchmark.
        """
        if self.benchmark_returns is not None:
            benchmark_sid = None
            benchmark_returns = self.benchmark_returns
        elif self.benchmark_file is not None:
            benchmark_sid = None
            benchmark_returns = get_benchmark_returns_from_file(
                self.benchmark_file,
            )
        elif self.benchmark_sid is not None:
            benchmark_sid = self.benchmark_sid
            benchmark_returns = None
        elif self.benchmark_symbol is not None:
            try:
                asset = asset_repository.lookup_symbol(
                    self.benchmark_symbol,
                    as_of_date=end_date,
                )
                benchmark_sid = asset.sid
                benchmark_returns = None
            except SymbolNotFound:
                raise ValueError(f"Symbol {self.benchmark_symbol} as a benchmark not found in this bundle.")
        elif self.no_benchmark:
            benchmark_sid = None
            benchmark_returns = pd.Series(
                index=pd.date_range(start_date, end_date, freq="1d").date,
                data=0.0,
            )

        else:
            self._logger.warning(
                "No benchmark configured. " "Assuming algorithm calls set_benchmark."
            )
            self._logger.warning(
                "Pass --benchmark-sid, --benchmark-symbol, or"
                " --benchmark-file to set a source of benchmark returns."
            )
            self._logger.warning(
                "Pass --no-benchmark to use a dummy benchmark " "of zero returns.",
            )
            benchmark_sid = None
            benchmark_returns = None

        return benchmark_sid, benchmark_returns
