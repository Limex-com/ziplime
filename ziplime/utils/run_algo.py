import click
import os
import sys
import warnings

from lime_trader.models.market import Period
from zipline.utils.paths import data_path

from ziplime.algorithm_live import LiveTradingAlgorithm
from ziplime.finance.blotter.blotter_live import BlotterLive
from ziplime.gens.brokers.broker import Broker
from ziplime.data.abstract_live_market_data_provider import AbstractLiveMarketDataProvider
from ziplime.data.data_portal_live import DataPortalLive
from ziplime.utils.bundle_utils import register_default_bundles

try:
    from pygments import highlight
    from pygments.lexers import PythonLexer
    from pygments.formatters import TerminalFormatter

    PYGMENTS = True
except ImportError:
    PYGMENTS = False
import logging
import pandas as pd
from toolz import concatv
from zipline.utils.calendar_utils import get_calendar

from ziplime.data import bundles
from zipline.data.benchmarks import get_benchmark_returns_from_file
from ziplime.data.data_portal import DataPortal
from zipline.finance import metrics
from ziplime.finance.trading import SimulationParameters
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.loaders import USEquityPricingLoader

import ziplime.utils.paths as pth
from zipline.extensions import load
from zipline.errors import SymbolNotFound
from ziplime.algorithm import TradingAlgorithm, NoBenchmark
from zipline.finance.blotter import Blotter

log = logging.getLogger(__name__)


class _RunAlgoError(click.ClickException, ValueError):
    """Signal an error that should have a different message if invoked from
    the cli.

    Parameters
    ----------
    pyfunc_msg : str
        The message that will be shown when called as a python function.
    cmdline_msg : str, optional
        The message that will be shown on the command line. If not provided,
        this will be the same as ``pyfunc_msg`
    """

    exit_code = 1

    def __init__(self, pyfunc_msg, cmdline_msg=None):
        if cmdline_msg is None:
            cmdline_msg = pyfunc_msg

        super(_RunAlgoError, self).__init__(cmdline_msg)
        self.pyfunc_msg = pyfunc_msg

    def __str__(self):
        return self.pyfunc_msg


# TODO: simplify
# flake8: noqa: C901
def _run(
        handle_data,
        initialize,
        before_trading_start,
        analyze,
        algofile,
        algotext,
        defines,
        data_frequency,
        capital_base,
        bundle,
        bundle_timestamp,
        start,
        end,
        output,
        trading_calendar,
        print_algo,
        metrics_set,
        local_namespace,
        environ,
        blotter: str,
        custom_loader,
        benchmark_spec,
        broker: Broker,
        market_data_provider: AbstractLiveMarketDataProvider,
):
    """Run a backtest for the given algorithm.

    This is shared between the cli and :func:`zipline.run_algo`.
    """

    bundle_data = bundles.load(
        name=bundle,
        environ=environ,
        timestamp=bundle_timestamp,
        period=Period.MINUTE if data_frequency == "minute" else Period.DAY,
    )

    if trading_calendar is None:
        trading_calendar = get_calendar("XNYS")

    # date parameter validation
    if not broker and trading_calendar.sessions_distance(start, end) < 1:
        raise _RunAlgoError(
            "There are no trading days between %s and %s"
            % (
                start.date(),
                end.date(),
            ),
        )

    benchmark_sid, benchmark_returns = benchmark_spec.resolve(
        asset_finder=bundle_data.asset_finder,
        start_date=start,
        end_date=end,
    )

    if algotext is not None:
        if local_namespace:
            ip = get_ipython()  # noqa
            namespace = ip.user_ns
        else:
            namespace = {}

        for assign in defines:
            try:
                name, value = assign.split("=", 2)
            except ValueError:
                raise ValueError(
                    "invalid define %r, should be of the form name=value" % assign,
                )
            try:
                # evaluate in the same namespace so names may refer to
                # eachother
                namespace[name] = eval(value, namespace)
            except Exception as e:
                raise ValueError(
                    "failed to execute definition for name %r: %s" % (name, e),
                )
    elif defines:
        raise _RunAlgoError(
            "cannot pass define without `algotext`",
            "cannot pass '-D' / '--define' without '-t' / '--algotext'",
        )
    else:
        namespace = {}
        if algofile is not None:
            algotext = algofile.read()

    if print_algo:
        if PYGMENTS:
            highlight(
                algotext,
                PythonLexer(),
                TerminalFormatter(),
                outfile=sys.stdout,
            )
        else:
            click.echo(algotext)

    first_trading_day = bundle_data.historical_data_reader.first_trading_day

    state_filename = None
    realtime_bar_target = None
    emission_rate = data_frequency
    if broker:
        data = DataPortalLive(
            asset_finder=bundle_data.asset_finder,
            broker=broker,
            trading_calendar=trading_calendar,
            first_trading_day=first_trading_day,
            historical_data_reader=bundle_data.historical_data_reader,
            adjustment_reader=bundle_data.adjustment_reader,
            future_minute_reader=bundle_data.historical_data_reader,
            future_daily_reader=bundle_data.historical_data_reader,
            market_data_provider=market_data_provider,
            fundamental_data_reader=bundle_data.fundamental_data_reader,
            fields=bundle_data.historical_data_reader.get_fields()
        )
        state_filename = f"{data_path(['state'])}"
        realtime_bar_target = f"{data_path(['realtime'])}"
        emission_rate = 'minute'
    else:
        data = DataPortal(
            bundle_data.asset_finder,
            trading_calendar=trading_calendar,
            first_trading_day=first_trading_day,
            historical_data_reader=bundle_data.historical_data_reader,
            fundamental_data_reader=bundle_data.fundamental_data_reader,
            adjustment_reader=bundle_data.adjustment_reader,
            future_minute_reader=bundle_data.historical_data_reader,
            future_daily_reader=bundle_data.historical_data_reader,
            fields=bundle_data.historical_data_reader.get_fields()
        )

    pipeline_loader = USEquityPricingLoader.without_fx(
        bundle_data.historical_data_reader,
        bundle_data.adjustment_reader,
    )

    def choose_loader(column):
        if column in USEquityPricing.columns:
            return pipeline_loader
        try:
            return custom_loader.get(column)
        except KeyError:
            raise ValueError("No PipelineLoader registered for column %s." % column)

    if isinstance(metrics_set, str):
        try:
            metrics_set = metrics.load(metrics_set)
        except ValueError as e:
            raise _RunAlgoError(str(e))

    try:
        blotter = load(Blotter, blotter)
    except ValueError as e:
        raise _RunAlgoError(str(e))

    sim_params = SimulationParameters(
        start_session=start,
        end_session=end,
        trading_calendar=trading_calendar,
        capital_base=capital_base,
        emission_rate=emission_rate,
        data_frequency=data_frequency,
    )
    try:
        if broker is None:
            tr = TradingAlgorithm(
                namespace=namespace,
                data_portal=data,
                get_pipeline_loader=choose_loader,
                trading_calendar=trading_calendar,
                sim_params=sim_params,
                metrics_set=metrics_set,
                blotter=blotter,
                benchmark_returns=benchmark_returns,
                benchmark_sid=benchmark_sid,
                **{
                    "initialize": initialize,
                    "handle_data": handle_data,
                    "before_trading_start": before_trading_start,
                    "analyze": analyze,
                }
                if algotext is None
                else {
                    "algo_filename": getattr(algofile, "name", "<algorithm>"),
                    "script": algotext,
                },
            )
        else:

            blotter_live = BlotterLive(data_frequency=data_frequency, broker=broker)
            tr = LiveTradingAlgorithm(
                broker=broker,
                state_filename=state_filename,
                realtime_bar_target=realtime_bar_target,
                namespace=namespace,
                data_portal=data,
                get_pipeline_loader=choose_loader,
                trading_calendar=trading_calendar,
                sim_params=sim_params,
                metrics_set=metrics_set,
                blotter=blotter_live,
                benchmark_returns=benchmark_returns,
                benchmark_sid=benchmark_sid,
                **{
                    "initialize": initialize,
                    "handle_data": handle_data,
                    "before_trading_start": before_trading_start,
                    "analyze": analyze,
                }
                if algotext is None
                else {
                    "algo_filename": getattr(algofile, "name", "<algorithm>"),
                    "script": algotext,
                },
            )
        tr.bundle_data = bundle_data
        tr.fundamental_data_bundle = bundle_data.fundamental_data_reader
        perf = tr.run()
    except NoBenchmark:
        raise _RunAlgoError(
            (
                "No ``benchmark_spec`` was provided, and"
                " ``zipline.api.set_benchmark`` was not called in"
                " ``initialize``."
            ),
            (
                "Neither '--benchmark-symbol' nor '--benchmark-sid' was"
                " provided, and ``zipline.api.set_benchmark`` was not called"
                " in ``initialize``. Did you mean to pass '--no-benchmark'?"
            ),
        )

    if output == "-":
        click.echo(str(perf))
    elif output != os.devnull:  # make the zipline magic not write any data
        perf.to_pickle(output)

    return perf


# All of the loaded extensions. We don't want to load an extension twice.
_loaded_extensions = set()


def load_extensions(default, extensions, strict, environ, reload=False):
    """Load all of the given extensions. This should be called by run_algo
    or the cli.

    Parameters
    ----------
    default : bool
        Load the default exension (~/.zipline/extension.py)?
    extension : iterable[str]
        The paths to the extensions to load. If the path ends in ``.py`` it is
        treated as a script and executed. If it does not end in ``.py`` it is
        treated as a module to be imported.
    strict : bool
        Should failure to load an extension raise. If this is false it will
        still warn.
    environ : mapping
        The environment to use to find the default extension path.
    reload : bool, optional
        Reload any extensions that have already been loaded.
    """
    if default:
        register_default_bundles()
        default_extension_path = pth.default_extension(environ=environ)
        pth.ensure_file(default_extension_path)
        # put the default extension first so other extensions can depend on
        # the order they are loaded
        extensions = concatv([default_extension_path], extensions)

    for ext in extensions:
        if ext in _loaded_extensions and not reload:
            continue
        try:
            # load all of the zipline extensionss
            if ext.endswith(".py"):
                with open(ext) as f:
                    ns = {}
                    exec(compile(f.read(), ext, "exec"), ns, ns)
            else:
                __import__(ext)
        except Exception as e:
            if strict:
                # if `strict` we should raise the actual exception and fail
                raise
            # without `strict` we should just log the failure
            warnings.warn("Failed to load extension: %r\n%s" % (ext, e), stacklevel=2)
        else:
            _loaded_extensions.add(ext)


def run_algorithm(
        start,
        end,
        initialize,
        capital_base,
        handle_data=None,
        before_trading_start=None,
        analyze=None,
        data_frequency: str = "daily",
        bundle: str = "lime",
        bundle_timestamp=None,
        trading_calendar=None,
        metrics_set="default",
        benchmark_returns=None,
        default_extension=True,
        extensions=(),
        strict_extensions=True,
        environ=os.environ,
        custom_loader=None,
        print_algo: bool = False,
        algotext=None,
        algofile=None,
        blotter="default",
        market_data_provider: AbstractLiveMarketDataProvider = None,
        broker: Broker = None
):
    """
    Run a trading algorithm.

    Parameters
    ----------
    start : datetime
        The start date of the backtest.
    end : datetime
        The end date of the backtest..
    initialize : callable[context -> None]
        The initialize function to use for the algorithm. This is called once
        at the very begining of the backtest and should be used to set up
        any state needed by the algorithm.
    capital_base : float
        The starting capital for the backtest.
    handle_data : callable[(context, BarData) -> None], optional
        The handle_data function to use for the algorithm. This is called
        every minute when ``data_frequency == 'minute'`` or every day
        when ``data_frequency == 'daily'``.
    before_trading_start : callable[(context, BarData) -> None], optional
        The before_trading_start function for the algorithm. This is called
        once before each trading day (after initialize on the first day).
    analyze : callable[(context, pd.DataFrame) -> None], optional
        The analyze function to use for the algorithm. This function is called
        once at the end of the backtest and is passed the context and the
        performance data.
    data_frequency : {'daily', 'minute'}, optional
        The data frequency to run the algorithm at.
    bundle : str, optional
        The name of the data bundle to use to load the data to run the backtest
        with. This defaults to 'quantopian-quandl'.
    bundle_timestamp : datetime, optional
        The datetime to lookup the bundle data for. This defaults to the
        current time.
    trading_calendar : TradingCalendar, optional
        The trading calendar to use for your backtest.
    metrics_set : iterable[Metric] or str, optional
        The set of metrics to compute in the simulation. If a string is passed,
        resolve the set with :func:`zipline.finance.metrics.load`.
    benchmark_returns : pd.Series, optional
        Series of returns to use as the benchmark.
    default_extension : bool, optional
        Should the default zipline extension be loaded. This is found at
        ``$ZIPLINE_ROOT/extension.py``
    extensions : iterable[str], optional
        The names of any other extensions to load. Each element may either be
        a dotted module path like ``a.b.c`` or a path to a python file ending
        in ``.py`` like ``a/b/c.py``.
    strict_extensions : bool, optional
        Should the run fail if any extensions fail to load. If this is false,
        a warning will be raised instead.
    environ : mapping[str -> str], optional
        The os environment to use. Many extensions use this to get parameters.
        This defaults to ``os.environ``.
    blotter : str or zipline.finance.blotter.Blotter, optional
        Blotter to use with this algorithm. If passed as a string, we look for
        a blotter construction function registered with
        ``zipline.extensions.register`` and call it with no parameters.
        Default is a :class:`zipline.finance.blotter.SimulationBlotter` that
        never cancels orders.

    Returns
    -------
    perf : pd.DataFrame
        The daily performance of the algorithm.

    See Also
    --------
    zipline.data.bundles.bundles : The available data bundles.
    """
    load_extensions(default_extension, extensions, strict_extensions, environ)

    benchmark_spec = BenchmarkSpec.from_returns(benchmark_returns)

    return _run(
        handle_data=handle_data,
        initialize=initialize,
        before_trading_start=before_trading_start,
        analyze=analyze,
        algofile=algofile,
        algotext=algotext,
        defines=(),
        data_frequency=data_frequency,
        capital_base=capital_base,
        bundle=bundle,
        bundle_timestamp=bundle_timestamp,
        start=start,
        end=end,
        output=os.devnull,
        trading_calendar=trading_calendar,
        print_algo=print_algo,
        metrics_set=metrics_set,
        local_namespace=False,
        environ=environ,
        blotter=blotter,
        custom_loader=custom_loader,
        benchmark_spec=benchmark_spec,
        broker=broker,
        market_data_provider=market_data_provider,
    )


class BenchmarkSpec:
    """
    Helper for different ways we can get benchmark data for the Zipline CLI and
    zipline.utils.run_algo.run_algorithm.

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
            benchmark_returns,
            benchmark_file,
            benchmark_sid,
            benchmark_symbol,
            no_benchmark,
    ):

        self.benchmark_returns = benchmark_returns
        self.benchmark_file = benchmark_file
        self.benchmark_sid = benchmark_sid
        self.benchmark_symbol = benchmark_symbol
        self.no_benchmark = no_benchmark

    @classmethod
    def from_cli_params(
            cls, benchmark_sid, benchmark_symbol, benchmark_file, no_benchmark
    ):

        return cls(
            benchmark_returns=None,
            benchmark_sid=benchmark_sid,
            benchmark_symbol=benchmark_symbol,
            benchmark_file=benchmark_file,
            no_benchmark=no_benchmark,
        )

    @classmethod
    def from_returns(cls, benchmark_returns):
        return cls(
            benchmark_returns=benchmark_returns,
            benchmark_file=None,
            benchmark_sid=None,
            benchmark_symbol=None,
            no_benchmark=benchmark_returns is None,
        )

    def resolve(self, asset_finder, start_date, end_date):
        """
        Resolve inputs into values to be passed to TradingAlgorithm.

        Returns a pair of ``(benchmark_sid, benchmark_returns)`` with at most
        one non-None value. Both values may be None if no benchmark source has
        been configured.

        Parameters
        ----------
        asset_finder : zipline.assets.AssetFinder
            Asset finder for the algorithm to be run.
        start_date : pd.Timestamp
            Start date of the algorithm to be run.
        end_date : pd.Timestamp
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
                asset = asset_finder.lookup_symbol(
                    self.benchmark_symbol,
                    as_of_date=end_date,
                )
                benchmark_sid = asset.sid
                benchmark_returns = None
            except SymbolNotFound:
                raise _RunAlgoError(
                    "Symbol %r as a benchmark not found in this bundle."
                    % self.benchmark_symbol
                )
        elif self.no_benchmark:
            benchmark_sid = None
            benchmark_returns = self._zero_benchmark_returns(
                start_date=start_date,
                end_date=end_date,
            )
        else:
            log.warning(
                "No benchmark configured. " "Assuming algorithm calls set_benchmark."
            )
            log.warning(
                "Pass --benchmark-sid, --benchmark-symbol, or"
                " --benchmark-file to set a source of benchmark returns."
            )
            log.warning(
                "Pass --no-benchmark to use a dummy benchmark " "of zero returns.",
            )
            benchmark_sid = None
            benchmark_returns = None

        return benchmark_sid, benchmark_returns

    @staticmethod
    def _zero_benchmark_returns(start_date, end_date):
        return pd.Series(
            index=pd.date_range(start_date, end_date, tz="utc"),
            data=0.0,
        )
