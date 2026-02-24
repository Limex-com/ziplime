"""
Backtest executor.

Takes an algorithm code string (just the lifecycle functions, no boilerplate),
wraps it with the required imports and run_simulation call, writes it to a temp
file, and executes the backtest.
"""
from __future__ import annotations

import datetime
import os
import pathlib
import tempfile
from dataclasses import dataclass, field
from typing import Optional

import pytz

from .agent import BacktestConfig
from .data_manager import WARMUP_BUFFER_DAYS


# ------------------------------------------------------------------ #
# Result dataclass                                                     #
# ------------------------------------------------------------------ #

@dataclass
class BacktestResult:
    """Structured result from a completed backtest."""
    total_return_pct: float
    annualized_return_pct: float
    sharpe_ratio: float
    max_drawdown_pct: float
    final_portfolio_value: float
    starting_capital: float
    start_date: str
    end_date: str
    symbols: list[str]
    num_trading_days: int
    errors: list[str] = field(default_factory=list)
    # benchmark-relative metrics (None when benchmark data unavailable)
    alpha: Optional[float] = None
    beta: Optional[float] = None
    # quantstats extended metrics (formatted text, ready to feed to LLM)
    qs_metrics_text: Optional[str] = None
    # path to the saved HTML report (None if quantstats not available)
    html_report_path: Optional[str] = None
    # path to the saved runnable strategy .py file
    strategy_file_path: Optional[str] = None
    # raw perf DataFrame (pandas) kept for programmatic access
    perf: object = None

    def to_summary_text(self) -> str:
        """
        Return a plain-text summary ready to be included in an LLM prompt.
        If quantstats metrics are available they are appended for richer context.
        """
        lines = [
            f"Strategy: {', '.join(self.symbols)}",
            f"Period: {self.start_date} to {self.end_date} ({self.num_trading_days} trading days)",
            f"Starting capital: ${self.starting_capital:,.0f}",
            f"Final portfolio value: ${self.final_portfolio_value:,.2f}",
            f"Total return: {self.total_return_pct:+.2f}%",
            f"Annualized return: {self.annualized_return_pct:+.2f}%",
            f"Sharpe ratio: {self.sharpe_ratio:.3f}",
            f"Max drawdown: {self.max_drawdown_pct:.2f}%",
        ]
        if self.alpha is not None:
            lines.append(f"Alpha (vs SPY): {self.alpha:+.4f}")
        if self.beta is not None:
            lines.append(f"Beta (vs SPY): {self.beta:.4f}")
        if self.errors:
            lines.append(f"Warnings: {len(self.errors)} non-fatal simulation errors")
        if self.qs_metrics_text:
            lines.append("\n--- QuantStats Extended Metrics ---")
            lines.append(self.qs_metrics_text)
        if self.html_report_path:
            lines.append(f"\nFull HTML report saved to: {self.html_report_path}")
        if self.strategy_file_path:
            lines.append(f"Strategy code saved to: {self.strategy_file_path}")
        return "\n".join(lines)


# ------------------------------------------------------------------ #
# Algorithm file template                                              #
# ------------------------------------------------------------------ #

ALGORITHM_IMPORTS = """\
import datetime
import numpy as np
import polars as pl
try:
    import talib
except ImportError:
    talib = None
from ziplime.finance.execution import MarketOrder, LimitOrder
"""


# ------------------------------------------------------------------ #
# Executor                                                             #
# ------------------------------------------------------------------ #

class BacktestExecutor:
    """
    Wraps algorithm code with boilerplate, runs run_simulation, and
    returns a structured BacktestResult with QuantStats metrics.
    """

    def __init__(self, data_manager):
        self.data_manager = data_manager

    async def run(
        self,
        algorithm_code: str,
        config: BacktestConfig,
    ) -> BacktestResult:
        """
        Execute a backtest for the given algorithm code and configuration.

        Parameters
        ----------
        algorithm_code:
            Python source with only the algorithm lifecycle functions
            (initialize, handle_data, etc.) — no imports, no run_simulation.
        config:
            BacktestConfig parsed from the LLM response.
        """
        from ziplime.core.run_simulation import run_simulation
        from ziplime.core.ingest_data import get_asset_service
        from ziplime.utils.bundle_utils import get_bundle_service

        start_date = _parse_date_tz(config.start_date)
        end_date   = _parse_date_tz(config.end_date)

        # Load the bundle starting WARMUP_BUFFER_DAYS earlier than the simulation
        # start so that bundle_start_date < simulation start_date (required by
        # BundleService.load_bundle) and warmup bars are available for indicators.
        bundle_load_start = start_date - datetime.timedelta(days=WARMUP_BUFFER_DAYS)
        # Extend bundle end by 1 day so the market-close timestamp of the last
        # trading day (e.g. 2024-12-31 16:00 ET) never exceeds the bundle's
        # stored end date, which is recorded at midnight.
        bundle_load_end = end_date + datetime.timedelta(days=1)

        bundle_service = get_bundle_service()
        asset_service  = get_asset_service(
            clear_asset_db=False,
            db_path=str(self.data_manager.get_assets_db_path()),
        )

        # Include the benchmark symbol in the bundle so BenchmarkSource can
        # look it up by sid — otherwise we get KeyError inside simulation_exchange.
        bundle_symbols = list(config.symbols)
        if config.benchmark and config.benchmark not in bundle_symbols:
            bundle_symbols.append(config.benchmark)

        market_data = await bundle_service.load_bundle(
            bundle_name=self.data_manager.get_bundle_name(),
            bundle_version=None,
            frequency=datetime.timedelta(days=1),
            start_date=bundle_load_start,
            end_date=bundle_load_end,
            symbols=bundle_symbols,
        )

        # Write algorithm to a temp file (run_simulation needs a file path)
        full_code = ALGORITHM_IMPORTS + "\n" + algorithm_code
        algo_file = _write_temp_algo(full_code)

        try:
            result = await run_simulation(
                start_date=start_date,
                end_date=end_date,
                trading_calendar="NYSE",
                algorithm_file=algo_file,
                total_cash=config.capital,
                market_data_source=market_data,
                custom_data_sources=[],
                emission_rate=datetime.timedelta(days=1),
                benchmark_asset_symbol=config.benchmark,
                stop_on_error=False,
                asset_service=asset_service,
            )
        finally:
            _safe_remove(algo_file)

        return self._build_result(result, config, start_date, end_date, algorithm_code)

    # ------------------------------------------------------------------ #
    # Private                                                              #
    # ------------------------------------------------------------------ #

    def _build_result(
        self,
        sim_result,
        config: BacktestConfig,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        algorithm_code: str = "",
    ) -> BacktestResult:
        """Convert raw simulation result into a BacktestResult with QuantStats."""
        perf = sim_result.perf if hasattr(sim_result, "perf") else None

        # ---- portfolio_value (handle both Polars and pandas) ----
        portfolio_values: list[float] = []
        if perf is not None and "portfolio_value" in perf.columns:
            col = perf["portfolio_value"]
            # Polars
            if hasattr(col, "drop_nulls"):
                portfolio_values = col.drop_nulls().to_list()
            else:
                # pandas
                portfolio_values = col.dropna().tolist()

        starting = config.capital
        final    = portfolio_values[-1] if portfolio_values else starting
        first    = portfolio_values[0]  if portfolio_values else starting

        total_return_pct = ((final - first) / first * 100) if first else 0.0

        num_days = max((end_date - start_date).days, 1)
        years    = num_days / 365.25
        ann_return_pct = (
            ((final / first) ** (1.0 / years) - 1) * 100
            if first and years > 0 else 0.0
        )

        errors = []
        if hasattr(sim_result, "errors") and sim_result.errors:
            errors = [str(e) for e in sim_result.errors]

        # ---- QuantStats + strategy file ----
        returns_series    = _extract_returns_series(perf)
        benchmark_returns = _extract_benchmark_returns_from_perf(perf)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        qs_metrics_text, html_report_path = _compute_quantstats(
            returns_series,
            config=config,
            benchmark_returns=benchmark_returns,
            ts=ts,
        )
        strategy_file_path = _save_strategy_file(algorithm_code, config, ts)

        # Fall back to manual Sharpe / drawdown if quantstats not available
        sharpe           = _manual_sharpe(perf)
        max_drawdown_pct = _manual_max_drawdown(portfolio_values)
        alpha            = None
        beta             = None

        # Prefer quantstats values when available
        if qs_metrics_text:
            sharpe_qs, dd_qs, alpha_qs, beta_qs = _parse_qs_key_metrics(qs_metrics_text)
            if sharpe_qs is not None:
                sharpe = sharpe_qs
            if dd_qs is not None:
                max_drawdown_pct = dd_qs
            alpha = alpha_qs
            beta  = beta_qs

        return BacktestResult(
            total_return_pct=round(total_return_pct, 2),
            annualized_return_pct=round(ann_return_pct, 2),
            sharpe_ratio=round(sharpe, 3),
            max_drawdown_pct=round(max_drawdown_pct, 2),
            final_portfolio_value=round(final, 2),
            starting_capital=config.capital,
            start_date=config.start_date,
            end_date=config.end_date,
            symbols=config.symbols,
            num_trading_days=len(portfolio_values),
            errors=errors,
            alpha=round(alpha, 4) if alpha is not None else None,
            beta=round(beta, 4) if beta is not None else None,
            qs_metrics_text=qs_metrics_text,
            html_report_path=html_report_path,
            strategy_file_path=strategy_file_path,
            perf=perf,
        )


# ------------------------------------------------------------------ #
# QuantStats helpers                                                   #
# ------------------------------------------------------------------ #

def _period_return_to_daily(series) -> object:
    """
    Convert a cumulative period-return Series (as stored in the perf DataFrame)
    to a daily-return Series suitable for QuantStats.

    perf stores each row's *total* return from the simulation start, so to get
    day-over-day returns we go:  price = 1 + period_return  →  pct_change().
    """
    import pandas as pd

    s = series.copy().astype(float)
    s.index = pd.to_datetime(s.index).tz_localize(None)
    prices  = 1.0 + s
    returns = prices.pct_change().dropna()
    # Guard: QuantStats crashes when std == 0 (flat equity curve)
    if returns.std() == 0:
        returns.iloc[-1] =  1e-6
        returns.iloc[-2] = -1e-6
    return returns


def _extract_returns_series(perf):
    """
    Extract a pandas Series of daily returns with a tz-naive DatetimeIndex.
    Tries the pre-computed 'returns' column first; falls back to deriving
    returns from 'algorithm_period_return' (cumulative) if needed.
    Returns None if extraction fails.
    """
    if perf is None:
        return None

    try:
        import pandas as pd

        df = perf.to_pandas() if hasattr(perf, "to_pandas") else perf

        # --- preferred: pre-computed daily returns column ---
        if "returns" in df.columns:
            if "date" in df.columns:
                returns = df.set_index("date")["returns"]
            elif isinstance(df.index, pd.DatetimeIndex):
                returns = df["returns"]
            else:
                returns = df["returns"].copy()
                returns.index = pd.to_datetime(returns.index)

            returns = returns.dropna()
            if not isinstance(returns.index, pd.DatetimeIndex):
                returns.index = pd.to_datetime(returns.index)
            returns.index = returns.index.tz_localize(None)
            return returns

        # --- fallback: derive from cumulative algorithm_period_return ---
        if "algorithm_period_return" in df.columns:
            col = df["algorithm_period_return"]
            if "date" in df.columns:
                col = col.set_axis(pd.to_datetime(df["date"]))
            elif not isinstance(df.index, pd.DatetimeIndex):
                col.index = pd.to_datetime(col.index)
            return _period_return_to_daily(col)

        return None

    except Exception:
        return None


def _extract_benchmark_returns_from_perf(perf) -> object:
    """
    Extract the benchmark daily returns directly from the simulation perf
    DataFrame using the 'benchmark_period_return' column (cumulative returns
    stored by ziplime).  Returns a renamed pandas Series or None.
    """
    if perf is None:
        return None

    try:
        import pandas as pd

        df = perf.to_pandas() if hasattr(perf, "to_pandas") else perf

        if "benchmark_period_return" not in df.columns:
            return None

        col = df["benchmark_period_return"]
        if "date" in df.columns:
            col = col.set_axis(pd.to_datetime(df["date"]))
        elif not isinstance(df.index, pd.DatetimeIndex):
            col.index = pd.to_datetime(col.index)

        bm = _period_return_to_daily(col)
        bm.name = "Benchmark"
        return bm if len(bm) > 5 else None

    except Exception:
        return None


def _compute_quantstats(
    returns_series,
    config: BacktestConfig,
    benchmark_returns=None,
    ts: Optional[str] = None,
) -> tuple[Optional[str], Optional[str]]:
    """
    Run QuantStats and return (metrics_text, html_report_path).
    When benchmark_returns is provided it is passed to QuantStats so that
    alpha, beta, and other relative metrics are computed.
    Both return values are None if quantstats is not installed or fails.
    """
    if returns_series is None or len(returns_series) < 5:
        return None, None

    try:
        import quantstats as qs

        import warnings
        warnings.filterwarnings("ignore")

        # --- metrics as text ---
        metrics_df = qs.reports.metrics(
            returns_series,
            benchmark=benchmark_returns,
            display=False,
            mode="full",
        )
        metrics_text = metrics_df.to_string()

        # --- HTML report ---
        report_dir = pathlib.Path.home() / ".ziplime" / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        if ts is None:
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        symbols   = "_".join(config.symbols[:3])  # keep filename short
        html_path = str(report_dir / f"report_{symbols}_{ts}.html")

        qs.reports.html(
            returns_series,
            benchmark=benchmark_returns,
            output=html_path,
            title=f"Ziplime AI — {', '.join(config.symbols)} "
                  f"({config.start_date} → {config.end_date})",
            download_filename=html_path,
        )

        return metrics_text, html_path

    except ImportError:
        return None, None
    except Exception:
        return None, None


def _save_strategy_file(
    algorithm_code: str,
    config: BacktestConfig,
    ts: str,
) -> Optional[str]:
    """
    Save the algorithm as a complete, runnable Python file and return its path.
    The file includes a header comment with backtest parameters and a
    run_simulation block so it can be executed directly:
        python ~/.ziplime/reports/strategy_AAPL_20240101_120000.py
    """
    try:
        report_dir = pathlib.Path.home() / ".ziplime" / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        symbols  = "_".join(config.symbols[:3])
        py_path  = report_dir / f"strategy_{symbols}_{ts}.py"

        header = (
            f'"""\n'
            f"Ziplime AI — generated strategy\n"
            f"Symbols   : {', '.join(config.symbols)}\n"
            f"Period    : {config.start_date} → {config.end_date}\n"
            f"Capital   : ${config.capital:,.0f}\n"
            f"Benchmark : {config.benchmark or 'none'}\n"
            f"Generated : {ts}\n"
            f"\n"
            f"Run manually:\n"
            f"    python {py_path}\n"
            f'"""\n'
        )

        run_block = (
            "\n\nif __name__ == '__main__':\n"
            "    import asyncio\n"
            "    import datetime\n"
            "    import pathlib\n"
            "    from ziplime.core.run_simulation import run_simulation\n"
            "    from ziplime.core.ingest_data import get_asset_service\n"
            "    from ziplime.utils.bundle_utils import get_bundle_service\n"
            "    from ziplime.data.data_sources.yahoo_finance_data_source import YahooFinanceDataSource\n"
            "    from ziplime.core.ingest_data import ingest_market_data\n"
            "\n"
            "    async def _run():\n"
            f"        start = datetime.datetime({config.start_date.replace('-', ', ')}, "
            "tzinfo=datetime.timezone.utc)\n"
            f"        end   = datetime.datetime({config.end_date.replace('-', ', ')}, "
            "tzinfo=datetime.timezone.utc)\n"
            f"        symbols = {config.symbols!r}\n"
            f"        capital = {config.capital}\n"
            f"        benchmark = {config.benchmark!r}\n"
            "\n"
            "        assets_db = pathlib.Path.home() / '.ziplime' / 'assets.sqlite'\n"
            "        bundle_name = 'ai_assistant_yahoo_daily'\n"
            "\n"
            "        asset_service = get_asset_service(\n"
            "            clear_asset_db=False, db_path=str(assets_db)\n"
            "        )\n"
            "        bundle_service = get_bundle_service()\n"
            "        market_data = await bundle_service.load_bundle(\n"
            "            bundle_name=bundle_name,\n"
            "            bundle_version=None,\n"
            "            frequency=datetime.timedelta(days=1),\n"
            "            start_date=start - datetime.timedelta(days=90),\n"
            "            end_date=end + datetime.timedelta(days=1),\n"
            "            symbols=symbols + ([benchmark] if benchmark else []),\n"
            "        )\n"
            "        result = await run_simulation(\n"
            "            start_date=start,\n"
            "            end_date=end,\n"
            "            trading_calendar='NYSE',\n"
            "            algorithm_file=__file__,\n"
            "            total_cash=capital,\n"
            "            market_data_source=market_data,\n"
            "            custom_data_sources=[],\n"
            "            emission_rate=datetime.timedelta(days=1),\n"
            "            benchmark_asset_symbol=benchmark,\n"
            "            stop_on_error=False,\n"
            "            asset_service=asset_service,\n"
            "        )\n"
            "        print(result)\n"
            "\n"
            "    asyncio.run(_run())\n"
        )

        py_path.write_text(header + ALGORITHM_IMPORTS + "\n" + algorithm_code + run_block)
        return str(py_path)

    except Exception:
        return None


def _parse_qs_key_metrics(
    metrics_text: str,
) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Parse key scalars from the quantstats metrics text.
    Returns (sharpe, max_drawdown_pct, alpha, beta).
    max_drawdown is negative (e.g. -15.3); alpha is annualised decimal.
    """
    sharpe = None
    max_dd = None
    alpha  = None
    beta   = None

    for line in metrics_text.splitlines():
        low = line.lower()
        try:
            val_str = line.split()[-1].replace("%", "").replace(",", "")
            val = float(val_str)
            if "sharpe" in low and sharpe is None:
                sharpe = val
            if "max drawdown" in low and max_dd is None:
                max_dd = -abs(val)
            if low.strip().startswith("alpha") and alpha is None:
                alpha = val
            if low.strip().startswith("beta") and beta is None:
                beta = val
        except (ValueError, IndexError):
            continue

    return sharpe, max_dd, alpha, beta


# ------------------------------------------------------------------ #
# Manual fallback metrics                                              #
# ------------------------------------------------------------------ #

def _manual_sharpe(perf) -> float:
    if perf is None or "returns" not in perf.columns:
        return 0.0
    try:
        import numpy as np
        col = perf["returns"]
        arr = col.drop_nulls().to_numpy() if hasattr(col, "drop_nulls") else col.dropna().to_numpy()
        if len(arr) > 1:
            std = float(np.std(arr))
            if std > 0:
                return float(np.mean(arr) / std * (252 ** 0.5))
    except Exception:
        pass
    return 0.0


def _manual_max_drawdown(portfolio_values: list[float]) -> float:
    if not portfolio_values:
        return 0.0
    peak = portfolio_values[0]
    max_dd = 0.0
    for v in portfolio_values:
        peak = max(peak, v)
        dd = (v - peak) / peak * 100
        max_dd = min(max_dd, dd)
    return round(max_dd, 2)


# ------------------------------------------------------------------ #
# General helpers                                                      #
# ------------------------------------------------------------------ #

def _parse_date_tz(date_str: str) -> datetime.datetime:
    """Parse YYYY-MM-DD string to timezone-aware datetime (NY time)."""
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    return dt.replace(tzinfo=pytz.timezone("America/New_York"))


def _write_temp_algo(code: str) -> str:
    """Write code to a temp file and return the file path."""
    fd, path = tempfile.mkstemp(suffix=".py", prefix="ziplime_ai_algo_")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(code)
    except Exception:
        os.close(fd)
        raise
    return path


def _safe_remove(path: str) -> None:
    """Delete a file, ignoring errors."""
    try:
        os.unlink(path)
    except OSError:
        pass
