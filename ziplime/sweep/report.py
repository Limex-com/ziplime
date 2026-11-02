"""Metrics per combination, the ranking, and the files a sweep leaves behind."""
import contextlib
import datetime
import io
import json
import math
import time
from collections import defaultdict
from pathlib import Path
from typing import Callable

import pandas as pd

from ziplime.core.run_simulation import run_simulation
from ziplime.exchanges.simulation_exchange import SimulationExchange
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.vectorized import to_execution_result
from ziplime.vectorized.kernel import compare_execution_results

ONE_DAY = datetime.timedelta(days=1)


def metrics(value: pd.DataFrame, initial_cash: float, fills, fees, rejects,
            periods_per_year: int) -> pd.DataFrame:
    """Each combination's metrics from its portfolio value curve (bars × combinations)."""
    returns = value.pct_change(fill_method=None).iloc[1:]
    volatility = returns.std()
    sharpe = (returns.mean() / volatility * math.sqrt(periods_per_year)).where(volatility > 0)
    return pd.DataFrame({
        "sharpe": sharpe,
        "total_return": value.iloc[-1] / initial_cash - 1.0,
        "max_drawdown": (value / value.cummax() - 1.0).min(),
        "final_value": value.iloc[-1],
        "fills": fills,
        "fees": fees,
        "rejects": rejects,
    })


def ranking(table: pd.DataFrame, sort_by: str, top: int, min_fills: int) -> str:
    shown = table[table["fills"] >= min_fills].head(top).copy()
    for column in ("total_return", "max_drawdown"):
        shown[column] = (shown[column] * 100).map("{:+.2f}%".format)
    shown["sharpe"] = shown["sharpe"].map("{:.2f}".format)
    shown["final_value"] = shown["final_value"].map("{:,.2f}".format)
    shown["fees"] = shown["fees"].map("{:,.2f}".format)
    return (f"Best {len(shown)} by {sort_by}"
            + (f" (at least {min_fills} fills)" if min_fills else "") + ":\n" + shown.to_string())


class Stages:
    """Where the time went: data, signals, the first GPU batch (compilation included), the rest."""

    def __init__(self):
        self.began = time.perf_counter()
        self.seconds = defaultdict(float)

    def add(self, name: str, seconds: float):
        self.seconds[name] += seconds

    def report(self) -> str:
        total = time.perf_counter() - self.began
        rows = dict(self.seconds)
        rows["other (metrics, sorting, files)"] = max(0.0, total - sum(rows.values()))
        lines = [f"  {name:<44} {seconds:8.2f} s" for name, seconds in rows.items()]
        return "Stages:\n" + "\n".join(lines) + f"\n  {'total':<44} {total:8.2f} s"


async def verify_with_engine(*, params, algorithm_file: Path, config_path: Path, calendar,
                             asset_service, bundle, universe, exact, close, commission, slippage,
                             future_commission=None, future_slippage=None,
                             say: Callable[[str], None]):
    """The best combination through run_simulation -- the event-driven engine -- and the trades
    compared."""
    from ziplime.sweep.market import parse_rate

    rate = parse_rate(params.emission_rate)
    say("\nVerifying with the ziplime engine (run_simulation with best_config.json)...")
    started = time.perf_counter()
    with contextlib.redirect_stdout(io.StringIO()):
        event = await run_simulation(
            start_date=datetime.datetime.combine(params.start_date, datetime.time.min,
                                                 tzinfo=calendar.tz),
            end_date=close.index[-1].to_pydatetime(),
            trading_calendar=params.trading_calendar, emission_rate=rate,
            total_cash=params.total_cash, market_data_source=bundle, custom_data_sources=[],
            algorithm_file=str(algorithm_file), config_file=str(config_path),
            stop_on_error=True, asset_service=asset_service, benchmark_asset_symbol=None,
            benchmark_returns=None, equity_commission=commission, equity_slippage=slippage,
            future_commission=future_commission, future_slippage=future_slippage,
            same_bar_execution=params.timing == "same_close",
            price_used_in_order_execution="open" if params.timing == "next_open" else "close",
            print_algo=False)
        # The sweep's trades through the same ledger and the same metrics (costs are already in
        # the trade prices)
        vector = await to_execution_result(
            portfolio=exact, listings=universe, prices=close, trading_calendar=calendar,
            exchange=SimulationExchange(
                name="VECTOR", country_code="US", trading_calendar=calendar, clock=None,
                cash_balance=params.total_cash, equity_slippage=NoSlippage(),
                future_slippage=NoSlippage(), equity_commission=NoCommission(),
                future_commission=NoCommission(), account_id="vectorized_account",
                is_default=True),
            emission_rate=rate)
    comparison = compare_execution_results(vector, event)
    say(f"  engine: {time.perf_counter() - started:.1f} s, {comparison.event_trades} trades; "
        f"sweep: {comparison.vector_trades} trades")

    # Several open orders in one instrument fill as separate trades in the engine and as one
    # in the sweep. The same total per bar and the same portfolio is the same execution, booked
    # differently.
    def per_bar(fills):
        total = defaultdict(float)
        for stamp, instrument, amount, *_ in fills:
            total[(stamp, instrument)] += amount
        return {key: round(value, 6) for key, value in total.items()}

    split = (not comparison.agrees and comparison.max_equity_gap < 0.01
             and per_bar(comparison.only_in_vector) == per_bar(comparison.only_in_event))
    verdict = "yes" if comparison.agrees else ("yes, per bar" if split else "NO")
    say(f"  trades agree: {verdict}; largest portfolio value gap: ${comparison.max_equity_gap:,.2f}")
    if split:
        say(f"  ({len(per_bar(comparison.only_in_vector))} times the engine filled several orders in "
            f"one instrument on one bar as separate trades, and the sweep as one)")
    elif not comparison.agrees:
        say(comparison.report())
    return comparison.agrees or split


def write_best_config(output: Path, config) -> Path:
    path = output / "best_config.json"
    path.write_text(json.dumps(config.model_dump(), indent=2) + "\n", encoding="utf-8")
    return path
