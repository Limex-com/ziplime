"""The way out for a strategy the sweep cannot convert: every combination through run_simulation.

Some strategies cannot run for every combination at once -- a model that reads the portfolio at
every bar, an order type or an instrument the sweep does not simulate. They still get swept: each
combination is an ordinary run of the engine, the combinations spread over the CPU's cores. That is
the engine itself, so its numbers need no verification; it is just slower, by the factor the
conversion would have saved.
"""
import asyncio
import concurrent.futures
import contextlib
import datetime
import io
import json
import logging
import multiprocessing
import os
import tempfile
import time
from pathlib import Path
from typing import Callable

import pandas as pd

from ziplime.sweep import report
from ziplime.sweep.market import parse_rate

#: Combinations per job: small enough to report progress, large enough to amortise a process's
#: imports.
PER_JOB = 4


def _bundle_parts(bundle) -> dict:
    """What a worker needs to rebuild the bundle: its rows and how they are stamped."""
    return dict(name=bundle.name, version=bundle.version, start_date=bundle.start_date,
                end_date=bundle.end_date, calendar=bundle.trading_calendar.name,
                frequency=bundle.frequency, original_frequency=bundle.original_frequency,
                data_type=bundle.data_type, timestamp=bundle.timestamp,
                data=bundle.data, sid_indexes=bundle.sid_indexes)


def _bundle_from(parts: dict):
    from ziplime.data.domain.data_bundle import DataBundle
    from ziplime.utils.calendar_utils import get_calendar

    parts = dict(parts)
    calendar = get_calendar(parts.pop("calendar"))
    return DataBundle(trading_calendar=calendar, **parts)


def _db_path(asset_service) -> str:
    url = asset_service._asset_repository.db_url
    return url.split("///", 1)[1] if "///" in url else url


async def _run_one(*, params, algorithm_file: str, config: dict, bundle, asset_service,
                   sources: list, folder: Path, record: bool = False):
    from ziplime.core.run_simulation import run_simulation
    from ziplime.utils.calendar_utils import get_calendar

    calendar = get_calendar(params.trading_calendar)
    path = folder / f"config_{os.getpid()}_{time.monotonic_ns()}.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            result = await run_simulation(
                start_date=datetime.datetime.combine(params.start_date, datetime.time.min,
                                                     tzinfo=calendar.tz),
                end_date=datetime.datetime.combine(params.end_date, datetime.time.max,
                                                   tzinfo=calendar.tz),
                trading_calendar=params.trading_calendar,
                emission_rate=parse_rate(params.emission_rate), total_cash=params.total_cash,
                market_data_source=bundle, custom_data_sources=list(sources),
                algorithm_file=algorithm_file, config_file=str(path), stop_on_error=True,
                asset_service=asset_service, benchmark_asset_symbol=None, benchmark_returns=None,
                equity_commission=params.equity_commission.model(),
                equity_slippage=params.equity_slippage.model(),
                future_commission=params.future_commission.model(),
                future_slippage=params.future_slippage.model(),
                same_bar_execution=params.timing == "same_close",
                price_used_in_order_execution="open" if params.timing == "next_open" else "close",
                print_algo=False)
    finally:
        path.unlink(missing_ok=True)
    perf = result.perf
    value = perf["portfolio_value"].astype(float)
    trades = [txn for row in perf["transactions"] for txn in row]
    return value, trades


def _row(index: int, value: pd.Series, trades: list, params) -> pd.DataFrame:
    table = report.metrics(pd.DataFrame({index: value.to_numpy()}), params.total_cash,
                           pd.Series([len(trades)], index=[index]),
                           pd.Series([float("nan")], index=[index]), 0, params.periods_per_year)
    table["unsupported"] = False
    return table


def _worker(job: dict) -> pd.DataFrame:
    """One process: its share of the combinations, each a run of the engine."""
    import structlog

    from ziplime.core.ingest_data import get_asset_service

    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.ERROR))

    async def go():
        asset_service = get_asset_service(db_path=job["db_path"])
        bundle = _bundle_from(job["bundle"])
        rows = []
        try:
            with tempfile.TemporaryDirectory() as folder:
                for index, config in job["configs"]:
                    value, trades = await _run_one(
                        params=job["params"], algorithm_file=job["algorithm_file"], config=config,
                        bundle=bundle, asset_service=asset_service, sources=[], folder=Path(folder))
                    rows.append(_row(index, value, trades, job["params"]))
        finally:
            await asset_service._asset_repository.engine.dispose()
        return pd.concat(rows)

    return asyncio.run(go())


async def sweep_on_engine(*, params, algorithm_file: Path, combinations, indices: list[int],
                          bundle, asset_service, sources: dict, say: Callable[[str], None],
                          stages) -> pd.DataFrame:
    """Every combination in ``indices`` through run_simulation, on ``workers`` processes."""
    workers = params.workers or os.cpu_count() or 1
    configs = [(index, combinations.config(index).model_dump()) for index in indices]
    started = time.perf_counter()
    # One in this process first: it times a run, and with sources that cannot travel to another
    # process (objects built in memory) it is the only way
    with tempfile.TemporaryDirectory() as folder:
        value, trades = await _run_one(params=params, algorithm_file=str(algorithm_file),
                                       config=configs[0][1], bundle=bundle,
                                       asset_service=asset_service, sources=list(sources.values()),
                                       folder=Path(folder))
    rows = [_row(configs[0][0], value, trades, params)]
    each = time.perf_counter() - started
    rest = configs[1:]
    parallel = workers if not sources else 1
    say(f"  one run of the engine takes {each:.1f} s; {len(rest):,} more on {parallel} "
        f"process{'es' if parallel > 1 else ''}: about {each * len(rest) / parallel / 60:.1f} min")
    if rest and parallel > 1:
        job = dict(params=params, algorithm_file=str(algorithm_file),
                   db_path=_db_path(asset_service), bundle=_bundle_parts(bundle))
        jobs = [dict(job, configs=rest[at:at + PER_JOB]) for at in range(0, len(rest), PER_JOB)]
        context = multiprocessing.get_context("spawn")
        done = 0
        with concurrent.futures.ProcessPoolExecutor(max_workers=parallel, mp_context=context) as pool:
            for table in pool.map(_worker, jobs):
                rows.append(table)
                done += len(table)
                if done % max(1, len(rest) // 10) < PER_JOB:
                    say(f"  {done + 1:,} of {len(configs):,} runs, "
                        f"{time.perf_counter() - started:.0f} s")
    else:
        with tempfile.TemporaryDirectory() as folder:
            for index, config in rest:
                value, trades = await _run_one(
                    params=params, algorithm_file=str(algorithm_file), config=config,
                    bundle=bundle, asset_service=asset_service, sources=list(sources.values()),
                    folder=Path(folder))
                rows.append(_row(index, value, trades, params))
    stages.add(f"engine runs ({len(configs):,}, {parallel} processes)", time.perf_counter() - started)
    return pd.concat(rows)


async def best_fills(*, params, algorithm_file: Path, config: dict, bundle, asset_service,
                     sources: dict, output: Path):
    """The best combination's trades from the engine, as best_fills.csv."""
    with tempfile.TemporaryDirectory() as folder:
        value, trades = await _run_one(params=params, algorithm_file=str(algorithm_file),
                                       config=config, bundle=bundle, asset_service=asset_service,
                                       sources=list(sources.values()), folder=Path(folder))
    pd.DataFrame([{"timestamp": txn.dt, "instrument": txn.asset.symbol, "amount": txn.amount,
                   "price": txn.price} for txn in trades]).to_csv(output / "best_fills.csv",
                                                                  index=False)
    return value, trades
