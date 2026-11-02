"""A parameter sweep from start to finish: ``run_sweep(params, base)``.

Three ways a strategy is computed, picked from the strategy itself:

* ``compute_signals`` returns ``entries``/``exits`` -- decisions are signals, and the vector kernel
  computes every combination (Metal on Apple GPUs, numpy elsewhere);
* otherwise its ordinary ``handle_data`` (with ``before_trading_start`` and scheduled functions)
  is converted to run for every combination at once, on indicators from ``context.indicators`` or
  ``compute_signals``.

Either way the best combinations are recomputed exactly (float64) and the best one is run through
run_simulation, and its trades compared with the sweep's.
"""
import asyncio
import concurrent.futures
import datetime
import importlib.util
import multiprocessing
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

from ziplime.core.ingest_data import get_asset_service
from ziplime.finance.cancel_policy import EODCancel, NeverCancel
from ziplime.sweep import check, engine, report
from ziplime.sweep import explain
from ziplime.sweep.convert import NotConvertible, convert
from ziplime.sweep.data import load_market_data, price_panel
from ziplime.sweep.initialize import initialize_each, load_algorithm, run_initialize
from ziplime.sweep.market import asof_rows, build_market, parse_rate
from ziplime.sweep.params import (
    DEFAULT_ASSET_DB, DEFAULT_STORAGE, Commission, Slippage, SweepError, SweepParams, expand_grid,
)
from ziplime.sweep.signals import (
    IndicatorSignals, SignalCache, check_causality, check_indicators, compute, signal_arrays,
)
from ziplime.utils.calendar_utils import get_calendar
from ziplime.utils.events import make_eventrule, time_rules, date_rules
from ziplime.vectorized.kernel import (
    ExecutionTiming, VectorKernelError, simulate_signals, simulate_signals_batch,
)
from ziplime.vectorized.kernel.costs import resolve_commission, resolve_slippage

#: Elements (combinations × instruments) per GPU call when batch_size is "auto": the size the
#: measurements on an M3 and an RTX 4090 ran fastest at.
AUTO_ELEMENTS = 500_000
#: For CuPy the cost that grows with the number of batches is Python's: every bar is a few hundred
#: calls whatever the batch's size, so fewer, larger batches are faster.
CUDA_ELEMENTS = 2_000_000
#: A rerun of this few combinations goes to the CPU: on a GPU each bar costs the same few hundred
#: kernel launches whether it carries ten combinations or forty thousand.
SMALL_RERUN = 5_000


def batch_size(params: SweepParams, n_columns: int) -> int:
    if params.batch_size != "auto":
        return int(params.batch_size)
    elements, most = AUTO_ELEMENTS, 60_000
    if params.backend == "cupy":
        # MLX on CUDA does not hold larger batches in 24 GB; CuPy does, and gains from them
        elements, most = CUDA_ELEMENTS, 200_000
    return int(min(most, max(500, elements // max(1, n_columns))))


def _apple_gpu() -> bool:
    try:
        import mlx.core as mx
        return bool(mx.metal.is_available())
    except (ImportError, AttributeError):
        return False


async def run_sweep(params: SweepParams, base: Path, say: Callable[[str], None] = print, *,
                    market_data=None, asset_service=None,
                    custom_data_sources: list | None = None) -> pd.DataFrame:
    """Run the sweep the file describes; returns every combination's metrics.

    ``market_data`` (a DataBundle) and ``asset_service`` stand in for the file's bundle and asset
    database, as run_simulation's ``market_data_source`` and ``asset_service`` do;
    ``custom_data_sources`` are the sources ``context.datasets`` may name, as for run_simulation. Whether the best
    combination's trades agreed with the engine is in ``table.attrs["verified"]``.
    """
    stages = report.Stages()
    started = time.perf_counter()
    combos = expand_grid(params.grid, params.where)
    algorithm, combinations = load_algorithm(params, base, combos)
    owned = asset_service is None
    if owned:
        asset_db = params.resolve(base, params.asset_db) or DEFAULT_ASSET_DB
        asset_service = get_asset_service(db_path=str(asset_db))
    try:
        return await _sweep(params, base, say, stages, started, combos, algorithm, combinations,
                            asset_service, market_data,
                            {source.name: source for source in custom_data_sources or []})
    finally:
        if owned:
            await asset_service._asset_repository.engine.dispose()


async def _sweep(params, base, say, stages, started, combos, algorithm, combinations, asset_service,
                 market_data, registered):
    found = {}
    first = await run_initialize(algorithm, asset_service, combinations.config(0), found)
    universe = first.universe
    if not universe:
        raise SweepError("initialize() asked for no instruments -- nothing to sweep over.")
    columns = list(universe)
    calendar = get_calendar(params.trading_calendar)

    # History for WARMUP: about WARMUP × 1.5 calendar days before the start
    since = params.start_date - datetime.timedelta(days=int(algorithm.warmup * 1.6) + 15)
    bundle = market_data or await load_market_data(
        bundle=params.bundle, version=params.bundle_version,
        storage=params.resolve(base, params.bundle_storage) or DEFAULT_STORAGE,
        cache_dir=params.resolve(base, params.cache_dir), universe=universe, calendar=calendar,
        first=since, last=params.end_date, say=say, rate=parse_rate(params.emission_rate))
    _same_frequency(bundle, params)
    panel, in_run = price_panel(bundle, universe, params.start_date, algorithm.warmup, say)
    if first.datasets:
        from ziplime.sweep.data import attach_datasets
        last = min(params.end_date, bundle.data["date"].max().date())
        panel = await attach_datasets(panel, first.datasets, universe, calendar, params.start_date,
                                      last, asset_service, registered)
    stages.add("preparation (algorithm, assets, data)", time.perf_counter() - started)

    # Signals as decisions -> the vector kernel; anything else -> handle_data, converted
    decisions = False
    if algorithm.compute_signals is not None and not first.indicators:
        decisions = "entries" in compute(algorithm, first_context(algorithm, asset_service, found),
                                         panel, combinations.config(0), False)
    common = dict(params=params, base=base, say=say, stages=stages, combos=combos,
                  algorithm=algorithm, combinations=combinations, asset_service=asset_service,
                  calendar=calendar, bundle=bundle, panel=panel, in_run=in_run, universe=universe,
                  columns=columns, found=found, first=first)
    if decisions:
        return await _sweep_signals(**common)
    common["registered"] = registered
    return await _sweep_handle_data(**common)


def first_context(algorithm, asset_service, found):
    from ziplime.sweep.initialize import SweepContext
    return SweepContext(algorithm, asset_service, found)


# -- handle_data ----------------------------------------------------------------------------------

def _same_frequency(bundle, params):
    """The bundle's bars are the run's bars. A finer bundle loaded at a coarser rate is stamped at
    each window's start, so the engine reads a bar's whole window at its first minute -- look-ahead
    a sweep would have to copy; it refuses instead."""
    rate = parse_rate(params.emission_rate)
    frequency = getattr(bundle, "frequency_td", None) or parse_rate(getattr(bundle, "frequency", None))
    if rate < datetime.timedelta(days=1) and frequency != rate:
        raise SweepError(f"The bundle's bars are every {frequency}, the run's every {rate}. Ingest "
                         f"the bundle at the run's frequency.")


def hooks_of(algorithm, initialized: list, calendar, market):
    """handle_data, before_trading_start and the scheduled functions, converted; and for each
    scheduled function the bars it runs at, per distinct initialize result -- decided by the
    engine's own rules (make_eventrule) over the run's bars."""
    module = algorithm.handle_data.__module__
    distinct = list({id(each): each for each in initialized}.values())
    names = [function.__name__ for function, *_ in distinct[0].scheduled]
    if any([function.__name__ for function, *_ in each.scheduled] != names for each in distinct):
        raise SweepError("initialize schedules different functions for different combinations.")
    try:
        handle = convert(algorithm.handle_data)
        before = (convert(algorithm.before_trading_start)
                  if getattr(algorithm.before_trading_start, "__module__", None) == module else None)
        scheduled = [convert(function) for function, *_ in distinct[0].scheduled]
    except NotConvertible as error:
        raise SweepError(f"The strategy cannot run for every combination at once:\n  {error}") from None

    # flags[k]: (bars, distinct results); position: each combination's distinct result
    position = {id(each): k for k, each in enumerate(distinct)}
    which = np.array([position[id(each)] for each in initialized])
    flags, computed = [], {}
    for slot in range(len(names)):
        columns = []
        for each in distinct:
            _, date_rule, time_rule, half_days = each.scheduled[slot]
            # A daily run has no time of day; an intraday one honours the time rule
            timed = (time_rule or time_rules.every_minute()) if market.intraday \
                else time_rules.every_minute()
            date_rule = date_rule or date_rules.every_day()
            # Combinations build their own rule objects; equal rules fire on the same bars
            key = (_rule_key(date_rule), _rule_key(timed), half_days)
            if key not in computed:
                rule = make_eventrule(date_rule, timed, calendar, half_days)
                computed[key] = [bool(rule.should_trigger(stamp)) for stamp in market.index]
            columns.append(computed[key])
        flags.append(np.array(columns, dtype=bool).T)
    hooks = engine.Hooks(handle_data=handle, before_trading_start=before, scheduled=scheduled)
    return hooks, flags, which


def compile_step(params: SweepParams) -> bool | None:
    """Whether to compile each bar: the file's choice, else always with MLX -- on Apple GPUs and on
    CUDA alike, where MLX keeps the compiled kernels between runs."""
    if params.compile is not None:
        return params.compile
    return True if params.backend == "mlx" else None


def _rule_key(rule):
    """What a date or time rule decides by: its class and its settings."""
    settings = tuple(sorted((name, repr(value)) for name, value in vars(rule).items()
                            if name not in ("cal", "_cal")))
    return type(rule).__name__, settings


def schedule_for(flags, which, chunk) -> list:
    return [matrix[:, which[chunk]] for matrix in flags]


def costs_for(params: SweepParams, initialized) -> SweepParams:
    """set_commission / set_slippage in initialize win over the sweep file, as over run_simulation's
    arguments."""
    changes = {}
    if "equities" in initialized.commission:
        changes["equity_commission"] = Commission.of(initialized.commission["equities"])
    if "futures" in initialized.commission:
        changes["future_commission"] = Commission.of(initialized.commission["futures"])
    if "equities" in initialized.slippage:
        changes["equity_slippage"] = Slippage.of(initialized.slippage["equities"])
    if "futures" in initialized.slippage:
        changes["future_slippage"] = Slippage.of(initialized.slippage["futures"])
    return params.model_copy(update=changes) if changes else params


def remap(stacks: dict, rows: np.ndarray) -> dict:
    """Signals on the panel's rows -> on the run's bars, each bar reading its latest row."""
    safe = np.maximum(rows, 0)
    return {name: np.where((rows >= 0)[None, :, None], stack[:, safe, :], np.nan)
            for name, stack in stacks.items()}


async def _sweep_handle_data(*, params, base, say, stages, combos, algorithm, combinations,
                             asset_service, calendar, bundle, panel, in_run, universe, columns,
                             found, first, registered):
    on_engine = dict(params=params, base=base, say=say, stages=stages, combos=combos,
                     combinations=combinations, bundle=bundle, asset_service=asset_service,
                     registered=registered)
    started = time.perf_counter()
    last = min(params.end_date, bundle.data["date"].max().date())
    market = await build_market(bundle.data, universe, calendar, params.start_date, last,
                                asset_service, rate=parse_rate(params.emission_rate))
    stages.add("market (spot rows, corporate actions)", time.perf_counter() - started)

    started = time.perf_counter()
    initialized = await initialize_each(algorithm, asset_service, combinations, found)
    try:
        hooks, flags, which = hooks_of(algorithm, initialized, calendar, market)
        check.check([f for hook in hooks.all()
                     for f in (hook.original, *hook.helper_originals.values())],
                    set(initialized[0].state), initialized[0].state, hooks=hooks.all(),
                    universe=initialized[0].universe)
    except SweepError as reason:
        return await _on_engine(reason, **on_engine)
    params = costs_for(params, initialized[0])
    costs = engine.resolve_costs(params, market)
    for each in {id(each): each for each in initialized}.values():
        if each.cancel_policy is not None and not isinstance(each.cancel_policy,
                                                             (EODCancel, NeverCancel)):
            raise SweepError(f"Cancel policy {type(each.cancel_policy).__name__} is not supported "
                             f"in a sweep.")
    cancel = np.array([isinstance(each.cancel_policy, EODCancel) for each in initialized])

    panel_rows = asof_rows(panel.index[in_run], market.index)
    indicators = cache = None
    if first.indicators:
        if algorithm.compute_signals is not None:
            raise SweepError("Indicators are declared both in compute_signals and with "
                             "context.indicators() -- keep one.")
        indicators = IndicatorSignals(initialized, panel, in_run, columns)
        check_indicators(initialized[0].indicators, panel, algorithm.warmup, combinations.config(0))
    elif algorithm.compute_signals is not None:
        context = first_context(algorithm, asset_service, found)
        cache = SignalCache(algorithm, context, panel, in_run, columns, decisions=False)
        check_causality(algorithm, context, panel, combinations.config(0), decisions=False)
    stages.add("initialize and indicators", time.perf_counter() - started)

    def signals_for(indices, configs):
        if indicators is not None:
            stacks, index = indicators.stacks(indices)
        elif cache is not None:
            stacks, index = cache.stacks(configs, int(in_run.sum()), len(columns), decisions=False)
        else:
            return {}, None
        return remap(stacks, panel_rows), index

    say(f"Algorithm: {params.algorithm_file}   sweep: {', '.join(params.grid)}")
    say(f"Data: {len(columns)} instruments × {market.n_bars} sessions "
        f"({market.index[0].date()} ... {market.index[-1].date()}), WARMUP {algorithm.warmup}")
    say(f"Combinations: {len(combos):,}   backend: {params.backend}   fills: {params.timing}   "
        f"decisions: handle_data (converted for every combination)"
        + (f"   indicators: {indicators.variants} distinct" if indicators else ""))

    windows = engine.Windows(market, None)
    run_kwargs = dict(market=market, hooks=hooks, timing=params.timing,
                      initial_cash=params.total_cash, costs=costs,
                      periods_per_year=params.periods_per_year, compile=compile_step(params))

    def per_chunk(chunk):
        return dict(schedule=schedule_for(flags, which, chunk), cancel_at_session_end=cancel[chunk])
    size = batch_size(params, len(columns))
    compiled_flags, compile_reasons = [], []

    def simulate(indices: list[int], backend: str) -> pd.DataFrame:
        if backend == "cpu":
            return simulate_parallel(indices)
        # Every order slot costs operations on every bar, compiled or not: start with one
        first_slots = engine.SLOT_STEPS[0]
        tables, short = [], []
        for at in range(0, len(indices), size):
            chunk = indices[at:at + size]
            configs = combinations.configs(chunk)
            started = time.perf_counter()
            stacks, index = signals_for(chunk, configs)
            stages.add("signals per batch", time.perf_counter() - started)
            started = time.perf_counter()
            result = engine.run(configs=configs, states=[initialized[i].state for i in chunk],
                                signals=stacks, signal_index=index,
                                backend="numpy" if backend == "exact" else backend,
                                windows=windows, order_slots=first_slots, rerun=False,
                                **run_kwargs, **per_chunk(chunk))
            elapsed = time.perf_counter() - started
            label = "exact recomputation (CPU, float64)" if backend == "exact" and \
                params.backend != "exact" else f"simulation ({backend})"
            if at == 0 and label.startswith("simulation"):
                stages.add(f"{label}, first batch" + (" (compiles)" if result.compiled else ""),
                           elapsed)
            else:
                stages.add(label + (", other batches" if label.startswith("simulation") else ""),
                           elapsed)
            compiled_flags.append(result.compiled)
            compile_reasons.extend(result.compile_reasons or [])
            table = _metrics(result, market.index, chunk, params)
            short += table.index[table["unsupported"]].tolist()
            tables.append(table)
        table = pd.concat(tables)
        more = [steps for steps in engine.SLOT_STEPS if steps > first_slots]
        if short and more:
            # Combinations that needed more open orders than the run kept: once, all together
            started = time.perf_counter()
            configs = combinations.configs(short)
            stacks, index = signals_for(short, configs)
            rerun_backend = ("numpy" if backend == "exact" or len(short) <= SMALL_RERUN
                             else backend)
            result = engine.run(configs=configs, states=[initialized[i].state for i in short],
                                signals=stacks, signal_index=index, backend=rerun_backend,
                                windows=windows, order_slots=more[0], **run_kwargs,
                                **per_chunk(short))
            table.loc[short] = _metrics(result, market.index, short, params)
            stages.add(f"combinations needing more open orders ({len(short):,})",
                       time.perf_counter() - started)
        return table

    def simulate_parallel(indices):
        started = time.perf_counter()
        workers = params.workers or os.cpu_count() or 1
        chunks = [indices[k::workers] for k in range(workers) if indices[k::workers]]
        jobs = []
        for chunk in chunks:
            configs = combinations.configs(chunk)
            stacks, index = signals_for(chunk, configs)
            jobs.append(dict(
                algorithm_file=str(params.resolve(base, params.algorithm_file)),
                scheduled=[hook.name for hook in hooks.scheduled],
                schedule=schedule_for(flags, which, chunk), cancel=cancel[chunk],
                configs=[c.model_dump() for c in configs],
                states=[initialized[i].state for i in chunk], signals=stacks, index=index,
                chunk=chunk, market=market, timing=params.timing, cash=params.total_cash,
                costs=costs, lengths=windows.lengths, params=params,
                size=size))
        context = multiprocessing.get_context("spawn")
        with concurrent.futures.ProcessPoolExecutor(max_workers=len(jobs), mp_context=context) as pool:
            tables = list(pool.map(_worker, jobs))
        stages.add(f"simulation (cpu, {len(jobs)} processes)", time.perf_counter() - started)
        return pd.concat(tables)

    try:
        table = pd.DataFrame(combos).join(simulate(list(range(len(combos))), params.backend))
    except NotConvertible as reason:
        return await _on_engine(SweepError(str(reason)), **on_engine)
    say("\n".join(explain.conversion(hooks=hooks, windows=windows, backend=params.backend,
                                      compiled=any(compiled_flags), reasons=compile_reasons,
                                      columns=columns)))
    nan_records = explain.warnings(windows, columns)
    return await _finish(params=params, base=base, say=say, stages=stages, table=table,
                         combos=combos, combinations=combinations, simulate=simulate,
                         columns=columns, market=market, calendar=calendar, bundle=bundle,
                         universe=universe, asset_service=asset_service,
                         variants=indicators.variants if indicators else len(cache.variants) if cache else 0,
                         best_run=lambda best: engine.run(
                             configs=[combinations.config(best)], states=[initialized[best].state],
                             signals=signals_for([best], [combinations.config(best)])[0],
                             signal_index=signals_for([best], [combinations.config(best)])[1],
                             backend="numpy", record=True, windows=windows, **run_kwargs,
                             **per_chunk([best])),
                         check_best=lambda best: _check_best(
                             indicators, cache, algorithm, asset_service, combinations, best,
                             panel, found, initialized),
                         compiled=any(compiled_flags), warnings=nan_records)


async def _on_engine(reason, *, params, base, say, stages, combos, combinations, bundle,
                     asset_service, registered):
    """The strategy cannot run for every combination at once: run each through the engine."""
    from ziplime.sweep import fallback

    if params.on_unsupported == "refuse":
        raise reason
    say(f"The strategy cannot run for every combination at once:\n  {reason}\n"
        f"Every combination goes through run_simulation instead, on the CPU's cores -- slower, "
        f"and the engine's own numbers (on_unsupported: refuse to stop here instead).")
    algorithm_file = params.resolve(base, params.algorithm_file)
    table = pd.DataFrame(combos).join(await fallback.sweep_on_engine(
        params=params, algorithm_file=algorithm_file, combinations=combinations,
        indices=list(range(len(combos))), bundle=bundle, asset_service=asset_service,
        sources=registered, say=say, stages=stages))
    table.index.name = "combo"
    table["precision"] = "engine"
    table = table.sort_values(params.sort_by, ascending=False)
    ranked = table[table["fills"] >= params.min_fills]
    if ranked.empty:
        raise SweepError(f"No combination made {params.min_fills} fills; lower min_fills.")
    say("\n" + report.ranking(ranked, params.sort_by, params.top, params.min_fills))
    output = params.resolve(base, params.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    table.to_csv(output / "sweep_results.csv")
    best = int(ranked.index[0])
    report.write_best_config(output, combinations.config(best))
    await fallback.best_fills(params=params, algorithm_file=algorithm_file,
                              config=combinations.config(best).model_dump(), bundle=bundle,
                              asset_service=asset_service, sources=registered, output=output)
    say(f"\nBest {combos[best]}: the engine's run. Written: {output}/sweep_results.csv, "
        f"best_config.json, best_fills.csv")
    say("\n" + stages.report())
    table.attrs["verified"] = True
    return table


def _check_best(indicators, cache, algorithm, asset_service, combinations, best, panel, found,
                initialized):
    """No look-ahead in the winner's signals either -- it is the combination that gets used."""
    if indicators is not None:
        check_indicators(initialized[best].indicators, panel, algorithm.warmup,
                         combinations.config(best))
    elif cache is not None:
        check_causality(algorithm, first_context(algorithm, asset_service, found), panel,
                        combinations.config(best), decisions=False)


def _metrics(result: engine.Result, index, chunk, params) -> pd.DataFrame:
    table = pd.DataFrame(result.metrics, index=chunk)
    table["fills"], table["fees"], table["rejects"] = result.fills, result.fees, 0
    table["unsupported"] = result.unsupported
    return table


def _worker(job: dict) -> pd.DataFrame:
    """One process of ``backend: cpu``: the strategy loaded afresh, its share of the combinations
    simulated with numpy in float64, and only the metrics sent back."""
    # Already one of several processes: computations on history are replayed here, not spread
    # over processes of its own
    os.environ["ZIPLIME_SWEEP_REPLAY_WORKERS"] = "1"
    module_name = "ziplime.ziplime_algorithm"
    spec = importlib.util.spec_from_file_location(module_name, job["algorithm_file"])
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    config_class = next(obj for obj in module.__dict__.values()
                        if isinstance(obj, type) and obj.__name__ == "AlgorithmConfig")
    configs = [config_class.model_validate(c) for c in job["configs"]]
    before = module.__dict__.get("before_trading_start")
    hooks = engine.Hooks(
        handle_data=convert(module.handle_data),
        before_trading_start=convert(before) if before is not None else None,
        scheduled=[convert(module.__dict__[name]) for name in job["scheduled"]])
    windows = engine.Windows(job["market"], None)
    windows.lengths = dict(job["lengths"])
    tables = []
    size = job["size"]
    for at in range(0, len(configs), size):
        part = slice(at, at + size)
        index = job["index"]
        if isinstance(index, dict):
            sub_index = {name: values[part] for name, values in index.items()}
        else:
            sub_index = None if index is None else index[part]
        result = engine.run(market=job["market"], hooks=hooks, configs=configs[part],
                            states=job["states"][part], signals=job["signals"],
                            signal_index=sub_index, timing=job["timing"],
                            initial_cash=job["cash"], costs=job["costs"], backend="numpy",
                            cancel_at_session_end=job["cancel"][part], windows=windows,
                            periods_per_year=job["params"].periods_per_year,
                            schedule=[matrix[:, part] for matrix in job["schedule"]])
        tables.append(_metrics(result, job["market"].index, job["chunk"][part], job["params"]))
    return pd.concat(tables)


# -- signals as decisions --------------------------------------------------------------------------

async def _sweep_signals(*, params, base, say, stages, combos, algorithm, combinations,
                         asset_service, calendar, bundle, panel, in_run, universe, columns,
                         found, first):
    if params.backend == "cupy":
        raise SweepError("The kernel for entries/exits signals exists for Metal (mlx) and the CPU "
                         "(exact); on CUDA a strategy is swept through its handle_data.")
    configs_all = None
    sizes = [params.size if params.size is not None else getattr(combinations.config(i), "size", None)
             for i in range(len(combos))] if len(combos) <= 200_000 else None
    if sizes is None or None in sizes:
        if params.size is None:
            raise SweepError('No order size: put "size" in the sweep file or a size field in '
                             'AlgorithmConfig.')
        sizes = [params.size] * len(combos)
    closes_at = pd.DatetimeIndex([calendar.session_close(stamp.date()).tz_convert(calendar.tz)
                                  for stamp in panel.index[in_run]])
    close = panel.close[columns].iloc[in_run].set_axis(closes_at)
    opens = panel.open[columns].iloc[in_run].set_axis(closes_at)
    volumes = panel.volume[columns].iloc[in_run].set_axis(closes_at)
    context = first_context(algorithm, asset_service, found)
    check_causality(algorithm, context, panel, combinations.config(0))
    commission, slippage = params.equity_commission.model(), params.equity_slippage.model()
    shared = dict(initial_cash=params.total_cash, execution=ExecutionTiming(execute_at=params.timing),
                  commission=commission, slippage=slippage, volumes=volumes,
                  opens=opens if params.timing == "next_open" else None)
    cache = SignalCache(algorithm, context, panel, in_run, columns, decisions=True)
    say(f"Algorithm: {params.algorithm_file}   sweep: {', '.join(params.grid)}")
    say(f"Data: {len(columns)} instruments × {len(close)} days "
        f"({close.index[0].date()} ... {close.index[-1].date()}), WARMUP {algorithm.warmup}")
    say(f"Combinations: {len(combos):,}   backend: {params.backend}   fills: {params.timing}   "
        f"decisions: entries/exits signals")
    size_per_call = params.batch_size if params.batch_size != "auto" else 2000

    def simulate(indices, backend):
        groups = defaultdict(list)
        for index in indices:
            groups[sizes[index]].append(index)
        tables = []
        for order_size, members in groups.items():
            for at in range(0, len(members), size_per_call):
                chunk = members[at:at + size_per_call]
                started = time.perf_counter()
                stacks, _ = cache.stacks(combinations.configs(chunk), len(close), len(columns))
                stages.add("signals per batch", time.perf_counter() - started)
                started = time.perf_counter()
                try:
                    batch = simulate_signals_batch(
                        close, **stacks, labels=chunk, size=order_size,
                        backend="mlx" if backend == "mlx" else "exact",
                        allow_short="short_entries" in stacks or "short_exits" in stacks, **shared)
                except VectorKernelError as error:
                    raise SweepError(f"ziplime refused: {error}") from None
                stages.add(f"simulation ({backend})" if backend == params.backend
                           else "exact recomputation (CPU, float64)", time.perf_counter() - started)
                part = report.metrics(batch.portfolio_value, params.total_cash, batch.fills,
                                      batch.fees,
                                      batch.rejects.drop(columns="no_execution_bar").sum(axis=1),
                                      params.periods_per_year)
                part["unsupported"] = False
                tables.append(part)
        return pd.concat(tables)

    def best_run(best):
        frames = {name: pd.DataFrame(values, index=close.index, columns=columns)
                  for name, values in signal_arrays(
                      compute(algorithm, context, panel, combinations.config(best)), in_run,
                      columns).items()}
        return simulate_signals(prices=close, **frames, size=sizes[best],
                                allow_short="short_entries" in frames or "short_exits" in frames,
                                **shared)

    table = pd.DataFrame(combos).join(simulate(list(range(len(combos))), params.backend))
    return await _finish(params=params, base=base, say=say, stages=stages, table=table,
                         combos=combos, combinations=combinations, simulate=simulate,
                         columns=columns, market=None, calendar=calendar, bundle=bundle,
                         universe=universe, asset_service=asset_service,
                         variants=len(cache.variants), best_run=best_run,
                         check_best=lambda best: check_causality(
                             algorithm, context, panel, combinations.config(best)),
                         compiled=False, close=close)


# -- ranking, files, verification -----------------------------------------------------------------

async def _finish(*, params, base, say, stages, table, combos, combinations, simulate, columns,
                  market, calendar, bundle, universe, asset_service, variants, best_run,
                  check_best, compiled, close=None, warnings=()):
    table.index.name = "combo"
    gpu = params.backend in ("mlx", "cupy")
    table["precision"] = "float32" if gpu else "float64"
    unsupported = int(table["unsupported"].sum())
    if unsupported:
        say(f"{unsupported:,} combinations had more than {engine.ORDER_SLOTS} open orders in one "
            f"instrument at once -- more than the sweep keeps; they are left out of the ranking "
            f"rather than reported with numbers that may differ from the engine.")
    table = table.sort_values(params.sort_by, ascending=False)

    # The GPU computes in float32; when decisions depend on the portfolio value, one share's
    # difference changes the path, so the top of the ranking is recomputed exactly
    if gpu and params.refine_top:
        top = table[(table["fills"] >= params.min_fills) & ~table["unsupported"]] \
            .head(params.refine_top).index.tolist()
        exact = simulate(top, "exact")
        table.loc[top, exact.columns] = exact
        table.loc[top, "precision"] = "float64"
        table = table.sort_values(params.sort_by, ascending=False)

    ranked = table[(table["fills"] >= params.min_fills) & ~table["unsupported"].astype(bool)]
    if ranked.empty:
        raise SweepError(f"No combination made {params.min_fills} fills (the most was "
                         f"{int(table['fills'].max())}); lower min_fills.")
    found = list(warnings) + explain.influence(table, list(combos[0]) if combos else [])
    if found:
        say("Warnings:\n" + "\n".join(found))
    say(f"Signals: {variants} distinct" + (" · compiled step" if compiled else ""))
    say("\n" + report.ranking(ranked, params.sort_by, params.top, params.min_fills))

    output = params.resolve(base, params.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    table.to_csv(output / "sweep_results.csv")
    best = int(ranked.index[0])
    config_path = report.write_best_config(output, combinations.config(best))
    check_best(best)
    started = time.perf_counter()
    exact = best_run(best)
    if isinstance(exact, engine.Result):
        exact = engine.as_vector_result(exact, market, params.timing, params.total_cash)
        close = pd.DataFrame(market.marks[market.session_end], index=market.session_index,
                             columns=columns)
    exact.fills_frame().to_csv(output / "best_fills.csv", index=False)
    stages.add("best combination, recorded (CPU)", time.perf_counter() - started)
    say(f"\nBest {combos[best]}: exact recomputation -- {len(exact.fills)} fills, final value "
        f"{float(exact.portfolio_value.iloc[-1]):,.2f} (the sweep showed "
        f"{float(ranked.iloc[0]['final_value']):,.2f})")
    say(f"Written: {output}/sweep_results.csv, best_config.json, best_fills.csv")

    table.attrs["verified"] = None
    if params.verify:
        started = time.perf_counter()
        table.attrs["verified"] = await report.verify_with_engine(
            params=params, algorithm_file=params.resolve(base, params.algorithm_file),
            config_path=config_path, calendar=calendar, asset_service=asset_service, bundle=bundle,
            universe=universe, exact=exact, close=close,
            commission=params.equity_commission.model(), slippage=params.equity_slippage.model(),
            future_commission=params.future_commission.model(),
            future_slippage=params.future_slippage.model(), say=say)
        stages.add("verification with run_simulation", time.perf_counter() - started)
    say("\n" + stages.report())
    return table
