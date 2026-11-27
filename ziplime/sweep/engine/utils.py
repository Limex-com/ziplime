"""Utils for engine."""

from collections.abc import Sequence
import numpy as np
from ziplime.sweep.backends import Backend
from ziplime.sweep.costs import Costs
from ziplime.sweep.errors import NotConvertible, Rediscover
from ziplime.sweep.market import Market
from ziplime.sweep.windows import Windows
from ziplime.sweep.params import SweepError
from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.sweep.engine.hooks import Hooks
from ziplime.sweep.engine.result import Result
from ziplime.sweep.engine.run_plain import RunPlain
from ziplime.sweep.engine.support import SLOT_STEPS
from ziplime.sweep.engine.simulation import _Simulation
from ziplime.sweep.engine.result_utils import _merge
from ziplime.sweep.engine.market_utils import _subset


def run(
    *,
    market: Market,
    hooks: Hooks,
    configs: list,
    states: list,
    signals: dict,
    signal_index: np.ndarray | dict[str, np.ndarray] | None,
    timing: str,
    initial_cash: float,
    costs: Costs,
    backend: str,
    cancel_at_session_end: bool | np.ndarray = False,
    compile: bool | None = None,
    record: bool = False,
    windows: Windows | None = None,
    periods_per_year: int = 252,
    schedule: list = (),
    order_slots: int = SLOT_STEPS[0],
    rerun: bool = True,
) -> Result:
    """Every combination of ``configs`` over the market's bars.

    ``schedule``: per scheduled function, the bars it runs at -- (bars,) when every combination
    agrees, (bars, combinations) when its rule depends on the config. ``cancel_at_session_end``:
    EODCancel -- a bool, or (combinations,) when set_cancel_policy depends on the config.

    Runs with ``order_slots`` open orders per instrument; the few combinations that needed more are
    run again with more, on their own, and their results put in place -- unless ``rerun`` is off,
    for a caller that collects them across batches and reruns them once.
    """
    arguments = dict(market=market, hooks=hooks, configs=configs, states=states, signals=signals,
                     signal_index=signal_index, timing=timing, initial_cash=initial_cash,
                     costs=costs, backend=backend, cancel_at_session_end=cancel_at_session_end,
                     compile=compile, record=record, windows=windows,
                     periods_per_year=periods_per_year, schedule=schedule)
    result = _run(order_slots=order_slots, **arguments)
    more = [steps for steps in SLOT_STEPS if steps > order_slots]
    if not rerun or not result.unsupported.any() or not more:
        return result
    needed = np.nonzero(result.unsupported)[0]
    again = run(order_slots=more[0], **_subset(arguments, needed))
    return _merge(result, again, needed)


def _run(
    *,
    market: Market,
    hooks: Hooks,
    configs: list[BaseAlgorithmConfig],
    states,
    signals: dict[str, np.ndarray],
    signal_index: np.ndarray | dict[str, np.ndarray] | None,
    timing: str,
    initial_cash: float,
    costs: Costs,
    backend: str,
    cancel_at_session_end: bool | np.ndarray,
    compile: bool | None,
    record: bool,
    windows: Windows | None,
    periods_per_year: int,
    schedule: Sequence[np.ndarray],
    order_slots: int,
) -> Result:
    b = Backend(backend)
    windows = windows or Windows(market, b)
    windows.backend = b

    def simulation(on: Backend | None = None):
        on = on or b
        windows.backend = on
        made = _Simulation(market, hooks, configs, states, signals, signal_index, timing,
                           initial_cash, costs, on, cancel_at_session_end, record, windows)
        made.periods_per_year = periods_per_year
        made.set_schedule(schedule)
        made.slots = order_slots
        return made

    plain_only = False
    for attempt in range(8):
        try:
            current = simulation()
            if attempt == 0 and any("history" in hook.source or "returns" in hook.source
                                    for hook in hooks.all()):
                current.discover()
            windows.prepare()
            use_compile = (b.lazy and not record) if compile is None else compile
            if use_compile and not plain_only and current.compilable():
                try:
                    return current.go(compiled=True)
                except RunPlain:
                    plain_only = True
                    windows.notes.add("a parameter of a computation on history changed during the "
                                      "run, so the bars ran without compiling")
                    current = simulation()
                except RuntimeError:
                    # The backend itself failed (MLX could not build a kernel). What it computes
                    # afterwards in the same process cannot be trusted, so this batch goes to the
                    # CPU in float64 -- slower, and right.
                    current = simulation(Backend("numpy"))
                    windows.prepare()
                except (NotConvertible, TypeError, ValueError, AttributeError) as error:
                    windows.notes.add(f"compiling the step failed ({type(error).__name__}: "
                                      f"{str(error)[:120]}), so the bars ran without compiling")
                    current = simulation()
            return current.go(compiled=False)
        except Rediscover:
            continue
    raise SweepError("data.history kept asking for windows the sweep had not prepared: its "
                     "bar_count, and the numbers a computation on history reads (a slice's "
                     "bounds, a period), have to depend on the config only.")
