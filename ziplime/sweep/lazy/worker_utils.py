"""Worker utils for lazy."""

import os
import types
from ziplime.sweep.lazy.replay_utils import compile_plan
from ziplime.sweep.lazy.replay_utils import replay_series


_WORKER = {}


def _start_worker(market_arrays: dict) -> None:
    from ziplime.sweep.market import HistoryRows

    market = types.SimpleNamespace(**market_arrays)
    _WORKER["rows"], _WORKER["n_bars"] = HistoryRows(market), market.n_bars
    os.environ["ZIPLIME_SWEEP_REPLAY_WORKERS"] = "1"


def _replay_job(job: tuple):
    key, params, columns = job
    plan = compile_plan(key, _WORKER["rows"])
    return replay_series(plan, _WORKER["n_bars"], columns, params)


def replay_workers() -> int:
    return max(1, int(os.environ.get("ZIPLIME_SWEEP_REPLAY_WORKERS", os.cpu_count() or 1)))
