"""Parameter sweeps: one strategy file, every combination of its parameters at once.

    ziplime sweep sweep.json

or from Python, with run_simulation's arguments and a grid::

    from ziplime.sweep import sweep_simulation
    table = await sweep_simulation(algorithm_file="trend.py", start_date=..., end_date=...,
                                   market_data_source=bundle, asset_service=assets,
                                   grid={"fast": [5, 10, 20], "slow": {"from": 30, "to": 90}})

or with a sweep file's fields::

    from ziplime.sweep import SweepParams, run_sweep
    table = await run_sweep(SweepParams.model_validate_json(path.read_text()), path.parent)

The strategy is the ordinary algorithm file run_simulation runs. Its ``handle_data`` is converted to
run for every combination on a GPU (``backend: mlx`` on Apple silicon, ``cupy`` on NVIDIA, ``dpnp``
on Intel) or across CPU cores (``backend: cpu``); the best combinations are recomputed exactly, and
the best one is run through run_simulation and its trades compared. See ``docs/backtesting/parameter-sweeps.md``.
"""
from ziplime.sweep.params import SweepError, SweepParams

__all__ = ["SweepError", "SweepParams", "run_sweep", "sweep_simulation"]


def __getattr__(name: str):
    # run_sweep brings the whole engine with it (run_simulation, the pipeline); a process that
    # only replays computations on history (lazy.py) imports this package without it
    if name == "run_sweep":
        from ziplime.sweep.runner import run_sweep
        return run_sweep
    if name == "sweep_simulation":
        from ziplime.sweep.simulation import sweep_simulation
        return sweep_simulation
    raise AttributeError(f"module 'ziplime.sweep' has no attribute {name!r}")
