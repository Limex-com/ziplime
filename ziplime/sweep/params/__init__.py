"""The sweep file: what to run, what to vary, and how to compute it.

Field names follow :func:`ziplime.core.run_simulation.run_simulation` wherever the two mean the
same thing, so a sweep file reads like the arguments of the run it stands for::

    {
      "algorithm_file": "strategies/moex_trend.py",
      "bundle": "grpc_daily_data",
      "start_date": "2025-04-07", "end_date": "2025-12-30",
      "trading_calendar": "XMOS",
      "total_cash": 1000000,
      "grid": {"fast": {"from": 2, "to": 20}, "slow": [20, 40, 60]},
      "where": "fast < slow",
      "backend": "mlx"
    }

Each key of ``grid`` is a field of the algorithm's ``AlgorithmConfig`` -- what a ``config_file``
would set for a single run -- or, for a nested field, a short name with an explicit ``path``.
"""

from ziplime.sweep.params.support import DEFAULT_STORAGE  # noqa: F401
from ziplime.sweep.params.support import DEFAULT_ASSET_DB  # noqa: F401
from ziplime.sweep.params.sweep_error import SweepError  # noqa: F401
from ziplime.sweep.params.strict import Strict  # noqa: F401
from ziplime.sweep.params.range import Range  # noqa: F401
from ziplime.sweep.params.commission import Commission  # noqa: F401
from ziplime.sweep.params.slippage import Slippage  # noqa: F401
from ziplime.sweep.params.config_path import ConfigPath  # noqa: F401
from ziplime.sweep.params.support import GridValue  # noqa: F401
from ziplime.sweep.params.support import Method  # noqa: F401
from ziplime.sweep.params.family import Family  # noqa: F401
from ziplime.sweep.params.synthetic import Synthetic  # noqa: F401
from ziplime.sweep.params.sweep_params import SweepParams  # noqa: F401
from ziplime.sweep.params.utils import config_paths  # noqa: F401
from ziplime.sweep.params.utils import config_for  # noqa: F401
from ziplime.sweep.params.utils import expand_grid  # noqa: F401


__all__ = [
    'DEFAULT_STORAGE',
    'DEFAULT_ASSET_DB',
    'SweepError',
    'Strict',
    'Range',
    'Commission',
    'Slippage',
    'ConfigPath',
    'GridValue',
    'Method',
    'Family',
    'Synthetic',
    'SweepParams',
    'config_paths',
    'config_for',
    'expand_grid',
]
