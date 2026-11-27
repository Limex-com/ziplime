"""The algorithm file, its config for each combination, and what its ``initialize`` declares.

The sweep runs the same :class:`AlgorithmFile` that run_simulation runs. ``initialize`` is called
with :class:`SweepContext` standing in for TradingAlgorithm: it can look instruments up, declare
indicators and set attributes on the context, which is everything ``initialize`` does before the
first bar.
"""

from ziplime.sweep.initialize.sweep_context import SweepContext  # noqa: F401
from ziplime.sweep.initialize.combinations import Combinations  # noqa: F401
from ziplime.sweep.initialize.utils import load_algorithm  # noqa: F401
from ziplime.sweep.initialize.initialized import Initialized  # noqa: F401
from ziplime.sweep.initialize.utils import run_initialize  # noqa: F401
from ziplime.sweep.initialize.symbol_utils import symbols_in  # noqa: F401
from ziplime.sweep.initialize.utils import initialize_each  # noqa: F401


__all__ = [
    'SweepContext',
    'Combinations',
    'load_algorithm',
    'Initialized',
    'run_initialize',
    'symbols_in',
    'initialize_each',
]
