"""Signals for every combination, each distinct one computed once.

Two sources, as in the engine: indicators declared with ``context.indicators(...)`` in initialize,
or the strategy's ``compute_signals``. Either way most combinations share most of their signals --
``rolling_mean(5)`` is the same array whatever ``slow`` is -- and the sweep computes each distinct
signal once and hands every combination an index into the distinct ones.
"""

from ziplime.sweep.signals.support import DECISION_NAMES  # noqa: F401
from ziplime.sweep.signals.compute_utils import compute  # noqa: F401
from ziplime.sweep.signals.compute_utils import check_causality  # noqa: F401
from ziplime.sweep.signals.compute_utils import signal_arrays  # noqa: F401
from ziplime.sweep.signals.signal_cache import SignalCache  # noqa: F401
from ziplime.sweep.signals.indicator_signals import IndicatorSignals  # noqa: F401
from ziplime.sweep.signals.compute_utils import check_indicators  # noqa: F401


__all__ = [
    'DECISION_NAMES',
    'compute',
    'check_causality',
    'signal_arrays',
    'SignalCache',
    'IndicatorSignals',
    'check_indicators',
]
