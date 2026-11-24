"""Comparing instruments with each other inside a converted strategy.

A rotation picks the best few instruments by a score::

    scores = {}
    for name, asset in context.universe.items():
        scores[name] = context.signals["momentum"][name]
    best = sorted(scores, key=scores.get, reverse=True)[:config.top_k]
    for name, asset in context.universe.items():
        target = weight if name in best else 0.0

In a sweep the loop is one pass with ``name`` standing for every instrument, so ``scores`` gets one
entry, an array (combinations × instruments). :class:`SweepDict` is that dict: a plain dict for any
other key, and for the instruments an array of values with a mask of which are present. Sorting it
ranks the instruments within each combination; ``[:k]``, ``in``, ``len``, ``sum(d.values())`` and
``max(d, key=...)`` are then masks and numbers per combination -- what the same calls give in the
engine, where ``scores`` is an ordinary dict of floats.
"""

from ziplime.sweep.cross.support import PAIRWISE  # noqa: F401
from ziplime.sweep.cross.column_utils import column_index  # noqa: F401
from ziplime.sweep.cross.column_utils import column_mask  # noqa: F401
from ziplime.sweep.cross.sweep_dict import SweepDict  # noqa: F401
from ziplime.sweep.cross.cross_section import CrossSection  # noqa: F401
from ziplime.sweep.cross.ranking_value import Ranking  # noqa: F401
from ziplime.sweep.cross.selection import Selection  # noqa: F401
from ziplime.sweep.cross.choice import Choice  # noqa: F401
from ziplime.sweep.cross.ranking_utils import rank  # noqa: F401
from ziplime.sweep.cross.ranking_utils import ranking  # noqa: F401
from ziplime.sweep.cross.instrument_utils import ranks_instruments  # noqa: F401
from ziplime.sweep.cross.names import Names  # noqa: F401
from ziplime.sweep.cross.instrument_utils import cross_section_of  # noqa: F401
from ziplime.sweep.cross.ranking_utils import reduce_across  # noqa: F401
from ziplime.sweep.cross.instrument_utils import as_ranking  # noqa: F401
from ziplime.sweep.cross.instrument_utils import merge_instruments  # noqa: F401
from ziplime.sweep.cross.instrument_utils import sort_instruments  # noqa: F401


__all__ = [
    'PAIRWISE',
    'column_index',
    'column_mask',
    'SweepDict',
    'CrossSection',
    'Ranking',
    'Selection',
    'Choice',
    'rank',
    'ranking',
    'ranks_instruments',
    'Names',
    'cross_section_of',
    'reduce_across',
    'as_ranking',
    'merge_instruments',
    'sort_instruments',
]
