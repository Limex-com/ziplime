"""``data.history`` over several instruments: the engine's frame, as a converted strategy reads it.

The engine returns one long frame -- a row per instrument per bar, with ``date``, ``sid`` and the
fields asked for -- and a strategy reads it per instrument in one of two ways::

    frame = await data.history(assets=list(context.universe.values()), bar_count=20,
                               fields=["close"])
    stats = frame.group_by("sid").agg(pl.col("close").mean().alias("mean"))
    for row in stats.iter_rows(named=True):
        ... row["sid"], row["mean"] ...
    for name, asset in context.universe.items():
        ... frame.filter(pl.col("sid") == asset.sid)["close"].mean() ...

The engine takes the last ``bar_count`` rows of each sid, so each instrument's rows are the window
``data.history(assets=[asset], ...)`` reads, and both forms come down to the per-instrument
windows a sweep already computes: arrays (combinations × instruments). ``pl`` inside converted
code is :class:`PolarsShim`, which knows ``pl.col``; the rest of polars is refused before the
first bar.
"""

from ziplime.sweep.frames.support import AGGREGATIONS  # noqa: F401
from ziplime.sweep.frames.support import HINT  # noqa: F401
from ziplime.sweep.frames.unsupported import Unsupported  # noqa: F401
from ziplime.sweep.frames.polars_shim import PolarsShim  # noqa: F401
from ziplime.sweep.frames.col import Col  # noqa: F401
from ziplime.sweep.frames.statistic import Statistic  # noqa: F401
from ziplime.sweep.frames.pick import Pick  # noqa: F401
from ziplime.sweep.frames.history import History  # noqa: F401
from ziplime.sweep.frames.groups import Groups  # noqa: F401
from ziplime.sweep.frames.table import Table  # noqa: F401
from ziplime.sweep.frames.row import Row  # noqa: F401


__all__ = [
    'AGGREGATIONS',
    'HINT',
    'Unsupported',
    'PolarsShim',
    'Col',
    'Statistic',
    'Pick',
    'History',
    'Groups',
    'Table',
    'Row',
]
