"""The market as the engine sees it, bar by bar, prepared once on the host in float64.

Everything the engine reads at a bar comes from the bundle's rows through two questions:

* the latest row at or before a moment (``get_spot_value``: ``date <= dt``, last row) -- what
  ``data.current``, the fill step, sizing and the portfolio marks read;
* the rows strictly before a moment (``get_data_by_limit(include_end_date=False)``) -- what
  ``data.history`` reads.

Answering them per instrument and per session once, here, is what lets the simulation run as
array code. A session with no row for an instrument therefore reads the previous row -- stale, as
in the engine -- and whether ``data.history`` includes the current bar depends, as in the engine,
on how the bundle stamps its bars: rows stamped at the session close end the window at the previous
bar, rows stamped at midnight include the current one.
"""

from ziplime.sweep.market.support import FIELDS  # noqa: F401
from ziplime.sweep.market.support import BEFORE_TRADING_START  # noqa: F401
from ziplime.sweep.market.dividend import Dividend  # noqa: F401
from ziplime.sweep.market.market import Market  # noqa: F401
from ziplime.sweep.market.rate_utils import parse_rate  # noqa: F401
from ziplime.sweep.market.session_utils import run_sessions  # noqa: F401
from ziplime.sweep.market.session_utils import bar_grid  # noqa: F401
from ziplime.sweep.market.utils import build_market  # noqa: F401
from ziplime.sweep.market.utils import corporate_actions  # noqa: F401
from ziplime.sweep.market.history_utils import asof_rows  # noqa: F401
from ziplime.sweep.market.support import WINDOW_OPS  # noqa: F401
from ziplime.sweep.market.support import DAILY_AGGREGATE  # noqa: F401
from ziplime.sweep.market.history_utils import daily_window_values  # noqa: F401
from ziplime.sweep.market.history_rows import HistoryRows  # noqa: F401
from ziplime.sweep.market.history_utils import window_values  # noqa: F401


__all__ = [
    'FIELDS',
    'BEFORE_TRADING_START',
    'Dividend',
    'Market',
    'parse_rate',
    'run_sessions',
    'bar_grid',
    'build_market',
    'corporate_actions',
    'asof_rows',
    'WINDOW_OPS',
    'DAILY_AGGREGATE',
    'daily_window_values',
    'HistoryRows',
    'window_values',
]
