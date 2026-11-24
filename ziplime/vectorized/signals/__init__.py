"""Vectorised signals inside an event-driven run.

A strategy is rarely all one thing. The indicators are array work -- a moving average, a z-score,
a cross -- and recomputing them bar by bar is the single most expensive thing an event-driven
engine does: ``data.history(bar_count=200)`` on every one of a thousand sessions re-reads two
hundred rows and re-averages them, a thousand times, for an answer that one ``rolling(200).mean()``
produces in a single pass. But the part that decides *what to do* with the signal usually cannot
be vectorised at all: it reads the portfolio, respects a risk limit, rolls a contract, or sizes
against cash that depends on every fill before it.

So this module splits the strategy rather than the run. ``compute_signals`` is computed once,
over whole arrays, before the first bar. ``handle_data`` then reads one row per bar and orders
through the ordinary blotter, with the ordinary slippage and commissions. Nothing about execution
changes; only the arithmetic moves.

Handing a strategy the entire price history and asking it to be careful is how look-ahead bias
gets into a backtest, so two things here make that structural rather than advisory:

* :class:`SignalPanel` slices by the simulation clock. At bar ``t`` there is no accessor, method
  or attribute that returns row ``t + 1``. The future is not guarded, it is absent.
* :func:`verify_causality` re-runs the strategy's own computation on truncated history and
  compares. A rolling mean gives the same answer either way; a full-sample z-score, a
  ``shift(-1)`` or a division by ``iloc[-1]`` does not, and says so before the first order.

The second is sound but not complete: it compares at sampled cut points, so it catches a
computation whose leakage shows up at those rows -- which is every accidental leak in practice,
since full-sample normalisation and negative shifts move nearly every row -- and cannot promise to
catch one contrived to hide between them. It is a smoke alarm, not a proof.
"""

from ziplime.vectorized.signals.support import DEFAULT_CUTS  # noqa: F401
from ziplime.vectorized.signals.support import PANEL_FIELDS  # noqa: F401
from ziplime.vectorized.signals.look_ahead_error import LookAheadError  # noqa: F401
from ziplime.vectorized.signals.price_panel import PricePanel  # noqa: F401
from ziplime.vectorized.signals.utils import as_of_panel  # noqa: F401
from ziplime.vectorized.signals.signal_row import SignalRow  # noqa: F401
from ziplime.vectorized.signals.signal_panel import SignalPanel  # noqa: F401
from ziplime.vectorized.signals.utils import normalise_signals  # noqa: F401
from ziplime.vectorized.signals.utils import verify_causality  # noqa: F401


__all__ = [
    "DEFAULT_CUTS",
    "PANEL_FIELDS",
    "LookAheadError",
    "PricePanel",
    "as_of_panel",
    "SignalRow",
    "SignalPanel",
    "normalise_signals",
    "verify_causality",
]
