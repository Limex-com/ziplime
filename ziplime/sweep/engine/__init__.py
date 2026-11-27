"""The engine's daily loop, run for every combination at once.

Bars still go one at a time -- a decision at bar t depends on the portfolio after bar t-1 -- and the
parallelism comes from the combinations: every value is an array (combinations × instruments). Each
session does what TradingAlgorithm.transform does, in the same order:

    session start        dividends earned (ex date) and paid (pay date); splits
    before_trading_start the strategy's hook, reading rows before 08:45; no orders
    bar                  marks synced to the close; futures settled
                         same_close: handle_data, scheduled functions, then fills at this bar
                         next_*:     fills of earlier orders, then handle_data, scheduled functions
    session end          auto-close past the last bar / auto-close date; EODCancel; marks

Fills follow the blotter: an order fills only on a bar with a price and volume; triggers of limit
and stop orders are tested against that price and, once met, stay met; FixedBasisPointsSlippage
lets through at most its share of the bar's volume and the rest waits; commissions are charged per
fill with the order's own minimum. Like the blotter, the sweep keeps every order separately -- up to
ORDER_SLOTS open orders per instrument, walked oldest first against the bar's volume allowance; a
combination that needs more is flagged ``unsupported`` instead of silently diverging.
"""

from ziplime.sweep.engine.support import EVALUATE_EVERY  # noqa: F401
from ziplime.sweep.engine.support import ORDER_SLOTS  # noqa: F401
from ziplime.sweep.engine.support import SLOT_STEPS  # noqa: F401
from ziplime.sweep.engine.run_plain import RunPlain  # noqa: F401
from ziplime.sweep.engine.cost_utils import resolve_costs  # noqa: F401
from ziplime.sweep.engine.hooks import Hooks  # noqa: F401
from ziplime.sweep.engine.result import Result  # noqa: F401
from ziplime.sweep.engine.utils import run  # noqa: F401
from ziplime.sweep.engine.support import BYTES_PER_ELEMENT_BAR  # noqa: F401
from ziplime.sweep.engine.support import QUEUE_BUDGET  # noqa: F401
from ziplime.sweep.engine.support import BARS_PER_CALL  # noqa: F401
from ziplime.sweep.engine.result_utils import as_vector_result  # noqa: F401


__all__ = [
    'EVALUATE_EVERY',
    'ORDER_SLOTS',
    'SLOT_STEPS',
    'RunPlain',
    'resolve_costs',
    'Hooks',
    'Result',
    'run',
    'BYTES_PER_ELEMENT_BAR',
    'QUEUE_BUDGET',
    'BARS_PER_CALL',
    'as_vector_result',
]
