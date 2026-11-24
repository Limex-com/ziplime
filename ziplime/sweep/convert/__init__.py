"""An ordinary ``handle_data``, run for every combination at once.

On a GPU every combination executes the same instructions, so Python that branches on a value --
``if fast > slow`` -- cannot run as written: the answer differs between combinations. :func:`convert`
reads the function's source and rewrites it mechanically, as TensorFlow's AutoGraph does:

    in the strategy                              after conversion
    -----------------------------------------    ------------------------------------------------
    for name, asset in context.universe...       one pass; name and asset stand for every instrument
    context.signals["fast"][name]                an array (combinations × instruments)
    if a > b: A  else: B                         both A and B run, each under its own mask
    x = ...  inside an if                        x changes only where the mask holds
    continue / return / return value             the mask switches the rest off for those lanes
    a and b, not a, x if c else y                elementwise and, not, where
    await context.order_target_percent(...)      an order only where the mask holds
    helper(x), a function of the same module     converted the same way

A value that is the same for every combination (a counter, ``len(context.universe)``) stays a
plain Python value and an ``if`` on it is an ordinary ``if``. What cannot be converted -- a loop over
instruments where one instrument's decision depends on the previous one, ``break``/``while`` on a
value that differs between combinations, ``try`` -- is refused with the line and the reason.
"""

from ziplime.sweep.convert.runtime import Runtime  # noqa: F401
from ziplime.sweep.convert.support import NUMPY_ELEMENTWISE  # noqa: F401
from ziplime.sweep.convert.support import NUMPY_REDUCTIONS  # noqa: F401
from ziplime.sweep.convert.support import NUMPY_CONSTANTS  # noqa: F401
from ziplime.sweep.convert.support import NUMPY_SUPPORTED  # noqa: F401
from ziplime.sweep.convert.foreign_utils import foreign  # noqa: F401
from ziplime.sweep.convert.support import SKIP  # noqa: F401
from ziplime.sweep.convert.support import RESULT  # noqa: F401
from ziplime.sweep.convert.support import RT  # noqa: F401
from ziplime.sweep.convert.support import M  # noqa: F401
from ziplime.sweep.convert.support import DEAD  # noqa: F401
from ziplime.sweep.convert.support import HOOKS  # noqa: F401
from ziplime.sweep.convert.foreign_utils import user_module  # noqa: F401
from ziplime.sweep.convert.converted import Converted  # noqa: F401
from ziplime.sweep.convert.conversion import convert  # noqa: F401
from ziplime.sweep.convert.support import MUTATING  # noqa: F401
from ziplime.sweep.convert.source_utils import context_writes  # noqa: F401


__all__ = [
    'Runtime',
    'NUMPY_ELEMENTWISE',
    'NUMPY_REDUCTIONS',
    'NUMPY_CONSTANTS',
    'NUMPY_SUPPORTED',
    'foreign',
    'SKIP',
    'RESULT',
    'RT',
    'M',
    'DEAD',
    'HOOKS',
    'user_module',
    'Converted',
    'convert',
    'MUTATING',
    'context_writes',
]
