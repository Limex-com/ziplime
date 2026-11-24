"""A strategy's own computations on price history, done on the host before the run.

A strategy often reads history as an array and computes on it with whatever it likes::

    p = (await data.history(assets=[asset], bar_count=250, fields=["close"]))["close"].to_numpy()
    rets = np.diff(p[-14:]) / p[-14:-1]
    if np.std(rets) * 100 <= config.limit: ...
    rsi = talib.RSI(p, timeperiod=config.period)[-1]

None of it runs on a GPU, and it does not have to: it reads history and numbers, not the
portfolio, so for every bar it is the same in every combination that agrees on the numbers. In a
sweep ``.to_numpy()`` gives a :class:`Lazy` -- a record of what is done to the rows: arithmetic,
indexing and slicing, NumPy's functions (all of them, ``np.maximum.accumulate`` included), other
libraries' functions (TA-Lib, SciPy...), methods. Nothing is computed while the strategy runs.

Where a number is needed -- a comparison, a branch, an order, a ranking -- the record becomes one:
the first time, the sweep notes it (and which distinct values of the config it reads) and starts
the run again; before the run it replays the record with the real rows and the real libraries at
every bar, once per distinct set of those values (:meth:`LazyRegistry.prepare`). At every bar the
step then reads the number like any other history window. The record has to end in a number:
an array the strategy only ever indexes, slices or reduces.

Values that differ between combinations -- a config field, a mask -- enter the record as
parameters: a slice ``p[-config.window:]`` or ``timeperiod=config.period`` is computed for each
distinct value and each combination reads its own. Which row is a combination's is found once a
batch, from what the first bar read; a compiled step only checks it (see
:meth:`LazyRegistry._compiled_value`). A record assigned under a condition that
differs between combinations becomes :class:`Select`: both records, chosen per combination once
they are numbers.
"""

import operator  # noqa: F401
from ziplime.sweep.lazy.support import where_in_strategy  # noqa: F401
from ziplime.sweep.lazy.values import Param  # noqa: F401
from ziplime.sweep.lazy.values import Leaf  # noqa: F401
from ziplime.sweep.lazy.lazy import Lazy  # noqa: F401
from ziplime.sweep.lazy.select import Select  # noqa: F401
from ziplime.sweep.lazy.select_utils import _distribute  # noqa: F401
from ziplime.sweep.lazy.select_utils import _arith  # noqa: F401
from ziplime.sweep.lazy.recording import is_lazy  # noqa: F401
from ziplime.sweep.lazy.recording import contains_lazy  # noqa: F401
from ziplime.sweep.lazy.recording import record  # noqa: F401
from ziplime.sweep.lazy.recording import _compare  # noqa: F401
from ziplime.sweep.lazy.support import CHANGING  # noqa: F401
from ziplime.sweep.lazy.recording import is_object_state  # noqa: F401
from ziplime.sweep.lazy.objects import Objects  # noqa: F401
from ziplime.sweep.lazy.recording import value_of  # noqa: F401
from ziplime.sweep.lazy.recording import leaf  # noqa: F401
from ziplime.sweep.lazy.replay_utils import replay  # noqa: F401
from ziplime.sweep.lazy.replay_utils import number  # noqa: F401
from ziplime.sweep.lazy.replay_utils import compile_plan  # noqa: F401
from ziplime.sweep.lazy.replay_utils import replay_series  # noqa: F401
from ziplime.sweep.lazy.series_utils import WINDOW_LOCAL  # noqa: F401
from ziplime.sweep.lazy.series_utils import ELEMENTWISE  # noqa: F401
from ziplime.sweep.lazy.series_utils import fast_series  # noqa: F401
from ziplime.sweep.lazy.support import PARALLEL_FROM  # noqa: F401
from ziplime.sweep.lazy.worker_utils import replay_workers  # noqa: F401
from ziplime.sweep.lazy.lazy_registry import LazyRegistry  # noqa: F401


__all__ = [
    'where_in_strategy',
    'Param',
    'Leaf',
    'Lazy',
    'is_lazy',
    'contains_lazy',
    'record',
    'CHANGING',
    'is_object_state',
    'Objects',
    'value_of',
    'leaf',
    'replay',
    'number',
    'compile_plan',
    'replay_series',
    'WINDOW_LOCAL',
    'ELEMENTWISE',
    'fast_series',
    'PARALLEL_FROM',
    'replay_workers',
    'LazyRegistry',
]


for _name in ("add", "sub", "mul", "truediv", "floordiv", "mod", "pow"):
    _forward, _reflected = _arith(_name)
    setattr(Select, f"__{_name}__", _forward)
    setattr(Select, f"__r{_name}__", _reflected)


for _name in ("lt", "le", "gt", "ge", "eq", "ne"):
    setattr(Select, f"__{_name}__",
            (lambda op: lambda self, other: _compare(op, self, other))(getattr(operator, _name)))


Select.__neg__ = lambda self: _distribute(operator.neg, self)


Select.__abs__ = lambda self: _distribute(operator.abs, self)
