"""Series utils for lazy."""

import operator
import numpy as np
from ziplime.sweep.market import Market
from ziplime.sweep.lazy.method import _Method
from ziplime.sweep.lazy.not_fast import _NotFast


WINDOW_LOCAL = {"ROC", "ROCP", "ROCR", "ROCR100", "MOM", "SMA", "WMA", "TRIMA", "SUM", "MAX", "MIN",
                "MIDPOINT", "STDDEV", "VAR", "LINEARREG", "LINEARREG_SLOPE",
                "LINEARREG_INTERCEPT", "LINEARREG_ANGLE", "TSF",
                # several inputs of one instrument's window (high, low, close...)
                "WILLR", "CCI", "AROONOSC", "MIDPRICE", "TRANGE", "MEDPRICE", "TYPPRICE",
                "WCLPRICE", "AVGPRICE", "CORREL", "BETA", "MFI", "ULTOSC",
                # several outputs: [j][-1]
                "AROON", "MINMAX", "BBANDS", "STOCHF", "STOCH"}


def _constant(key, params):
    if key[0] == "value":
        return key[2]
    if key[0] == "param":
        return params[key[1]]
    raise _NotFast()


def _window_of(key, params):
    """``rows.to_numpy()`` or ``rows.to_numpy()[-k:]``: (leaf key, k or None)."""
    k = None
    if key[0] == "call" and key[1] is operator.getitem and len(key[2]) == 2 \
            and key[2][1][0] == "slice":
        start, stop, step = key[2][1][1:]
        start = _constant(start, params)
        if _constant(stop, params) is not None or _constant(step, params) is not None or \
                not isinstance(start, int) or start >= 0:
            raise _NotFast()
        key, k = key[2][0], -start
    if not (key[0] == "call" and isinstance(key[1], _Method) and key[1].name == "to_numpy"
            and len(key[2]) == 1 and not key[3] and key[2][0][0] == "leaf"):
        raise _NotFast()
    leaf = key[2][0]
    if leaf[5]:  # days built from intraday rows: a window of its own, not a slice of one series
        raise _NotFast()
    return leaf, k


ELEMENTWISE = {operator.add, operator.sub, operator.mul, operator.truediv, operator.pow,
               operator.neg, operator.abs}


def fast_series(key, params, market: Market, columns: list[int]) -> np.ndarray | None:
    """A record at every bar without replaying it bar by bar, if it is made of
    ``talib.X(history[-k:], ...)[-1]`` for functions in WINDOW_LOCAL -- each run once over each
    instrument's whole history and read at the bar's last row, the same number as the window's --
    numbers and arithmetic. None for any other record, or for history with gaps of NaN (the
    function would carry them along the series, not within one window)."""
    try:
        with np.errstate(all="ignore"):
            result = _series(key, params, market, columns)
    except (_NotFast, IndexError, TypeError):
        return None
    if not isinstance(result, np.ndarray):
        return None
    return np.broadcast_to(result, (market.n_bars, len(columns))).astype(np.float64)


def _series(key, params, market: Market, columns: list[int]):
    if key[0] in ("value", "param"):
        value = _constant(key, params)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise _NotFast()
        return value
    if key[0] != "call" or key[3]:
        raise _NotFast()
    if key[1] is operator.getitem and len(key[2]) == 2 and key[2][1][0] in ("value", "param"):
        try:
            return _row_of_window(key, params, market, columns)
        except _NotFast:
            return _last_of_window_local(key, params, market, columns)
    if key[1] in ELEMENTWISE:
        return key[1](*[_series(arg, params, market, columns) for arg in key[2]])
    raise _NotFast()


def _row_of_window(key, params, market: Market, columns: list[int]) -> np.ndarray:
    """``history[-k:][j]``: one row of the window -- the window's first (``[0]``) or last
    (``[-1]``) price -- at every bar, by index; NaN where the window has no such row (an
    IndexError at that bar)."""
    position = _constant(key[2][1], params)
    if isinstance(position, bool) or not isinstance(position, int):
        raise _NotFast()
    leaf, k = _window_of(key[2][0], params)
    _, where, field, length, before, _ = leaf
    length = int(_constant(length, params))
    ends_all = market.history_end_before if before else market.history_end
    result = np.full((market.n_bars, len(columns)), np.nan)
    for j, column in enumerate(columns):
        column = column if where == "all" else where
        ends = ends_all[:, column].astype(np.int64)
        width = np.minimum(ends, length if k is None else min(length, k))
        row = ends - width + position if position >= 0 else ends + position
        valid = (position < width) if position >= 0 else (-position <= width)
        values = market.rows[field][:, column]
        result[:, j] = np.where(valid, values[np.clip(row, 0, len(values) - 1)], np.nan)
    return result


def _last_of_window_local(key, params, market: Market, columns: list[int]) -> np.ndarray:
    """``talib.X(window, ...)[-1]``, or ``talib.X(window...)[j][-1]`` for its j-th output, with
    one or more windows of one instrument's history (high, low, close)."""
    if _constant(key[2][1], params) != -1:
        raise _NotFast()
    inner, output = key[2][0], None
    if inner[0] == "call" and inner[1] is operator.getitem and len(inner[2]) == 2 \
            and inner[2][1][0] in ("value", "param"):
        output = _constant(inner[2][1], params)
        if isinstance(output, bool) or not isinstance(output, int):
            raise _NotFast()
        inner = inner[2][0]
    if inner[0] != "call" or isinstance(inner[1], (tuple, _Method)) or not inner[2]:
        raise _NotFast()
    function = inner[1]
    if not (getattr(function, "__module__", "") or "").startswith("talib") or \
            getattr(function, "__name__", None) not in WINDOW_LOCAL:
        raise _NotFast()
    windows = []
    for arg in inner[2]:
        try:
            windows.append(_window_of(arg, params))
        except _NotFast:
            break
    if not windows:
        raise _NotFast()
    shape = {(leaf[1], _constant(leaf[3], params), leaf[4], k) for leaf, k in windows}
    if len(shape) != 1:
        raise _NotFast()  # windows of different instruments or lengths
    fields = [leaf[2] for leaf, _ in windows]
    (leaf, k) = windows[0]
    rest = [_constant(arg, params) for arg in inner[2][len(windows):]]
    options = {name: _constant(arg, params) for name, arg in inner[3]}
    _, where, _, length, before, _ = leaf
    length = int(_constant(length, params))
    import talib.abstract

    described = talib.abstract.Function(function.__name__)
    names = list(described.parameters)
    described.set_parameters(**dict(zip(names, rest)), **options)
    if any(name.endswith("matype") and value != 0 for name, value in described.parameters.items()):
        raise _NotFast()  # an exponential or other smoothed average remembers where it began
    lookback = described.lookback
    ends_all = market.history_end_before if before else market.history_end
    result = np.full((market.n_bars, len(columns)), np.nan)
    for j, column in enumerate(columns):
        column = column if where == "all" else where
        ends = ends_all[:, column].astype(np.int64)
        count = int(ends.max()) if len(ends) else 0
        if count == 0:
            continue
        inputs = []
        for field in fields:
            if field not in market.rows:
                raise _NotFast()
            series = np.ascontiguousarray(market.rows[field][:count, column], dtype=np.float64)
            if np.isnan(series).any():
                raise _NotFast()
            inputs.append(series)
        try:
            out = function(*inputs, *rest, **options)
        except Exception:
            raise _NotFast() from None
        if isinstance(out, tuple) != (output is not None):
            raise _NotFast()
        if output is not None:
            out = out[output]
        width = np.minimum(ends, length if k is None else min(length, k))
        valid = (ends > 0) & (width > lookback)
        result[:, j] = np.where(valid, out[np.maximum(ends - 1, 0)], np.nan)
    return result
