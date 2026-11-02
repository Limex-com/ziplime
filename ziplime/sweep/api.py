"""``context``, ``data`` and ``context.portfolio`` as a converted strategy sees them.

The same methods as TradingAlgorithm and BarData, computed for every combination at once: a value
is an array (combinations × instruments), or (combinations × 1) / (1 × instruments) where it does
not vary along an axis. Order sizing follows the engine's formulas line for line -- see the
methods below for which line of TradingAlgorithm each mirrors.
"""
import datetime
import types

import numpy as np

from ziplime.sweep.convert import ALL, NotConvertible, VectorUniverse, Vectorized, is_array
from ziplime.sweep.params import SweepError


class Columns:
    """Which column an instrument is."""

    def __init__(self, universe: dict, backend):
        self.names = list(universe)
        self.by_name = {name: k for k, name in enumerate(self.names)}
        self.by_asset = {listing: k for k, listing in enumerate(universe.values())}
        self.by_sid = {getattr(listing, "sid", None): k for k, listing in enumerate(universe.values())}
        self.backend = backend
        # Comparisons with the loop variable ("name == 'JNJ'") read the universe from here
        type(ALL)._columns = (self.names, list(universe.values()),
                              lambda values: backend.xp.array(values))

    def index(self, asset) -> int:
        if asset in self.by_asset:
            return self.by_asset[asset]
        if isinstance(asset, str) and asset in self.by_name:
            return self.by_name[asset]
        if isinstance(asset, int) and asset in self.by_sid:
            return self.by_sid[asset]
        raise SweepError(f"{asset!r} is not in context.universe: the sweep simulates the "
                         f"universe's instruments only.")

    def select(self, values, asset):
        if asset is ALL:
            return values
        column = self.index(asset)
        return values[:, column:column + 1]

    def only(self, asset):
        """(1 × instruments): True at this instrument."""
        if asset is ALL:
            return True
        mask = np.zeros((1, len(self.names)), dtype=bool)
        mask[0, self.index(asset)] = True
        return self.backend.xp.array(mask)

    def assets(self, assets) -> list:
        if assets is ALL or isinstance(assets, VectorUniverse):
            return [ALL]
        if isinstance(assets, dict):
            assets = list(assets.values())
        if not isinstance(assets, (list, tuple, set, frozenset)):
            return [assets]
        return list(assets)


# -- per-instrument state on the context -----------------------------------------------------------

def asset_map(value, columns: "Columns") -> bool:
    """A dict a sweep can hold as arrays: keyed by the universe's instruments (names, listings or
    sids), with numbers for values."""
    if not isinstance(value, dict):
        return False
    for key, item in value.items():
        try:
            columns.index(key)
        except SweepError:
            return False
        if not isinstance(item, (int, float, bool, np.number)):
            return False
    return True


class AssetState:
    """``context.entry = {}`` -- a dict keyed by instrument -- for every combination: the values and
    which keys are present, each an array (combinations × instruments).

    Writes, deletions and ``in`` act where the statement's mask holds, as an ``if`` in the strategy
    would decide them; a loop over its items runs for the instruments it holds.
    """

    def __init__(self, initial: dict, columns: Columns, context):
        self._columns, self._context = columns, context
        xp = columns.backend.xp
        values = np.full((1, len(columns.names)), np.nan)
        present = np.zeros((1, len(columns.names)), dtype=bool)
        for key, item in initial.items():
            values[0, columns.index(key)] = float(item)
            present[0, columns.index(key)] = True
        self._v, self._p = columns.backend.array(values), xp.array(present)

    @property
    def _xp(self):
        return self._columns.backend.xp

    def _where(self, key):
        runtime = self._context._runtime
        mask = runtime.mask if runtime is not None else True
        if runtime is None:
            return self._columns.only(key)
        return runtime.and_(mask, lambda: self._columns.only(key))

    # what the compiled step carries
    def dump(self, like):
        return self._v + like * 0, self._xp.logical_or(self._p, like != like)

    def load(self, values, present):
        self._v, self._p = values, present

    # the dict's behaviour
    def __getitem__(self, key):
        return self._columns.select(self._v, key)

    def __setitem__(self, key, value):
        where = self._where(key)
        xp = self._xp
        self._v = xp.where(where, value, self._v) if where is not True \
            else value + xp.zeros(self._v.shape)
        self._p = xp.logical_or(self._p, where) if where is not True else \
            xp.logical_or(self._p, True)

    def get(self, key, default=None):
        runtime = self._context._runtime
        fallback = (runtime.nan if runtime is not None else np.nan) if default is None else default
        return self._xp.where(self._columns.select(self._p, key),
                              self._columns.select(self._v, key), fallback)

    def sweep_delete(self, key):
        where = self._where(key)
        self._p = self._xp.logical_and(self._p, self._xp.logical_not(where)) \
            if where is not True else self._p & False

    __delitem__ = sweep_delete

    def pop(self, key, default=None):
        value = self.get(key, default)
        self.sweep_delete(key)
        return value

    def clear(self):
        self.sweep_delete(ALL)

    def sweep_contains(self, key):
        return self._columns.select(self._p, key)

    def sweep_len(self):
        return self._xp.sum(self._p, axis=1, keepdims=True)

    def items(self):
        return Vectorized([(ALL, self._v)], mask=self._p)

    def keys(self):
        return Vectorized([ALL], mask=self._p)

    def values(self):
        return Vectorized([self._v], mask=self._p)

    def __iter__(self):
        return iter(self.keys())

    def __len__(self):
        raise NotConvertible("len(context.dict) differs between combinations; it is supported in "
                             "converted code, where it is a number per combination.")


# -- signals ------------------------------------------------------------------------------------

class ByName:
    """``signals["fast"]``: ``[name]`` -- that instrument's column, ``[every instrument]`` -- all."""

    def __init__(self, values, columns: Columns):
        self._values, self._columns = values, columns

    def __getitem__(self, name):
        if name is ALL:
            return self._values
        try:
            column = self._columns.by_name[name]
        except KeyError:
            raise KeyError(f"No instrument {name!r}; there are {self._columns.names}.") from None
        return self._values[:, column:column + 1]


class Signals:
    def __init__(self, backend, columns: Columns):
        self._backend, self._columns, self.now = backend, columns, {}

    def _raw(self, name):
        try:
            return self.now[name]
        except KeyError:
            raise KeyError(f"No signal {name!r}. There are: {sorted(self.now)}.") from None

    def __getitem__(self, name):
        return ByName(self._raw(name), self._columns)

    def is_ready(self, name):
        """(combinations × 1): the signal has a number for some instrument at this bar."""
        xp, row = self._backend.xp, self._raw(name)
        if row.dtype == self._backend.bool:
            return True
        return ~xp.all(xp.isnan(row), axis=1, keepdims=True)


# -- data ---------------------------------------------------------------------------------------

class Column:
    """One field of ``data.current(...)`` / ``data.history(...)``: ``[k]`` is the k-th asset asked
    for (``data.current(assets=[a, b])["close"][1]``)."""

    def __init__(self, values: list):
        self._values = values

    def __getitem__(self, k):
        if isinstance(k, int):
            return self._values[k]
        raise NotConvertible("Index a data.current column by position: frame['close'][0].")

    def __len__(self):
        return len(self._values)


class Frame:
    def __init__(self, columns: dict):
        self._columns = columns

    def __getitem__(self, field):
        try:
            return self._columns[field]
        except KeyError:
            raise KeyError(f"Field {field!r} was not asked for; asked for {list(self._columns)}.") from None


class WindowColumn:
    """One field of ``data.history(assets=[asset], bar_count=n, ...)``: its statistics."""

    def __init__(self, data, asset, field, length, frequency=None):
        self._data, self._asset, self._field, self._length = data, asset, field, length
        self._frequency = frequency

    def _op(self, op):
        return self._data._window(self._asset, self._field, op, self._length, self._frequency)

    def mean(self):
        return self._op("mean")

    def sum(self):
        return self._op("sum")

    def std(self, ddof: int = 1):
        if ddof != 1:
            raise NotConvertible("history(...).std() is supported with ddof=1 only.")
        return self._op("std")

    def var(self, ddof: int = 1):
        if ddof != 1:
            raise NotConvertible("history(...).var() is supported with ddof=1 only.")
        return self._op("var")

    def min(self):
        return self._op("min")

    def max(self):
        return self._op("max")

    def median(self):
        return self._op("median")

    def first(self):
        return self._op("first")

    def last(self):
        return self._op("last")

    def count(self):
        return self._op("count")

    def len(self):
        return self._op("count")

    def __len__(self):
        raise NotConvertible("len(window) is a different number per combination; use "
                             "window['close'].count().")

    def sweep_len(self):
        from ziplime.sweep.lazy import record
        return record(len, (self._lazy(),))

    def __getitem__(self, k):
        if isinstance(k, int) and not isinstance(k, bool) and k == 0:
            return self._op("first")
        if isinstance(k, int) and not isinstance(k, bool) and k == -1:
            return self._op("last")
        return self._lazy()[k]

    def _lazy(self):
        """The rows themselves, for the strategy's own computations on them (lazy.py)."""
        from ziplime.sweep.lazy import leaf

        data = self._data
        column = ALL if self._asset is ALL else data._columns.index(self._asset)
        return leaf(data, column, self._field, self._length, data.before,
                    data._daily(self._frequency))

    def __getattr__(self, name):
        # Anything else of the engine's polars Series -- to_numpy(), pct_change(), rolling_mean()
        if name.startswith("__") or name.startswith("sweep_") or name in ("shape", "dtype", "mask"):
            raise AttributeError(name)
        return getattr(self._lazy(), name)


class Window:
    def __init__(self, data, asset, length, fields, frequency=None):
        self._data, self._asset, self._length, self._fields = data, asset, length, fields
        self._frequency = frequency

    def __getitem__(self, field):
        if self._fields is not None and field not in self._fields:
            raise KeyError(f"Field {field!r} was not asked for.")
        return WindowColumn(self._data, self._asset, field, self._length, self._frequency)

    def filter(self, *args, **kwargs):
        raise NotConvertible("A history frame of one instrument is not filtered in a sweep; read "
                             "its statistics: window['close'].mean().")


class Data:
    """``data``: current values, history windows and can_trade, for the bar being simulated."""

    def __init__(self, columns: Columns, backend, windows):
        self._columns, self._backend, self._windows = columns, backend, windows
        #: {field: (1 × instruments)} at this bar; set by the simulation
        self.now = {}
        #: before_trading_start reads earlier rows
        self.before = False
        self.dt = None
        self.stopped = None

    @property
    def current_dt(self):
        return self.dt

    def current(self, assets=None, fields=None, data_source=None, **kwargs):
        if fields is None:
            raise NotConvertible("data.current needs fields=[...].")
        if data_source is not None:
            raise NotConvertible("data.current(data_source=...) is not supported in a sweep.")
        fields = [fields] if isinstance(fields, str) else list(fields)
        assets = self._columns.assets(assets)
        source = self.now["spot_before" if self.before else "spot"]
        missing = [field for field in fields if field not in source]
        if missing:
            raise SweepError(f"The bundle has no {missing} column.")
        return Frame({field: Column([self._columns.select(source[field], asset) for asset in assets])
                      for field in fields})

    def history(self, assets=None, bar_count=None, frequency=None, fields=None, data_source=None,
                since=None, **kwargs):
        if since is not None or data_source is not None:
            raise NotConvertible("data.history(since=...) and data_source are not supported in a "
                                 "sweep; use bar_count.")
        if bar_count is None:
            raise NotConvertible("data.history needs bar_count in a sweep.")
        from ziplime.sweep.frames import History

        # The universe itself (context.universe, its values(), list(...) of them) outside a loop
        # over it is every instrument in one frame; [asset] inside the loop is one per pass.
        everything = isinstance(assets, (VectorUniverse, Vectorized))
        if isinstance(assets, Vectorized) and assets.mask is not True:
            raise NotConvertible("data.history over some of the instruments chosen per "
                                 "combination is not supported in a sweep; ask for the universe.")
        chosen = self._columns.assets(assets)
        fields = None if fields is None else ([fields] if isinstance(fields, str) else list(fields))
        if everything or len(chosen) > 1:
            if not everything and any(asset is ALL for asset in chosen):
                raise NotConvertible("data.history over the loop's instrument and others at once "
                                     "is not supported in a sweep.")
            return History(self, [ALL] if everything else chosen, bar_count, fields, frequency)
        return Window(self, chosen[0], bar_count, fields, frequency)

    def _daily(self, frequency) -> bool:
        """Whether history reads days built from the run's rows (an intraday run asking for days,
        which is history's default) or the rows themselves."""
        from ziplime.sweep.market import parse_rate

        market = self._windows.market
        asked = parse_rate(frequency)
        if not market.intraday:
            if asked != datetime.timedelta(days=1):
                raise NotConvertible("A daily run's history is read in days.")
            return False
        if asked == datetime.timedelta(days=1):
            return True
        if asked == market.rate:
            return False
        raise NotConvertible(f"history(frequency={frequency!r}) on a {market.rate} run: a sweep "
                             f"reads the run's own bars or days.")

    def _window(self, asset, field, op, length, frequency=None):
        return self._columns.select(
            self._windows.value(field, op, length, self.before, self._daily(frequency)), asset)

    def returns(self, assets=None, bar_count=None, field="close", data_source=None):
        """BarData.returns: ``{instrument: last / first - 1}`` over its last ``bar_count`` rows,
        absent where it has fewer than two or a non-positive first price."""
        from ziplime.sweep.cross import SweepDict

        if data_source is not None:
            raise NotConvertible("data.returns(data_source=...) is not supported in a sweep.")
        if bar_count is None:
            raise NotConvertible("data.returns needs bar_count.")
        assets = self._columns.assets(assets)
        xp = self._backend.xp
        daily = self._daily(None)
        first = self._windows.value(field, "first", bar_count, self.before, daily)
        last = self._windows.value(field, "last", bar_count, self.before, daily)
        count = self._windows.value(field, "count", bar_count, self.before, daily)
        result = SweepDict(self._runtime)
        result._values = last / first - 1.0
        present = xp.logical_and(count >= 2, first > 0)
        if not assets == [ALL]:
            chosen = None
            for asset in assets:
                here = self._columns.only(asset)
                chosen = here if chosen is None else xp.logical_or(chosen, here)
            present = xp.logical_and(present, chosen)
        result._present = present
        return result

    def can_trade(self, assets=None):
        assets = self._columns.assets(assets)
        tradable = self._backend.xp.logical_not(self.stopped)
        if len(assets) == 1:
            return self._columns.select(tradable, assets[0])
        return [self._columns.select(tradable, asset) for asset in assets]


# -- portfolio ----------------------------------------------------------------------------------

class Portfolio:
    """The ledger's portfolio snapshot at this bar."""

    def __init__(self, columns: Columns):
        self._columns = columns

    def update(self, xp, cash, held, marks, multiplier, is_future):
        self.cash = cash
        self.held = held
        exposure = held * marks * multiplier
        self.positions_exposure = xp.sum(exposure, axis=1, keepdims=True)
        # Futures are not part of the portfolio's value: their gains arrive as cash
        self.positions_value = xp.sum(xp.where(is_future, 0.0, held * marks), axis=1, keepdims=True)
        self.portfolio_value = cash + self.positions_value

    def get_asset_positions_amount(self, asset, *args, **kwargs):
        return self._columns.select(self.held, asset)

    @property
    def positions(self):
        return Positions(self)


class Position:
    """``portfolio.positions.get(asset)``: the position's amount, for every combination."""

    def __init__(self, amount):
        self.amount = amount

    def sweep_truth(self):
        """The engine keeps only open positions: a missing one is None, an open one is truthy."""
        return self.amount != 0

    def __getattr__(self, name):
        if name.startswith("__") or name.startswith("sweep_") or name in ("shape", "dtype"):
            raise AttributeError(name)
        raise NotConvertible(f"position.{name} is not simulated in a sweep; position.amount is.")


class Positions:
    """``context.portfolio.positions`` looked up by instrument, as the engine's map allows: only
    open positions are in it."""

    def __init__(self, portfolio: Portfolio):
        self._portfolio = portfolio

    def _amount(self, asset):
        if isinstance(asset, tuple):
            raise NotConvertible("portfolio.positions[(exchange, account, asset)] is not "
                                 "supported in a sweep; look the position up by the asset.")
        return self._portfolio.get_asset_positions_amount(asset)

    def get(self, asset, default=None):
        # A position that is not open has amount 0, as the default's .amount would be read
        return Position(self._amount(asset))

    def __getitem__(self, asset):
        return Position(self._amount(asset))

    def sweep_contains(self, asset):
        return self._amount(asset) != 0

    def sweep_len(self):
        xp = self._portfolio._columns.backend.xp
        return xp.sum(self._portfolio.held != 0, axis=1, keepdims=True)

    def sweep_truth(self):
        return self.sweep_len() > 0

    def _refuse(self, *args, **kwargs):
        raise NotConvertible("Iterating portfolio.positions is not supported in a sweep (the "
                             "engine's keys are (exchange, account, asset)); iterate "
                             "context.universe and read positions.get(asset).amount.")

    __iter__ = keys = values = items = _refuse


# -- execution styles ---------------------------------------------------------------------------

class Style:
    """MarketOrder / LimitOrder / StopOrder / StopLimitOrder whose prices may be arrays."""

    def __init__(self, limit_price=None, stop_price=None):
        self.limit_price, self.stop_price = limit_price, stop_price


def MarketOrder(*args, **kwargs):
    return Style()


def LimitOrder(limit_price, *args, **kwargs):
    return Style(limit_price=limit_price)


def StopOrder(stop_price, *args, **kwargs):
    return Style(stop_price=stop_price)


def StopLimitOrder(limit_price, stop_price, *args, **kwargs):
    return Style(limit_price=limit_price, stop_price=stop_price)


STYLES = {"MarketOrder": MarketOrder, "LimitOrder": LimitOrder, "StopOrder": StopOrder,
          "StopLimitOrder": StopLimitOrder}


def _consistent_round(xp, value):
    """Half away from zero, as ziplime's consistent_round."""
    return xp.sign(value) * xp.floor(abs(value) + 0.5)


def asymmetric_round(xp, price, prefer_round_down, tick: float = 0.01):
    """finance.execution.asymmetric_round_price for arrays: buy limits round down, sell limits up,
    stops the other way."""
    diff = (0.95 - 0.5) * tick - np.finfo(float).eps * 10
    shifted = xp.where(prefer_round_down, price - diff, price + diff)
    rounded = tick * _consistent_round(xp, shifted / tick)
    return xp.where(abs(rounded) < 1e-12, 0.0, rounded)


# -- the context ----------------------------------------------------------------------------------

class Context:
    """TradingAlgorithm for every combination: what a converted strategy calls on ``context``."""

    def __init__(self, *, config, signals: Signals, portfolio: Portfolio, data: Data, backend,
                 universe: dict, columns: Columns, costs, state: dict):
        self.algorithm = types.SimpleNamespace(config=config)
        self.signals, self.portfolio, self.universe = signals, portfolio, VectorUniverse(universe)
        self._data, self._backend, self._columns, self._costs = data, backend, columns, costs
        self.xp = backend.xp
        self._runtime = None
        self._in_before_trading_start = False
        self.reset_orders()
        for name, value in state.items():
            self.set_state(name, value)

    def set_state(self, name, value):
        """What initialize put on the context; a dict keyed by instrument becomes an AssetState."""
        if asset_map(value, self._columns):
            value = AssetState(value, self._columns, self)
        setattr(self, name, value)

    # What the simulation reads back after the strategy's hooks ran
    def reset_orders(self):
        #: This bar's orders, in the order they were placed: (amount, limit, stop)
        self.new_orders = []

    # -- placing an order
    def _place(self, asset, amount, style=None, limit_price=None, stop_price=None):
        if self._in_before_trading_start:
            raise SweepError("Orders are not allowed in before_trading_start (the engine raises "
                             "OrderInBeforeTradingStart).")
        xp, runtime = self.xp, self._runtime
        if style is not None and not isinstance(style, Style):
            raise NotConvertible(f"Order style {type(style).__name__} is not supported in a sweep.")
        limit = limit_price if limit_price is not None else (style.limit_price if style else None)
        stop = stop_price if stop_price is not None else (style.stop_price if style else None)
        # int(round_if_near_integer(amount)): within 1e-4 of an integer rounds, else truncates
        nearest = xp.round(amount)
        amount = xp.where(abs(amount - nearest) <= 1e-4, nearest, amount)
        amount = xp.sign(amount) * xp.floor(abs(amount))
        amount = xp.where(xp.isnan(amount), 0.0, amount)
        # _can_order_asset: nothing past the instrument's last bar or auto-close date
        when = runtime.and_(runtime.mask, lambda: self._columns.only(asset))
        when = runtime.and_(when, lambda: xp.logical_not(self._data.stopped))
        amount = amount * xp.ones_like(self.portfolio.held) if when is True else \
            xp.where(when, amount, 0.0)
        is_buy = amount > 0
        zero = xp.zeros_like(amount)
        nan = zero + self._data.now["nan"]
        limit = nan if limit is None else asymmetric_round(xp, limit + zero, is_buy)
        stop = nan if stop is None else asymmetric_round(xp, stop + zero, xp.logical_not(is_buy))
        self.new_orders.append((amount, limit, stop))

    def _value_amount(self, value):
        """_calculate_order_value_amount: value / (price × multiplier); a zero price orders none."""
        price = self._data.now["sizing_price"]
        shares = value / (price * self._data.now["multiplier"])
        return self.xp.where(price == 0, 0.0, shares)

    def _percent_amount(self, percent):
        """_calculate_order_percent_amount, line by line."""
        xp, costs = self.xp, self._costs
        value = self.portfolio.portfolio_value * percent
        requested = self._value_amount(value)
        projected = costs.projected(xp, requested, value)
        new_quantity = self._value_amount(value - projected)
        estimated = costs.maximum_quantity(xp, value - projected, self._data.now)
        return xp.minimum(estimated, new_quantity)

    # -- the order API
    def order(self, asset, amount, style=None, limit_price=None, stop_price=None, **kwargs):
        self._place(asset, amount + self.xp.zeros_like(self.portfolio.held), style,
                    limit_price, stop_price)

    def order_value(self, asset, value, style=None, limit_price=None, stop_price=None, **kwargs):
        self._place(asset, self._value_amount(value), style, limit_price, stop_price)

    def order_percent(self, asset, percent, style=None, **kwargs):
        self._costs.require_percent_sizing()
        self._place(asset, self._percent_amount(percent), style)

    def order_target(self, asset, target, style=None, **kwargs):
        self._place(asset, target - self.portfolio.held, style)

    def order_target_value(self, asset, target, style=None, **kwargs):
        self._place(asset, self._value_amount(target) - self.portfolio.held, style)

    def order_target_percent(self, asset, target, style=None, reserved_percentage_for_fees=None,
                             **kwargs):
        self._costs.require_percent_sizing()
        self._place(asset, self._percent_amount(target) - self.portfolio.held, style)

    def rebalance(self, weights, style=None, exchange_name=None):
        """TradingAlgorithm.rebalance: every named instrument to its weight (order_target_percent),
        a zero weight and every held instrument not named closed by an order for minus the
        position; an instrument that cannot trade is left alone."""
        from ziplime.sweep.cross import SweepDict

        xp, held = self.xp, self.portfolio.held
        if isinstance(weights, SweepDict) and weights.arrays:
            weight, named = weights._values + held * 0, xp.logical_or(weights._present, held != held)
        else:
            weight, named = held * 0, held != held
            for key, value in dict(weights or {}).items():
                here = self._columns.only(key)
                weight = xp.where(here, value, weight)
                named = xp.logical_or(named, here)
        target = self._percent_amount(weight) - held
        close = -held
        amount = xp.where(xp.logical_and(named, weight != 0), target,
                          xp.where(held != 0, close, 0.0))
        self._costs.require_percent_sizing()
        self._place(ALL, amount, style)

    def get_open_orders(self, asset=None, *args, **kwargs):
        pending = self._data.now["pending"] != 0
        if asset is None:
            return self.xp.any(pending, axis=1, keepdims=True)
        return self._columns.select(pending, asset)

    def get_datetime(self, *args, **kwargs):
        return self._data.dt

    def symbol(self, symbol, mic=None, asset_type=None, country_code=None, **kwargs):
        """An instrument of the universe by its ticker: handle_data may look instruments up."""
        if "@" in symbol:
            symbol, mic = symbol.split("@")
        for listing in dict.values(self.universe):
            if getattr(listing, "symbol", None) == symbol and \
                    (mic is None or getattr(listing, "mic", None) == mic):
                return listing
        raise SweepError(f"context.symbol({symbol!r}) asks for an instrument outside "
                         f"context.universe; a sweep simulates the universe's instruments -- "
                         f"look it up in initialize.")

    def symbols(self, *symbols, **kwargs):
        return [self.symbol(symbol, **kwargs) for symbol in symbols]

    def record(self, *args, **kwargs):
        return None


def numeric(value) -> bool:
    return isinstance(value, (bool, int, float, np.number)) or is_array(value)
