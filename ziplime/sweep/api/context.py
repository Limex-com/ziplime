"""Context for api."""

import types
from ziplime.sweep.all_assets import ALL
from ziplime.sweep.costs import Costs
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.vectorized_containers import VectorUniverse
from ziplime.sweep.params import SweepError
from ziplime.assets.domain.asset_type import AssetType
from ziplime.sweep.backends import Backend
from ziplime.sweep.api.columns import Columns
from ziplime.sweep.api.data import Data
from ziplime.sweep.api.portfolio import Portfolio
from ziplime.sweep.api.signals import Signals
from ziplime.sweep.api.style import Style
from ziplime.sweep.api.asset_utils import asset_map
from ziplime.sweep.api.price_utils import asymmetric_round


class Context:
    """TradingAlgorithm for every combination: what a converted strategy calls on ``context``."""

    def __init__(self, *, config, signals: Signals, portfolio: Portfolio, data: Data, backend: Backend,
                 universe: dict, columns: Columns, costs: Costs, state: dict) -> None:
        self.algorithm = types.SimpleNamespace(config=config)
        self.signals, self.portfolio, self.universe = signals, portfolio, VectorUniverse(universe)
        self._data, self._backend, self._columns, self._costs = data, backend, columns, costs
        self.xp = backend.xp
        self._runtime = None
        self._in_before_trading_start = False
        self.reset_orders()
        for name, value in state.items():
            self.set_state(name, value)

    def set_state(self, name: str, value) -> None:
        """What initialize put on the context; a dict keyed by instrument becomes an AssetState."""
        from ziplime.sweep.api.asset_state import AssetState

        if asset_map(value, self._columns):
            value = AssetState(value, self._columns, self)
        setattr(self, name, value)

    # What the simulation reads back after the strategy's hooks ran
    def reset_orders(self) -> None:
        #: This bar's orders, in the order they were placed: (amount, limit, stop)
        self.new_orders = []

    # -- placing an order
    def _place(self, asset, amount, style: Style | None = None, limit_price=None, stop_price=None) -> None:
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
    def order(self, asset, amount, style: Style | None = None, limit_price=None, stop_price=None, **kwargs) -> None:
        self._place(asset, amount + self.xp.zeros_like(self.portfolio.held), style,
                    limit_price, stop_price)

    def order_value(
        self,
        asset,
        value,
        style: Style | None = None,
        limit_price=None,
        stop_price=None,
        **kwargs,
    ) -> None:
        self._place(asset, self._value_amount(value), style, limit_price, stop_price)

    def order_percent(self, asset, percent, style: Style | None = None, **kwargs) -> None:
        self._costs.require_percent_sizing()
        self._place(asset, self._percent_amount(percent), style)

    def order_target(self, asset, target, style: Style | None = None, **kwargs) -> None:
        self._place(asset, target - self.portfolio.held, style)

    def order_target_value(self, asset, target, style: Style | None = None, **kwargs) -> None:
        self._place(asset, self._value_amount(target) - self.portfolio.held, style)

    def order_target_percent(self, asset, target, style: Style | None = None, reserved_percentage_for_fees=None,
                             **kwargs) -> None:
        self._costs.require_percent_sizing()
        self._place(asset, self._percent_amount(target) - self.portfolio.held, style)

    def rebalance(self, weights, style: Style | None = None, exchange_name: str | None = None) -> None:
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

    def symbol(
        self,
        symbol: str,
        mic: str | None = None,
        asset_type: AssetType | None = None,
        country_code: str | None = None,
        **kwargs,
    ):
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

    def symbols(self, *symbols: str, **kwargs):
        return [self.symbol(symbol, **kwargs) for symbol in symbols]

    def record(self, *args, **kwargs) -> None:
        return None
