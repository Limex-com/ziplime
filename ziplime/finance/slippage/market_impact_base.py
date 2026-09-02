import datetime
import math
from abc import abstractmethod

import numpy as np

from ziplime.errors import HistoryWindowStartsBeforeData
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.slippage.slippage_model import SlippageModel, SQRT_252
from ziplime.finance.utils import fill_price_worse_than_limit_price
from ziplime.utils.cache import ExpiringCache


class MarketImpactBase(SlippageModel):
    """Base class for slippage models which compute a simulated price impact
    according to a history lookback.
    """

    NO_DATA_VOLATILITY_SLIPPAGE_IMPACT = 10.0 / 10000

    def __init__(self):
        super(MarketImpactBase, self).__init__()
        self._window_data_cache = ExpiringCache()

    @abstractmethod
    def get_txn_volume(self, volume: float, order):
        """Return the number of shares/contracts we would like to fill in this bar.

        Parameters
        ----------
        volume : float
            Total volume traded in the current bar.
        order : Order

        Return
        ------
        int : the number of shares
        """
        raise NotImplementedError("get_txn_volume")

    @abstractmethod
    def get_simulated_impact(
            self,
            order,
            current_price,
            current_volume,
            txn_volume,
            mean_volume,
            volatility,
    ):
        """Calculate simulated price impact.

        Parameters
        ----------
        order : The order being processed.
        current_price : Current price of the asset being ordered.
        current_volume : Volume of the asset being ordered for the current bar.
        txn_volume : Number of shares/contracts being ordered.
        mean_volume : Trailing ADV of the asset.
        volatility : Annualized daily volatility of returns.

        Return
        ------
        int : impact on the current price.
        """
        raise NotImplementedError("get_simulated_impact")

    async def process_order(self, exchange: Exchange, dt: datetime.datetime, order,
                            price: float = None):
        """Fill an order at a price moved by this model's simulated market impact.

        Rewritten against the async exchange API. The previous body was written for zipline's
        synchronous ``BarData`` portal and referenced an undefined ``data`` name, so any futures
        backtest using the default slippage model raised immediately.
        """
        if order.open_amount == 0:
            return None, None

        current = await exchange.get_spot_value(
            assets=frozenset({order.asset}),
            fields=frozenset({"volume", "high", "low"}),
            dt=dt,
        )
        if len(current) == 0 or len(current["volume"]) == 0:
            return None, None

        volume = current["volume"][0]
        if not volume:
            return None, None

        # Price to use is the midpoint of the bar's range.
        bar_price = np.mean([current["high"][0], current["low"][0]])
        if bar_price is None or np.isnan(bar_price):
            bar_price = price
        if bar_price is None or np.isnan(bar_price):
            return None, None

        mean_volume, volatility = await self._get_window_data(
            exchange=exchange, asset=order.asset, dt=dt, window_length=20)

        txn_volume = int(min(self.get_txn_volume(volume=volume, order=order),
                             abs(order.open_amount)))

        # If the computed transaction volume is zero or a decimal value, 'int'
        # will round it down to zero. In that case just bail.
        if txn_volume == 0:
            return None, None

        if mean_volume == 0 or np.isnan(volatility):
            # If this is the first day the contract exists or there is no
            # volume history, default to a conservative estimate of impact.
            simulated_impact = bar_price * self.NO_DATA_VOLATILITY_SLIPPAGE_IMPACT
        else:
            simulated_impact = self.get_simulated_impact(
                order=order,
                current_price=bar_price,
                current_volume=volume,
                txn_volume=txn_volume,
                mean_volume=mean_volume,
                volatility=volatility,
            )

        impacted_price = bar_price + math.copysign(simulated_impact, order.direction)

        if fill_price_worse_than_limit_price(impacted_price, order):
            return None, None

        return impacted_price, math.copysign(txn_volume, order.direction)

    async def _get_window_data(self, exchange: Exchange, asset, dt: datetime.datetime,
                               window_length: int):
        """Return the trailing mean volume and annualised close-price volatility of ``asset``.

        The window excludes the current bar, so that impact is estimated from history rather than
        from the bar being filled.

        Returns:
            ``(mean volume, annualised volatility)``; ``(0, nan)`` when there is not enough history,
            which callers treat as "assume a conservative fixed impact".
        """
        cache_key = (asset.sid, dt)
        try:
            values = self._window_data_cache.get(asset, cache_key)
        except KeyError:
            history = await exchange.get_data_by_limit(
                fields=frozenset({"volume", "close"}),
                limit=window_length + 1,
                end_date=dt,
                frequency=datetime.timedelta(days=1),
                assets=frozenset({asset}),
                include_end_date=False,
            )
            if len(history) < 2:
                return 0, np.nan

            volumes = np.asarray(history["volume"], dtype=float)
            closes = np.asarray(history["close"], dtype=float)
            returns = np.diff(closes) / closes[:-1]
            with np.errstate(invalid="ignore"):
                close_volatility = np.std(returns, ddof=1) if len(returns) > 1 else np.nan
            values = {
                "volume": float(np.mean(volumes)),
                "close": close_volatility * SQRT_252,
            }
            self._window_data_cache.set(asset, values, cache_key)

        return values["volume"], values["close"]

