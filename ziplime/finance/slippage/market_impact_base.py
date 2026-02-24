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
    def get_txn_volume(self, data, order):
        """Return the number of shares we would like to order in this minute.

        Parameters
        ----------
        data : BarData
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

    def process_order(self, exchange: Exchange, dt:datetime.datetime, order):
        if order.open_amount == 0:
            return None, None

        minute_data = data.current(order.asset, ["volume", "high", "low"])
        mean_volume, volatility = self._get_window_data(data, order.asset, 20)

        # Price to use is the average of the minute bar's open and close.
        price = np.mean([minute_data["high"], minute_data["low"]])

        volume = minute_data["volume"]
        if not volume:
            return None, None

        txn_volume = int(min(self.get_txn_volume(data, order), abs(order.open_amount)))

        # If the computed transaction volume is zero or a decimal value, 'int'
        # will round it down to zero. In that case just bail.
        if txn_volume == 0:
            return None, None

        if mean_volume == 0 or np.isnan(volatility):
            # If this is the first day the contract exists or there is no
            # volume history, default to a conservative estimate of impact.
            simulated_impact = price * self.NO_DATA_VOLATILITY_SLIPPAGE_IMPACT
        else:
            simulated_impact = self.get_simulated_impact(
                order=order,
                current_price=price,
                current_volume=volume,
                txn_volume=txn_volume,
                mean_volume=mean_volume,
                volatility=volatility,
            )

        impacted_price = price + math.copysign(simulated_impact, order.direction)

        if fill_price_worse_than_limit_price(impacted_price, order):
            return None, None

        return impacted_price, math.copysign(txn_volume, order.direction)

    def _get_window_data(self, data, asset, window_length):
        """Internal utility method to return the trailing mean volume over the
        past 'window_length' days, and volatility of close prices for a
        specific asset.

        Parameters
        ----------
        data : The BarData from which to fetch the daily windows.
        asset : The Asset whose data we are fetching.
        window_length : Number of days of history used to calculate the mean
            volume and close price volatility.

        Returns
        -------
        (mean volume, volatility)
        """
        try:
            values = self._window_data_cache.get(asset, data.current_session)
        except KeyError:
            try:
                # Add a day because we want 'window_length' complete days,
                # excluding the current day.
                volume_history = data.history(
                    asset,
                    "volume",
                    window_length + 1,
                    "1d",
                )
                close_history = data.history(
                    asset,
                    "close",
                    window_length + 1,
                    "1d",
                )
            except HistoryWindowStartsBeforeData:
                # If there is not enough data to do a full history call, return
                # values as if there was no data.
                return 0, np.nan

            # Exclude the first value of the percent change array because it is
            # always just NaN.
            close_volatility = (
                close_history[:-1]
                .pct_change()[1:]
                .std(
                    skipna=False,
                )
            )
            values = {
                "volume": volume_history[:-1].mean(),
                "close": close_volatility * SQRT_252,
            }
            self._window_data_cache.set(asset, values, data.current_session)

        return values["volume"], values["close"]
