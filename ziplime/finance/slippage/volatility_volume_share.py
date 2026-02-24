import math

from ziplime.finance.constants import ROOT_SYMBOL_TO_ETA, DEFAULT_ETA

from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.finance.slippage.market_impact_base import MarketImpactBase
from ziplime.utils.dummy import DummyMapping


class VolatilityVolumeShare(MarketImpactBase):
    """Model slippage for futures contracts according to the following formula:

        new_price = price + (price * MI / 10000),

    where 'MI' is market impact, which is defined as:

        MI = eta * sigma * sqrt(psi)

    - ``eta`` is a constant which varies by root symbol.
    - ``sigma`` is 20-day annualized volatility.
    - ``psi`` is the volume traded in the given bar divided by 20-day ADV.

    Parameters
    ----------
    volume_limit : float
        Maximum percentage (as a decimal) of a bar's total volume that can be
        traded.
    eta : float or dict
        Constant used in the market impact formula. If given a float, the eta
        for all futures contracts is the same. If given a dictionary, it must
        map root symbols to the eta for contracts of that symbol.
    """

    NO_DATA_VOLATILITY_SLIPPAGE_IMPACT = 7.5 / 10000
    allowed_asset_types = (FuturesContract,)

    def __init__(self, volume_limit, eta=ROOT_SYMBOL_TO_ETA):
        super(VolatilityVolumeShare, self).__init__()
        self.volume_limit = volume_limit

        # If 'eta' is a constant, use a dummy mapping to treat it as a
        # dictionary that always returns the same value.
        # NOTE: This dictionary does not handle unknown root symbols, so it may
        # be worth revisiting this behavior.
        if isinstance(eta, (int, float)):
            self._eta = DummyMapping(float(eta))
        else:
            # Eta is a dictionary. If the user's dictionary does not provide a
            # value for a certain contract, fall back on the pre-defined eta
            # values per root symbol.
            self._eta = {**ROOT_SYMBOL_TO_ETA, **eta}

    def __repr__(self):
        if isinstance(self._eta, DummyMapping):
            # Eta is a constant, so extract it.
            eta = self._eta["dummy key"]
        else:
            eta = "<varies>"
        return "{class_name}(volume_limit={volume_limit}, eta={eta})".format(
            class_name=self.__class__.__name__,
            volume_limit=self.volume_limit,
            eta=eta,
        )

    def get_simulated_impact(
            self,
            order,
            current_price,
            current_volume,
            txn_volume,
            mean_volume,
            volatility,
    ):
        try:
            eta = self._eta[order.asset.root_symbol]
        except Exception:
            eta = DEFAULT_ETA

        psi = txn_volume / mean_volume

        market_impact = eta * volatility * math.sqrt(psi)

        # We divide by 10,000 because this model computes to basis points.
        # To convert from bps to % we need to divide by 100, then again to
        # convert from % to fraction.
        return (current_price * market_impact) / 10000

    def get_txn_volume(self, data, order):
        volume = data.current(order.asset, "volume")
        return volume * self.volume_limit
