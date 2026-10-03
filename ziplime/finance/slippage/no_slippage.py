import datetime

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.order import Order
from ziplime.finance.slippage.slippage_model import SlippageModel


class NoSlippage(SlippageModel):
    """A slippage model where all orders fill immediately and completely at the
    current close price.

    Notes
    -----
    This is primarily used for testing.
    """

    async def process_order(self, exchange: Exchange, dt: datetime.datetime, order: Order,
                            price: float = None) -> tuple[float, float]:
        """Fill the whole open amount at ``price``, or at the bar's close when none is given.

        The signature had drifted from the one :meth:`SlippageModel.simulate` calls -- it took no
        ``price`` and referred to a ``data`` that does not exist here -- so every order routed
        through this model raised instead of filling.
        """
        if price is None:
            current_val = await exchange.get_spot_value(
                assets=frozenset({order.asset}), fields=frozenset({"close"}), dt=dt)
            price = current_val["close"][0]
        if price is None:
            return None, 0
        return price, order.open_amount

    async def order_target_percentage_maximum_quantity(self, exchange: Exchange, dt: datetime.datetime,
                                                       asset: ExchangeAsset,
                                                       percentage: float,
                                                       available_cash: float) -> tuple[float, float]:
        current_val = await exchange.get_spot_value(assets=frozenset({asset}), fields=frozenset({"close", "volume", }),
                                                    dt=dt)
        price = current_val["close"][0]
        target_cash = available_cash
        max_quantity = target_cash / price
        shares_to_fill = abs(max_quantity)
        return price, shares_to_fill
