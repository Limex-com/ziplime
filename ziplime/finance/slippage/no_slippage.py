import datetime

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.slippage.slippage_model import SlippageModel


class NoSlippage(SlippageModel):
    """A slippage model where all orders fill immediately and completely at the
    current close price.

    Notes
    -----
    This is primarily used for testing.
    """

    @staticmethod
    async def process_order(exchange: Exchange, dt: datetime.datetime, order):
        return (
            data.current(order.asset, "close"),
            order.amount,
        )

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
