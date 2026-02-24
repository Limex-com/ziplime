import datetime

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
    def process_order(exchange: Exchange, dt:datetime.datetime, order):
        return (
            data.current(order.asset, "close"),
            order.amount,
        )

