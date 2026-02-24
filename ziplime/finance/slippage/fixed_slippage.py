import datetime

from ziplime.exchanges.exchange import Exchange
from ziplime.finance.slippage.slippage_model import SlippageModel


class FixedSlippage(SlippageModel):
    """Simple model assuming a fixed-size spread for all assets.

    Parameters
    ----------
    spread : float, optional
        Size of the assumed spread for all assets.
        Orders to buy will be filled at ``close + (spread / 2)``.
        Orders to sell will be filled at ``close - (spread / 2)``.

    Notes
    -----
    This model does not impose limits on the size of fills. An order for an
    asset will always be filled as soon as any trading activity occurs in the
    order's asset, even if the size of the order is greater than the historical
    volume.
    """

    def __init__(self, spread=0.0):
        super(FixedSlippage, self).__init__()
        self.spread = spread

    def __repr__(self):
        return "{class_name}(spread={spread})".format(
            class_name=self.__class__.__name__,
            spread=self.spread,
        )

    def process_order(self, exchange: Exchange, dt:datetime.datetime, order):
        price = exchange.current(order.asset, "close")

        return (price + (self.spread / 2.0 * order.direction), order.amount)
