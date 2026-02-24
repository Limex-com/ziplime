import datetime
import math
from pandas import isnull

from ziplime.errors import LiquidityExceeded
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.slippage.slippage_model import SlippageModel, DEFAULT_EQUITY_VOLUME_SLIPPAGE_BAR_LIMIT
from ziplime.finance.utils import fill_price_worse_than_limit_price


class VolumeShareSlippage(SlippageModel):
    """Model slippage as a quadratic function of percentage of historical volume.

    Orders to buy will be filled at::

       price * (1 + price_impact * (volume_share ** 2))

    Orders to sell will be filled at::

       price * (1 - price_impact * (volume_share ** 2))

    where ``price`` is the close price for the bar, and ``volume_share`` is the
    percentage of minutely volume filled, up to a max of ``volume_limit``.

    Parameters
    ----------
    volume_limit : float, optional
        Maximum percent of historical volume that can fill in each bar. 0.5
        means 50% of historical volume. 1.0 means 100%. Default is 0.025 (i.e.,
        2.5%).
    price_impact : float, optional
        Scaling coefficient for price impact. Larger values will result in more
        simulated price impact. Smaller values will result in less simulated
        price impact. Default is 0.1.
    """

    def __init__(
            self,
            volume_limit=DEFAULT_EQUITY_VOLUME_SLIPPAGE_BAR_LIMIT,
            price_impact=0.1,
    ):
        super(VolumeShareSlippage, self).__init__()

        self.volume_limit = volume_limit
        self.price_impact = price_impact

    def __repr__(self):
        return """
{class_name}(
    volume_limit={volume_limit},
    price_impact={price_impact})
""".strip().format(
            class_name=self.__class__.__name__,
            volume_limit=self.volume_limit,
            price_impact=self.price_impact,
        )

    def process_order(self, exchange: Exchange, dt:datetime.datetime, order):
        volume = data.current(order.asset, "volume")

        max_volume = self.volume_limit * volume

        # price impact accounts for the total volume of transactions
        # created against the current minute bar
        remaining_volume = max_volume - self.volume_for_bar
        if remaining_volume < 1:
            # we can't fill any more transactions
            raise LiquidityExceeded()

        # the current order amount will be the min of the
        # volume available in the bar or the open amount.
        cur_volume = int(min(remaining_volume, abs(order.open_amount)))

        if cur_volume < 1:
            return None, None

        # tally the current amount into our total amount ordered.
        # total amount will be used to calculate price impact
        total_volume = self.volume_for_bar + cur_volume

        volume_share = min(total_volume / volume, self.volume_limit)

        price = data.current(order.asset, "close")

        # BEGIN
        #
        # Remove this block after fixing data to ensure volume always has
        # corresponding price.
        if isnull(price):
            return
        # END

        simulated_impact = (
                volume_share ** 2 * math.copysign(self.price_impact, order.direction) * price
        )
        impacted_price = price + simulated_impact

        if fill_price_worse_than_limit_price(impacted_price, order):
            return None, None

        return (impacted_price, math.copysign(cur_volume, order.direction))

