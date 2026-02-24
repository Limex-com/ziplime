import datetime

from ziplime.errors import LiquidityExceeded
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.order import Order
from ziplime.finance.slippage.slippage_model import SlippageModel


class FixedBasisPointsSlippage(SlippageModel):
    """
    Model slippage as a fixed percentage difference from historical minutely
    close price, limiting the size of fills to a fixed percentage of historical
    minutely volume.

    Orders to buy are filled at::

        historical_price * (1 + (basis_points * 0.0001))

    Orders to sell are filled at::

        historical_price * (1 - (basis_points * 0.0001))

    Fill sizes are capped at::

        historical_volume * volume_limit

    Parameters
    ----------
    basis_points : float, optional
        Number of basis points of slippage to apply for each fill. Default
        is 5 basis points.
    volume_limit : float, optional
        Fraction of trading volume that can be filled each minute. Default is
        10% of trading volume.

    Notes
    -----
    - A basis point is one one-hundredth of a percent.
    - This class, default-constructed, is ziplime's default slippage model for
      equities.
    """

    def __init__(self, basis_points=5.0, volume_limit=0.1):
        super(FixedBasisPointsSlippage, self).__init__()
        if volume_limit <= 0:
            raise ValueError("volume_limit must be positive.")
        if basis_points <= 0:
            raise ValueError("volume_limit must be positive.")

        self.basis_points = basis_points
        self.percentage = float(self.basis_points) / 10000.0
        self.volume_limit = volume_limit

    def __repr__(self):
        return """
{class_name}(
    basis_points={basis_points},
    volume_limit={volume_limit},
)
""".strip().format(
            class_name=self.__class__.__name__,
            basis_points=self.basis_points,
            volume_limit=self.volume_limit,
        )

    def process_order(self, exchange: Exchange, dt:datetime.datetime, order: Order) -> tuple[float, float]:
        current_val = exchange.current(assets=frozenset({order.asset}), fields=frozenset({"close", "volume"}), dt=dt)
        volume = current_val["volume"][0]
        max_volume = int(self.volume_limit * volume)

        price = current_val["close"][0]
        shares_to_fill = min(abs(order.open_amount), max_volume - self.volume_for_bar)

        if shares_to_fill == 0:
            raise LiquidityExceeded()

        return (
            price + price * (self.percentage * order.direction),
            shares_to_fill * order.direction,
        )
