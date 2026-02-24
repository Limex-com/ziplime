from enum import Enum


class OrderSide(Enum):
    """
    Enum representing trade types or directions.
    """
    BUY = "BUY"  # Buy/Long order
    SELL = "SELL"  # Sell/Short order
    RANGE = "RANGE"  # Range order (e.g., for AMM liquidity provision)

    def opposite(self):
        """
        Returns the opposite trade type.
        """
        if self == OrderSide.BUY:
            return OrderSide.SELL
        elif self == OrderSide.SELL:
            return OrderSide.BUY
        return self  # RANGE doesn't have a direct opposite
