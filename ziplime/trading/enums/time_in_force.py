from enum import Enum


class TimeInForce(Enum):
    """
    Enum representing time-in-force parameters for orders.
    """
    GTC = "GTC"  # Good Till Cancelled
    IOC = "IOC"  # Immediate Or Cancel
    FOK = "FOK"  # Fill Or Kill
    GTD = "GTD"  # Good Till Date
    DAY = "DAY"  # Valid for the trading day
    AT_THE_OPENING = "OPG"  # Execute at market opening
    AT_THE_CLOSE = "CLS"  # Execute at market close
