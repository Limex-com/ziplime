"""Utils for execution."""

from sys import float_info
from numpy import isfinite
import ziplime.utils.math_utils as zp_math
from ziplime.errors import BadOrderParameters
from ziplime.utils.compat import consistent_round


def asymmetric_round_price(price: float, prefer_round_down: bool, tick_size: float, diff: float = 0.95) -> float:
    """
    Asymmetric rounding function for adjusting prices to the specified number
    of places in a way that "improves" the price. For limit prices, this means
    preferring to round down on buys and preferring to round up on sells.
    For stop prices, it means the reverse.

    If prefer_round_down == True:
        When .05 below to .95 above a specified decimal place, use it.
    If prefer_round_down == False:
        When .95 below to .05 above a specified decimal place, use it.

    In math-speak:
    If prefer_round_down: [<X-1>.0095, X.0195) -> round to X.01.
    If not prefer_round_down: (<X-1>.0005, X.0105] -> round to X.01.
    """
    precision = zp_math.number_of_decimal_places(tick_size)
    multiplier = int(tick_size * (10 ** precision))
    diff -= 0.5  # shift the difference down
    diff *= 10.0 ** -precision  # adjust diff to precision of tick size
    diff *= multiplier  # adjust diff to value of tick_size

    # Subtracting an epsilon from diff to enforce the open-ness of the upper
    # bound on buys and the lower bound on sells.  Using the actual system
    # epsilon doesn't quite get there, so use a slightly less epsilon-ey value.
    epsilon = float_info.epsilon * 10
    diff = diff - epsilon

    # relies on rounding half away from zero, unlike numpy's bankers' rounding
    rounded = tick_size * consistent_round(
        (price - (diff if prefer_round_down else -diff)) / tick_size
    )
    if zp_math.tolerant_equals(rounded, 0.0):
        return 0.0
    return rounded


def check_stoplimit_prices(price: float, label: str) -> None:
    """
    Check to make sure the stop/limit prices are reasonable and raise
    a BadOrderParameters exception if not.
    """
    try:
        if not isfinite(float(price)):
            raise BadOrderParameters(
                msg=f"Attempted to place an order with a {label} price "
                f"of {price}."
            )
    # This catches arbitrary objects
    except TypeError as exc:
        raise BadOrderParameters(
            msg=f"Attempted to place an order with a {label} price "
            f"of {type(price)}."
        ) from exc

    if price < 0:
        raise BadOrderParameters(
            msg=f"Can't place a {label} order with a negative price."
        )
