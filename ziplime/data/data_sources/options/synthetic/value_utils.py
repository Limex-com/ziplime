"""Value utils for synthetic."""

import math


def _as_float(value) -> float | None:
    if value is None:
        return None
    number = float(value)
    return None if math.isnan(number) else number


def _round_to_tick(price: float, tick_size: float) -> float:
    """Round to the contract's tick, never below zero.

    Options are quoted on a tick and a backtest that prices them off it fills at prices the
    exchange would not have accepted -- which matters more here than elsewhere, because a 0DTE
    wing's whole premium is a few ticks.
    """
    if tick_size <= 0:
        return max(price, 0.0)
    return max(round(price / tick_size) * tick_size, 0.0)
