"""Factory for execution."""

from ziplime.finance.execution.execution_style import ExecutionStyle
from ziplime.finance.execution.limit_order import LimitOrder
from ziplime.finance.execution.market_order import MarketOrder
from ziplime.finance.execution.stop_limit_order import StopLimitOrder
from ziplime.finance.execution.stop_order import StopOrder


def make_execution_style(limit_price: float | None = None, stop_price: float | None = None,
                         style: ExecutionStyle | None = None) -> ExecutionStyle:
    """Resolve the shorthand order arguments into a single execution style.

    ``limit_price=N`` means :class:`LimitOrder`, ``stop_price=M`` means :class:`StopOrder`, both
    together mean :class:`StopLimitOrder`, and neither means :class:`MarketOrder`. An explicit
    ``style`` wins, but combining it with a loose price is an error rather than a silent choice of
    one over the other.

    Raises:
        ValueError: if ``style`` is given alongside ``limit_price`` or ``stop_price``.
    """
    if style is not None:
        if limit_price is not None or stop_price is not None:
            raise ValueError(
                "Pass either an execution style or limit_price/stop_price, not both."
            )
        return style
    if limit_price is not None and stop_price is not None:
        return StopLimitOrder(limit_price=limit_price, stop_price=stop_price)
    if limit_price is not None:
        return LimitOrder(limit_price=limit_price)
    if stop_price is not None:
        return StopOrder(stop_price=stop_price)
    return MarketOrder()
