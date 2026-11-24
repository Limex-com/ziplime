"""Support for code_rules."""

from __future__ import annotations


#: Hooks the engine looks up by name, and how many positional parameters each
#: is called with. Anything else at module level is the author's own business.
HOOKS = {
    "initialize": 1,
    "handle_data": 2,
    "before_trading_start": 2,
    "analyze": 2,
}


REQUIRED_HOOKS = ("initialize", "handle_data")


AWAITED_CALLS = frozenset({
    "symbol", "history", "current",
    "order", "order_value", "order_percent",
    "order_target", "order_target_value", "order_target_percent",
    "get_asset_positions", "get_portfolio", "get_account",
})


PANDAS_ACCESSORS = frozenset({"iloc", "loc", "at", "iat"})
