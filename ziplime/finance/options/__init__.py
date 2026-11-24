"""Option pricing, Greeks, chains and multi-leg strategies.

Import-time note: everything here that prices an option needs ``vollib``, which is an optional
dependency (``pip install "ziplime[options]"``). Nothing in the ziplime core imports this package,
so an install without it runs every non-option backtest unchanged.
"""
from ziplime.finance.options.greek_values import Greeks
from ziplime.finance.options.greeks import black_scholes_price, greeks, implied_volatility, time_to_expiry

__all__ = [
    "Greeks",
    "black_scholes_price",
    "greeks",
    "implied_volatility",
    "time_to_expiry",
]
