"""Utils for margin."""

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.margin.support import DEFAULT_MARGIN_CURRENCY


def notional(asset: ExchangeAsset, amount: float, price: float) -> float:
    """Return the absolute notional of a futures position, in the contract's quote currency."""
    multiplier = getattr(asset.asset, "multiplier", 1.0)
    return abs(amount) * price * multiplier


def quote_currency_of(asset: ExchangeAsset) -> str:
    """Currency ``asset`` is quoted in, taken from its quote asset."""
    quote = getattr(asset, "quote", None)
    return getattr(quote, "asset_name", None) or DEFAULT_MARGIN_CURRENCY


def margin_currency_of(asset: ExchangeAsset) -> str:
    """Currency the exchange collects margin for ``asset`` in.

    A futures contract declares its margin currency explicitly. Instruments
    without that field use their listing's quote currency, including margined
    options. A known quote currency takes precedence over the fallback default.
    """
    declared = getattr(asset.asset, "margin_currency", None)
    return declared or quote_currency_of(asset)
