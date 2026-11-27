"""Utils for chain."""

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.option_contract import OptionContract


def contract_of(listing: ExchangeAsset) -> OptionContract:
    """The :class:`OptionContract` behind a listing, or a legible error.

    Every helper here starts with this, because the failure it catches -- handing an equity listing
    to an option helper -- otherwise surfaces as ``AttributeError: 'Equity' object has no attribute
    'strike'`` several frames away.
    """
    instrument = getattr(listing, "asset", None)
    if not isinstance(instrument, OptionContract):
        raise TypeError(
            f"{getattr(listing, 'symbol', listing)!r} is not an option listing: its instrument is "
            f"{type(instrument).__name__}, not OptionContract.")
    return instrument
