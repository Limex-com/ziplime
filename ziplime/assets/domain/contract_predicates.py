"""Predicates for selecting futures delivery cycles."""

from ziplime.assets.entities.exchange_asset import ExchangeAsset


def delivery_predicate(codes: set[str], contract: ExchangeAsset) -> bool:
    """True when the contract's delivery month code is one of ``codes``.

    Venues encode the delivery month as the character before the
    year digits, so the code is read from the end of the ticker rather than a fixed offset.
    """
    symbol = contract.symbol
    for tail in (2, 3):
        if len(symbol) > tail and symbol[-tail].isalpha() and symbol[-(tail - 1):].isdigit():
            return symbol[-tail] in codes
    return False
