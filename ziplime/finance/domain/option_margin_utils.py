"""Option margin classification for ledger accounting."""

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.option_contract import OptionContract


def _is_margined_option(instrument: Asset) -> bool:
    """Whether ``instrument`` is an option that settles variation margin rather than a premium.

    A free function rather than a method because three separate places have to agree on it -- the
    cash flow at a trade, the value of the position, and the margin it ties up -- and a
    disagreement between any two of them double-counts or loses the position entirely.
    """
    return isinstance(instrument, OptionContract) and instrument.premium_style.is_margined
