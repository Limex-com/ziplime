"""Selecting contracts out of an option chain.

A 0DTE strategy cannot name the contracts it trades. The chain is listed fresh every session and
every symbol in it changes daily -- ``SPY240614C00523000`` exists for one day and is never seen
again -- so a strategy says *which* contract it wants ("the 25-delta put", "two strikes above the
money", "the wing five dollars out") and something has to turn that into a listing. That is what
this module is.

Selection is deliberately explicit about ties and misses. Asking for a strike a chain does not
carry returns the nearest one and says which, rather than silently trading a different contract
from the one the strategy's numbers were computed for.
"""

from ziplime.finance.options.chain.utils import contract_of  # noqa: F401
from ziplime.finance.options.chain.option_chain import OptionChain  # noqa: F401
from ziplime.finance.options.chain.grouping import group_by_expiry  # noqa: F401


__all__ = [
    'contract_of',
    'OptionChain',
    'group_by_expiry',
]
