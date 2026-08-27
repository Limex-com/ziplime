"""The ordered chain of contracts behind a continuous future."""
import datetime
from functools import partial

from ziplime.assets.entities.exchange_asset import ExchangeAsset

#: Adjustment styles a continuous future may use to splice prices across a roll.
ADJUSTMENT_STYLES = {'add', 'mul', None}


def delivery_predicate(codes: set[str], contract: ExchangeAsset) -> bool:
    """True when the contract's delivery month code is one of ``codes``.

    Both CME (``PLF16``) and FORTS (``SiZ6``) encode the delivery month as the character before the
    year digits, so the code is read from the end of the ticker rather than a fixed offset.
    """
    symbol = contract.symbol
    for tail in (2, 3):
        if len(symbol) > tail and symbol[-tail].isalpha() and symbol[-(tail - 1):].isdigit():
            return symbol[-tail] in codes
    return False


march_cycle_delivery_predicate = partial(delivery_predicate, set(['H', 'M', 'U', 'Z']))

CHAIN_PREDICATES = {
    'EL': march_cycle_delivery_predicate,
    'ME': march_cycle_delivery_predicate,
    'PL': partial(delivery_predicate, set(['F', 'J', 'N', 'V'])),
    'PA': march_cycle_delivery_predicate,

    # The majority of trading in these currency futures is done on a
    # March quarterly cycle (Mar, Jun, Sep, Dec) but contracts are
    # listed for the first 3 consecutive months from the present day. We
    # want the continuous futures to be composed of just the quarterly
    # contracts.
    'JY': march_cycle_delivery_predicate,
    'CD': march_cycle_delivery_predicate,
    'AD': march_cycle_delivery_predicate,
    'BP': march_cycle_delivery_predicate,

    # Gold and silver contracts trade on an unusual specific set of months.
    'GC': partial(delivery_predicate, set(['G', 'J', 'M', 'Q', 'V', 'Z'])),
    'XG': partial(delivery_predicate, set(['G', 'J', 'M', 'Q', 'V', 'Z'])),
    'SV': partial(delivery_predicate, set(['H', 'K', 'N', 'U', 'Z'])),
    'YS': partial(delivery_predicate, set(['H', 'K', 'N', 'U', 'Z'])),
}


class OrderedContracts:
    """A futures chain in order of expiration, with lookups a roll needs.

    Contracts are :class:`ExchangeAsset` listings whose ``asset`` is a
    :class:`~ziplime.assets.entities.futures_contract.FuturesContract`; their lifecycle dates are
    plain :class:`datetime.date` values.

    Args:
        root_symbol: Root symbol of the chain.
        contracts: Listings of the chain, in any order; sorted here by expiration.
        chain_predicate: Optional filter deciding which contracts belong to the chain, used to keep
            a continuous future on a single delivery cycle.
    """

    def __init__(self, root_symbol: str, contracts: list[ExchangeAsset], chain_predicate=None):
        self.root_symbol = root_symbol
        if chain_predicate is None:
            def chain_predicate(contract):
                return True

        included = []
        for contract in sorted(contracts, key=lambda c: (c.asset.expiration_date, c.sid)):
            # A contract whose listing starts on or after its auto close date never trades.
            if contract.start_date >= contract.auto_close_date:
                continue
            if not chain_predicate(contract):
                continue
            included.append(contract)

        self.contracts = included
        self.sid_to_index = {contract.sid: index for index, contract in enumerate(included)}
        self.sid_to_contract = {contract.sid: contract for contract in included}

    def __len__(self) -> int:
        return len(self.contracts)

    @property
    def start_date(self) -> datetime.date | None:
        return min((c.start_date for c in self.contracts), default=None)

    @property
    def end_date(self) -> datetime.date | None:
        return max((c.end_date for c in self.contracts), default=None)

    def contract_before_auto_close(self, dt: datetime.date) -> ExchangeAsset | None:
        """Return the first contract that has not reached its auto close date by ``dt``."""
        for contract in self.contracts:
            if contract.auto_close_date > dt:
                return contract
        return self.contracts[-1] if self.contracts else None

    def contract_at_offset(self, sid: int, offset: int, start_cap: datetime.date) -> ExchangeAsset | None:
        """Return the contract ``offset`` places further down the chain from ``sid``.

        ``None`` if the chain ends first, or if that contract had not started trading by
        ``start_cap``.
        """
        index = self.sid_to_index.get(sid)
        if index is None:
            return None
        target = index + offset
        if target >= len(self.contracts):
            return None
        contract = self.contracts[target]
        return contract if contract.start_date <= start_cap else None

    def active_chain(self, starting_sid: int, dt: datetime.date) -> list[ExchangeAsset]:
        """Return the contracts from ``starting_sid`` onwards that had started trading by ``dt``."""
        index = self.sid_to_index.get(starting_sid)
        if index is None:
            return []
        return [c for c in self.contracts[index:] if c.start_date <= dt]
