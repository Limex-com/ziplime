from functools import partial

from numpy import array, iinfo
from pandas import Timestamp

from ziplime.assets.domain.contract_node import ContractNode


def delivery_predicate(codes, contract):
    # This relies on symbols that are construct following a pattern of
    # root symbol + delivery code + year, e.g. PLF16
    # This check would be more robust if the future contract class had
    # a 'delivery_month' member.
    delivery_code = contract.symbol[-3]
    return delivery_code in codes

march_cycle_delivery_predicate = partial(delivery_predicate,
                                         set(['H', 'M', 'U', 'Z']))

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

ADJUSTMENT_STYLES = {'add', 'mul', None}

class OrderedContracts:
    """A container for aligned values of a future contract chain, in sorted order
    of their occurrence.
    Used to get answers about contracts in relation to their auto close
    dates and start dates.

    Members
    -------
    root_symbol : str
        The root symbol of the future contract chain.
    contracts : deque
        The contracts in the chain in order of occurrence.
    start_dates : long[:]
        The start dates of the contracts in the chain.
        Corresponds by index with contract_sids.
    auto_close_dates : long[:]
        The auto close dates of the contracts in the chain.
        Corresponds by index with contract_sids.
    future_chain_predicates : dict
        A dict mapping root symbol to a predicate function which accepts a contract
    as a parameter and returns whether or not the contract should be included in the
    chain.

    Instances of this class are used by the simulation engine, but not
    exposed to the algorithm.
    """

    # cdef readonly object root_symbol
    # cdef readonly object _head_contract
    # cdef readonly dict sid_to_contract
    # cdef readonly int64_t _start_date
    # cdef readonly int64_t _end_date
    # cdef readonly object chain_predicate

    def __init__(self, root_symbol, contracts, chain_predicate=None):

        self.root_symbol = root_symbol

        self.sid_to_contract = {}

        self._start_date = iinfo('int64').max
        self._end_date = 0

        if chain_predicate is None:
            chain_predicate = lambda x: True

        self._head_contract = None
        prev = None
        while contracts:
            contract = contracts.popleft()

            # It is possible that the first contract in our list has a start
            # date on or after its auto close date. In that case the contract
            # is not tradable, so do not include it in the chain.
            if prev is None and contract.start_date >= contract.auto_close_date:
                continue

            if not chain_predicate(contract):
                continue

            self._start_date = min(contract.start_date.value, self._start_date)
            self._end_date = max(contract.end_date.value, self._end_date)

            curr = ContractNode(contract)
            self.sid_to_contract[contract.sid] = curr
            if self._head_contract is None:
                self._head_contract = curr
                prev = curr
                continue
            curr.prev = prev
            prev.next = curr
            prev = curr

    def contract_before_auto_close(self, dt_value):
        """Get the contract with next upcoming auto close date."""
        curr = self._head_contract
        while curr.next is not None:
            if curr.contract.auto_close_date.value > dt_value:
                break
            curr = curr.next
        return curr.contract.sid

    def contract_at_offset(self, sid, offset, start_cap):
        """Get the sid which is the given sid plus the offset distance.
        An offset of 0 should be reflexive.
        """
        # cdef Py_ssize_t i
        curr = self.sid_to_contract[sid]
        i = 0
        while i < offset:
            if curr.next is None:
                return None
            curr = curr.next
            i += 1
        if curr.contract.start_date.value <= start_cap:
            return curr.contract.sid
        else:
            return None

    def active_chain(self,  starting_sid, dt_value):
        curr = self.sid_to_contract[starting_sid]
        contracts = []

        while curr is not None:
            if curr.contract.start_date.value <= dt_value:
                contracts.append(curr.contract.sid)
            curr = curr.next

        return array(contracts, dtype='int64')

    @property
    def start_date(self):
            return Timestamp(self._start_date)

    @property
    def end_date(self):
            return Timestamp(self._end_date)
