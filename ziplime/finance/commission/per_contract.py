from collections import defaultdict

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.commission import DEFAULT_PER_CONTRACT_COST, calculate_per_unit_commission
from ziplime.finance.commission.future_commission_model import FutureCommissionModel
from ziplime.finance.constants import FUTURE_EXCHANGE_FEES_BY_SYMBOL, DEFAULT_MOEX_EXCHANGE_FEE
from ziplime.utils.dummy import DummyMapping
from toolz import merge


class PerContract(FutureCommissionModel):
    """
    Calculates a commission for a transaction based on a per contract cost with
    an optional minimum cost per trade.

    Parameters
    ----------
    cost : float or dict
        The amount of commissions paid per contract traded. If given a float,
        the commission for all futures contracts is the same. If given a
        dictionary, it must map root symbols to the commission cost for
        contracts of that symbol.
    exchange_fee : float or dict
        A flat-rate fee charged by the exchange per trade. This value is a
        constant, one-time charge no matter how many contracts are being
        traded. If given a float, the fee for all contracts is the same. If
        given a dictionary, it must map root symbols to the fee for contracts
        of that symbol.
    min_trade_cost : float, optional
        The minimum amount of commissions paid per trade.
    """

    def __init__(
        self,
        cost,
        exchange_fee,
        min_trade_cost,
    ):
        # If 'cost' or 'exchange fee' are constants, use a dummy mapping to
        # treat them as a dictionary that always returns the same value.
        # NOTE: These dictionary does not handle unknown root symbols, so it
        # may be worth revisiting this behavior.
        if isinstance(cost, (int, float)):
            self._cost_per_contract = DummyMapping(float(cost))
        else:
            # Cost per contract is a dictionary. If the user's dictionary does
            # not provide a commission cost for a certain contract, fall back
            # on the pre-defined cost values per root symbol.
            self._cost_per_contract = defaultdict(
                lambda: DEFAULT_PER_CONTRACT_COST, **cost
            )

        if isinstance(exchange_fee, (int, float)):
            self._exchange_fee = DummyMapping(float(exchange_fee))
        else:
            # Exchange fee is a dictionary. If the user's dictionary does not
            # provide an exchange fee for a certain contract, fall back on the
            # pre-defined exchange fees per root symbol.
            # defaultdict, not a plain merge: the pre-defined tables cover CME and MOEX roots, and
            # a root missing from both used to raise KeyError in the middle of a simulation.
            self._exchange_fee = defaultdict(
                lambda: DEFAULT_MOEX_EXCHANGE_FEE,
                merge(FUTURE_EXCHANGE_FEES_BY_SYMBOL, exchange_fee),
            )

        self.min_trade_cost = min_trade_cost or 0

    def __repr__(self):
        if isinstance(self._cost_per_contract, DummyMapping):
            # Cost per contract is a constant, so extract it.
            cost_per_contract = self._cost_per_contract["dummy key"]
        else:
            cost_per_contract = "<varies>"

        if isinstance(self._exchange_fee, DummyMapping):
            # Exchange fee is a constant, so extract it.
            exchange_fee = self._exchange_fee["dummy key"]
        else:
            exchange_fee = "<varies>"

        return (
            "{class_name}(cost_per_contract={cost_per_contract}, "
            "exchange_fee={exchange_fee}, min_trade_cost={min_trade_cost})".format(
                class_name=self.__class__.__name__,
                cost_per_contract=cost_per_contract,
                exchange_fee=exchange_fee,
                min_trade_cost=self.min_trade_cost,
            )
        )

    def calculate(self, order, transaction):
        root_symbol = order.asset.asset.root_symbol
        cost_per_contract = self._cost_per_contract[root_symbol]
        exchange_fee = self._exchange_fee[root_symbol]

        return calculate_per_unit_commission(
            order=order,
            transaction=transaction,
            cost_per_unit=cost_per_contract,
            initial_commission=exchange_fee,
            min_trade_cost=self.min_trade_cost,
        )
    def calculate_for_asset(self, asset: ExchangeAsset, quantity: int, transaction_amount: float) -> float:
        root_symbol = asset.asset.root_symbol
        cost_per_contract = self._cost_per_contract[root_symbol]
        exchange_fee = self._exchange_fee[root_symbol]

        return max(self.min_trade_cost, abs(quantity * cost_per_contract)  + exchange_fee)