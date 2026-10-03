from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.commission.option_commission_model import OptionCommissionModel

#: What a retail broker charges per option contract, in the contract's quote currency. Brokers
#: quote this per *contract*, never per share, which is the whole reason options need their own
#: model rather than reusing PerShare.
DEFAULT_PER_OPTION_CONTRACT_COST = 0.65

#: Exchange and regulatory fees, per contract. Small, and fatal to ignore on 0DTE: a strategy that
#: opens and closes a four-legged structure every session pays this 1 000 times a year per lot,
#: and the whole edge of a premium-selling strategy is of the same order.
DEFAULT_OPTION_EXCHANGE_FEE = 0.05


class PerOptionContract(OptionCommissionModel):
    """A flat charge per contract traded, plus per-contract exchange fees.

    This is how option commissions actually work, and the difference from an equity model is not
    cosmetic. A per-share model charges on ``amount``, which for an option is a count of contracts
    covering a hundred shares each -- so it undercharges by roughly the multiplier. A per-dollar
    model charges on notional, which for a five-cent wing is nearly nothing even though the broker
    still bills 65 cents for it.

    The fee floor is what makes cheap contracts expensive to trade, and it is the reason a 0DTE
    structure with four legs costs about 2.80 to open and another 2.80 to close before any
    slippage -- against a credit that may be 100. A backtest that leaves it out overstates the
    return of every premium-selling strategy.

    Args:
        cost: Broker commission per contract.
        exchange_fee: Exchange and regulatory fees per contract.
        min_trade_cost: Floor per order, charged once on the order's first fill.
    """

    def __init__(self, cost: float = DEFAULT_PER_OPTION_CONTRACT_COST,
                 exchange_fee: float = DEFAULT_OPTION_EXCHANGE_FEE,
                 min_trade_cost: float = 0.0):
        self.cost_per_contract = float(cost)
        self.exchange_fee = float(exchange_fee)
        self.min_trade_cost = min_trade_cost or 0.0

    def __repr__(self):
        return (f"{self.__class__.__name__}(cost_per_contract={self.cost_per_contract}, "
                f"exchange_fee={self.exchange_fee}, min_trade_cost={self.min_trade_cost})")

    def calculate(self, order, transaction) -> float:
        contracts = abs(transaction.amount)
        commission = contracts * (self.cost_per_contract + self.exchange_fee)
        if order.commission == 0:
            return max(self.min_trade_cost, commission)
        # The order's minimum has already been charged; only the marginal part is due.
        return commission

    def calculate_for_asset(self, asset: ExchangeAsset, quantity: int,
                            transaction_amount: float) -> float:
        return max(self.min_trade_cost,
                   abs(quantity) * (self.cost_per_contract + self.exchange_fee))
