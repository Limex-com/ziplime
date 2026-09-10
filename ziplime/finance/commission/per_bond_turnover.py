from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.bonds import BondBook
from ziplime.finance.commission.bond_commission_model import BondCommissionModel


class PerBondTurnover(BondCommissionModel):
    """Charge a fraction of the money actually transacted.

    This is how bond desks bill: a percentage of turnover, not a fee per unit. Which matters,
    because a bond quote is a percentage of face value -- ``amount * price`` is about a tenth of
    the money that changes hands on a 1000-unit nominal, so a per-share model would undercharge by
    that factor. Turnover here is the **dirty** amount, accrued interest included, since that is
    what settles.

    Args:
        cost: Commission as a fraction of turnover. ``0.0003`` is 3 basis points, a realistic
            retail rate.
        min_trade_cost: Floor per trade, in the bond's currency.
        bond_book: Schedules used to convert a quote into money. Defaults to an empty book, which
            still applies the face-value conversion -- only the accrued-interest part of turnover
            is missed, and that is at most a coupon's worth.
    """

    def __init__(self, cost: float = 0.0003, min_trade_cost: float = 0.0,
                 bond_book: BondBook | None = None):
        self.cost_per_turnover = float(cost)
        self.min_trade_cost = min_trade_cost or 0.0
        self.bond_book = bond_book if bond_book is not None else BondBook()

    def __repr__(self):
        return (f"{self.__class__.__name__}(cost_per_turnover={self.cost_per_turnover}, "
                f"min_trade_cost={self.min_trade_cost})")

    def _turnover(self, asset: ExchangeAsset, amount: float, quoted_price: float, dt) -> float:
        bond = asset.asset
        if not isinstance(bond, Bond):
            return abs(amount * quoted_price)
        return abs(amount) * self.bond_book.dirty_value(bond, quoted_price, dt)

    def calculate(self, order, transaction) -> float:
        turnover = self._turnover(asset=transaction.asset, amount=transaction.amount,
                                  quoted_price=transaction.price, dt=transaction.dt)
        commission = turnover * self.cost_per_turnover
        if order.commission == 0:
            return max(self.min_trade_cost, commission)
        # The minimum has already been charged on this order; only the marginal part is due.
        return commission

    def calculate_for_asset(self, asset: ExchangeAsset, quantity: int,
                            transaction_amount: float) -> float:
        return max(self.min_trade_cost, abs(transaction_amount) * self.cost_per_turnover)
