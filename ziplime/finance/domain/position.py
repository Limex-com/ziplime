"""
Position Tracking
=================

    +-----------------+----------------------------------------------------+
    | key             | value                                              |
    +=================+====================================================+
    | asset           | the asset held in this position                    |
    +-----------------+----------------------------------------------------+
    | amount          | whole number of shares in the position             |
    +-----------------+----------------------------------------------------+
    | last_sale_price | price at last sale of the asset on the exchange    |
    +-----------------+----------------------------------------------------+
    | cost_basis      | the volume weighted average price paid per share   |
    +-----------------+----------------------------------------------------+

"""
import dataclasses
import datetime

from ziplime.assets.entities.asset import Asset


@dataclasses.dataclass
class Position:
    asset: Asset
    amount: int
    cost_basis: float
    last_sale_price: float
    last_sale_date: datetime.datetime | None

    def __repr__(self):
        return f"asset: {self.asset}, amount: {self.amount}, cost_basis: {self.cost_basis}," \
               f"last_sale_price: {self.last_sale_price}"

    def to_dict(self):
        """
        Creates a dictionary representing the state of this position.
        Returns a dict object of the form:
        """
        return {
            "sid": self.asset,
            "amount": self.amount,
            "cost_basis": self.cost_basis,
            "last_sale_price": self.last_sale_price,
        }
