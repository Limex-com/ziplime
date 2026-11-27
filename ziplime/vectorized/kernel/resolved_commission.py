"""Commission parameters consumed by the vector kernel."""

from __future__ import annotations
import dataclasses


@dataclasses.dataclass(frozen=True)
class ResolvedCommission:
    """A commission model reduced to four numbers the compiled loop can use.

    The model itself offers `calculate_for_asset`, which is already order-free -- so until the
    inner loop was compiled there was nothing to reduce and this did not exist. Numba cannot call
    into an arbitrary Python object, and a commission is charged on the path that decides whether
    the *next* order fits, so it cannot be computed afterwards either.

    Every commission model the vector path supports fits one expression:

        max(min_trade, per_unit x |quantity| + per_dollar x |value| + fixed)

    NoCommission zeroes all four; PerShare sets `per_unit` and `min_trade`; PerTrade sets `fixed`;
    PerDollar sets `per_dollar`. `CommissionParityTests` runs each model both ways and requires
    the same number, which is what keeps this from drifting away from the models it stands for.
    """

    per_unit: float = 0.0
    per_dollar: float = 0.0
    fixed: float = 0.0
    min_trade: float = 0.0

    def charge(self, quantity: float, value: float) -> float:
        """One fill's commission. Magnitudes in, non-negative out."""
        return max(self.min_trade,
                   self.per_unit * abs(quantity) + self.per_dollar * abs(value) + self.fixed)
