"""Option sensitivity values and arithmetic."""

import dataclasses


@dataclasses.dataclass(frozen=True)
class Greeks:
    """One option's price and sensitivities, per unit of the underlying.

    See the module docstring for the scaling of each field; they are ``vollib``'s conventions and
    they are not all per-year or all per-1.00.
    """

    price: float
    delta: float
    gamma: float
    #: Per calendar day.
    theta: float
    #: Per volatility point.
    vega: float
    #: Per percentage point of the interest rate.
    rho: float
    #: The volatility these were computed at. Echoed back so a caller that passed an implied
    #: volatility solved from a market price does not have to carry it alongside.
    volatility: float
    #: Years to expiry the figures were computed for. Zero means the contract has expired and the
    #: figures are the expiry limits.
    time_to_expiry: float

    @property
    def is_expired(self) -> bool:
        return self.time_to_expiry <= 0.0
