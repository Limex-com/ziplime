"""Option leg for strategies."""

import dataclasses
from ziplime.assets.domain.option_type import OptionType
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.options.chain import contract_of


@dataclasses.dataclass(frozen=True)
class OptionLeg:
    """One contract and how many of it the structure holds.

    Attributes:
        listing: The option listing.
        ratio: Signed contracts per unit of the structure. ``+1`` long one, ``-2`` short two.
            Fractional ratios are allowed and are how a 1x2 ratio spread is expressed per unit.
    """

    listing: ExchangeAsset
    ratio: float

    def __post_init__(self) -> None:
        contract_of(self.listing)  # rejects a non-option listing at construction
        if self.ratio == 0:
            raise ValueError(f"A leg of zero contracts is not a leg ({self.listing.symbol}).")

    @property
    def contract(self):
        return contract_of(self.listing)

    @property
    def strike(self) -> float:
        return self.contract.strike

    @property
    def option_type(self) -> OptionType:
        return self.contract.option_type

    @property
    def multiplier(self) -> float:
        return self.contract.multiplier

    def payoff_at(self, underlying_price: float) -> float:
        """What this leg is worth at expiry, in money, per unit of the structure."""
        return (self.contract.intrinsic_value(underlying_price) * self.multiplier * self.ratio)
