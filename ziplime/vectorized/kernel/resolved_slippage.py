"""Slippage parameters consumed by the vector kernel."""

from __future__ import annotations
import dataclasses


@dataclasses.dataclass(frozen=True)
class ResolvedSlippage:
    """A slippage model reduced to what the kernel needs: a price rule and a volume cap."""

    #: Fraction of the price added when buying, subtracted when selling. 0.0005 = 5bps.
    percentage: float = 0.0
    #: Absolute spread; half of it is added when buying, subtracted when selling.
    spread: float = 0.0
    #: Largest share of a bar's volume one order may take, or None for no cap.
    volume_limit: float | None = None

    def fill_price(self, price: float, direction: int) -> float:
        """The price an order of this direction fills at. `direction` is +1 buy, -1 sell.

        Mirrors `FixedBasisPointsSlippage.process_order` (`price + price * percentage * direction`)
        and `FixedSlippage.process_order` (`price + spread / 2 * direction`). Both, so a model
        carrying each is expressed without a second code path.
        """
        return price + price * self.percentage * direction + self.spread / 2.0 * direction

    def cap(self, wanted: float, volume: float | None) -> float:
        """How much of `wanted` the bar can absorb. Magnitudes, not signed amounts.

        A cap never turns an order around: the event-driven model clamps remaining capacity at
        zero for exactly this reason, and the same clamp is here. `None` volume means the caller
        gave no volume frame, which is "no opinion" rather than "a bar that trades nothing" --
        capping to zero there would silently stop every strategy that did not pass volumes.
        """
        if self.volume_limit is None or volume is None:
            return wanted
        return min(wanted, max(0.0, float(volume) * self.volume_limit))
