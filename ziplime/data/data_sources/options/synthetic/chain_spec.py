"""Chain spec for synthetic."""

import dataclasses


@dataclasses.dataclass(frozen=True)
class ChainSpec:
    """The strike grid to list each session.

    The defaults are the grid OPRA actually lists for SPY, measured from the live reference feed:
    a fine step near the money and a coarse one in the wings. A real 0DTE chain on a 764 spot ran
    155 strikes from 550 to 950 -- 51 of them at one-dollar spacing between 740 and 790, the rest
    at five.

    That two-tier shape is not cosmetic. A uniform grid either makes the wings unreachable (too
    few strikes) or the chain enormous (too many), and it misprices what a wing *is*: five dollars
    out of the money at a one-dollar step is five strikes, at a five-dollar step it is one, and
    any strategy that picks its wing by counting strikes behaves differently under the two.

    Attributes:
        near_step: Spacing near the money.
        near_reach: How far from the money, in price units, the fine step extends.
        far_step: Spacing beyond that. Set equal to ``near_step`` for a uniform grid.
        far_reach: Furthest strike from the money. The real chain reaches roughly 25% either way;
            the default is narrower because every strike is a contract to store and price, and
            nothing trades that far out on the day.
        multiplier: Units of underlying per contract.
        tick_size: Minimum price increment.
    """

    near_step: float = 1.0
    near_reach: float = 20.0
    far_step: float = 5.0
    far_reach: float = 60.0
    multiplier: float = 100.0
    tick_size: float = 0.01

    def __post_init__(self) -> None:
        if self.near_step <= 0 or self.far_step <= 0:
            raise ValueError("Strike steps must be positive.")
        if self.far_reach < self.near_reach:
            raise ValueError(
                f"far_reach ({self.far_reach}) is where the strikes stop and near_reach "
                f"({self.near_reach}) is where the fine step ends, so it cannot be the smaller.")

    def strikes_around(self, spot: float) -> list[float]:
        """The listed strikes for a session whose reference price is ``spot``.

        Centred on the nearest strike of the fine grid to ``spot``, which is how an exchange lists
        them: the grid is fixed and the money moves through it, rather than the grid being
        recentred on every print.
        """
        if spot <= 0:
            raise ValueError(f"Cannot build a chain around a non-positive price ({spot}).")
        centre = round(spot / self.near_step) * self.near_step
        strikes = {round(centre + n * self.near_step, 6)
                   for n in range(-int(self.near_reach // self.near_step),
                                  int(self.near_reach // self.near_step) + 1)}
        # The wings continue on their own coarser grid, aligned to it rather than to the centre --
        # a real chain's five-dollar strikes land on multiples of five, not on spot plus five.
        far = int(self.far_reach // self.far_step) + 1
        base = round(centre / self.far_step) * self.far_step
        strikes |= {round(base + n * self.far_step, 6) for n in range(-far, far + 1)}
        return sorted(k for k in strikes
                      if k > 0 and abs(k - centre) <= self.far_reach + 1e-9)
