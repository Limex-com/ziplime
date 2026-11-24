"""Liquidity model for synthetic."""

import dataclasses
import math


@dataclasses.dataclass(frozen=True)
class LiquidityModel:
    """Volume, open interest and spreads, shaped the way a 0DTE chain's actually are.

    None of this is calibrated. It exists so that liquidity-aware code -- selecting a strike that
    trades, sizing against volume, rejecting a spread too wide to cross -- has something with the
    right *shape* to work against, and so that a strategy which ignores liquidity entirely does
    not look identical to one that does not.

    Attributes:
        atm_volume: Contracts traded in an at-the-money strike **in one bar**, at the busiest part
            of the session. It is per bar, so it has to match the emission rate: 400 is a
            plausible minute on a liquid US chain, and a few thousand a plausible day on a MOEX
            one. Leaving a minute-sized figure on daily bars caps every fill at a handful of
            contracts and silently makes position sizing irrelevant -- whatever a strategy asks
            for, the volume limit decides.
        moneyness_width: Standard deviation, in percent away from the money, of the volume
            profile across strikes.
        relative_spread: Half the bid-ask as a fraction of the premium.
        minimum_half_spread: Floor on the half-spread, in price units. A one-cent market is the
            tightest anything quotes.
    """

    atm_volume: float = 400.0
    moneyness_width: float = 1.1
    relative_spread: float = 0.02
    minimum_half_spread: float = 0.005

    def volume(self, spot: float, strike: float, session_fraction: float) -> float:
        """Contracts traded in this bar, by distance from the money and time of day."""
        if spot <= 0:
            return 0.0
        x = (strike - spot) / spot * 100.0
        across_strikes = math.exp(-0.5 * (x / self.moneyness_width) ** 2)
        # A U through the session: busy at the open, quiet at lunch, busiest into the close, which
        # for 0DTE is far more pronounced than for anything else -- the contract is expiring.
        f = min(max(session_fraction, 0.0), 1.0)
        through_the_day = 0.6 + 1.4 * (f ** 3) + 0.5 * math.exp(-((f / 0.15) ** 2))
        return self.atm_volume * across_strikes * through_the_day

    def half_spread(self, price: float) -> float:
        return max(self.minimum_half_spread, price * self.relative_spread)
