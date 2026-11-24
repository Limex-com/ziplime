"""Option strategy for strategies."""

import dataclasses
import math
from collections.abc import Sequence
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.options.greek_values import Greeks
from ziplime.finance.options.greeks import aggregate_greeks, greeks, position_greeks
from ziplime.finance.options.strategies.option_leg import OptionLeg
from ziplime.finance.options.strategies.support import _UNBOUNDED_PROBE


@dataclasses.dataclass(frozen=True)
class OptionStrategy:
    """A named set of legs, and the analytics that follow from them.

    Build one with the module-level constructors (:func:`vertical_spread`, :func:`iron_condor`,
    and so on) or directly from legs. Nothing here places an order; :meth:`orders` returns what to
    trade and the strategy file does the ordering, so a structure can be priced and rejected
    without touching the blotter.
    """

    name: str
    legs: tuple[OptionLeg, ...]

    def __post_init__(self) -> None:
        if not self.legs:
            raise ValueError("A strategy needs at least one leg.")
        expiries = {leg.contract.expiration_date for leg in self.legs}
        underlyings = {leg.contract.underlying_symbol for leg in self.legs}
        if len(underlyings) > 1:
            raise ValueError(f"A strategy spans one underlying; got {sorted(underlyings)}.")
        object.__setattr__(self, "_expiries", expiries)

    @property
    def is_single_expiry(self) -> bool:
        """False for a calendar or diagonal, whose legs expire on different days.

        :meth:`payoff_at` assumes every leg expires together, so it refuses a multi-expiry
        structure rather than reporting the payoff of one that does not exist.
        """
        return len(self._expiries) == 1

    @property
    def strikes(self) -> list[float]:
        return sorted({leg.strike for leg in self.legs})

    def orders(self, quantity: int = 1) -> list[tuple[ExchangeAsset, int]]:
        """``(listing, contracts)`` pairs to trade ``quantity`` units of the structure.

        Sizes are rounded to whole contracts, and a leg that rounds to zero is dropped with the
        rest kept -- which is worth noticing, because a ratio spread sized too small stops being a
        ratio spread. Check the result rather than assuming it has as many legs as the structure.
        """
        sized = [(leg.listing, int(round(leg.ratio * quantity))) for leg in self.legs]
        return [(listing, amount) for listing, amount in sized if amount != 0]

    # -- payoff ----------------------------------------------------------------------------

    def payoff_at(self, underlying_price: float) -> float:
        """Value of the structure at expiry, in money, before premium."""
        if not self.is_single_expiry:
            raise ValueError(
                f"{self.name} has legs expiring on {sorted(self._expiries)}. A payoff at expiry is "
                f"only defined when they expire together; price the near leg and revalue the far "
                f"one instead.")
        return math.fsum(leg.payoff_at(underlying_price) for leg in self.legs)

    def profit_at(self, underlying_price: float, net_premium: float) -> float:
        """P&L at expiry: payoff less what the structure cost.

        ``net_premium`` is what :meth:`net_premium` returns -- positive for a debit.
        """
        return self.payoff_at(underlying_price) - net_premium

    def net_premium(self, prices: dict[int, float]) -> float:
        """What the structure costs to put on, in money: positive a debit, negative a credit.

        Args:
            prices: Premium per unit of the underlying, keyed by listing sid -- what
                ``data.current(listing, "close")`` returns, not the price of a whole contract.

        Raises:
            KeyError: if a leg has no price. Deliberately not silently skipped: a condor priced on
                three of its four legs is not a condor, and the resulting credit looks plausible.
        """
        total = 0.0
        for leg in self.legs:
            try:
                price = prices[leg.listing.sid]
            except KeyError:
                raise KeyError(
                    f"No price for {leg.listing.symbol} (sid {leg.listing.sid}), which is a leg of "
                    f"{self.name}. Every leg has to be priced before the structure can be.") from None
            total += price * leg.multiplier * leg.ratio
        return total

    def payoff_curve(self, prices: Sequence[float]) -> list[float]:
        """The payoff evaluated at each price, for plotting."""
        return [self.payoff_at(price) for price in prices]

    def kinks(self) -> list[float]:
        """Underlying prices where the payoff changes slope -- the strikes, ascending."""
        return self.strikes

    def breakevens(self, net_premium: float) -> list[float]:
        """Underlying prices at expiry where the structure breaks exactly even.

        Solved on the segments between strikes rather than sampled, so a breakeven between two
        adjacent strikes is exact and one outside the strikes is found on the terminal ray.
        Returns them ascending; a structure that never crosses zero returns an empty list.
        """
        strikes = self.strikes
        span = (max(strikes) - min(strikes)) or max(strikes[0], 1.0)
        # Bounds beyond every kink, so the two terminal rays are searched as well.
        points = [max(min(strikes) - span, 0.0)] + strikes + [max(strikes) + span]

        found = []
        for left, right in zip(points, points[1:]):
            if right <= left:
                continue
            low = self.profit_at(left, net_premium)
            high = self.profit_at(right, net_premium)
            if low == 0.0:
                found.append(left)
            if low * high < 0.0:
                # The payoff is linear between kinks, so the crossing is exact.
                found.append(left + (right - left) * low / (low - high))
        if self.profit_at(points[-1], net_premium) == 0.0:
            found.append(points[-1])
        return sorted({round(price, 10) for price in found})

    def max_profit(self, net_premium: float) -> float:
        """Best P&L at expiry, or ``inf`` when unbounded (a long call has no ceiling)."""
        return self._extreme(net_premium, best=True)

    def max_loss(self, net_premium: float) -> float:
        """Worst P&L at expiry as a negative number, or ``-inf`` when unbounded.

        A naked short call is unbounded and says so. Anything that reports a finite number here is
        a defined-risk structure, which for 0DTE is the difference between a bad day and a margin
        call -- the whole loss can arrive inside one session.
        """
        return self._extreme(net_premium, best=False)

    def _extreme(self, net_premium: float, best: bool) -> float:
        """Extremes of a piecewise-linear payoff: check the kinks, then the two rays."""
        strikes = self.strikes
        span = (max(strikes) - min(strikes)) or max(strikes[0], 1.0)
        far_low = max(min(strikes) - span * _UNBOUNDED_PROBE, 0.0)
        far_high = max(strikes) + span * _UNBOUNDED_PROBE

        at_kinks = [self.profit_at(strike, net_premium) for strike in strikes]
        at_zero = self.profit_at(0.0, net_premium)
        low_ray = self.profit_at(far_low, net_premium)
        high_ray = self.profit_at(far_high, net_premium)

        # Slope of each terminal ray decides whether it runs away. The lower ray is bounded by
        # price zero -- the underlying cannot go below it -- so only the upper one can be infinite.
        rises_up = high_ray > self.profit_at(max(strikes), net_premium) + 1e-9
        falls_up = high_ray < self.profit_at(max(strikes), net_premium) - 1e-9

        candidates = at_kinks + [at_zero, low_ray, high_ray]
        if best:
            return math.inf if rises_up else max(candidates)
        return -math.inf if falls_up else min(candidates)

    # -- risk ------------------------------------------------------------------------------

    def greeks(self, *, spot: float, years_to_expiry: float, volatility: float,
               rate: float = 0.0, quantity: int = 1) -> Greeks:
        """Greeks of the whole structure, summed across legs and scaled to ``quantity`` units.

        One flat ``volatility`` for every leg. That is a simplification and a consequential one --
        a real chain is skewed, and the wings of a condor trade at a materially higher implied
        volatility than its body, so a vega computed this way understates the structure's exposure
        to the skew moving. Pass the volatility you actually mean, per structure, or read the
        legs' own implied volatilities and aggregate with
        :func:`~ziplime.finance.options.greeks.aggregate_greeks`.
        """
        return aggregate_greeks([
            position_greeks(
                greeks(leg.option_type, spot, leg.strike, years_to_expiry, rate, volatility),
                amount=leg.ratio * quantity, multiplier=leg.multiplier)
            for leg in self.legs
        ])
