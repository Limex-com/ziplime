"""Multi-leg option structures: how to build them, and what they are worth.

An option strategy is a set of legs with signed sizes, and almost everything anyone wants to know
about one -- where it breaks even, the most it can make, the most it can lose, what it is worth if
the underlying finishes here -- follows from the legs by arithmetic. This module does that
arithmetic once, instead of once per strategy file.

Two numbers that are constantly confused and are kept apart here:

* **payoff** is what the structure is worth at expiry, before what was paid for it. Never negative
  for a long option, always non-positive for a short one.
* **profit** is payoff minus the net premium. This is the P&L, and it is what breakevens, maximum
  profit and maximum loss are all measured on.

Everything is quoted **per one unit of the structure** in money, with the contract multiplier
already applied: an iron condor whose maximum loss is 400 loses 400 per condor, not 4.00. Size it
by multiplying, or pass ``quantity`` to :meth:`OptionStrategy.orders`.

Payoffs are exact rather than sampled. A piecewise-linear function of the underlying bends only at
the strikes, so the breakevens are solved on the segments between them and the extremes are
evaluated at the kinks -- which is why a wide iron condor reports its true maximum loss instead of
whatever the sampling grid happened to land on.
"""
import dataclasses
import math
from collections.abc import Iterable, Sequence

from ziplime.assets.domain.option_type import OptionType
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.options.chain import OptionChain, contract_of
from ziplime.finance.options.greeks import Greeks, aggregate_greeks, greeks, position_greeks

#: How far beyond the outermost strike the payoff is probed when deciding whether a structure's
#: profit is unbounded. Anything that is still rising 50% past the widest strike is rising forever:
#: the payoff is linear out there, so one point past the last kink settles it.
_UNBOUNDED_PROBE = 1.5


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

    def __post_init__(self):
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

    def __post_init__(self):
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


# -- constructors ---------------------------------------------------------------------------
#
# Each takes listings already chosen from a chain, so selection (by strike, by offset, by delta)
# stays in OptionChain and structure stays here.


def _leg(listing: ExchangeAsset, ratio: float) -> OptionLeg:
    return OptionLeg(listing=listing, ratio=ratio)


def single(listing: ExchangeAsset, ratio: float = 1.0) -> OptionStrategy:
    """One option, long or short. The degenerate case, so the analytics work on it too."""
    contract = contract_of(listing)
    side = "long" if ratio > 0 else "short"
    return OptionStrategy(name=f"{side} {contract.option_type.value.lower()} {contract.strike:g}",
                          legs=(_leg(listing, ratio),))


def vertical_spread(long_listing: ExchangeAsset, short_listing: ExchangeAsset) -> OptionStrategy:
    """Long one strike, short another of the same side and expiry.

    Direction follows from the strikes rather than being named: long the lower call is a bull call
    spread, long the higher put is a bear put spread. Both are defined-risk, which is what makes
    them the workhorse 0DTE structure.

    Raises:
        ValueError: if the legs are different sides or the same strike.
    """
    long_contract, short_contract = contract_of(long_listing), contract_of(short_listing)
    if long_contract.option_type is not short_contract.option_type:
        raise ValueError(
            f"A vertical spread is one side: got a {long_contract.option_type.value.lower()} and a "
            f"{short_contract.option_type.value.lower()}. For two sides you want a strangle, a "
            f"risk reversal or an iron condor.")
    if long_contract.strike == short_contract.strike:
        raise ValueError(f"A vertical spread needs two strikes; both legs are "
                         f"{long_contract.strike:g}.")
    kind = "call" if long_contract.is_call else "put"
    direction = "bull" if long_contract.strike < short_contract.strike else "bear"
    return OptionStrategy(
        name=f"{direction} {kind} spread {long_contract.strike:g}/{short_contract.strike:g}",
        legs=(_leg(long_listing, 1.0), _leg(short_listing, -1.0)))


def straddle(call_listing: ExchangeAsset, put_listing: ExchangeAsset,
             ratio: float = 1.0) -> OptionStrategy:
    """A call and a put at the same strike. ``ratio=-1`` sells it.

    Raises:
        ValueError: if the strikes differ -- that is a strangle, not a straddle.
    """
    call, put = contract_of(call_listing), contract_of(put_listing)
    _require_sides(call, put)
    if call.strike != put.strike:
        raise ValueError(f"A straddle shares one strike; got {call.strike:g} and {put.strike:g}. "
                         f"Two different strikes is a strangle.")
    side = "long" if ratio > 0 else "short"
    return OptionStrategy(name=f"{side} straddle {call.strike:g}",
                          legs=(_leg(call_listing, ratio), _leg(put_listing, ratio)))


def strangle(call_listing: ExchangeAsset, put_listing: ExchangeAsset,
             ratio: float = 1.0) -> OptionStrategy:
    """An out-of-the-money call and put. ``ratio=-1`` sells it."""
    call, put = contract_of(call_listing), contract_of(put_listing)
    _require_sides(call, put)
    side = "long" if ratio > 0 else "short"
    return OptionStrategy(name=f"{side} strangle {put.strike:g}/{call.strike:g}",
                          legs=(_leg(call_listing, ratio), _leg(put_listing, ratio)))


def butterfly(lower: ExchangeAsset, body: ExchangeAsset, upper: ExchangeAsset,
              ratio: float = 1.0) -> OptionStrategy:
    """Long the wings, short two of the body -- all one side, three strikes.

    ``ratio=-1`` inverts it into a short butterfly. Maximum value at expiry is at the body strike,
    which is what makes it the structure for "it finishes right about here".
    """
    contracts = [contract_of(listing) for listing in (lower, body, upper)]
    sides = {contract.option_type for contract in contracts}
    if len(sides) > 1:
        raise ValueError("A butterfly is one side; use iron_butterfly for calls and puts together.")
    strikes = [contract.strike for contract in contracts]
    if not strikes[0] < strikes[1] < strikes[2]:
        raise ValueError(f"A butterfly needs ascending strikes; got {strikes}.")
    return OptionStrategy(
        name=f"{'long' if ratio > 0 else 'short'} butterfly {strikes[0]:g}/{strikes[1]:g}/{strikes[2]:g}",
        legs=(_leg(lower, ratio), _leg(body, -2.0 * ratio), _leg(upper, ratio)))


def iron_condor(long_put: ExchangeAsset, short_put: ExchangeAsset,
                short_call: ExchangeAsset, long_call: ExchangeAsset) -> OptionStrategy:
    """Sell a put spread and a call spread around the money; the long wings cap the risk.

    Listed in strike order, lowest first. The classic premium-selling structure and the one most
    0DTE volume is in, because it is defined-risk on both sides and every leg expires today.

    Raises:
        ValueError: if the four strikes are not ascending or the sides are wrong.
    """
    lp, sp, sc, lc = (contract_of(listing)
                      for listing in (long_put, short_put, short_call, long_call))
    if not (lp.is_put and sp.is_put):
        raise ValueError("The two lower legs of an iron condor are puts.")
    if not (sc.is_call and lc.is_call):
        raise ValueError("The two upper legs of an iron condor are calls.")
    strikes = [lp.strike, sp.strike, sc.strike, lc.strike]
    if not strikes[0] < strikes[1] < strikes[2] < strikes[3]:
        raise ValueError(
            f"An iron condor needs ascending strikes (long put, short put, short call, long call); "
            f"got {strikes}.")
    return OptionStrategy(
        name=f"iron condor {strikes[0]:g}/{strikes[1]:g}/{strikes[2]:g}/{strikes[3]:g}",
        legs=(_leg(long_put, 1.0), _leg(short_put, -1.0),
              _leg(short_call, -1.0), _leg(long_call, 1.0)))


def iron_butterfly(long_put: ExchangeAsset, short_put: ExchangeAsset,
                   short_call: ExchangeAsset, long_call: ExchangeAsset) -> OptionStrategy:
    """An iron condor whose short strikes coincide -- maximum credit, narrowest profit zone.

    Built here rather than delegated to :func:`iron_condor`, and that is the whole of the fix this
    function needed: a condor requires its four strikes to be *strictly* ascending, an iron
    butterfly requires the middle two to be *equal*, and delegating meant every call raised
    "An iron condor needs ascending strikes ... got [300, 310, 310, 320]" -- a message about a
    structure the caller had not asked for, describing as a mistake the one thing that makes this
    one what it is. The function could not succeed for any input.

    Raises:
        ValueError: if the sides are wrong, the short legs are not at one strike, or the wings do
            not sit outside it.
    """
    lp, sp, sc, lc = (contract_of(listing)
                      for listing in (long_put, short_put, short_call, long_call))
    if not (lp.is_put and sp.is_put):
        raise ValueError("The two lower legs of an iron butterfly are puts.")
    if not (sc.is_call and lc.is_call):
        raise ValueError("The two upper legs of an iron butterfly are calls.")
    if sp.strike != sc.strike:
        raise ValueError(f"An iron butterfly's short legs share a strike; got {sp.strike:g} and "
                         f"{sc.strike:g}. Different strikes make it an iron condor.")
    body = sp.strike
    if not lp.strike < body < lc.strike:
        raise ValueError(
            f"An iron butterfly's wings sit outside its body; got put wing {lp.strike:g}, body "
            f"{body:g}, call wing {lc.strike:g}. A wing at or inside the body caps nothing.")
    return OptionStrategy(
        name=f"iron butterfly {lp.strike:g}/{body:g}/{lc.strike:g}",
        legs=(_leg(long_put, 1.0), _leg(short_put, -1.0),
              _leg(short_call, -1.0), _leg(long_call, 1.0)))


def risk_reversal(long_call: ExchangeAsset, short_put: ExchangeAsset) -> OptionStrategy:
    """Long an out-of-the-money call financed by a short out-of-the-money put.

    Roughly the payoff of owning the underlying, for little or no premium, and with the downside
    fully retained -- which is exactly why the maximum loss is large and finite rather than absent.
    """
    call, put = contract_of(long_call), contract_of(short_put)
    _require_sides(call, put)
    return OptionStrategy(name=f"risk reversal {put.strike:g}/{call.strike:g}",
                          legs=(_leg(long_call, 1.0), _leg(short_put, -1.0)))


def ratio_spread(long_listing: ExchangeAsset, short_listing: ExchangeAsset,
                 short_ratio: float = 2.0) -> OptionStrategy:
    """Long one, short ``short_ratio`` further out. Cheap, and the short side is naked.

    The maximum loss is unbounded on the short wing, and :meth:`OptionStrategy.max_loss` returns
    ``-inf`` to say so. On a 0DTE contract that is not a theoretical caveat: the whole move happens
    inside the session the structure was opened in.
    """
    long_contract, short_contract = contract_of(long_listing), contract_of(short_listing)
    if long_contract.option_type is not short_contract.option_type:
        raise ValueError("A ratio spread is one side.")
    if short_ratio <= 0:
        raise ValueError(f"short_ratio is how many are sold and must be positive; got {short_ratio}.")
    kind = "call" if long_contract.is_call else "put"
    return OptionStrategy(
        name=f"1x{short_ratio:g} {kind} ratio spread "
             f"{long_contract.strike:g}/{short_contract.strike:g}",
        legs=(_leg(long_listing, 1.0), _leg(short_listing, -short_ratio)))


def custom(name: str, legs: Iterable[tuple[ExchangeAsset, float]]) -> OptionStrategy:
    """Any structure, as ``(listing, ratio)`` pairs, with the same analytics as the named ones."""
    return OptionStrategy(name=name,
                          legs=tuple(_leg(listing, ratio) for listing, ratio in legs))


def _require_sides(call, put) -> None:
    if not call.is_call:
        raise ValueError(f"Expected a call, got a put at {call.strike:g}.")
    if not put.is_put:
        raise ValueError(f"Expected a put, got a call at {put.strike:g}.")


# -- convenience over a chain ----------------------------------------------------------------


def condor_around(chain: OptionChain, spot: float, *, body_steps: int = 1,
                  wing_steps: int = 3) -> OptionStrategy:
    """An iron condor centred on the money, measured in listed strikes.

    ``body_steps`` is how far out the short strikes sit and ``wing_steps`` the long ones, so the
    default sells one strike out and buys three. Convenient, and the one structure worth a
    shortcut: it is most of what 0DTE premium selling is.
    """
    if wing_steps <= body_steps:
        raise ValueError(
            f"The long wings sit outside the short strikes: wing_steps={wing_steps} must exceed "
            f"body_steps={body_steps}, or the structure is inverted.")
    return iron_condor(
        long_put=chain.strike_offset(spot, OptionType.PUT, wing_steps),
        short_put=chain.strike_offset(spot, OptionType.PUT, body_steps),
        short_call=chain.strike_offset(spot, OptionType.CALL, body_steps),
        long_call=chain.strike_offset(spot, OptionType.CALL, wing_steps),
    )
