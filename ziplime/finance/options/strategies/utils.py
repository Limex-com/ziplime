"""Utils for strategies."""

from collections.abc import Iterable
from ziplime.assets.domain.option_type import OptionType
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.options.chain import OptionChain, contract_of
from ziplime.finance.options.strategies.option_leg import OptionLeg
from ziplime.finance.options.strategies.option_strategy import OptionStrategy


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


def _require_sides(call: ExchangeAsset, put: ExchangeAsset) -> None:
    if not call.is_call:
        raise ValueError(f"Expected a call, got a put at {call.strike:g}.")
    if not put.is_put:
        raise ValueError(f"Expected a put, got a call at {put.strike:g}.")


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
