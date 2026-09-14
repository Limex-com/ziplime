"""Selecting contracts out of an option chain.

A 0DTE strategy cannot name the contracts it trades. The chain is listed fresh every session and
every symbol in it changes daily -- ``SPY240614C00523000`` exists for one day and is never seen
again -- so a strategy says *which* contract it wants ("the 25-delta put", "two strikes above the
money", "the wing five dollars out") and something has to turn that into a listing. That is what
this module is.

Selection is deliberately explicit about ties and misses. Asking for a strike a chain does not
carry returns the nearest one and says which, rather than silently trading a different contract
from the one the strategy's numbers were computed for.
"""
import dataclasses
import datetime
from collections.abc import Iterable, Sequence

from ziplime.assets.domain.option_type import OptionType
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.option_contract import OptionContract
from ziplime.finance.options.greeks import greeks


def contract_of(listing: ExchangeAsset) -> OptionContract:
    """The :class:`OptionContract` behind a listing, or a legible error.

    Every helper here starts with this, because the failure it catches -- handing an equity listing
    to an option helper -- otherwise surfaces as ``AttributeError: 'Equity' object has no attribute
    'strike'`` several frames away.
    """
    instrument = getattr(listing, "asset", None)
    if not isinstance(instrument, OptionContract):
        raise TypeError(
            f"{getattr(listing, 'symbol', listing)!r} is not an option listing: its instrument is "
            f"{type(instrument).__name__}, not OptionContract.")
    return instrument


@dataclasses.dataclass(frozen=True)
class OptionChain:
    """The contracts listed on one underlying for one expiry, with the selectors a strategy needs.

    Built by :meth:`ziplime.trading.trading_algorithm.TradingAlgorithm.option_chain`, which is what
    a strategy calls. Constructing one directly is fine too -- it is a plain value object over a
    list of listings.

    Attributes:
        underlying_symbol: Ticker the chain is written on.
        expiration_date: The single expiry these contracts share. A chain spanning expiries is not
            one chain; :meth:`expiring_on` splits it.
        listings: The contracts, in no particular order. Use the selectors rather than indexing.
    """

    underlying_symbol: str
    expiration_date: datetime.date
    listings: tuple[ExchangeAsset, ...]

    @classmethod
    def from_listings(cls, listings: Iterable[ExchangeAsset],
                      expiration_date: datetime.date | None = None) -> "OptionChain":
        """Build a chain from listings, optionally keeping only one expiry.

        Raises:
            ValueError: if the listings are empty, span more than one underlying, or -- when
                ``expiration_date`` is not given -- span more than one expiry.
        """
        kept = [listing for listing in listings
                if expiration_date is None
                or contract_of(listing).expiration_date == expiration_date]
        if not kept:
            raise ValueError(
                f"No option listings"
                f"{f' expiring {expiration_date}' if expiration_date else ''}. A 0DTE chain exists "
                f"only on its own expiration session -- check the session, not the universe.")
        underlyings = {contract_of(listing).underlying_symbol for listing in kept}
        if len(underlyings) > 1:
            raise ValueError(f"A chain covers one underlying; got {sorted(underlyings)}.")
        expiries = {contract_of(listing).expiration_date for listing in kept}
        if len(expiries) > 1:
            raise ValueError(
                f"A chain covers one expiry; got {sorted(expiries)}. Pass expiration_date to pick "
                f"one, or call expiring_on.")
        return cls(underlying_symbol=underlyings.pop(), expiration_date=expiries.pop(),
                   listings=tuple(kept))

    def expiring_on(self, expiration_date: datetime.date) -> "OptionChain":
        """The sub-chain for one expiry."""
        return OptionChain.from_listings(self.listings, expiration_date=expiration_date)

    def is_zero_dte_on(self, session: datetime.date) -> bool:
        """Whether this chain expires on ``session``."""
        return self.expiration_date == session

    def __len__(self) -> int:
        return len(self.listings)

    def __iter__(self):
        return iter(self.listings)

    # -- filtering -------------------------------------------------------------------------

    def of_type(self, option_type: OptionType) -> list[ExchangeAsset]:
        """Every listing of one side, ordered by strike."""
        return sorted((listing for listing in self.listings
                       if contract_of(listing).option_type is option_type),
                      key=lambda listing: contract_of(listing).strike)

    @property
    def calls(self) -> list[ExchangeAsset]:
        return self.of_type(OptionType.CALL)

    @property
    def puts(self) -> list[ExchangeAsset]:
        return self.of_type(OptionType.PUT)

    @property
    def strikes(self) -> list[float]:
        """Every distinct strike in the chain, ascending."""
        return sorted({contract_of(listing).strike for listing in self.listings})

    # -- selection -------------------------------------------------------------------------

    def at_strike(self, strike: float, option_type: OptionType) -> ExchangeAsset | None:
        """The contract at exactly ``strike``, or ``None``.

        Exact to a tenth of a cent, which is the resolution an OCC symbol carries. Use
        :meth:`nearest_strike` when the strike was computed rather than read off the chain.
        """
        for listing in self.of_type(option_type):
            if abs(contract_of(listing).strike - strike) < 5e-4:
                return listing
        return None

    def nearest_strike(self, strike: float, option_type: OptionType) -> ExchangeAsset:
        """The listed contract closest to ``strike``.

        Ties go to the lower strike, so the choice is deterministic across runs rather than
        depending on the order the chain came back in.

        Raises:
            ValueError: if the chain has no contract of that side at all.
        """
        candidates = self.of_type(option_type)
        if not candidates:
            raise ValueError(f"Chain {self.underlying_symbol} {self.expiration_date} has no "
                             f"{option_type.value.lower()}s.")
        return min(candidates,
                   key=lambda listing: (abs(contract_of(listing).strike - strike),
                                        contract_of(listing).strike))

    def atm(self, spot: float, option_type: OptionType) -> ExchangeAsset:
        """The contract whose strike is nearest the underlying."""
        return self.nearest_strike(spot, option_type)

    def strike_offset(self, spot: float, option_type: OptionType, steps: int) -> ExchangeAsset:
        """``steps`` strikes away from the money, counted in listed strikes rather than in money.

        Positive is further out of the money for either side -- ``steps=2`` on a put is two strikes
        *below* the money, on a call two strikes above -- so a structure reads the same whichever
        wing it is on. Clamped at the ends of the chain rather than raising, because a chain
        generated around the money genuinely runs out, and a strategy asking for a wing past the
        edge wants the edge.
        """
        strikes = self.strikes
        if not strikes:
            raise ValueError(f"Chain {self.underlying_symbol} {self.expiration_date} is empty.")
        atm_strike = contract_of(self.atm(spot, option_type)).strike
        index = strikes.index(atm_strike)
        target = index + steps * option_type.sign
        target = max(0, min(len(strikes) - 1, target))
        return self.nearest_strike(strikes[target], option_type)

    def nearest_delta(self, target_delta: float, option_type: OptionType, *, spot: float,
                      years_to_expiry: float, volatility: float,
                      rate: float = 0.0) -> ExchangeAsset:
        """The contract whose Black-Scholes delta is closest to ``target_delta``.

        ``target_delta`` is given as a magnitude -- pass ``0.25`` for "the 25-delta put", not
        ``-0.25``; the side already says the sign.

        This is a *model* delta at the volatility you pass, not a quoted one. On a 0DTE chain that
        is a real limitation and a visible one: with minutes left almost every contract has a delta
        of 0 or 1 and "the 25-delta put" stops being a meaningful way to name a strike. Selecting
        by :meth:`strike_offset` or by moneyness degrades far more gracefully near the close.
        """
        magnitude = abs(target_delta)
        candidates = self.of_type(option_type)
        if not candidates:
            raise ValueError(f"Chain {self.underlying_symbol} {self.expiration_date} has no "
                             f"{option_type.value.lower()}s.")

        def distance(listing: ExchangeAsset) -> tuple[float, float]:
            contract = contract_of(listing)
            delta = greeks(option_type, spot, contract.strike, years_to_expiry, rate,
                           volatility).delta
            return abs(abs(delta) - magnitude), contract.strike

        return min(candidates, key=distance)

    def spot_from(self, prices: dict[int, float]) -> float | None:
        """Infer the underlying's price from put-call parity across the chain.

        Useful when the chain is all you have -- a quote feed that carries options but not their
        underlying. Uses the strike where the call and put prices are closest, which is the one
        nearest the money and therefore the one where parity is least distorted by discounting.
        Returns ``None`` if no strike has both sides quoted.
        """
        best = None
        for strike in self.strikes:
            call = self.at_strike(strike, OptionType.CALL)
            put = self.at_strike(strike, OptionType.PUT)
            if call is None or put is None:
                continue
            call_price, put_price = prices.get(call.sid), prices.get(put.sid)
            if call_price is None or put_price is None:
                continue
            gap = abs(call_price - put_price)
            if best is None or gap < best[0]:
                best = (gap, strike + call_price - put_price)
        return None if best is None else best[1]


def group_by_expiry(listings: Sequence[ExchangeAsset]) -> dict[datetime.date, OptionChain]:
    """Split a flat list of option listings into one chain per expiry."""
    expiries = sorted({contract_of(listing).expiration_date for listing in listings})
    return {expiry: OptionChain.from_listings(listings, expiration_date=expiry)
            for expiry in expiries}
