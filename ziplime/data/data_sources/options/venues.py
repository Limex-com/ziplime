"""Option conventions per venue, so a source states where it is rather than what it assumes.

An option is five conventions, not one instrument, and a venue fixes all five at once: how the
premium settles, whether it can be exercised early, what it settles into, how many units a
contract covers, and what the contract is called. Get any one of them wrong and the book is wrong
in a way that does not raise.

The one preset here was **measured** against the live Limex reference feed rather than looked up:

=====================  ============================
                       ``OPRA`` (SPY)
=====================  ============================
premium                UPFRONT -- paid at trade
exercise               AMERICAN
settlement             PHYSICAL
contract covers        100 shares
named                  ``SPY   260914C00765000``
listed on              ``OPRA``
=====================  ============================

Other venues disagree on every one of those, and they are added from outside rather than listed
here: build an :class:`OptionVenue` and hand it to :func:`register_venue`. A venue whose contracts
are margined, European, cash settled and natively named is a row, not a branch -- which is the
whole reason the conventions are data. Venues for a particular market live with that market's
connector; ziplime carries the one its own sources speak.

One measured trap is worth naming, because it is what makes ``contract_size`` a venue property
rather than a feed field. At least one feed reports ``multiplier = 0`` and puts the real size in
``contract_size``. An adapter that maps fields by name therefore builds a contract whose premium,
value and settlement are all zero -- silently. :class:`OptionVenue` takes the size from the venue
instead, and :class:`~ziplime.assets.entities.option_contract.OptionContract` refuses a zero
multiplier outright.
"""
import dataclasses
import datetime
from collections.abc import Callable

from ziplime.assets.domain.exercise_style import ExerciseStyle
from ziplime.assets.domain.option_type import OptionType
from ziplime.assets.domain.premium_style import PremiumStyle
from ziplime.assets.domain.settlement_type import SettlementType
from ziplime.data.data_sources.options.source import ContractSpec

@dataclasses.dataclass(frozen=True)
class OptionVenue:
    """The conventions one venue lists options under.

    Attributes:
        name: Short identifier, e.g. ``"OPRA"``.
        mic: Default market identifier for the listings. Not always the venue's own name -- MOEX
            options are listed under ``RTSX``, the derivatives section. A caller may override it:
            which exchange row a listing attaches to is a deployment question (it decides the
            calendar), while the conventions below are the instrument.
        premium_style: Paid at the trade, or margined daily.
        exercise_style: European or American.
        settlement_type: What the contract settles into.
        contract_size: Units of the underlying per contract. Used as the multiplier, because a
            feed's own ``multiplier`` field cannot be relied on -- see the module docstring.
        tick_size: Minimum price increment.
        symbol_format: Name of the naming scheme, for reporting. ``"occ"`` unless a
            ``symbol_formatter`` says otherwise.
        symbol_formatter: Builds the venue's native contract name from
            ``(root, strike, option_type, expiration_date)``. ``None`` means the contract is named
            by its OCC symbol, which every caller already knows how to build.
    """

    name: str
    mic: str
    premium_style: PremiumStyle
    exercise_style: ExerciseStyle
    settlement_type: SettlementType
    contract_size: float
    tick_size: float = 0.01
    symbol_format: str = "occ"
    symbol_formatter: Callable[[str, float, OptionType, datetime.date], str] | None = None

    def symbol_for(self, underlying_symbol: str, strike: float, option_type: OptionType,
                   expiration_date: datetime.date, root: str | None = None) -> str | None:
        """The venue's own name for a contract, or ``None`` when that is its OCC symbol.

        ``None`` rather than the OCC string so that :class:`ContractSpec` keeps its default and
        nothing has to remember which venues use which.
        """
        if self.symbol_formatter is None:
            return None
        return self.symbol_formatter(root or underlying_symbol[:2], strike, option_type,
                                     expiration_date)

    def contract(self, underlying_symbol: str, expiration_date: datetime.date,
                 option_type: OptionType, strike: float, listed_date: datetime.date,
                 root: str | None = None, vendor_id: str | None = None,
                 mic: str | None = None) -> ContractSpec:
        """A :class:`ContractSpec` with this venue's conventions already applied.

        This is the whole point of the class: a source names the venue once and cannot then get
        the premium style right and the exercise style wrong.
        """
        return ContractSpec(
            underlying_symbol=underlying_symbol,
            expiration_date=expiration_date,
            option_type=option_type,
            strike=strike,
            mic=mic or self.mic,
            listed_date=listed_date,
            multiplier=self.contract_size,
            tick_size=self.tick_size,
            exercise_style=self.exercise_style,
            settlement_type=self.settlement_type,
            premium_style=self.premium_style,
            symbol=self.symbol_for(underlying_symbol, strike, option_type, expiration_date, root),
            vendor_id=vendor_id,
        )


#: US equity and ETF options, as OPRA lists them. Measured on SPY.
OPRA = OptionVenue(
    name="OPRA",
    mic="OPRA",
    premium_style=PremiumStyle.UPFRONT,
    exercise_style=ExerciseStyle.AMERICAN,
    settlement_type=SettlementType.PHYSICAL,
    contract_size=100.0,
    tick_size=0.01,
    symbol_format="occ",
)

VENUES: dict[str, OptionVenue] = {OPRA.name: OPRA}


def get_venue(name: str) -> OptionVenue:
    """Look a venue up by name.

    Raises:
        KeyError: naming the venues that are known, because the answer to "which ones are there"
            is short and is what the caller wants next.
    """
    try:
        return VENUES[name.upper()]
    except KeyError:
        raise KeyError(f"Unknown option venue {name!r}. Known: {', '.join(sorted(VENUES))}.") from None


def register_venue(venue: OptionVenue, *, replace: bool = False) -> OptionVenue:
    """Add ``venue`` to the registry, so a source can name it like any built-in.

    This is how a market's own package contributes its conventions: ziplime ships the venues its
    own sources speak, and anything else -- a margined, European, natively named contract on
    another exchange -- registers itself at import time rather than being listed here.

    Args:
        replace: Permit overwriting a venue of the same name. Off by default, because two packages
            registering the same name usually means one of them is about to be silently ignored,
            and a venue is five conventions at once: the wrong one is wrong in a way that does not
            raise, it just books the wrong numbers.

    Raises:
        ValueError: when the name is taken and ``replace`` is not set.
    """
    existing = VENUES.get(venue.name)
    if existing is not None and not replace:
        if existing == venue:
            return existing
        raise ValueError(
            f"Option venue {venue.name!r} is already registered with different conventions. Pass "
            f"replace=True to override it deliberately, or register under another name.")
    VENUES[venue.name] = venue
    return venue
