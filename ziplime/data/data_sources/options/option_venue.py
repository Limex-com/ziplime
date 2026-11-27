"""Trading conventions for an option venue."""

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
