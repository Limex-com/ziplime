"""A single listed option contract."""
import datetime
import re
from dataclasses import dataclass

from ziplime.assets.domain.exercise_style import ExerciseStyle
from ziplime.assets.domain.option_type import OptionType
from ziplime.assets.domain.premium_style import PremiumStyle
from ziplime.assets.domain.settlement_type import SettlementType
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.exchange_asset import ExchangeAsset

#: OCC-style option symbol: root, expiry, side, strike in thousandths.
#: ``SPY240614C00523000`` is the 523.00 call on SPY expiring 2024-06-14.
_OCC = re.compile(r"^(?P<root>[A-Z0-9.\-]{1,6})(?P<expiry>\d{6})(?P<side>[CP])(?P<strike>\d{8})$")

#: Strikes are carried in the symbol as thousandths of a unit, which is what makes 523.5 and
#: 523.505 distinct symbols rather than the same one rounded.
STRIKE_SCALE = 1000


def format_occ_symbol(underlying_symbol: str, expiration_date: datetime.date,
                      option_type: OptionType, strike: float) -> str:
    """Build the OCC symbol for a contract, e.g. ``SPY240614C00523000``.

    This is the identifier every real option feed speaks, so it is what a ziplime listing is named
    by: a chain that arrives over gRPC tomorrow carries these strings, and they have to resolve to
    the same listings the synthetic generator created today.
    """
    thousandths = int(round(strike * STRIKE_SCALE))
    if thousandths <= 0:
        raise ValueError(f"Strike must be positive, got {strike}.")
    return (f"{underlying_symbol.upper()}{expiration_date:%y%m%d}"
            f"{option_type.letter}{thousandths:08d}")


def parse_occ_symbol(symbol: str) -> tuple[str, datetime.date, OptionType, float]:
    """Take an OCC symbol apart into ``(root, expiration, option type, strike)``.

    Accepts the space-padded 21-character form the OCC itself publishes as well as the compact
    form most vendors use.

    Raises:
        ValueError: if the symbol is not an OCC option symbol.
    """
    match = _OCC.match(symbol.replace(" ", "").upper())
    if match is None:
        raise ValueError(
            f"{symbol!r} is not an OCC option symbol. Expected root + YYMMDD + C/P + strike in "
            f"thousandths, for example SPY240614C00523000.")
    expiry = datetime.datetime.strptime(match["expiry"], "%y%m%d").date()
    return (match["root"], expiry, OptionType.from_letter(match["side"]),
            int(match["strike"]) / STRIKE_SCALE)


@dataclass(frozen=True)
class OptionContract(Asset):
    """One strike, one expiry, one side.

    An option is not a futures contract with a strike bolted on, and the two are accounted for
    differently at every step. A futures position has no inherent value -- it is worth the
    variation margin since the last mark, which the ledger settles daily -- while an option is
    **bought and paid for**: the premium leaves cash at the trade, and the position is an asset (or
    a liability, when short) worth ``amount x price x multiplier`` from then on. Confusing the two
    double-counts the premium.

    Attributes:
        underlying_exchange_asset: The listing whose price the payoff is measured against, when it
            is one this database carries. Required to settle at expiry, and required for any Greek:
            both need the underlying's price, and only this says where to read it.
        underlying_asset: The instrument behind that listing.
        underlying_symbol: Its ticker, carried separately because it is the root of the OCC symbol
            and is present even for an underlying the database does not list.
        option_type: Call or put.
        strike: The exercise price, in the contract's quote currency.
        expiration_date: Last session the contract trades. For the 0DTE contracts this package is
            built around, this is also the session it was listed on.
        multiplier: Units of the underlying per contract -- 100 for US equity and ETF options.
            Every cash amount an option produces goes through it: premium, value, and settlement.
        tick_size: Smallest price increment quoted.
        exercise_style: European or American; see :class:`ExerciseStyle`. Early exercise is not
            modelled, the field records the instrument.
        settlement_type: Cash settles the intrinsic value at expiry; physical delivers the
            underlying. **Only cash settlement is modelled.** A physically settled contract is
            still settled in cash at intrinsic and says so, rather than silently pretending shares
            changed hands -- which for an in-the-money SPY call is a 52 000 dollar difference in
            what the account then holds.
        premium_style: Whether the premium is paid at the trade or margined daily. This is what
            separates an OPRA book from a MOEX one and it changes the accounting completely; see
            :class:`~ziplime.assets.domain.premium_style.PremiumStyle`.
    """

    underlying_exchange_asset: ExchangeAsset | None
    underlying_asset: Asset | None
    underlying_symbol: str
    option_type: OptionType
    strike: float
    expiration_date: datetime.date
    multiplier: float = 100.0
    tick_size: float = 0.01
    exercise_style: ExerciseStyle = ExerciseStyle.AMERICAN
    settlement_type: SettlementType = SettlementType.CASH
    premium_style: PremiumStyle = PremiumStyle.UPFRONT

    def __post_init__(self):
        # Every cash amount an option produces is a price times this number: the premium paid, the
        # value of the position, the settlement at expiry. A zero multiplier makes all three zero
        # and nothing raises -- the contract simply trades for free and settles for nothing.
        #
        # This is not hypothetical. Limex's reference feed reports `multiplier = 0` for MOEX
        # options and carries the real size in `contract_size`, so an adapter that maps the fields
        # by name produces exactly this. Refuse it here, where the message can say what to look at.
        if not self.multiplier or self.multiplier <= 0:
            raise ValueError(
                f"{self.asset_name}: an option's multiplier must be positive, got "
                f"{self.multiplier!r}. Every amount the contract produces -- premium, position "
                f"value, settlement -- is multiplied by it, so zero would price the contract at "
                f"nothing in every one of them. Some feeds leave `multiplier` empty and carry the "
                f"size in `contract_size`; use that instead.")

    def __hash__(self):
        return hash(self.id)

    @property
    def occ_symbol(self) -> str:
        """This contract's OCC symbol."""
        return format_occ_symbol(self.underlying_symbol, self.expiration_date,
                                 self.option_type, self.strike)

    @property
    def is_margined(self) -> bool:
        """Whether this contract settles variation margin instead of being paid for."""
        return self.premium_style.is_margined

    @property
    def is_call(self) -> bool:
        return self.option_type is OptionType.CALL

    @property
    def is_put(self) -> bool:
        return self.option_type is OptionType.PUT

    def is_zero_dte_on(self, session: datetime.date) -> bool:
        """Whether this contract expires on ``session`` -- the defining property of a 0DTE trade."""
        return self.expiration_date == session

    def intrinsic_value(self, underlying_price: float) -> float:
        """Value of one unit at expiry against ``underlying_price``, before the multiplier."""
        return self.option_type.intrinsic_value(underlying_price, self.strike)

    def settlement_value(self, underlying_price: float) -> float:
        """What one contract pays out at expiry: intrinsic value times the multiplier."""
        return self.intrinsic_value(underlying_price) * self.multiplier

    def moneyness(self, underlying_price: float) -> float:
        """How far in the money, as a fraction of the strike.

        Positive is in the money for either side, so calls and puts can be ranked together.
        """
        if not self.strike:
            return 0.0
        return self.option_type.sign * (underlying_price - self.strike) / self.strike
