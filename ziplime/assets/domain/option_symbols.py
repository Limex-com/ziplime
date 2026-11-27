"""OCC symbol formatting and parsing."""

import datetime
import re
from ziplime.assets.domain.option_type import OptionType


_OCC = re.compile(r"^(?P<root>[A-Z0-9.\-]{1,6})(?P<expiry>\d{6})(?P<side>[CP])(?P<strike>\d{8})$")


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
