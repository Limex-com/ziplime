"""CME/NYMEX futures conventions, for the US contracts Finam mirrors.

Finam carries a slice of the CME complex alongside MOEX, which makes cross-venue work possible:
MOEX's natural gas future and NYMEX's Henry Hub future track the same underlying and are both
quoted in dollars, so the basis between them is a real, tradable number.

Two things differ from FORTS and both matter when parsing:

* the ticker carries a **two-digit** year (``NGZ25``, not FORTS' ``NGZ5``), so there is no decade
  ambiguity to resolve;
* the contract is **physically delivered** and margined in **dollars**, where MOEX's gas contract
  is cash-settled and margined in roubles;
* the contract name ends in ``MonYY`` (``NATURAL GAS HENRY HUB FUTURE Dec25``) rather than
  ``ROOT-M.YY``.

Expiration dates come from ``GetAsset``, which answers for archived contracts too. No local rule
reproduces the CME calendar -- Henry Hub gas stops trading three business days before the delivery
month starts -- so nothing here tries to.
"""
import datetime
import re

from ziplime.assets.domain.settlement_type import SettlementType
from ziplime.data.data_sources.finam.moex_futures import (
    MONTH_CODES, MONTH_NUMBER_TO_CODE, ExpiryRule, MoexFuturesRoot,
)

#: Sessions before the last trading day at which a deliverable position is force-closed.
#:
#: NYMEX Henry Hub gas stops trading three business days before the delivery month begins, and a
#: position carried past that becomes an obligation to deliver or receive physical gas. Closing a
#: few sessions early is what a trader who does not own a pipeline actually does.
DELIVERY_NOTICE_SESSIONS = 3

#: NYMEX, where Finam lists Henry Hub natural gas and the crude complex.
XNYM_MIC = "XNYM"
#: COMEX (metals) and CME proper, listed here for completeness of the MIC set Finam returns.
XCEC_MIC = "XCEC"
XCME_MIC = "XCME"

#: ``NG`` + month code + two-digit year. Anchored, so the many ``NGT``/``QG`` variants Finam also
#: carries -- trade-at-settlement and e-mini products -- do not match.
CME_TICKER_RE = re.compile(r"^(?P<root>[A-Z]{1,3})(?P<month>[FGHJKMNQUVXZ])(?P<year>\d{2})$")

#: ``... Dec25`` at the end of the instrument name.
CME_NAME_RE = re.compile(r"(?P<month>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
                         r"(?P<year>\d{2})\s*$")
_MONTH_ABBREVIATIONS = {name: number for number, name in enumerate(
    ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"), start=1)}

ALL_MONTHS = tuple(range(1, 13))


#: Root symbols on a non-MOEX venue are namespaced ``CODE.MIC``.
#:
#: The venue's own code is not unique across exchanges -- ``NG`` is natural gas on both MOEX and
#: NYMEX, and they are different contracts with different sizes and expiries. ``futures_root_symbols``
#: keys a chain by root symbol alone, so without a namespace the second venue ingested would
#: overwrite the first. MOEX keeps its bare codes as the connector's primary venue.
def namespaced_root(code: str, mic: str) -> str:
    """Return the ziplime root symbol for a venue's own root code."""
    return f"{code}.{mic}"


#: CME roots Finam mirrors. Multipliers are the value of one price point in the quote currency:
#: Henry Hub gas is quoted in $/MMBtu over a 10 000 MMBtu contract, so a one-cent move is $100.
CME_FUTURES_ROOTS: dict[str, MoexFuturesRoot] = {
    namespaced_root("NG", XNYM_MIC): MoexFuturesRoot(
        root_symbol=namespaced_root("NG", XNYM_MIC),
        description="Henry Hub natural gas (NYMEX)",
        multiplier=10_000.0, tick_size=0.001, quote_currency="USD",
        chain_months=ALL_MONTHS, expiry_rule=ExpiryRule.FROM_API, fx_dependent=False,
        mic=XNYM_MIC,
        # Henry Hub gas is physically delivered, and CME collects margin in dollars -- unlike
        # MOEX, which quotes its gas contract in dollars but margins it in roubles.
        settlement_type=SettlementType.PHYSICAL,
        margin_currency="USD",
    ),
}


def parse_cme_ticker(ticker: str, mic: str = XNYM_MIC) -> tuple[str, int, int] | None:
    """Split a CME ticker into ``(namespaced root, month, year)``; ``None`` if it is not one.

    The two-digit year is unambiguous, unlike FORTS' single digit.
    """
    match = CME_TICKER_RE.match(ticker or "")
    if match is None:
        return None
    return (namespaced_root(match.group("root"), mic), MONTH_CODES[match.group("month")],
            2000 + int(match.group("year")))


def parse_cme_contract_name(name: str) -> tuple[int, int] | None:
    """Read ``(month, year)`` off a CME contract name such as ``... Dec25``."""
    match = CME_NAME_RE.search(name or "")
    if match is None:
        return None
    return _MONTH_ABBREVIATIONS[match.group("month")], 2000 + int(match.group("year"))


def format_cme_ticker(root_symbol: str, month: int, year: int) -> str:
    """Build a CME ticker, e.g. ``('NG.XNYM', 12, 2025)`` -> ``'NGZ25'``."""
    code = root_symbol.split(".", 1)[0]
    return f"{code}{MONTH_NUMBER_TO_CODE[month]}{year % 100:02d}"


def cme_fallback_expiry(month: int, year: int) -> datetime.date:
    """Last-resort expiry when the API gives none: the 25th of the preceding month.

    Only a placeholder. Henry Hub gas stops trading three business days before the delivery month
    begins, which lands near the 25th-28th, but the real dates come from ``GetAsset`` and this is
    never used for a contract the API knows about.
    """
    month, year = (12, year - 1) if month == 1 else (month - 1, year)
    return datetime.date(year, month, 25)
