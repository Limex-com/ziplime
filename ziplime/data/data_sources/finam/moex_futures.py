"""MOEX FORTS (derivatives market, MIC ``RTSX``) contract conventions.

Contract identity comes from Finam: ``AllAssets`` enumerates every contract including archived ones,
and ``GetAsset`` returns an exact expiration date for both listed and archived contracts. This
module supplies what the API does *not*:

* parsing of FORTS tickers (``SiZ6``) and contract names (``Si-12.26``),
* :data:`MOEX_FUTURES_ROOTS`, the price multiplier and tick size per root.

**On ``contract_size``**: it is the size of the underlying (Si is $1000), not the value of a price
point, so it must not be used as a multiplier. Si is quoted per contract -- one point is one rouble,
multiplier 1 -- while its ``contract_size`` is 1000. The multipliers below were derived from the
exchange's own margin requirements (``GetAssetParams.long_initial_margin / long_risk_rate`` divided
by the contract price) and cross-checked against traded prices;
:func:`ziplime.data.data_sources.finam.finam_asset_data_source.FinamAssetDataSource` re-checks them
against live margin during ingestion and warns on disagreement.
"""
import calendar
import dataclasses
import datetime
import enum
import re

from ziplime.assets.domain.settlement_type import SettlementType

RTSX_MIC = "RTSX"

#: Delivery month code -> month number. Standard futures month codes, used by FORTS as well.
MONTH_CODES: dict[str, int] = {
    "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
    "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12,
}
MONTH_NUMBER_TO_CODE: dict[int, str] = {month: code for code, month in MONTH_CODES.items()}

#: The March quarterly cycle, which is what almost every liquid FORTS contract trades on.
QUARTERLY_MONTHS = (3, 6, 9, 12)

#: Contract names carry the delivery month and a two-digit year, in either the modern form
#: ``Si-12.26`` or the older ``Si-11.12(SiX2)``.
CONTRACT_NAME_RE = re.compile(r"^(?P<root>[^-]+)-(?P<month>\d{1,2})\.(?P<year>\d{2})")


class ExpiryRule(enum.Enum):
    """Fallback for deriving a last trading day when the API does not supply one."""

    #: Third Thursday of the delivery month -- the FORTS default.
    THIRD_THURSDAY = "third_thursday"
    #: Perpetual ("вечный") futures: no delivery month and no expiry.
    PERPETUAL = "perpetual"
    #: Commodity contracts (BR, NG) whose expiry follows the underlying's own calendar and cannot
    #: be derived locally; these always take their date from the API.
    FROM_API = "from_api"


@dataclasses.dataclass(frozen=True)
class MoexFuturesRoot:
    """Specification of a FORTS futures root.

    Attributes:
        root_symbol: FORTS root code as it appears in the ticker, e.g. ``Si`` in ``SiZ6``.
        description: Human readable name of the underlying.
        multiplier: Value of one full price point, in ``quote_currency``. Position P&L is
            ``(price - prev_price) * multiplier * amount``.
        tick_size: Minimum price increment, in price units.
        quote_currency: Currency the contract is quoted in.
        chain_months: Delivery months that make up the contract chain.
        expiry_rule: Fallback for the last trading day when the API gives none.
        fx_dependent: True for contracts quoted in USD. MOEX settles their variation margin in
            roubles at a rate that moves daily, so P&L computed with ``multiplier`` is in dollars,
            not roubles, and converting it needs an FX series ziplime does not apply.
        mic: Exchange the root trades on. Defaults to MOEX derivatives; the CME roots Finam
            mirrors carry their own MIC (see :mod:`.cme_futures`).
        settlement_type: Cash or physical delivery. MOEX settles its index, currency and commodity
            futures in cash, but its single-stock futures deliver the shares.
        margin_currency: Currency the exchange collects margin in. **MOEX collects roubles for
            every contract, including the ones it quotes in dollars** -- verified against
            ``GetAssetParams``, where ``long_initial_margin.currency_code`` is ``RUB`` for
            NG, BR, GOLD and RTS while ``quote_currency`` is ``USD``.
    """

    root_symbol: str
    description: str
    multiplier: float
    tick_size: float
    quote_currency: str = "RUB"
    chain_months: tuple[int, ...] = QUARTERLY_MONTHS
    expiry_rule: ExpiryRule = ExpiryRule.THIRD_THURSDAY
    fx_dependent: bool = False
    mic: str = RTSX_MIC
    settlement_type: SettlementType = SettlementType.CASH
    margin_currency: str = "RUB"

    @property
    def is_deliverable(self) -> bool:
        return self.settlement_type.is_deliverable


#: Contract specifications for the liquid part of FORTS, keyed by ticker root.
MOEX_FUTURES_ROOTS: dict[str, MoexFuturesRoot] = {
    # --- FX -----------------------------------------------------------------------------------
    # Si and Eu are quoted per contract: one point is one rouble.
    "Si": MoexFuturesRoot("Si", "USD/RUB", multiplier=1.0, tick_size=1.0),
    "Eu": MoexFuturesRoot("Eu", "EUR/RUB", multiplier=1.0, tick_size=1.0),
    # CNY/RUB is quoted per yuan over a 1000 yuan contract.
    "CR": MoexFuturesRoot("CR", "CNY/RUB", multiplier=1000.0, tick_size=0.001),
    # --- Indices ------------------------------------------------------------------------------
    # RTS is quoted in index points worth $0.02 each.
    "RI": MoexFuturesRoot("RI", "RTS index", multiplier=0.02, tick_size=10.0,
                          quote_currency="USD", fx_dependent=True),
    "MX": MoexFuturesRoot("MX", "MOEX index (MIX)", multiplier=1.0, tick_size=25.0),
    "MM": MoexFuturesRoot("MM", "MOEX index mini (MXI)", multiplier=10.0, tick_size=0.05),
    # --- Commodities, quoted in USD -------------------------------------------------------------
    "BR": MoexFuturesRoot("BR", "Brent oil", multiplier=10.0, tick_size=0.01,
                          quote_currency="USD", chain_months=tuple(range(1, 13)),
                          expiry_rule=ExpiryRule.FROM_API, fx_dependent=True),
    "NG": MoexFuturesRoot("NG", "Natural gas", multiplier=100.0, tick_size=0.001,
                          quote_currency="USD", chain_months=tuple(range(1, 13)),
                          expiry_rule=ExpiryRule.FROM_API, fx_dependent=True),
    "GD": MoexFuturesRoot("GD", "Gold (USD/oz)", multiplier=1.0, tick_size=0.1,
                          quote_currency="USD", fx_dependent=True),
    "SV": MoexFuturesRoot("SV", "Silver (USD/oz)", multiplier=10.0, tick_size=0.01,
                          quote_currency="USD", fx_dependent=True),
    # --- Commodities, quoted in RUB -------------------------------------------------------------
    "GL": MoexFuturesRoot("GL", "Gold (RUB/g)", multiplier=1.0, tick_size=0.1),
    # --- Single stock: quoted per contract, and physically delivered ----------------------------
    # MOEX single-stock futures deliver the underlying shares, so a position must be closed before
    # the notice date rather than carried into expiry.
    "SR": MoexFuturesRoot("SR", "Sberbank ord.", multiplier=1.0, tick_size=1.0,
                          settlement_type=SettlementType.PHYSICAL),
    "GZ": MoexFuturesRoot("GZ", "Gazprom", multiplier=1.0, tick_size=1.0,
                          settlement_type=SettlementType.PHYSICAL),
    "LK": MoexFuturesRoot("LK", "Lukoil", multiplier=1.0, tick_size=1.0,
                          settlement_type=SettlementType.PHYSICAL),
    "RN": MoexFuturesRoot("RN", "Rosneft", multiplier=1.0, tick_size=1.0,
                          settlement_type=SettlementType.PHYSICAL),
    "VB": MoexFuturesRoot("VB", "VTB", multiplier=1.0, tick_size=1.0,
                          settlement_type=SettlementType.PHYSICAL),
    # --- Perpetual futures: quoted per unit of the underlying -----------------------------------
    "USDRUBF": MoexFuturesRoot("USDRUBF", "USD/RUB perpetual", multiplier=1000.0, tick_size=0.01,
                               chain_months=(), expiry_rule=ExpiryRule.PERPETUAL),
    "CNYRUBF": MoexFuturesRoot("CNYRUBF", "CNY/RUB perpetual", multiplier=1000.0, tick_size=0.001,
                               chain_months=(), expiry_rule=ExpiryRule.PERPETUAL),
    "EURRUBF": MoexFuturesRoot("EURRUBF", "EUR/RUB perpetual", multiplier=1000.0, tick_size=0.01,
                               chain_months=(), expiry_rule=ExpiryRule.PERPETUAL),
    "GLDRUBF": MoexFuturesRoot("GLDRUBF", "Gold/RUB perpetual", multiplier=1.0, tick_size=0.1,
                               chain_months=(), expiry_rule=ExpiryRule.PERPETUAL),
    "IMOEXF": MoexFuturesRoot("IMOEXF", "MOEX index perpetual", multiplier=10.0, tick_size=0.5,
                              chain_months=(), expiry_rule=ExpiryRule.PERPETUAL),
    "SBERF": MoexFuturesRoot("SBERF", "Sberbank perpetual", multiplier=100.0, tick_size=0.01,
                             chain_months=(), expiry_rule=ExpiryRule.PERPETUAL),
    "GAZPF": MoexFuturesRoot("GAZPF", "Gazprom perpetual", multiplier=100.0, tick_size=0.01,
                             chain_months=(), expiry_rule=ExpiryRule.PERPETUAL),
}

#: Roots whose ticker carries no delivery month/year suffix.
PERPETUAL_ROOTS = frozenset(
    root.root_symbol for root in MOEX_FUTURES_ROOTS.values()
    if root.expiry_rule is ExpiryRule.PERPETUAL
)

#: Roots ordered longest-first, so that ``USDRUBF`` is matched before a hypothetical ``US``.
_ROOTS_BY_LENGTH = tuple(sorted(MOEX_FUTURES_ROOTS, key=len, reverse=True))


def format_ticker(root_symbol: str, month: int, year: int) -> str:
    """Build a FORTS ticker, e.g. ``('Si', 12, 2026)`` -> ``'SiZ6'``."""
    if root_symbol in PERPETUAL_ROOTS:
        return root_symbol
    return f"{root_symbol}{MONTH_NUMBER_TO_CODE[month]}{year % 10}"


def parse_ticker(ticker: str, reference_year: int | None = None) -> tuple[str, int | None, int | None]:
    """Split a FORTS ticker into ``(root_symbol, month, year)``.

    FORTS encodes the year as a single digit, so the decade is recovered by picking the year
    closest to ``reference_year`` (today by default). Prefer :func:`parse_contract_name`, which
    reads an unambiguous two-digit year off the instrument name. Perpetual tickers come back with
    ``(root, None, None)``.

    Raises:
        ValueError: if the ticker does not follow the FORTS convention.
    """
    if ticker in PERPETUAL_ROOTS:
        return ticker, None, None
    if len(ticker) < 3:
        raise ValueError(f"Not a FORTS futures ticker: {ticker!r}")

    root_symbol, month_code, year_digit = ticker[:-2], ticker[-2], ticker[-1]
    if month_code not in MONTH_CODES or not year_digit.isdigit():
        raise ValueError(f"Not a FORTS futures ticker: {ticker!r}")

    if reference_year is None:
        reference_year = datetime.date.today().year
    decade_start = reference_year - 4
    year = decade_start - decade_start % 10 + int(year_digit)
    if year < decade_start:
        year += 10
    return root_symbol, MONTH_CODES[month_code], year


def parse_contract_name(name: str) -> tuple[int, int] | None:
    """Read ``(month, year)`` off a contract name such as ``Si-12.26`` or ``Si-11.12(SiX2)``.

    The name carries a two-digit year, which resolves the decade that a FORTS ticker's single
    year digit leaves ambiguous -- ``SiZ9`` is both the 2019 and the 2029 contract. Returns
    ``None`` when the name is empty or does not follow the convention.
    """
    match = CONTRACT_NAME_RE.match(name or "")
    if match is None:
        return None
    month = int(match.group("month"))
    if not 1 <= month <= 12:
        return None
    # FORTS started trading in the 2000s, so a two-digit year is unambiguous.
    return month, 2000 + int(match.group("year"))


def root_symbol_of(ticker: str) -> str:
    """Return the root symbol of a FORTS ticker, falling back to a prefix match on the spec table."""
    try:
        return parse_ticker(ticker)[0]
    except ValueError:
        for root_symbol in _ROOTS_BY_LENGTH:
            if ticker.startswith(root_symbol):
                return root_symbol
        return ticker


def third_thursday(year: int, month: int) -> datetime.date:
    """Return the third Thursday of the given month."""
    first_thursday = 1 + (calendar.THURSDAY - calendar.monthrange(year, month)[0]) % 7
    return datetime.date(year, month, first_thursday + 14)


def last_trading_day(root_symbol: str, month: int, year: int) -> datetime.date:
    """Fallback last trading day for a contract, used only when the API supplies none.

    Accurate for the quarterly FORTS contracts (verified against the last traded bar of SiZ0, SiZ1,
    SiZ2, SiZ4 and SiZ5). Commodity roots such as BR and NG follow their underlying's calendar --
    BR-12.26 expires on 2026-12-01, NG-12.26 on 2026-12-29 -- and are marked
    :attr:`ExpiryRule.FROM_API` because no local rule reproduces them.
    """
    return third_thursday(year, month)


def generate_contract_tickers(root_symbol: str, start: datetime.date,
                              end: datetime.date) -> list[str]:
    """List candidate tickers for ``root_symbol`` expiring between ``start`` and ``end`` + 1 year.

    Only a fallback for when the instrument listing is unavailable; ``AllAssets`` enumerates the
    real contracts, archived ones included.
    """
    root = MOEX_FUTURES_ROOTS.get(root_symbol)
    if root is not None and root.expiry_rule is ExpiryRule.PERPETUAL:
        return [root_symbol]

    months = root.chain_months if root is not None else QUARTERLY_MONTHS
    tickers = []
    for year in range(start.year, end.year + 2):
        for month in months:
            expiry = last_trading_day(root_symbol, month, year)
            if start <= expiry <= end.replace(year=end.year + 1):
                tickers.append(format_ticker(root_symbol, month, year))
    return tickers
