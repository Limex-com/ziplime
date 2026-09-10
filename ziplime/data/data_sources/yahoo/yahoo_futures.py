"""Futures chains on Yahoo Finance: real dated contracts, and the specifications Yahoo omits.

Yahoo carries **individual dated contracts** under ``ROOT + month code + 2-digit year + exchange
suffix`` -- ``ESZ26.CME``, ``CLX26.NYM`` -- and each one has its own price history, its own
expiration date and its own place on the curve. That is a real chain, and it is what makes rolls,
calendar spreads and term structure possible against real data.

Two limits shape what can be built on it, and both are worth knowing before planning a study:

* **Expired contracts are removed.** ``ESZ24.CME`` returns 404, and so does every contract that has
  already settled. A chain can therefore be walked *forward* from today but not reconstructed
  backwards, so a long historical backtest across many past rolls is not available here.
  Contracts that have not yet expired carry their **full** history -- ``ESU27.CME`` goes back two
  and a half years -- so the curve itself is deep even though the past rolls are gone.
* **Volume is unreliable on dated contracts.** Most sessions report zero, even for the front
  month. A volume-based roll finder has nothing to work with, so
  :class:`~ziplime.assets.domain.roll_finder.CalendarRollFinder` is the one to use with this data.
  The continuous ``ROOT=F`` series does carry real volume.

Yahoo also publishes a **continuous front-month series** per root, ``ROOT=F``, which it rolls on
its own timing with no record of when. The ingest does not use it -- a stitched series has no chain
and no curve -- but it is described here (:func:`is_continuous_ticker`, :func:`root_for_continuous`)
so a caller that asks for ``ES=F`` by name can still be told what contract specification is behind
it, and because it is the one place Yahoo reports usable futures volume.

There is also nothing at all about the **contract specification** in the payload. The multiplier --
the number that turns a quote into money -- is absent, so it comes from the table below,
transcribed from the exchanges' published terms. Each entry is checked in
:mod:`tests.test_yahoo_futures` against the notional and tick value it implies: an E-mini S&P
contract is about $296 000 of exposure at 5 916 index points and its tick is $12.50, which is a
figure anyone who trades it recognises. A wrong multiplier passes every type check and silently
misstates every position by a factor of a hundred.
"""
import datetime
from dataclasses import dataclass

import structlog

from ziplime.assets.domain.settlement_type import SettlementType

_logger = structlog.get_logger(__name__)

#: Yahoo's ticker for the continuous front-month series of a root.
CONTINUOUS_SUFFIX = "=F"

#: Futures month codes, in calendar order. The letter in a contract ticker.
MONTH_CODES = "FGHJKMNQUVXZ"

#: Month code -> calendar month.
MONTH_OF_CODE = {code: index + 1 for index, code in enumerate(MONTH_CODES)}

#: The quarterly cycle most index futures list on.
QUARTERLY = "HMUZ"
#: Every month, which energy and metals list on.
MONTHLY = MONTH_CODES
#: The grain cycle.
GRAIN = "HKNUZ"


@dataclass(frozen=True)
class YahooFuturesRoot:
    """A contract root Yahoo carries, with the specification Yahoo does not publish.

    Attributes:
        root_symbol: Short code used as the chain identifier and as the ticker prefix.
        description: What the contract is.
        mic: Exchange the contract lists on, as a MIC.
        exchange_suffix: What Yahoo appends to a dated ticker, e.g. ``.CME``.
        contract_months: Month codes this root lists contracts for.
        multiplier: Money one full price point is worth. **The field that must be right.**
        tick_size: Smallest price increment.
        quote_currency: Currency the quote and the multiplier are in.
        settlement_type: Cash or physical delivery, from the contract specification.
        reliable_continuous_volume: Whether the ``ROOT=F`` series reports usable volume. Dated
            contracts report almost none regardless.
    """

    root_symbol: str
    description: str
    mic: str
    exchange_suffix: str
    multiplier: float
    tick_size: float
    contract_months: str = QUARTERLY
    quote_currency: str = "USD"
    settlement_type: SettlementType = SettlementType.PHYSICAL
    reliable_continuous_volume: bool = True

    @property
    def continuous_ticker(self) -> str:
        """Yahoo's ticker for this root's continuous front-month series."""
        return f"{self.root_symbol}{CONTINUOUS_SUFFIX}"


#: Contract specifications, transcribed from the exchanges' published terms. Every multiplier and
#: tick size is checked against the notional and tick value it implies -- see the module docstring.
YAHOO_FUTURES_ROOTS: dict[str, YahooFuturesRoot] = {
    "ES": YahooFuturesRoot(
        root_symbol="ES", description="E-mini S&P 500", mic="XCME", exchange_suffix=".CME",
        multiplier=50.0, tick_size=0.25, contract_months=QUARTERLY,
        settlement_type=SettlementType.CASH),
    "NQ": YahooFuturesRoot(
        root_symbol="NQ", description="E-mini Nasdaq 100", mic="XCME", exchange_suffix=".CME",
        multiplier=20.0, tick_size=0.25, contract_months=QUARTERLY,
        settlement_type=SettlementType.CASH),
    "CL": YahooFuturesRoot(
        root_symbol="CL", description="WTI crude oil", mic="XNYM", exchange_suffix=".NYM",
        multiplier=1_000.0, tick_size=0.01, contract_months=MONTHLY),
    "NG": YahooFuturesRoot(
        root_symbol="NG", description="Henry Hub natural gas", mic="XNYM", exchange_suffix=".NYM",
        multiplier=10_000.0, tick_size=0.001, contract_months=MONTHLY),
    "GC": YahooFuturesRoot(
        root_symbol="GC", description="Gold", mic="XCEC", exchange_suffix=".CMX",
        multiplier=100.0, tick_size=0.10, contract_months=MONTHLY,
        reliable_continuous_volume=False),
    "SI": YahooFuturesRoot(
        root_symbol="SI", description="Silver", mic="XCEC", exchange_suffix=".CMX",
        multiplier=5_000.0, tick_size=0.005, contract_months=MONTHLY,
        reliable_continuous_volume=False),
    "ZC": YahooFuturesRoot(
        # Corn is quoted in cents per bushel on a 5 000 bushel contract, so one price point is
        # 5000/100 = 50 dollars. Reading the contract size as the multiplier overstates it 100x.
        root_symbol="ZC", description="Corn", mic="XCBT", exchange_suffix=".CBT",
        multiplier=50.0, tick_size=0.25, contract_months=GRAIN),
    "6E": YahooFuturesRoot(
        root_symbol="6E", description="Euro FX", mic="XCME", exchange_suffix=".CME",
        multiplier=125_000.0, tick_size=0.00005, contract_months=QUARTERLY),
}

#: Roots keyed by their continuous ticker, for looking a series up by what the tape calls it.
ROOTS_BY_CONTINUOUS_TICKER = {r.continuous_ticker: r for r in YAHOO_FUTURES_ROOTS.values()}


@dataclass(frozen=True)
class ContractTicker:
    """A dated contract ticker, decomposed."""

    ticker: str
    root_symbol: str
    month_code: str
    year: int

    @property
    def delivery_month(self) -> int:
        return MONTH_OF_CODE[self.month_code]


def contract_ticker(root: YahooFuturesRoot, month_code: str, year: int) -> str:
    """Yahoo's ticker for one dated contract, e.g. ``CLX26.NYM``.

    The year is two digits, which is ambiguous across decades but is what Yahoo uses; the caller
    supplies a full year and the chain is generated forward from a known point, so nothing here has
    to guess which decade ``26`` means.
    """
    return f"{root.root_symbol}{month_code}{year % 100:02d}{root.exchange_suffix}"


def parse_contract_ticker(ticker: str) -> ContractTicker | None:
    """Decompose a dated Yahoo futures ticker, or return ``None`` if it is not one.

    The two-digit year is resolved into the current century, which is right for every contract
    Yahoo still carries -- it removes them once they expire.
    """
    for root in YAHOO_FUTURES_ROOTS.values():
        prefix, suffix = root.root_symbol, root.exchange_suffix
        if not (ticker.startswith(prefix) and ticker.endswith(suffix)):
            continue
        middle = ticker[len(prefix):-len(suffix)]
        if len(middle) != 3 or middle[0] not in MONTH_CODES or not middle[1:].isdigit():
            continue
        century = datetime.date.today().year // 100 * 100
        return ContractTicker(ticker=ticker, root_symbol=root.root_symbol,
                              month_code=middle[0], year=century + int(middle[1:]))
    return None


def candidate_tickers(root: YahooFuturesRoot, start: datetime.date,
                      months_ahead: int = 30) -> list[str]:
    """Every contract ticker of ``root`` that could exist between ``start`` and ``months_ahead``.

    Generated rather than looked up: Yahoo has no endpoint that lists a chain, so the contracts
    have to be guessed from the root's listing cycle and then probed. Expired ones simply return no
    data, which is how the real chain is discovered.
    """
    tickers: list[str] = []
    year, month = start.year, start.month
    for _ in range(months_ahead + 1):
        code = MONTH_CODES[month - 1]
        if code in root.contract_months:
            tickers.append(contract_ticker(root, code, year))
        month += 1
        if month > 12:
            month, year = 1, year + 1
    return tickers


def is_continuous_ticker(ticker: str) -> bool:
    """Whether a ticker names a continuous front-month series rather than a dated contract."""
    return ticker.endswith(CONTINUOUS_SUFFIX)


def root_for_continuous(ticker: str) -> YahooFuturesRoot | None:
    """The specification behind a continuous ticker, or ``None`` if it is not in the table."""
    return ROOTS_BY_CONTINUOUS_TICKER.get(ticker)


def root_for_contract(ticker: str) -> YahooFuturesRoot | None:
    """The specification behind a dated contract ticker, or ``None`` if it is not in the table.

    ``None`` rather than a default: without a multiplier the contract cannot be priced, and
    assuming 1.0 would misstate every position silently.
    """
    parsed = parse_contract_ticker(ticker)
    return YAHOO_FUTURES_ROOTS.get(parsed.root_symbol) if parsed else None


def parse_expiry(info: dict) -> datetime.date | None:
    """Read a contract's expiry from ``Ticker.info``, which returns it as a Unix timestamp."""
    raw = info.get("expireDate")
    if not raw:
        return None
    try:
        return datetime.datetime.fromtimestamp(int(raw), tz=datetime.timezone.utc).date()
    except (TypeError, ValueError, OSError):
        _logger.warning("Unusable expiry on a Yahoo futures ticker", expire_date=raw)
        return None


def fallback_expiry(root: YahooFuturesRoot, month_code: str, year: int) -> datetime.date:
    """An approximate expiry for a contract whose metadata Yahoo does not answer for.

    The third Friday of the delivery month for index futures, and the twentieth for everything
    else. **This is a convention, not the exchange's rule** -- energy contracts in particular
    settle on a business-day count before the delivery month starts. It exists so a contract with
    missing metadata is still ordered correctly within its chain, not so that expiry dates can be
    trusted to the day; a warning is logged whenever it is used.
    """
    month = MONTH_OF_CODE[month_code]
    if root.settlement_type is SettlementType.CASH:
        day = datetime.date(year, month, 1)
        fridays = [d for d in (day + datetime.timedelta(days=i) for i in range(31))
                   if d.month == month and d.weekday() == 4]
        return fridays[2] if len(fridays) >= 3 else fridays[-1]
    return datetime.date(year, month, 20)
