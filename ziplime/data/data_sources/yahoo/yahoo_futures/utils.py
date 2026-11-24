"""Utils for yahoo_futures."""

import datetime
from ziplime.assets.domain.settlement_type import SettlementType
from ziplime.data.data_sources.yahoo.yahoo_futures.support import CONTINUOUS_SUFFIX
from ziplime.data.data_sources.yahoo.yahoo_futures.contract_ticker_value import ContractTicker
from ziplime.data.data_sources.yahoo.yahoo_futures.support import MONTH_CODES
from ziplime.data.data_sources.yahoo.yahoo_futures.support import MONTH_OF_CODE
from ziplime.data.data_sources.yahoo.yahoo_futures.roots import ROOTS_BY_CONTINUOUS_TICKER
from ziplime.data.data_sources.yahoo.yahoo_futures.roots import YAHOO_FUTURES_ROOTS
from ziplime.data.data_sources.yahoo.yahoo_futures.yahoo_futures_root import YahooFuturesRoot
from ziplime.data.data_sources.yahoo.yahoo_futures.support import _logger


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
