"""Synthetic option fixtures for the 0DTE suite.

Deliberately hand-built rather than generated: the tests that check the generator must not be
written against the generator's own output.
"""
import datetime
from zoneinfo import ZoneInfo

from ziplime.assets.domain.option_type import OptionType
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.assets.entities.option_contract import OptionContract, format_occ_symbol
from ziplime.finance.options.chain import OptionChain

FAR_PAST = datetime.date(1900, 1, 1)
FAR_FUTURE = datetime.date(2099, 1, 1)
TZ = ZoneInfo("America/New_York")
EXCHANGE = ExchangeInfo(mic="ARCX", name="NYSE Arca", canonical_name="ARCA", country_code="US")
USD = Currency(id=2, isin=None, asset_name="USD", start_date=FAR_PAST, end_date=FAR_FUTURE,
               first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)

#: The session every fixture here is listed on and expires on. 0DTE: those are the same day.
EXPIRY = datetime.date(2024, 6, 14)
SESSION_CLOSE = datetime.datetime.combine(EXPIRY, datetime.time(16, 0), tzinfo=TZ)
#: The user's worked example: SPY at 523.41 with dollar strikes from 515 to 531.
SPOT = 523.41


def underlying(sid: int = 1000, symbol: str = "SPY") -> ExchangeAsset:
    equity = Equity(id=1, isin=None, asset_name=symbol, start_date=FAR_PAST, end_date=FAR_FUTURE,
                    first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)
    return ExchangeAsset(sid=sid, symbol=symbol, start_date=FAR_PAST, end_date=FAR_FUTURE,
                         first_traded=FAR_PAST, auto_close_date=FAR_FUTURE, external_id=symbol,
                         exchange=EXCHANGE, asset=equity, quote=USD)


def make_option(sid: int, strike: float, option_type: OptionType,
                expiration: datetime.date = EXPIRY, multiplier: float = 100.0,
                underlying_listing: ExchangeAsset | None = None) -> ExchangeAsset:
    """A listed 0DTE contract: listed and expiring on the same session."""
    listing = underlying_listing if underlying_listing is not None else underlying()
    name = format_occ_symbol(listing.symbol, expiration, option_type, strike)
    contract = OptionContract(
        id=sid + 50_000, isin=None, asset_name=name,
        start_date=expiration, end_date=expiration, first_traded=expiration,
        auto_close_date=expiration,
        underlying_exchange_asset=listing, underlying_asset=listing.asset,
        underlying_symbol=listing.symbol, option_type=option_type, strike=strike,
        expiration_date=expiration, multiplier=multiplier)
    return ExchangeAsset(sid=sid, symbol=name, start_date=expiration, end_date=expiration,
                         first_traded=expiration, auto_close_date=expiration, external_id=name,
                         exchange=EXCHANGE, asset=contract, quote=USD)


def make_chain(strikes: range = range(515, 532), expiration: datetime.date = EXPIRY,
               underlying_listing: ExchangeAsset | None = None) -> OptionChain:
    """The 34-contract chain from the worked example: 515..531, calls and puts."""
    listing = underlying_listing if underlying_listing is not None else underlying()
    contracts, sid = [], 1
    for strike in strikes:
        for option_type in (OptionType.CALL, OptionType.PUT):
            contracts.append(make_option(sid, float(strike), option_type, expiration, 100.0,
                                         listing))
            sid += 1
    return OptionChain.from_listings(contracts)
