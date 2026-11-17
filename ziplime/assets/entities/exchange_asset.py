import datetime
from dataclasses import dataclass

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.exchange_info import ExchangeInfo


@dataclass(frozen=True)
class ExchangeAsset:
    sid: int | None  # if SID is None, then it is a new asset and we want it to have automatically assigned SID
    symbol: str
    start_date: datetime.date | None
    end_date: datetime.date | None
    first_traded: datetime.date | None
    auto_close_date: datetime.date | None
    external_id: str
    exchange: ExchangeInfo
    asset: Asset
    quote: Asset

    def __hash__(self) -> int:
        return hash(self.sid)

    def __str__(self) -> str:
        return f"{self.symbol}-{self.mic}-({self.sid})"

    @property
    def mic(self) -> str:
        return self.exchange.mic

    @property
    def trading_calendar(self):
        """The schedule this listing's own venue keeps, or ``None`` if the venue is not known.

        A listing carries its venue but not that venue's hours, which is why everything used to
        judge it by the simulation's calendar instead -- correct while a run stays on one market,
        and wrong the moment it does not: on a Moscow clock an American listing is asked for a
        price at 10:01 Moscow, which is 03:01 in New York.

        ``None`` means the venue is not in :data:`~ziplime.utils.venue_calendars.VENUE_CALENDARS`,
        and every caller falls back to the simulation calendar for it -- the behaviour that
        predates this property.
        """
        from ziplime.utils.venue_calendars import calendar_for_mic

        return calendar_for_mic(self.mic)
