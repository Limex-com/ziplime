import datetime
from dataclasses import dataclass

from ziplime.assets.entities.dividend_payout import DividendPayout
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.domain.investment_event import InvestmentEvent, InvestmentKey
from ziplime.finance.domain.investment_event_kind import InvestmentEventKind


@dataclass(frozen=True)
class DividendCashPayment:
    """An earned native payment, retaining the holding's account and listing."""

    dividend_payout: DividendPayout
    amount: float
    exchange_name: str
    trading_account_id: str
    exchange_asset: ExchangeAsset
    earned_at: datetime.datetime | None = None

    @property
    def key(self) -> InvestmentKey:
        return self.exchange_name, self.trading_account_id, self.exchange_asset

    def to_event(self) -> InvestmentEvent:
        dividend = self.dividend_payout
        return InvestmentEvent(
            id=(f"dividend-{dividend.asset.id}-{dividend.ex_date}-"
                f"{dividend.pay_date}-{dividend.amount}-{dividend.currency.asset_name}"),
            dt=self.earned_at or datetime.datetime.combine(
                dividend.ex_date, datetime.time.min, tzinfo=datetime.timezone.utc),
            kind=InvestmentEventKind.INCOME,
            key=self.key,
            currency=dividend.currency.asset_name,
            amount=self.amount,
        )
