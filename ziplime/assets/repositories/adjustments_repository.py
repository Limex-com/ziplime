import datetime
import polars as pl
from typing import Any, Self

from ziplime.assets.entities.asset import Asset

from ziplime.assets.models.divident_payout_model import DividendPayoutModel
from ziplime.assets.models.stock_dividend_payout_model import StockDividendPayoutModel


class AdjustmentRepository:
    async def get_splits(self, assets: frozenset[Asset], dt: datetime.date): ...

    async def get_dividend_payouts(self, sid: int, trading_days: pl.Series) -> list[DividendPayoutModel]: ...

    async def get_stock_dividends(self, sid: int, trading_days: pl.Series) -> list[StockDividendPayoutModel]: ...

    async def load_pricing_adjustments(self, columns, dates, assets): ...

    def to_json(self): ...

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Self: ...
