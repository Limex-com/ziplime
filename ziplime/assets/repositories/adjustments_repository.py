import datetime
from collections.abc import Sequence
from typing import Any

import polars as pl
import pandas as pd
from typing import Self

from ziplime.assets.entities.asset import Asset

from ziplime.assets.models.divident_payout_model import DividendPayoutModel
from ziplime.assets.models.stock_dividend_payout_model import StockDividendPayoutModel


class AdjustmentRepository:
    async def get_splits(self, assets: frozenset[Asset], dt: datetime.date): ...

    async def get_dividend_payouts(self, sid: int, trading_days: pl.Series) -> list[DividendPayoutModel]: ...

    async def get_stock_dividends(self, sid: int, trading_days: pl.Series) -> list[StockDividendPayoutModel]: ...

    async def load_pricing_adjustments(
        self,
        columns: Sequence[str],
        dates: pd.DatetimeIndex,
        assets: pd.Index,
    ) -> list[Any]: ...

    def to_json(self) -> dict[str, Any]: ...

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Self: ...
