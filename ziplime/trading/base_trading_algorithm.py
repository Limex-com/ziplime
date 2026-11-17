from typing import Callable
from collections.abc import Iterator

from exchange_calendars import ExchangeCalendar
import pandas as pd

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.finance.asset_restrictions import Restrictions
from ziplime.finance.domain.order import Order
from ziplime.finance.execution import ExecutionStyle
from ziplime.utils.event_rule import EventRule
from ziplime.finance.cancel_policy import CancelPolicy
from ziplime.finance.commission.commission_model import CommissionModel
from ziplime.finance.slippage.slippage_model import SlippageModel
from ziplime.pipeline.pipeline import Pipeline


class BaseTradingAlgorithm:
    def schedule_function(
            self,
            func: Callable,
            date_rule: EventRule = None,
            time_rule: EventRule = None,
            half_days: bool = True,
            calendar: ExchangeCalendar | None = None,
    ): ...

    def record(self, *args, **kwargs): ...

    def continuous_future(
            self, root_symbol_str: str, offset: int = 0, roll: str = "volume", adjustment: str = "mul",
            mic: str | None = None
    ): ...

    async def symbol(
            self, symbol_str: str, asset_type: AssetType = AssetType.EQUITY,
            exchange_name: str = None,
            country_code: str | None = None
    ) -> ExchangeAsset | None: ...

    async def symbols(self, *args: str, **kwargs) -> list[ExchangeAsset]: ...

    async def sid(self, sid: int) -> ExchangeAsset | None: ...

    async def future_symbol(self, symbol: str, exchange_name: str = None) -> FuturesContract | None: ...

    async def order(self, asset: ExchangeAsset, amount: int, style: ExecutionStyle,
                    exchange_name: str | None = None) -> Order | None: ...

    async def order_value(self, asset: ExchangeAsset, value: float, limit_price: float | None = None,
                          stop_price: float | None = None,
                          style: ExecutionStyle | None = None): ...

    def get_datetime(self): ...

    def set_slippage(self, us_equities: SlippageModel | None = None, us_futures: SlippageModel | None = None): ...

    def set_commission(
            self,
            us_equities: CommissionModel | None = None,
            us_futures: CommissionModel | None = None,
    ): ...

    def set_cancel_policy(self, cancel_policy: CancelPolicy): ...

    def set_symbol_lookup_date(self, dt): ...

    async def order_percent(
            self, asset: ExchangeAsset, percent: float, style: ExecutionStyle
    ): ...

    async def order_target(
            self, asset: ExchangeAsset, target: int, style: ExecutionStyle
    ): ...

    async def order_target_value(
            self, asset: ExchangeAsset, target: float, style: ExecutionStyle
    ): ...

    async def order_target_percent(
            self, asset: ExchangeAsset, target: float,
            style: ExecutionStyle
    ): ...

    def get_open_orders(self, asset: ExchangeAsset | None = None): ...

    def get_order(self, order_id: str) -> Order | None: ...

    async def cancel_order(self, order_id: str, relay_status: bool = True) -> None: ...

    def set_max_leverage(self, max_leverage: float): ...

    def set_min_leverage(self, min_leverage: float, grace_period: pd.Timedelta): ...

    def set_max_position_size(
        self,
        asset: ExchangeAsset | None = None,
        max_shares: int | None = None,
        max_notional: float | None = None,
        on_error: str = "fail",
    ): ...

    def set_max_order_size(
        self,
        asset: ExchangeAsset | None = None,
        max_shares: int | None = None,
        max_notional: float | None = None,
        on_error: str = "fail",
    ): ...

    def set_max_order_count(self, max_count: int, on_error: str = "fail"): ...

    def set_asset_restrictions(self, restrictions: Restrictions, on_error: str = "fail"): ...

    def set_long_only(self, on_error: str = "fail"): ...

    def attach_pipeline(
        self,
        pipeline: Pipeline,
        name: str,
        chunks: int | Iterator[int] | None = None,
        eager: bool = True,
    ): ...

    def pipeline_output(self, name: str): ...
