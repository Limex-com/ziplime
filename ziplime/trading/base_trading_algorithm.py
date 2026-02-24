from typing import Callable

from exchange_calendars import ExchangeCalendar

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.finance.asset_restrictions import Restrictions
from ziplime.finance.domain.order import Order
from ziplime.finance.execution import ExecutionStyle
from ziplime.utils.events import EventRule


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
            self, root_symbol_str: str, offset: int = 0, roll: str = "volume", adjustment: str = "mul"
    ): ...

    async def symbol(
            self, symbol_str: str, asset_type: AssetType = AssetType.EQUITY,
            exchange_name: str = None,
            country_code: str | None = None
    ) -> Asset | None: ...

    def symbols(self, *args, **kwargs): ...

    async def sid(self, sid: int) -> Asset | None: ...

    async def future_symbol(self, symbol: str, exchange_name: str = None) -> FuturesContract | None: ...

    async def order(self, asset: Asset, amount: int, style: ExecutionStyle,
                    exchange_name: str | None = None) -> Order | None: ...

    async def order_value(self, asset: Asset, value: float, limit_price: float | None = None,
                          stop_price: float | None = None,
                          style: ExecutionStyle | None = None): ...

    def get_datetime(self): ...

    def set_slippage(self, us_equities=None, us_futures=None): ...

    def set_commission(self, us_equities=None, us_futures=None): ...

    def set_cancel_policy(self, cancel_policy): ...

    def set_symbol_lookup_date(self, dt): ...

    async def order_percent(
            self, asset: Asset, percent: float, style: ExecutionStyle
    ): ...

    async def order_target(
            self, asset: Asset, target: int, style: ExecutionStyle
    ): ...

    async def order_target_value(
            self, asset: Asset, target: float, style: ExecutionStyle
    ): ...

    async def order_target_percent(
            self, asset: Asset, target: float,
            style: ExecutionStyle
    ): ...

    def get_open_orders(self, asset=None): ...

    def get_order(self, order_id) -> Order | None: ...

    def cancel_order(self, order_id: str, relay_status: bool = True) -> None: ...

    def set_max_leverage(self, max_leverage): ...

    def set_min_leverage(self, min_leverage, grace_period): ...

    def set_max_position_size(
            self, asset=None, max_shares=None, max_notional=None, on_error="fail"
    ): ...

    def set_max_order_size(
            self, asset=None, max_shares=None, max_notional=None, on_error="fail"
    ): ...

    def set_max_order_count(self, max_count, on_error="fail"): ...

    def set_asset_restrictions(self, restrictions: Restrictions, on_error: str = "fail"): ...

    def set_long_only(self, on_error="fail"):...

    def attach_pipeline(self, pipeline, name, chunks=None, eager=True): ...

    def pipeline_output(self, name): ...
