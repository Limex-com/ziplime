"""Sweep context for initialize."""

from collections.abc import Callable
import polars as pl
from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.core.algorithm_file import AlgorithmFile
from ziplime.sweep.params import SweepError
from ziplime.assets.services.asset_service import AssetService
from ziplime.finance.cancel_policy import CancelPolicy
from ziplime.finance.commission.commission_model import CommissionModel
from ziplime.finance.slippage.slippage_model import SlippageModel
from ziplime.utils.event_rule import EventRule


class SweepContext:
    """TradingAlgorithm as ``initialize`` sees it: the config, instrument lookup, indicators."""

    #: The context's own attributes; everything else on it was set by the algorithm's initialize
    OWN = {"algorithm", "_asset_service", "_found", "requested", "_indicators", "_scheduled",
           "_commission", "_slippage", "_cancel_policy", "universe", "datasets"}

    def __init__(self, algorithm: AlgorithmFile, asset_service: AssetService, found: dict | None = None) -> None:
        self.algorithm = algorithm
        self._asset_service = asset_service
        #: Instruments already looked up: initialize runs once per distinct config
        self._found = {} if found is None else found
        #: Every instrument the algorithm asked for, in order
        self.requested = []
        self._indicators = {}
        #: ``schedule_function`` calls, in order: (function, date_rule, time_rule, half_days)
        self._scheduled = []
        #: What set_commission / set_slippage / set_cancel_policy chose
        self._commission, self._slippage, self._cancel_policy = {}, {}, None

    async def symbol(self, symbol: str, mic: str = None, asset_type: AssetType = AssetType.EQUITY,
                     country_code: str | None = None):
        if "@" in symbol:
            symbol, mic = symbol.split("@")
        key = (symbol, mic, asset_type)
        if key not in self._found:
            self._found[key] = await self._asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=symbol, mic=mic), asset_type=asset_type)
        asset = self._found[key]
        if asset is None:
            raise SweepError(f"{symbol} (mic={mic}) is not in the asset database.")
        if asset not in self.requested:
            self.requested.append(asset)
        return asset

    async def symbols(self, *symbols: str, **kwargs):
        return [await self.symbol(symbol, **kwargs) for symbol in symbols]

    def indicators(self, **expressions: pl.Expr) -> None:
        """As TradingAlgorithm.indicators: the expressions are kept, not computed."""
        self._indicators.update(expressions)

    def schedule_function(
        self,
        func: Callable,
        date_rule: EventRule | None = None,
        time_rule: EventRule | None = None,
        half_days: bool = True,
        calendar=None,
    ) -> None:
        self._scheduled.append((func, date_rule, time_rule, half_days))

    def set_commission(
        self,
        us_equities: CommissionModel | None = None,
        us_futures: CommissionModel | None = None,
        bonds: CommissionModel | None = None,
    ) -> None:
        if bonds is not None:
            raise SweepError("Bonds are not supported in a sweep.")
        self._commission.update({
            k: v for k, v in (("equities", us_equities), ("futures", us_futures)) if v is not None
        })

    def set_slippage(
        self,
        us_equities: SlippageModel | None = None,
        us_futures: SlippageModel | None = None,
        bonds: SlippageModel | None = None,
    ) -> None:
        if bonds is not None:
            raise SweepError("Bonds are not supported in a sweep.")
        self._slippage.update({
            k: v for k, v in (("equities", us_equities), ("futures", us_futures)) if v is not None
        })

    def set_cancel_policy(self, cancel_policy: CancelPolicy) -> None:
        self._cancel_policy = cancel_policy

    def state(self) -> dict:
        """What initialize put on the context (``context.i = 0``, ...)."""
        return {name: value for name, value in vars(self).items() if name not in self.OWN}
