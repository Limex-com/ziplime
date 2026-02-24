import asyncio
import datetime
from pathlib import Path

from ziplime.utils.calendar_utils import get_calendar

from ziplime.assets.domain.ordered_contracts import CHAIN_PREDICATES
from ziplime.assets.repositories.sqlalchemy_adjustments_repository import SqlAlchemyAdjustmentRepository
from ziplime.assets.repositories.sqlalchemy_asset_repository import SqlAlchemyAssetRepository
from ziplime.assets.services.asset_service import AssetService
from ziplime.constants.data_type import DataType
from ziplime.core.algorithm_file import AlgorithmFile
from ziplime.data.services.bundle_service import BundleService
from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry
from ziplime.exchanges.lime_trader_sdk.lime_trader_sdk_exchange import LimeTraderSdkExchange
from ziplime.finance.metrics import default_metrics
from ziplime.gens.domain.realtime_clock import RealtimeClock
from ziplime.exchanges.exchange import Exchange
from ziplime.utils.run_algo import run_algorithm


async def _run_live_trading(
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        trading_calendar: str,
        emission_rate: datetime.timedelta,
        cash_balance: float,
        bundle_name: str,
        algorithm_file: str,
        exchange: Exchange = None,
        config_file: str | None = None,

):
    # benchmark_spec = BenchmarkSpec(
    #     benchmark_returns=None,
    #     benchmark_sid=benchmark_sid,
    #     benchmark_symbol=benchmark_symbol,
    #     benchmark_file=benchmark_file,
    #     no_benchmark=no_benchmark,
    # )
    calendar = get_calendar(trading_calendar)

    bundle_storage_path = str(Path(Path.home(), ".ziplime", "data"))
    bundle_registry = FileSystemBundleRegistry(base_data_path=bundle_storage_path)
    bundle_service = BundleService(bundle_registry=bundle_registry)
    data_bundle = None
    if bundle_name is not None:
        data_bundle = await bundle_service.load_bundle(bundle_name=bundle_name, bundle_version=None,
                                                       data_type=DataType.CUSTOM)

    algo = AlgorithmFile(algorithm_file=algorithm_file, algorithm_config_file=config_file)
    timedelta_diff_from_current_time = -datetime.timedelta(seconds=0)

    clock = RealtimeClock(
        trading_calendar=calendar,
        start_date=start_date,
        end_date=end_date,
        emission_rate=emission_rate,
        timedelta_diff_from_current_time=timedelta_diff_from_current_time
    )

    if exchange is None:
        exchange = LimeTraderSdkExchange(
            name="LIME",
            country_code="US",
            trading_calendar=calendar,
            data_bundle=data_bundle,
            cash_balance=cash_balance,
            clock=clock
        )

    db_url = f"sqlite+aiosqlite:///{str(Path(Path.home(), ".ziplime", "assets.sqlite").absolute())}"
    assets_repository = SqlAlchemyAssetRepository(db_url=db_url, future_chain_predicates=CHAIN_PREDICATES)
    adjustments_repository = SqlAlchemyAdjustmentRepository(db_url=db_url)
    asset_service = AssetService(asset_repository=assets_repository, adjustments_repository=adjustments_repository)

    return await run_algorithm(
        algorithm=algo,
        asset_service=asset_service,
        print_algo=True,
        metrics_set=default_metrics(),
        # benchmark_spec=benchmark_spec,
        custom_loader=None,
        exchanges=[exchange],
        clock=clock,
    )


def run_live_trading(
        trading_calendar: str,
        algorithm_file: str,
        total_cash: float,
        emission_rate: datetime.timedelta,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        bundle_name: str | None = None,
        config_file: str | None = None,
        exchange: Exchange | None = None
):
    return asyncio.run(_run_live_trading(start_date=start_date, end_date=end_date, trading_calendar=trading_calendar,
                                         cash_balance=total_cash,
                                         algorithm_file=algorithm_file,
                                         config_file=config_file, bundle_name=bundle_name, exchange=exchange,
                                         emission_rate=emission_rate))
