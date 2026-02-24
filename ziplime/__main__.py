import asyncio
import datetime
import logging
import os
from pathlib import Path

import asyncclick as click

from ziplime.assets.domain.ordered_contracts import CHAIN_PREDICATES
from ziplime.assets.repositories.sqlalchemy_adjustments_repository import SqlAlchemyAdjustmentRepository
from ziplime.assets.repositories.sqlalchemy_asset_repository import SqlAlchemyAssetRepository
from ziplime.constants.fundamental_data import FUNDAMENTAL_DATA_COLUMNS
from ziplime.data.services.bundle_service import BundleService
from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry
from ziplime.data.services.limex_hub_data_source import LimexHubDataSource
from ziplime.data.services.file_system_parquet_bundle_storage import FileSystemParquetBundleStorage
from ziplime.domain.benchmark_spec import BenchmarkSpec
from ziplime.domain.data_frequency import DataFrequency
from ziplime.finance.commission import DEFAULT_MINIMUM_COST_PER_FUTURE_TRADE, DEFAULT_PER_CONTRACT_COST, PerContract, \
    DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE, DEFAULT_PER_SHARE_COST, PerShare
from ziplime.finance.constants import FUTURE_EXCHANGE_FEES_BY_SYMBOL
from ziplime.finance.domain.simulation_paremeters import SimulationParameters
from ziplime.finance.metrics import default_metrics
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
from ziplime.finance.slippage.slippage_model import DEFAULT_FUTURE_VOLUME_SLIPPAGE_BAR_LIMIT
from ziplime.finance.slippage.volatility_volume_share import VolatilityVolumeShare
from ziplime.gens.domain.simulation_clock import SimulationClock
from ziplime.exchanges.lime_trader_sdk.lime_trader_sdk_exchange import LimeTraderSdkExchange
from ziplime.exchanges.simulation_exchange import SimulationExchange
from ziplime.utils.date_utils import strip_time_and_timezone_info
from ziplime.utils.run_algo import run_algorithm
from exchange_calendars import get_calendar as ec_get_calendar

from asyncclick import DateTime
from ziplime.utils.cli import Timestamp

from ziplime.utils.bundle_utils import get_fundamental_data_provider, get_data_source


def validate_date_range(date_min: datetime.datetime, date_max: datetime.datetime):
    def _validate_date_range(ctx, param, value):
        if not date_min < value.replace(tzinfo=datetime.timezone.utc) < date_max:
            raise click.BadParameter(f"Must be between {date_min} and {date_max}")
        return value

    return _validate_date_range


@click.group()
@click.pass_context
async def main(ctx):
    """Top level ziplime entry point."""
    # install a logging handler before performing any other operations
    logging.basicConfig(
        format="[%(asctime)s-%(levelname)s][%(name)s]\n %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


@main.command(context_settings=dict(
))
@click.option(
    "-b",
    "--bundle",
    help="The data bundle to ingest.",
)
@click.option(
    "-c",
    "--trading-calendar",
    help="Default calendar to use.",
)
@click.option(
    "--start-date",
    type=DateTime(),
    callback=validate_date_range(date_min=datetime.datetime(year=1800,
                                                            month=1,
                                                            day=1,
                                                            tzinfo=datetime.timezone.utc
                                                            ),
                                 date_max=datetime.datetime.now(tz=datetime.timezone.utc),
                                 )
)
@click.option(
    "--end-date",
    type=DateTime(),
    callback=validate_date_range(date_min=datetime.datetime(year=1800,
                                                            month=1,
                                                            day=1,
                                                            tzinfo=datetime.timezone.utc
                                                            ),
                                 date_max=datetime.datetime.now(tz=datetime.timezone.utc),
                                 )
)
@click.option(
    "--frequency",
    default="1d",
    type=click.Choice([df.value for df in DataFrequency]),
)
@click.option("-s", '--symbols')
@click.option('--fundamental-data')
@click.option(
    "--show-progress/--no-show-progress",
    default=True,
    help="Print progress information to the terminal.",
)
@click.option(
    "--historical-market-data-provider",
    type=click.Choice(['limex-hub', 'lime-trader-sdk']),
    default="limex-hub",
    help="Market data provider for historical data",
    show_default=True,
)
@click.option(
    "--fundamental-data-provider",
    type=click.Choice(['limex-hub', ]),
    default="limex-hub",
    help="Fundamental data provider",
    show_default=True,
)
@click.option(
    "--skip-fundamental-data",
    default=False,
    is_flag=True,
    help="If passed, fundamental data won't be ingested.",
)
@click.option(
    "--bundle-storage-path",
    default=Path(Path.home(), ".ziplime", "data"),
    show_default=True,
    help="Path to the bundle storage on filesystem.",
)
@click.pass_context
async def ingest(ctx, bundle, start_date, end_date, frequency, symbols, fundamental_data, show_progress,
                 trading_calendar,
                 historical_market_data_provider,
                 fundamental_data_provider,
                 skip_fundamental_data,
                 bundle_storage_path,
                 ):
    """Top level ziplime entry point."""
    symbols_parsed = symbols.split(',') if symbols else None
    if skip_fundamental_data:
        fundamental_data_list = set()
    else:
        fundamental_data_list = fundamental_data.split(",") if fundamental_data else None
    # install a logging handler before performing any other operations
    fundamental_data_list_cols = fundamental_data_list if fundamental_data_list is not None else [
        col.name for col in
        FUNDAMENTAL_DATA_COLUMNS
    ]
    fundamental_data_provider_instance = get_fundamental_data_provider(code=fundamental_data_provider)
    fundamental_data_column_names = fundamental_data_provider_instance.get_fundamental_data_column_names(
        fundamental_data_fields=set(fundamental_data_list_cols))
    fundamental_data_cols = (
        [
            col
            for col in FUNDAMENTAL_DATA_COLUMNS
            if col.name in fundamental_data_column_names
        ]
        if fundamental_data_list is not None
        else FUNDAMENTAL_DATA_COLUMNS
    )
    bundle_registry = FileSystemBundleRegistry(base_data_path=bundle_storage_path)
    bundle_service = BundleService(bundle_registry=bundle_registry)
    bundle_storage = FileSystemParquetBundleStorage(base_data_path=bundle_storage_path, compression_level=5)
    data_bundle_source = LimexHubDataSource.from_env()

    calendar = ec_get_calendar(trading_calendar, start=start_date - datetime.timedelta(days=30))

    bundle_version = str(int(datetime.datetime.now(tz=calendar.tz).timestamp()))
    assets_repository = SqlAlchemyAssetRepository(base_storage_path=bundle_storage_path,
                                                  bundle_name=bundle,
                                                  bundle_version=bundle_version,
                                                  future_chain_predicates=CHAIN_PREDICATES)
    adjustments_repository = SqlAlchemyAdjustmentRepository(base_storage_path=bundle_storage_path,
                                                            bundle_name=bundle,
                                                            bundle_version=bundle_version)

    await bundle_service.ingest_bundle(
        date_start=start_date.replace(tzinfo=calendar.tz),
        date_end=end_date.replace(tzinfo=calendar.tz),
        bundle_storage=bundle_storage,
        data_bundle_source=data_bundle_source,
        frequency=DataFrequency(frequency).to_timedelta(),
        symbols=symbols_parsed,
        name=bundle,
        bundle_version=bundle_version,
        trading_calendar=calendar,
        assets_repository=assets_repository,
        adjustments_repository=adjustments_repository,
        forward_fill_missing_ohlcv_data=True
    )


@main.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.option(
    "--bundle-storage-path",
    default=Path(Path.home(), ".ziplime", "data"),
    show_default=True,
    help="Path to the bundle storage on filesystem.",
)
@click.option(
    "-b",
    "--bundle",
    metavar="BUNDLE-NAME",
    show_default=True,
    help="The data bundle to clean.",
)
@click.option(
    "-e",
    "--before",
    type=Timestamp(),
    help="Clear all data before TIMESTAMP."
         " This may not be passed with -k / --keep-last",
)
@click.option(
    "-a",
    "--after",
    type=Timestamp(),
    help="Clear all data after TIMESTAMP"
         " This may not be passed with -k / --keep-last",
)
@click.option(
    "-k",
    "--keep-last",
    type=int,
    metavar="N",
    help="Clear all but the last N downloads."
         " This may not be passed with -e / --before or -a / --after",
)
@click.pass_context
async def clean(ctx, bundle_storage_path, bundle, before, after, keep_last):
    """Top level ziplime entry point."""

    bundle_registry = FileSystemBundleRegistry(base_data_path=bundle_storage_path)

    bundle_service = BundleService(bundle_registry=bundle_registry)
    await bundle_service.clean(bundle_name=bundle, after=after, before=before,
                               keep_last=keep_last)


@main.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.option(
    "--bundle-storage-path",
    default=Path(Path.home(), ".ziplime", "data"),
    show_default=True,
    help="Path to the bundle storage on filesystem.",
)
@click.pass_context
async def bundles(ctx, bundle_storage_path):
    """Top level ziplime entry point."""
    bundle_registry = FileSystemBundleRegistry(base_data_path=bundle_storage_path)

    for bundle in await bundle_registry.list_bundles():
        click.echo(f"{bundle["name"]} {bundle["version"]} - {bundle['timestamp']}")


@main.command()
@click.option(
    "-f",
    "--algofile",
    default=None,
    type=click.File("r"),
    help="The file that contains the algorithm to run.",
)
@click.option(
    "--emission-rate",
    type=click.Choice([df.value for df in DataFrequency]),
    show_default=True,
    help="Emission rate of the simulation.",
)
@click.option(
    "--capital-base",
    type=float,
    help="The starting capital for the simulation.",
)
@click.option(
    "-b",
    "--bundle",
    metavar="BUNDLE-NAME",
    help="The data bundle to use for the simulation.",
)
@click.option(
    "-bf",
    "--benchmark-file",
    default=None,
    type=click.Path(exists=True, dir_okay=False, readable=True, path_type=str),
    help="The csv file that contains the benchmark returns",
)
@click.option(
    "--benchmark-symbol",
    default=None,
    type=click.STRING,
    help="The symbol of the instrument to be used as a benchmark "
         "(should exist in the ingested bundle)",
)
@click.option(
    "--benchmark-sid",
    default=None,
    type=int,
    help="The sid of the instrument to be used as a benchmark "
         "(should exist in the ingested bundle)",
)
@click.option(
    "--no-benchmark",
    is_flag=True,
    default=False,
    help="If passed, use a benchmark of zero returns.",
)
@click.option(
    "-s",
    "--start-date",
    type=click.DateTime(),
    help="The start date of the simulation.",
)
@click.option(
    "-e",
    "--end-date",
    type=click.DateTime(),
    help="The end date of the simulation.",
)
@click.option(
    "-o",
    "--output",
    default="-",
    metavar="FILENAME",
    show_default=True,
    help="The location to write the perf data. If this is '-' the perf will"
         " be written to stdout.",
)
@click.option(
    "--trading-calendar",
    metavar="TRADING-CALENDAR",
    default="NYSE",
    help="The calendar you want to use e.g. XLON. XNYS is the default.",
)
@click.option(
    "--print-algo/--no-print-algo",
    is_flag=True,
    default=False,
    help="Print the algorithm to stdout.",
)
@click.option(
    "--exchange-name",
    default='LIME',
    help="The exchange to use for trading.",
    show_default=True,
)
@click.option(
    "--exchange-type",
    default='simulation',
    type=click.Choice(['simulation', 'lime-trader-sdk']),
    help="The exchange to use for trading.",
    show_default=True,
)
@click.option(
    "--live-market-data-provider",
    type=click.Choice(['lime-trader-sdk']),
    default=None,
    help="Market data provider for live trading",
)
@click.option(
    "--bundle-storage-path",
    default=Path(Path.home(), ".ziplime", "data"),
    show_default=True,
    help="Path to the bundle storage on filesystem.",
)
@click.pass_context
async def run(
        ctx,
        algofile,
        emission_rate,
        capital_base,
        bundle,
        benchmark_file,
        benchmark_symbol,
        benchmark_sid,
        no_benchmark,
        start_date,
        end_date,
        output,
        trading_calendar,
        print_algo,
        bundle_storage_path,
        exchange_name: str,
        exchange_type: str,
        live_market_data_provider: str | None,
):
    """Run a backtest for the given algorithm."""

    calendar = ec_get_calendar(trading_calendar,
                               start=strip_time_and_timezone_info(start_date) - datetime.timedelta(days=30))

    benchmark_spec = BenchmarkSpec(
        benchmark_returns=None,
        benchmark_sid=benchmark_sid,
        benchmark_symbol=benchmark_symbol,
        benchmark_file=benchmark_file,
        no_benchmark=no_benchmark,
    )
    algotext = None
    if algofile is not None:
        algotext = algofile.read()
    bundle_registry = FileSystemBundleRegistry(base_data_path=bundle_storage_path)

    if exchange_type == "simulation":
        exchange_class = SimulationExchange(
            name=exchange_name,
            equity_slippage=FixedBasisPointsSlippage(),
            equity_commission=PerShare(
                cost=DEFAULT_PER_SHARE_COST,
                min_trade_cost=DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE,

            ),
            future_slippage=VolatilityVolumeShare(
                volume_limit=DEFAULT_FUTURE_VOLUME_SLIPPAGE_BAR_LIMIT,
            ),
            future_commission=PerContract(
                cost=DEFAULT_PER_CONTRACT_COST,
                exchange_fee=FUTURE_EXCHANGE_FEES_BY_SYMBOL,
                min_trade_cost=DEFAULT_MINIMUM_COST_PER_FUTURE_TRADE
            ),
        )
    elif exchange_type == "lime-trader-sdk":
        exchange_class = LimeTraderSdkExchange(
            name=exchange_name,
            lime_sdk_credentials_file=None
        )

    else:
        raise Exception("Not valid exchange.")
    max_shares = int(1e11)

    sim_params = SimulationParameters(
        start_date=start_date.replace(tzinfo=calendar.tz),
        end_date=end_date.replace(tzinfo=calendar.tz),
        trading_calendar=calendar,
        capital_base=capital_base,
        emission_rate=DataFrequency(emission_rate).to_timedelta(),
        max_shares=max_shares,
        exchange=exchange_class,
        bundle_name=bundle
    )

    clock = SimulationClock(
        sessions=sim_params.sessions,
        market_opens=sim_params.market_opens,
        market_closes=sim_params.market_closes,
        before_trading_start_minutes=sim_params.before_trading_start_minutes,
        emission_rate=sim_params.emission_rate,
        timezone=sim_params.trading_calendar.tz
    )
    timedelta_diff_from_current_time = datetime.datetime.now(tz=sim_params.trading_calendar.tz) - start_date.replace(
        tzinfo=sim_params.trading_calendar.tz)

    # clock = RealtimeClock(
    #     sessions=sim_params.sessions,
    #     market_opens=sim_params.market_opens,
    #     market_closes=sim_params.market_closes,
    #     before_trading_start_minutes=sim_params.before_trading_start_minutes,
    #     emission_rate=sim_params.emission_rate,
    #     timezone=sim_params.trading_calendar.tz,
    #     timedelta_diff_from_current_time=-timedelta_diff_from_current_time
    # )

    result = await run_algorithm(
        algofile=getattr(algofile, "name", "<algorithm>"),
        algotext=algotext,
        print_algo=print_algo,
        metrics_set=default_metrics(),
        benchmark_spec=benchmark_spec,
        custom_loader=None,
        missing_data_bundle_source=get_data_source(
            live_market_data_provider) if live_market_data_provider is not None else None,
        bundle_registry=bundle_registry,
        simulation_params=sim_params,
        clock=clock

    )

    if output == "-":
        click.echo(str(result))
    elif output != os.devnull:  # make the ziplime magic not write any data
        # TODO: test this
        result.to_pickle(output)
    return result


if __name__ == "__main__":
    asyncio.run(main())
