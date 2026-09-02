import datetime
import structlog

from ziplime.finance.slippage.equity_slippage_model import EquitySlippageModel
from ziplime.finance.slippage.future_slippage_model import FutureSlippageModel
from ziplime.gens.domain.trading_clock import TradingClock
from ziplime.trading.trading_algorithm_execution_result import TradingAlgorithmExecutionResult
from ziplime.trading.trading_algorithm_execution_status import TradingAlgorithmExecutionStatus
from ziplime.utils.calendar_utils import get_calendar

from ziplime.assets.services.asset_service import AssetService
from ziplime.core.algorithm_file import AlgorithmFile
from ziplime.data.services.data_source import DataSource
from ziplime.finance.commission import PerShare, DEFAULT_PER_SHARE_COST, DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE, \
    PerContract, DEFAULT_PER_CONTRACT_COST, DEFAULT_MINIMUM_COST_PER_FUTURE_TRADE, EquityCommissionModel, \
    FutureCommissionModel, BondCommissionModel, PerBondTurnover
from ziplime.finance.constants import DEFAULT_BOND_COMMISSION_RATE
from ziplime.finance.constants import FUTURE_EXCHANGE_FEES_BY_SYMBOL
from ziplime.finance.margin import FuturesMarginModel
from ziplime.finance.metrics import default_metrics
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
from ziplime.finance.slippage.slippage_model import (
    DEFAULT_FUTURE_VOLUME_SLIPPAGE_BAR_LIMIT, SlippageModel,
)
from ziplime.finance.slippage.volatility_volume_share import VolatilityVolumeShare
from ziplime.gens.domain.simulation_clock import SimulationClock
from ziplime.exchanges.exchange import Exchange
from ziplime.exchanges.simulation_exchange import SimulationExchange
from ziplime.utils.run_algo import run_algorithm, run_algorithm_iter
import polars as pl
from typing import AsyncIterator
from typing import Literal
from ziplime.exchanges.repositories.in_memory_exchange_repository import InMemoryExchangeRepository

logger = structlog.get_logger(__name__)


async def run_simulation(
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        trading_calendar: str,
        emission_rate: datetime.timedelta,
        total_cash: float,
        market_data_source: DataSource,
        custom_data_sources: list[DataSource],
        algorithm_file: str,
        stop_on_error: bool,
        asset_service: AssetService,
        exchange: Exchange = None,
        default_exchange_name: str = "LIME",
        config_file: str | None = None,
        benchmark_asset_symbol: str | None = None,
        benchmark_returns: pl.Series | None = None,
        equity_commission: EquityCommissionModel | None = None,
        future_commission: FutureCommissionModel | None = None,
        equity_slippage: EquitySlippageModel | None = None,
        future_slippage: FutureSlippageModel | None = None,
        bond_commission: BondCommissionModel | None = None,
        bond_slippage: SlippageModel | None = None,
        clock: TradingClock | None = None,
        max_leverage: float = 1.0,
        same_bar_execution: bool = True,
        futures_margin_model: FuturesMarginModel | None = None,
        print_algo: bool = True,
        price_used_in_order_execution: Literal["open", "close", "low", "high"] = "close"
) -> TradingAlgorithmExecutionResult:
    """
    Run a trading algorithm simulation within a defined time period and trading environment.

    This function initializes the simulation's trading calendar, algorithm file, simulation
    clock, exchange, and other associated components. It sets up the runtime environment and
    then executes the algorithm asynchronously, using the provided market data source, custom
    data sources, and optional benchmark data.

    Args:
        start_date (datetime.datetime): The datetime marking the start of the simulation.
        end_date (datetime.datetime): The datetime marking the end of the simulation.
        trading_calendar (str): The identifier for the trading calendar to use.
        emission_rate (datetime.timedelta): The frequency of emission for simulation data.
        total_cash (float): The starting cash balance for the simulation account.
        market_data_source (DataSource): The primary market data source to use in the simulation.
        custom_data_sources (list[DataSource]): List of custom data sources for additional data.
        algorithm_file (str): Path to the Python file containing the trading algorithm.
        stop_on_error (bool): Whether the simulation should stop on encountering an error.
        exchange (Exchange, optional): Exchange instance to use for the simulation. Defaults to None.
        config_file (str | None, optional): Path to the configuration file for the algorithm. Defaults to None.
        benchmark_asset_symbol (str | None, optional): Symbol for an asset to use as the benchmark. Defaults to None.
        benchmark_returns (pl.Series | None, optional): Custom benchmark returns to use for evaluation. Defaults to None.
        asset_service (AssetService): Service for managing assets.
        equity_commission: Model used to calculate fees when trading equities.
                           If not specified, ziplime.finance.commission.PerShare model is used with a default
                           share cost of 0.001 and minimum cost of trade 0.00
       future_commission: Model used to calculate fees when trading futures.
                          If not specified, ziplime.finance.commission.PerContract model is used with a default
                          cost per contract of 0.85 and minimum cost of trade 0.00


    Returns:
        Coroutine: The coroutine to execute the simulation and produce output results.
    """
    calendar = get_calendar(trading_calendar)

    algo = AlgorithmFile(algorithm_file=algorithm_file, algorithm_config_file=config_file)
    if clock is None:
        clock = SimulationClock(
            trading_calendar=calendar,
            start_date=start_date,
            end_date=end_date,
            emission_rate=emission_rate,
        )
    if equity_commission is None:
        equity_commission = PerShare(
            cost=DEFAULT_PER_SHARE_COST,
            min_trade_cost=DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE,
        )
    if future_commission is None:
        future_commission = PerContract(
            cost=DEFAULT_PER_CONTRACT_COST,
            exchange_fee=FUTURE_EXCHANGE_FEES_BY_SYMBOL,
            min_trade_cost=DEFAULT_MINIMUM_COST_PER_FUTURE_TRADE
        )
    if bond_commission is None:
        # Bond desks bill a percentage of turnover, not a fee per unit; see PerBondTurnover.
        bond_commission = PerBondTurnover(cost=DEFAULT_BOND_COMMISSION_RATE)
    if equity_slippage is None:
        equity_slippage = FixedBasisPointsSlippage()
    if future_slippage is None:
        future_slippage = VolatilityVolumeShare(
            volume_limit=DEFAULT_FUTURE_VOLUME_SLIPPAGE_BAR_LIMIT,
        )
    if exchange is None:
        exchange = SimulationExchange(
            name=default_exchange_name,
            country_code="US",
            trading_calendar=calendar,
            data_source=market_data_source,
            equity_slippage=equity_slippage,
            equity_commission=equity_commission,
            future_slippage=future_slippage,
            future_commission=future_commission,
            bond_slippage=bond_slippage,
            bond_commission=bond_commission,
            cash_balance=total_cash,
            clock=clock,
            price_used_in_order_execution=price_used_in_order_execution,
            account_id="simulation_account",
            is_default=True
        )

    exchange_repository = InMemoryExchangeRepository()
    await exchange_repository.add_exchange(exchange)
    return await run_algorithm(
        algorithm=algo,
        asset_service=asset_service,
        print_algo=print_algo,
        metrics_set=default_metrics(),
        custom_loader=None,
        clock=clock,
        benchmark_returns=benchmark_returns,
        benchmark_asset_symbol=benchmark_asset_symbol,
        stop_on_error=stop_on_error,
        custom_data_sources=custom_data_sources,
        max_leverage=max_leverage,
        same_bar_execution=same_bar_execution,
        futures_margin_model=futures_margin_model,
        price_used_in_order_execution=price_used_in_order_execution,
        exchange_repository=exchange_repository
    )


async def run_simulation_iter(
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        trading_calendar: str,
        emission_rate: datetime.timedelta,
        total_cash: float,
        market_data_source: DataSource,
        custom_data_sources: list[DataSource],
        algorithm_file: str,
        stop_on_error: bool,
        asset_service: AssetService,
        exchange: Exchange = None,
        default_exchange_name: str = "LIME",
        config_file: str | None = None,
        benchmark_asset_symbol: str | None = None,
        benchmark_returns: pl.Series | None = None,
        equity_commission: EquityCommissionModel | None = None,
        future_commission: FutureCommissionModel | None = None,
        equity_slippage: EquitySlippageModel | None = None,
        future_slippage: FutureSlippageModel | None = None,
        bond_commission: BondCommissionModel | None = None,
        bond_slippage: SlippageModel | None = None,
        clock: TradingClock | None = None,
        max_leverage: float = 1.0,
        same_bar_execution: bool = True,
        futures_margin_model: FuturesMarginModel | None = None,
        print_algo: bool = True,
        price_used_in_order_execution: Literal["open", "close", "low", "high"] = "close"
) -> AsyncIterator[TradingAlgorithmExecutionStatus]:
    """
    Run a trading algorithm simulation within a defined time period and trading environment.

    This function initializes the simulation's trading calendar, algorithm file, simulation
    clock, exchange, and other associated components. It sets up the runtime environment and
    then executes the algorithm asynchronously, using the provided market data source, custom
    data sources, and optional benchmark data.

    Args:
        start_date (datetime.datetime): The datetime marking the start of the simulation.
        end_date (datetime.datetime): The datetime marking the end of the simulation.
        trading_calendar (str): The identifier for the trading calendar to use.
        emission_rate (datetime.timedelta): The frequency of emission for simulation data.
        total_cash (float): The starting cash balance for the simulation account.
        market_data_source (DataSource): The primary market data source to use in the simulation.
        custom_data_sources (list[DataSource]): List of custom data sources for additional data.
        algorithm_file (str): Path to the Python file containing the trading algorithm.
        stop_on_error (bool): Whether the simulation should stop on encountering an error.
        exchange (Exchange, optional): Exchange instance to use for the simulation. Defaults to None.
        config_file (str | None, optional): Path to the configuration file for the algorithm. Defaults to None.
        benchmark_asset_symbol (str | None, optional): Symbol for an asset to use as the benchmark. Defaults to None.
        benchmark_returns (pl.Series | None, optional): Custom benchmark returns to use for evaluation. Defaults to None.
        asset_service (AssetService): Service for managing assets.
        equity_commission: Model used to calculate fees when trading equities.
                           If not specified, ziplime.finance.commission.PerShare model is used with a default
                           share cost of 0.001 and minimum cost of trade 0.00
       future_commission: Model used to calculate fees when trading futures.
                          If not specified, ziplime.finance.commission.PerContract model is used with a default
                          cost per contract of 0.85 and minimum cost of trade 0.00


    Returns:
        Coroutine: The coroutine to execute the simulation and produce output results.
    """
    calendar = get_calendar(trading_calendar)

    algo = AlgorithmFile(algorithm_file=algorithm_file, algorithm_config_file=config_file)
    if clock is None:
        clock = SimulationClock(
            trading_calendar=calendar,
            start_date=start_date,
            end_date=end_date,
            emission_rate=emission_rate,
        )
    if equity_commission is None:
        equity_commission = PerShare(
            cost=DEFAULT_PER_SHARE_COST,
            min_trade_cost=DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE,
        )
    if future_commission is None:
        future_commission = PerContract(
            cost=DEFAULT_PER_CONTRACT_COST,
            exchange_fee=FUTURE_EXCHANGE_FEES_BY_SYMBOL,
            min_trade_cost=DEFAULT_MINIMUM_COST_PER_FUTURE_TRADE
        )
    if bond_commission is None:
        # Bond desks bill a percentage of turnover, not a fee per unit; see PerBondTurnover.
        bond_commission = PerBondTurnover(cost=DEFAULT_BOND_COMMISSION_RATE)
    if equity_slippage is None:
        equity_slippage = FixedBasisPointsSlippage()
    if future_slippage is None:
        future_slippage = VolatilityVolumeShare(
            volume_limit=DEFAULT_FUTURE_VOLUME_SLIPPAGE_BAR_LIMIT,
        )

    if exchange is None:
        if same_bar_execution and price_used_in_order_execution != "close":
            logger.warning(
                "You are using same bar execution while price_used_in_order_execution is not 'close'. "
                "Your simulation might be wrong.")

        exchange = SimulationExchange(
            name=default_exchange_name,
            country_code="US",
            trading_calendar=calendar,
            data_source=market_data_source,
            equity_slippage=equity_slippage,
            equity_commission=equity_commission,
            future_slippage=future_slippage,
            future_commission=future_commission,
            bond_slippage=bond_slippage,
            bond_commission=bond_commission,
            cash_balance=total_cash,
            clock=clock,
            price_used_in_order_execution=price_used_in_order_execution,
            is_default=True,
            account_id="simulation_account"
        )
    exchange_repository = InMemoryExchangeRepository()
    await exchange_repository.add_exchange(exchange)

    async for status in run_algorithm_iter(
            algorithm=algo,
            asset_service=asset_service,
            print_algo=print_algo,
            metrics_set=default_metrics(),
            custom_loader=None,
            clock=clock,
            benchmark_returns=benchmark_returns,
            benchmark_asset_symbol=benchmark_asset_symbol,
            stop_on_error=stop_on_error,
            custom_data_sources=custom_data_sources,
            max_leverage=max_leverage,
            same_bar_execution=same_bar_execution,
        futures_margin_model=futures_margin_model,
            price_used_in_order_execution=price_used_in_order_execution,
            exchange_repository=exchange_repository
    ):
        yield status
