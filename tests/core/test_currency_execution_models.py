import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import polars as pl
import pytest

from tests.finance.currency_fixtures import currency, listing
from tests.finance.investment_fixtures import SBER, START
from ziplime.core.algorithm_file import AlgorithmFile
from ziplime.core.run_simulation import run_simulation, run_simulation_iter
from ziplime.exchanges.repositories.in_memory_exchange_repository import InMemoryExchangeRepository
from ziplime.exchanges.simulation_exchange import SimulationExchange
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.commission.per_share import PerShare
from ziplime.finance.commission.per_trade import PerTrade
from ziplime.finance.execution import MarketOrder
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.gens.domain.simulation_clock import SimulationClock
from ziplime.utils import run_algo
from ziplime.utils.calendar_utils import get_calendar

DAY = datetime.timedelta(days=1)
PAIR = listing(currency("USD", 1), SBER.quote, sid=3)


def make_exchange(clock, equity_slippage, equity_commission, **currency_models):
    return SimulationExchange(
        name="SIM", country_code="US", trading_calendar=clock.trading_calendar, clock=clock,
        cash_balance=10000, equity_slippage=equity_slippage, equity_commission=equity_commission,
        future_slippage=NoSlippage(), future_commission=NoCommission(),
        account_id="account", is_default=True, **currency_models)


async def fill_both(exchange):
    async def prices(assets, **kwargs):
        asset = next(iter(assets))
        price = 82.0 if asset == PAIR else 100.0
        return pl.DataFrame({"sid": [asset.sid], "date": [START], "volume": [1_000_000.0],
                             "open": [price], "close": [price], "low": [price], "high": [price]})

    exchange.get_spot_value = AsyncMock(side_effect=prices)
    fx = await exchange.order(PAIR, 10, MarketOrder())
    stock = await exchange.order(SBER, 2, MarketOrder())
    fx.dt = stock.dt = START
    transactions, commissions, closed = await exchange.get_transactions(
        {SBER: {stock.id: stock}, PAIR: {fx.id: fx}}, START, same_bar_execution=True)
    assert closed == [fx, stock]
    assert all(fee.transaction in transactions for fee in commissions)
    return {transaction.asset: transaction for transaction in transactions}, {
        fee.asset: fee.amount for fee in commissions}


@pytest.mark.asyncio
async def test_default_currency_fills_and_fees_are_independent_of_equity_models():
    calendar = get_calendar("XNYS")
    clock = SimulationClock(start_date=START, end_date=START + DAY, trading_calendar=calendar, emission_rate=DAY)
    exchange = make_exchange(clock, FixedBasisPointsSlippage(basis_points=100), PerShare(cost=10, min_trade_cost=0))

    transactions, fees = await fill_both(exchange)

    assert transactions[PAIR].price == 82.0
    assert PAIR not in fees
    assert transactions[SBER].price == 101.0
    assert fees[SBER] == 20


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["algorithm", "algorithm_iter", "simulation", "simulation_iter"])
@pytest.mark.parametrize("preconfigured_exchange", [False, True])
async def test_currency_models_reach_execution_through_all_entrypoints(
        tmp_path, monkeypatch, entrypoint, preconfigured_exchange):
    calendar = get_calendar("XNYS")
    clock = SimulationClock(start_date=START, end_date=START + DAY, trading_calendar=calendar, emission_rate=DAY)
    equity_slippage = NoSlippage()
    equity_commission = PerShare(cost=0.5, min_trade_cost=0)
    currency_slippage = FixedBasisPointsSlippage(basis_points=100)
    currency_commission = PerTrade(cost=7)
    models = {"currency_slippage": currency_slippage, "currency_commission": currency_commission}
    exchange = make_exchange(clock, equity_slippage, equity_commission,
                             **(models if preconfigured_exchange else {}))
    algorithm_path = tmp_path / "strategy.py"
    algorithm_path.write_text("")

    class Executor:
        async def run_algorithm(self, trading_algorithm):
            prepared = await trading_algorithm.exchange_repository.get_default_exchange()
            assert prepared.get_slippage_model(PAIR) is currency_slippage
            assert prepared.get_commission_model(PAIR) is currency_commission
            assert prepared.get_slippage_model(SBER) is equity_slippage
            assert prepared.get_commission_model(SBER) is equity_commission
            transactions, fees = await fill_both(prepared)
            assert transactions[PAIR].price == pytest.approx(82.82)
            assert fees[PAIR] == 7
            assert transactions[SBER].price == 100
            assert fees[SBER] == 1
            return SimpleNamespace(errors=[], exchange=prepared)

        async def run_algorithm_iter(self, trading_algorithm):
            yield SimpleNamespace(result=await self.run_algorithm(trading_algorithm))

    monkeypatch.setattr(run_algo, "TradingAlgorithmExecutor", Executor)
    options = {} if preconfigured_exchange else models
    await InMemoryExchangeRepository.get_default_exchange.cache.clear()
    try:
        if entrypoint.startswith("algorithm"):
            repository = InMemoryExchangeRepository()
            await repository.add_exchange(exchange)
            kwargs = dict(
                algorithm=AlgorithmFile(str(algorithm_path)), asset_service=Mock(), print_algo=False,
                metrics_set=[], custom_loader=None, clock=clock, custom_data_sources=[],
                exchange_repository=repository, **options)
            if entrypoint == "algorithm":
                result = await run_algo.run_algorithm(**kwargs)
            else:
                statuses = [status async for status in run_algo.run_algorithm_iter(**kwargs)]
                result = statuses[-1].result
        else:
            kwargs = dict(
                start_date=START, end_date=START + DAY, trading_calendar="XNYS", emission_rate=DAY,
                total_cash=10000, market_data_source=SimpleNamespace(name="prices", frequency=DAY),
                custom_data_sources=[], algorithm_file=str(algorithm_path), stop_on_error=True,
                asset_service=Mock(), print_algo=False, clock=clock,
                equity_slippage=equity_slippage, equity_commission=equity_commission,
                **options)
            if preconfigured_exchange:
                kwargs["exchange"] = exchange
            if entrypoint == "simulation":
                result = await run_simulation(**kwargs)
            else:
                statuses = [status async for status in run_simulation_iter(**kwargs)]
                result = statuses[-1].result
        assert not result.errors
    finally:
        await InMemoryExchangeRepository.get_default_exchange.cache.clear()
