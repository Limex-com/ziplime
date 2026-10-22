import datetime
from dataclasses import replace

import numpy as np
import pandas as pd
import polars as pl
import pytest

from tests.finance.currency_fixtures import currency, equity_listing, listing
from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation, run_simulation_iter
from ziplime.constants.data_type import DataType
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services.currency_conversion_service import CurrencyConversionService
from ziplime.data.services.file_system_delta_lake_bundle_storage import FileSystemDeltaLakeBundleStorage
from ziplime.exchanges.simulation_exchange import SimulationExchange
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.commission.per_share import PerShare
from ziplime.finance.domain.currency_reporting import CurrencyReporting
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.utils.calendar_utils import get_calendar
from ziplime.vectorized.adapter import to_execution_result
from ziplime.vectorized.kernel import ExecutionTiming, simulate_signals


@pytest.mark.asyncio
@pytest.mark.parametrize(argnames="intraday", argvalues=[False, True])
@pytest.mark.parametrize(argnames="investment", argvalues=[False, True])
async def test_delta_rates_event_vector_and_streaming_results_agree(tmp_path, intraday, investment):
    asset_service = get_asset_service(db_path=str(tmp_path / "assets.sqlite"))
    try:
        calendar = get_calendar("XNYS")
        sessions = pd.date_range(start="2024-01-02", periods=3, freq="B")
        if intraday:
            index = calendar.sessions_minutes(start=sessions[0], end=sessions[-1])
        else:
            index = pd.DatetimeIndex(data=[calendar.session_close(session=session) for session in sessions])
        index = index.tz_convert(tz=calendar.tz)
        frequency = datetime.timedelta(minutes=1) if intraday else datetime.timedelta(days=1)
        start, end = datetime.date(year=2020, month=1, day=1), datetime.date(year=2030, month=1, day=1)
        await asset_service.save_exchanges(exchanges=[equity_listing().exchange])
        eur, usd = await asset_service.save_currencies(currencies=[
            replace(currency(name="EUR", identity=1), id=None, start_date=start, end_date=end,
                    first_traded=start, auto_close_date=end),
            replace(currency(name="USD", identity=2), id=None, start_date=start, end_date=end,
                    first_traded=start, auto_close_date=end),
        ])
        [asset] = await asset_service.save_equities(equities=[
            replace(equity_listing().asset, id=None, start_date=start, end_date=end,
                    first_traded=start, auto_close_date=end)])
        [equity, fx_listing] = await asset_service.save_exchange_assets(exchange_assets=[
            replace(equity_listing(), sid=None, symbol="TEST", asset=asset, quote=eur,
                    start_date=start, end_date=end, first_traded=start, auto_close_date=end),
            replace(listing(asset=eur, quote=usd), sid=None, start_date=start, end_date=end,
                    first_traded=start, auto_close_date=end),
        ])
        initial = calendar.session_open(session=sessions[0]).to_pydatetime()
        closes = [calendar.session_close(session=session).to_pydatetime() for session in sessions]
        rate_stamps = [initial - datetime.timedelta(days=1), *closes]
        rates = pl.DataFrame(data={
            "sid": [fx_listing.sid] * 4, "date": rate_stamps, "close": [1.0, 1.1, 1.2, 1.3],
        })
        fx_bundle = DataBundle(
            name="fx", version="1", start_date=rate_stamps[0], end_date=closes[-1],
            trading_calendar=calendar, frequency=datetime.timedelta(days=1),
            original_frequency=datetime.timedelta(days=1), data_type=DataType.MARKET_DATA,
            timestamp=closes[-1], data=rates)
        storage = FileSystemDeltaLakeBundleStorage(base_data_path=str(tmp_path))
        await storage.store_bundle(data_bundle=fx_bundle, merge=False)
        # Resolve the listing again from the assets DB, then load its bars through Delta storage.
        from ziplime.assets.entities.asset_symbol import AssetSymbol
        stored_listing = await asset_service.get_exchange_currency_by_symbol(
            symbol=AssetSymbol(symbol=fx_listing.symbol, mic="XNYS"))
        conversion = await CurrencyConversionService.from_bundle(
            storage=storage, bundle=fx_bundle, listings=[stored_listing], start=initial, end=closes[-1])
        reporting = CurrencyReporting(
            base_currency="EUR",
            currencies=("USD", "EUR"),
            conversion=conversion,
            investment_pnl=investment,
        )
        commission = PerShare(cost=0.5, min_trade_cost=0) if investment else NoCommission()
        prices = pd.DataFrame(data={"TEST": np.linspace(start=10, stop=12, num=len(index))}, index=index)
        data = pl.DataFrame(data={
            "sid": [equity.sid] * len(index), "date": index.to_pydatetime().tolist(),
            **{field: prices["TEST"].to_list() for field in ("open", "high", "low", "close")},
            "volume": [1e6] * len(index),
        })
        bundle = DataBundle(
            name="market", version="1", start_date=index[0].to_pydatetime(),
            end_date=index[-1].to_pydatetime(), trading_calendar=calendar, frequency=frequency,
            original_frequency=frequency, data_type=DataType.MARKET_DATA,
            timestamp=index[-1].to_pydatetime(), data=data,
            sid_indexes={equity.sid: (0, len(index))})
        algorithm = tmp_path / "algorithm.py"
        algorithm.write_text(
            data="from ziplime.finance.execution import MarketOrder\n"
            "async def initialize(context):\n"
            "    context.asset = await context.symbol('TEST', mic='XNYS')\n"
            "    context.bought = False\n"
            "async def handle_data(context, data):\n"
            "    if not context.bought:\n"
            "        await context.order(asset=context.asset, amount=2, style=MarketOrder())\n"
            "        if context.currency_reporting.investment_pnl:\n"
            "            orders = list(context.new_orders.values())\n"
            "            valued = await context.value_orders(orders, currency='USD')\n"
            "            assert valued.gross_value > 0\n"
            "            assert orders[0].submitted_valuations['USD'].monetary.value > 0\n"
            "        context.bought = True\n"
            "    if context.currency_reporting.investment_pnl:\n"
            "        snapshot = await context.investment_pnl(currency='USD')\n"
            "        context.record(investment_now=snapshot.total_pnl)\n"
        )
        kwargs = dict(
            start_date=datetime.datetime.combine(date=sessions[0].date(), time=datetime.time.min, tzinfo=calendar.tz),
            end_date=datetime.datetime.combine(date=sessions[-1].date(), time=datetime.time.max, tzinfo=calendar.tz),
            trading_calendar="XNYS", emission_rate=frequency, total_cash=100,
            market_data_source=bundle, custom_data_sources=[], algorithm_file=str(algorithm),
            stop_on_error=True, asset_service=asset_service, equity_commission=commission,
            equity_slippage=NoSlippage(), print_algo=False, intraday_metrics=intraday,
            currency_reporting=reporting,
        )
        event = await run_simulation(**kwargs)
        entries = pd.DataFrame(data=False, index=index, columns=["TEST"])
        entries.iloc[0, 0] = True
        vector = simulate_signals(
            prices=prices, entries=entries, exits=entries & False, size=2, initial_cash=100,
            execution=ExecutionTiming.same_close(), assets={"TEST": equity}, commission=commission)
        exchange = SimulationExchange(
            name="LIME", country_code="US", trading_calendar=calendar, clock=None, cash_balance=100,
            equity_slippage=NoSlippage(), future_slippage=NoSlippage(),
            equity_commission=NoCommission(), future_commission=NoCommission(),
            account_id="simulation_account", is_default=True)
        replay = await to_execution_result(
            portfolio=vector,
            listings={"TEST": equity},
            prices=prices,
            trading_calendar=calendar,
            exchange=exchange,
            emission_rate=frequency,
            currency_reporting=reporting,
        )
        assert list(event.perf_by_currency) == ["USD", "EUR"]
        assert len(event.perf_by_currency["USD"]) == 3
        for code in reporting.currencies:
            for column in ("pnl", "returns", "portfolio_value", "capital_used", "ending_cash"):
                np.testing.assert_allclose(
                    actual=event.perf_by_currency[code][column], desired=replay.perf_by_currency[code][column],
                    rtol=0, atol=1e-10, err_msg=f"{code}.{column}")
        for column in ("pnl", "returns", "portfolio_value", "capital_used"):
            np.testing.assert_allclose(
                actual=event.perf[column],
                desired=event.perf_by_currency["EUR"][column],
                atol=1e-10,
            )
        ending_value = 103 if investment else 104
        assert event.perf_by_currency["USD"]["portfolio_value"].iloc[-1] == pytest.approx(expected=ending_value * 1.3)
        if investment:
            expected_rate = 1.0 if intraday else 1.1
            expected_cost = 21 * expected_rate
            assert event.perf_by_currency["USD"]["investment_cost"].iloc[-1] == pytest.approx(expected=expected_cost)
            assert event.perf_by_currency["USD"]["investment_fees"].iloc[-1] == pytest.approx(expected=expected_rate)
            assert event.perf_by_currency["USD"]["investment_unrealized_pnl"].iloc[-1] == pytest.approx(
                expected=24 * 1.3 - expected_cost)
            np.testing.assert_allclose(
                actual=event.perf_by_currency["USD"]["investment_pnl"],
                desired=replay.perf_by_currency["USD"]["investment_pnl"], atol=1e-10)
        assert event.perf_by_currency["USD"]["capital_used"].iloc[0] == pytest.approx(
            expected=-(21 if investment else 20) * (1.0 if intraday else 1.1))
        kwargs["custom_data_sources"] = []
        statuses = [status async for status in run_simulation_iter(**kwargs)]
        assert "metrics_by_currency" in statuses[-1].perf
        for code in reporting.currencies:
            pd.testing.assert_series_equal(
                left=statuses[-1].result.perf_by_currency[code]["pnl"], right=event.perf_by_currency[code]["pnl"])
        if investment and not intraday:
            [us_equity] = await asset_service.save_exchange_assets(exchange_assets=[
                replace(equity, sid=None, quote=usd, symbol="USD_TEST")])
            us_data = data.with_columns(
                pl.lit(value=us_equity.sid).cast(data.schema["sid"]).alias("sid"),
                *[pl.lit(value=30.0).alias(field) for field in ("open", "high", "low", "close")])
            kwargs["market_data_source"] = DataBundle(
                name="mixed-market", version="1",
                start_date=bundle.start_date, end_date=bundle.end_date,
                trading_calendar=calendar, frequency=frequency, original_frequency=frequency,
                data_type=DataType.MARKET_DATA, timestamp=bundle.timestamp, data=pl.concat(items=[data, us_data]),
                sid_indexes={equity.sid: (0, len(index)), us_equity.sid: (len(index), len(index) * 2)})
            kwargs["currency_reporting"] = replace(reporting, opening_cash={
                ("LIME", "simulation_account", "EUR"): 100,
                ("LIME", "simulation_account", "USD"): 30,
            })
            algorithm.write_text(data=algorithm.read_text().replace(
                "        context.bought = True\n",
                "        us_asset = await context.symbol('USD_TEST', mic='XNYS')\n"
                "        await context.order(asset=us_asset, amount=1, style=MarketOrder())\n"
                "        both = await context.value_orders(list(context.new_orders.values()), currency='USD')\n"
                "        assert len(both.orders) == 2\n"
                "        assert abs(both.gross_value - 52) < 1e-9\n"
                "        assert both.net_value == both.gross_value\n"
                "        assert [v.monetary.native_currency for v in both.orders] == ['EUR', 'USD']\n"
                "        context.bought = True\n"))
            mixed = await run_simulation(**kwargs)
            usd_perf = mixed.perf_by_currency["USD"]
            assert usd_perf["portfolio_value"].iloc[-1] == pytest.approx(expected=103 * 1.3 + 29.5)
            assert usd_perf["investment_cost"].iloc[-1] == pytest.approx(expected=21 * 1.1 + 30.5)
            assert usd_perf["cash_fx_pnl"].iloc[-1] == pytest.approx(expected=100 * (1.3 - 1.0) - 21 * (1.3 - 1.1))
    finally:
        await asset_service._asset_repository.engine.dispose()
