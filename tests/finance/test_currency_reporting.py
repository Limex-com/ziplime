import datetime
from dataclasses import replace

import numpy as np
import polars as pl
import pytest
import empyrical

from tests.finance.currency_fixtures import conversion, currency, equity_listing, listing, reporting_tracker
from ziplime.data.services.currency_conversion_service import CurrencyConversionService
from ziplime.finance.domain.currency_reporting import CurrencyReporting
from ziplime.finance.domain.transaction import Transaction
from ziplime.sources.currency_benchmark_source import CurrencyBenchmarkSource
from ziplime.trading.currency_results import currency_performance

UTC = datetime.timezone.utc


def test_rates_use_metadata_inverse_and_only_known_observations():
    stamps = [datetime.datetime(year=2024, month=1, day=day, tzinfo=UTC) for day in (1, 3)]
    fx = conversion(stamps=stamps, rates=[1.1, 1.2])
    middle = stamps[0] + datetime.timedelta(days=1)
    assert fx.rate(base="EUR", quote="USD", dt=middle) == 1.1
    assert fx.rate(base="USD", quote="EUR", dt=middle) == pytest.approx(expected=1 / 1.1)
    assert fx.rate(base="EUR", quote="EUR", dt=middle) == 1
    with pytest.raises(expected_exception=ValueError, match="fresh"):
        fx.rate(base="EUR", quote="USD", dt=stamps[0] - datetime.timedelta(seconds=1))
    with pytest.raises(expected_exception=ValueError, match="fresh"):
        fx.rate(base="EUR", quote="USD", dt=stamps[-1] + datetime.timedelta(days=8))
    with pytest.raises(expected_exception=ValueError, match="timezone"):
        fx.rate(base="EUR", quote="USD", dt=datetime.datetime(year=2024, month=1, day=1))
    with pytest.raises(expected_exception=ValueError, match="listing"):
        fx.rate(base="EUR", quote="GBP", dt=middle)


@pytest.mark.parametrize(argnames="rate", argvalues=[0.0, -1.0, float("nan"), float("inf"), None])
def test_invalid_rates_fail_explicitly(rate):
    with pytest.raises(expected_exception=ValueError, match="finite and positive"):
        conversion(stamps=[datetime.datetime(year=2024, month=1, day=1, tzinfo=UTC)], rates=[rate])


def test_reporting_currencies_preserve_order_and_reject_duplicates():
    fx = conversion(stamps=[datetime.datetime(year=2024, month=1, day=1, tzinfo=UTC)], rates=[1.1])
    assert CurrencyReporting(base_currency="EUR", currencies=("USD", "EUR"), conversion=fx).currencies == ("USD", "EUR")
    with pytest.raises(expected_exception=ValueError, match="unique"):
        CurrencyReporting(base_currency="EUR", currencies=("EUR", "EUR"), conversion=fx)


@pytest.mark.asyncio
@pytest.mark.parametrize(argnames="intraday", argvalues=[False, True])
async def test_fx_only_pnl_returns_and_histories_are_independent(intraday):
    tracker, native, sessions, closes = reporting_tracker(intraday=intraday)
    await tracker.handle_start_of_simulation()
    packets = []
    for ix, (session, close) in enumerate(zip(sessions, closes)):
        native.start_of_session(session_label=session)
        await tracker.handle_market_open(session_label=session)
        if intraday:
            tracker.handle_minute_close(dt=close)
        native.end_of_session(session_ix=ix)
        packets.append(tracker.handle_market_close(dt=close))
    frames = currency_performance(packets=packets)
    assert list(frames) == ["EUR", "USD"]
    np.testing.assert_allclose(actual=frames["EUR"]["pnl"], desired=0)
    np.testing.assert_allclose(actual=frames["USD"]["pnl"], desired=[10, 10, 10])
    np.testing.assert_allclose(actual=frames["USD"]["returns"], desired=[0.1, 1.2 / 1.1 - 1, 1.3 / 1.2 - 1])
    np.testing.assert_allclose(
        actual=tracker._currency_trackers["USD"].daily_returns_array, desired=frames["USD"]["returns"])
    assert frames["USD"]["benchmark_period_return"].eq(other=0).all()
    assert native.portfolio.cash == 100
    assert tracker._currency_trackers["USD"].portfolio.pnl == pytest.approx(expected=30)
    assert frames["USD"]["algo_volatility"].iloc[-1] == pytest.approx(
        expected=empyrical.annual_volatility(returns=frames["USD"]["returns"].to_numpy()))
    assert tracker._currency_trackers["USD"]._metrics is not tracker._currency_trackers["EUR"]._metrics
    assert all(child._ledger is native for child in tracker._currency_trackers.values())
    assert not np.shares_memory(tracker._currency_trackers["USD"].daily_returns_array,
                                tracker._currency_trackers["EUR"].daily_returns_array)
    assert list(tracker.handle_simulation_end()["risk_report_by_currency"]) == ["EUR", "USD"]


@pytest.mark.asyncio
async def test_cash_flows_use_event_rates_and_deposits_are_not_pnl():
    tracker, native, sessions, closes = reporting_tracker()
    await tracker.handle_start_of_simulation()
    native.start_of_session(session_label=sessions[0])
    await tracker.handle_market_open(session_label=sessions[0])
    # A fee at the initial rate, followed by a deposit after the rate changes.
    native.set_reporting_datetime(dt=tracker._trading_calendar.session_open(session=sessions[0]).to_pydatetime())
    native._cash_flow(amount=-5)
    await native.update_portfolio()
    native.set_reporting_datetime(dt=closes[0])
    await native.capital_change(change_amount=50)
    native.end_of_session(session_ix=0)
    packet = tracker.handle_market_close(dt=closes[0])["metrics_by_currency"]["USD"]
    assert packet["daily_perf"]["capital_used"] == -5
    assert packet["daily_perf"]["pnl"] == pytest.approx(expected=4.5)
    assert packet["daily_perf"]["returns"] == pytest.approx(expected=0.95 * 1.1 - 1)
    assert packet["daily_perf"]["portfolio_value"] == pytest.approx(expected=159.5)


@pytest.mark.asyncio
async def test_mixed_quote_is_rejected_before_execution():
    tracker, native, _, closes = reporting_tracker()
    await tracker.handle_start_of_simulation()
    asset = replace(equity_listing(), quote=currency(name="USD", identity=2))
    with pytest.raises(expected_exception=ValueError, match="single EUR book"):
        native.process_transaction(transaction=Transaction(
            id="test", amount=1, dt=closes[0], price=10,
            asset=asset, exchange_name="XNYS", trading_account_id="account"))
    assert native.portfolio.cash == 100
    assert not native.transactions()


@pytest.mark.asyncio
async def test_benchmark_is_recomputed_in_reporting_currency():
    tracker, _, sessions, closes = reporting_tracker()
    source = tracker._benchmark_source
    source.benchmark_asset = equity_listing()
    source._precalculated_series = pl.DataFrame(data={"date": closes, "close": [100.0, 110.0, 110.0]})
    benchmark = CurrencyBenchmarkSource(
        source=source, reporting=tracker._currency_reporting, currency="USD",
        initial_dt=tracker._trading_calendar.session_open(session=sessions[0]).to_pydatetime())
    result = benchmark.daily_returns_by_session(sessions=sessions)
    np.testing.assert_allclose(actual=result["pct_change"], desired=[0.1, 1.1 * 1.2 / 1.1 - 1, 1.3 / 1.2 - 1])
    intraday = benchmark.get_range(start_dt=closes[0], end_dt=closes[-1])
    np.testing.assert_allclose(actual=intraday["pct_change"], desired=result["pct_change"])
    np.testing.assert_allclose(actual=benchmark.get_range(start_dt=closes[1], end_dt=closes[-1])["pct_change"],
                               desired=result["pct_change"][1:])


@pytest.mark.asyncio
async def test_supplied_benchmark_returns_require_currency_and_are_translated():
    tracker, _, sessions, closes = reporting_tracker()
    source = tracker._benchmark_source
    source.benchmark_returns = pl.Series(values=[0.01, 0.02, -0.01])
    reporting = tracker._currency_reporting
    initial = tracker._trading_calendar.session_open(session=sessions[0]).to_pydatetime()
    with pytest.raises(expected_exception=ValueError, match="benchmark_currency"):
        CurrencyBenchmarkSource(source=source, reporting=reporting, currency="USD", initial_dt=initial)
    benchmark = CurrencyBenchmarkSource(
        source=source, reporting=replace(reporting, benchmark_currency="EUR"), currency="USD", initial_dt=initial)
    np.testing.assert_allclose(actual=benchmark.daily_returns_by_session(sessions=sessions)["pct_change"],
                               desired=[1.01 * 1.1 - 1, 1.02 * 1.2 / 1.1 - 1, 0.99 * 1.3 / 1.2 - 1])
    np.testing.assert_allclose(actual=benchmark.get_range(start_dt=closes[0], end_dt=closes[-1])["pct_change"],
                               desired=benchmark.daily_returns_by_session(sessions=sessions)["pct_change"])


def test_ambiguous_pair_selection_is_rejected():
    stamp = datetime.datetime(year=2024, month=1, day=1, tzinfo=UTC)
    pair = listing(asset=currency(name="EUR", identity=1), quote=currency(name="USD", identity=2))
    with pytest.raises(expected_exception=ValueError, match="exactly one"):
        CurrencyConversionService(
            listings=[pair, replace(pair, sid=4)],
            data=pl.DataFrame(data={"sid": [3, 4], "date": [stamp, stamp], "close": [1.1, 1.2]}))
