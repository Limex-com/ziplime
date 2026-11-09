from unittest.mock import patch

import pytest

from examples.currencies.run_investment_pnl import run_example
from ziplime.finance.domain.cash_event_kind import CashEventKind
from ziplime.finance.monetary_valuation_service import MonetaryValuationService


@pytest.mark.asyncio
async def test_example_shows_mixed_order_values_historical_costs_and_post_sale_cash_fx(tmp_path):
    with patch(
            target="ziplime.trading.trading_algorithm.MonetaryValuationService", wraps=MonetaryValuationService,
    ) as service_factory:
        result = await run_example(workspace=tmp_path)
    service_factory.assert_called_once_with(
        conversion=result.trading_algorithm.currency_conversion,
        bond_book=result.trading_algorithm._ledger.bond_book,
    )
    service = result.trading_algorithm._get_monetary_valuation_service()
    assert service is result.trading_algorithm._get_monetary_valuation_service()
    assert service.conversion is result.trading_algorithm.currency_conversion
    assert service.bond_book is result.trading_algorithm._ledger.bond_book
    assert result.errors == []
    assert list(result.perf_by_currency) == ["EUR", "USD"]
    usd = result.perf_by_currency["USD"]
    assert len(usd) == 5
    assert usd["submitted_orders_usd"].iloc[0] == pytest.approx(expected=10_000 / 82 + 150)
    assert usd["sber_pnl_eur"].iloc[1] == pytest.approx(expected=1000)
    assert usd["sber_pnl_usd"].iloc[1] == pytest.approx(expected=55 - 10_000 / 82)
    assert usd["investment_realized_pnl"].iloc[2] == pytest.approx(expected=55 - 10_000 / 82)
    assert usd["investment_unrealized_pnl"].iloc[-1] == pytest.approx(expected=0)
    assert usd["cash_fx_pnl"].iloc[-1] == pytest.approx(expected=-5)
    assert usd["portfolio_value"].iloc[-1] == pytest.approx(expected=225)
    assert usd["pnl"].sum() == pytest.approx(expected=200 - (10_000 / 82 + 150))
    assert usd["pnl"].iloc[-1] == pytest.approx(expected=0)
    history = result.trading_algorithm.investment_history()
    assert len([event for event in history.cash_events if event.kind == CashEventKind.EXTERNAL]) == 1
    assert len([event for event in history.cash_events if event.kind == CashEventKind.FX]) == 2
