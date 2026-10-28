import datetime
from dataclasses import replace
from unittest.mock import Mock

import pandas as pd
import polars as pl
import pytest

from tests.finance.currency_fixtures import currency, listing
from tests.finance.investment_fixtures import END, KEY, SBER, START, fill
from tests.finance.test_investment_ledger import ledger, transaction
from ziplime.data.services.currency_conversion_service import CurrencyConversionService
from ziplime.finance.domain.commission import Commission
from ziplime.finance.domain.investment_history import InvestmentHistory
from ziplime.finance.domain.ledger import Ledger


@pytest.mark.asyncio
async def test_session_indexes_share_original_fills_and_late_fees_can_find_prior_sessions():
    native, _ = ledger(opening_cash={("MOEX", "account", "EUR"): 10100})
    bought = transaction("buy", 100, 100)
    native.process_transaction(bought)
    assert native.transactions()[0] is native.transaction_history[0] is bought

    native.start_of_session(END)
    assert native.transactions() == []
    sold = transaction("sell", -40, 110, dt=END)
    native.process_transaction(sold)
    fee = Commission(asset=SBER, order=None, amount=100, transaction=bought, dt=END, id="late-fee")
    native.process_commission(fee, None)
    native.process_commission(fee, None)
    await native.update_portfolio()

    assert native.transactions() == [sold]
    assert native.transaction_history == (bought, sold)
    result = native.investment_snapshot("USD", END)
    assert result.historical_cost == pytest.approx(6000 / 82 + 0.3)
    assert result.realized_pnl == pytest.approx(4400 / 200 - 4000 / 82 - 0.2)
    assert result.fees == 0.5
    assert native.cash_balance("EUR", "MOEX", "account") == 4400


def test_additional_currency_uses_existing_fifo_matches_and_reports_do_not_rematch_fills():
    native, reporting = ledger()
    pound = currency("GBP", 6)
    fx = CurrencyConversionService(
        listings=[listing(currency("USD", 1), SBER.quote, sid=3), listing(pound, SBER.quote, sid=4)],
        data=pl.DataFrame({"sid": [3, 3, 4, 4], "date": [START, END, START, END],
                           "close": [82., 200., 100., 250.]}))
    native = Ledger(pd.DatetimeIndex([START, END]), datetime.timedelta(days=1))
    native.enable_investment_accounting(replace(reporting, conversion=fx), START,
                                        {("MOEX", "account", "EUR"): 10000})
    native._match_transaction = Mock(wraps=native._match_transaction)
    native.process_transaction(transaction("buy", 100, 100))
    native.process_transaction(transaction("sell", -40, 110, dt=END))
    retained = native._buy_lots_by_asset[KEY][0]

    pound_result = native.investment_snapshot("GBP", END)
    assert pound_result.historical_cost == 60
    assert pound_result.realized_pnl == pytest.approx(4400 / 250 - 4000 / 100)
    assert pound_result.market_value == pytest.approx(6600 / 250)
    for code in ("USD", "EUR", "GBP", "GBP"):
        native.investment_snapshot(code, END)
    assert native._match_transaction.call_count == 2
    assert native._buy_lots_by_asset[KEY][0] is retained
    assert native.position_tracker.positions[KEY].amount == 60


def test_missing_fx_does_not_discard_an_executed_fill_and_recovers_without_reexecution():
    native, reporting = ledger(opening_cash={("MOEX", "account", "EUR"): 10000})
    usd_asset = replace(SBER, sid=55, quote=currency("USD", 1))
    original_rate = reporting.conversion.rate

    def missing_rate(base, quote, dt):
        if (base, quote) == ("USD", "EUR"):
            raise ValueError("Missing execution-time FX")
        return original_rate(base, quote, dt)

    reporting.conversion.rate = Mock(side_effect=missing_rate)
    executed = transaction("actual-fill", 5, 30, asset=usd_asset)
    native.process_transaction(executed)
    assert native.transaction_history == (executed,)
    assert native.position_tracker.positions[("MOEX", "account", usd_asset)].amount == 5
    assert native.cash_balances(START) == {
        ("MOEX", "account", "EUR"): 10000, ("MOEX", "account", "USD"): -150}
    with pytest.raises(ValueError, match="execution-time FX"):
        native.investment_snapshot("EUR", START)

    reporting.conversion.rate = original_rate
    native.process_transaction(replace(executed))
    result = native.investment_snapshot("EUR", START)
    assert result.historical_cost == 150 * 82
    assert result.total_pnl == 0
    assert len(native.transaction_history) == 1


def test_old_history_without_quotes_restores_cost_and_deduplicates_actual_fill():
    source, reporting = ledger(opening_cash={("MOEX", "account", "EUR"): 10000})
    old_fill = fill("buy", 100, 100)
    history = InvestmentHistory(events=(old_fill,), cash_events=source.cash_events)
    restored = Ledger(pd.DatetimeIndex([START, END]), datetime.timedelta(days=1))
    restored.position_tracker.update_position(
        asset=SBER, exchange_name="MOEX", trading_account_id="account", amount=100,
        cost_basis=100, last_sale_price=100, last_sale_date=START)
    restored.enable_investment_accounting(replace(reporting, investment_history=history), START, {})
    restored.process_transaction(transaction("buy", 100, 100))

    assert restored.investment_snapshot("USD", START).historical_cost == pytest.approx(10000 / 82)
    assert restored.investment_history().events == history.events
    assert restored.position_tracker.positions[KEY].amount == 100
    assert restored.cash_balance("EUR", "MOEX", "account") == 0
    assert restored.portfolio.cash_flow == 0
