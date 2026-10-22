from dataclasses import replace

import pytest

from tests.finance.currency_fixtures import currency
from tests.finance.investment_fixtures import book, fill, SBER, KEY, START, END
from tests.bond_fixtures import make_zero_coupon_bond
from tests.options_fixtures import make_option
from ziplime.assets.domain.option_type import OptionType
from ziplime.finance.domain.cash_event import CashEvent
from ziplime.finance.domain.cash_event_kind import CashEventKind
from ziplime.finance.domain.investment_event import InvestmentEvent
from ziplime.finance.investment_book import InvestmentBook


def snapshot(investments, code="USD", price=110, dt=END):
    return investments.snapshot(currency=code, dt=dt, marks={KEY: (price, dt)})


def test_sber_native_gain_is_reporting_currency_loss():
    investments = book()
    investments.record(event=fill(identity="buy", quantity=100, price=100))
    rub = snapshot(investments=investments, code="RUB")
    usd = snapshot(investments=investments)
    assert rub.unrealized_pnl == 1000
    assert usd.historical_cost == pytest.approx(expected=10000 / 82)
    assert usd.market_value == 55
    assert usd.unrealized_pnl == pytest.approx(expected=55 - 10000 / 82)
    assert usd.unrealized_pnl / usd.historical_cost == pytest.approx(expected=-0.549)


def test_partial_disposals_use_historical_fifo_costs_and_current_proceeds():
    investments = book()
    investments.record(event=fill(identity="first", quantity=100, price=100))
    investments.record(event=fill(identity="second", quantity=50, price=110, dt=END))
    investments.record(event=fill(identity="sell", quantity=-120, price=110, dt=END))
    result = snapshot(investments=investments)
    assert result.realized_pnl == pytest.approx(expected=55 - 10000 / 82)
    assert result.historical_cost == pytest.approx(expected=30 * 110 / 200)
    assert result.unrealized_pnl == pytest.approx(expected=0)
    assert result.positions[0].quantity == 30


def test_late_acquisition_fees_allocate_to_remaining_and_disposed_quantity():
    investments = book()
    investments.record(event=fill(identity="buy", quantity=100, price=100))
    investments.record(event=fill(identity="sell", quantity=-40, price=110, dt=END))
    investments.record(event=InvestmentEvent(
        id="fee", dt=END, kind="fee", key=KEY, currency="RUB", amount=100, transaction_id="buy"))
    investments.record(event=InvestmentEvent(
        id="exit-fee", dt=END, kind="fee", key=KEY, currency="RUB", amount=20, transaction_id="sell"))
    result = snapshot(investments=investments)
    assert result.fees == pytest.approx(expected=120 / 200)
    assert result.historical_cost == pytest.approx(expected=6000 / 82 + 0.3)
    assert result.realized_pnl == pytest.approx(expected=4400 / 200 - 4000 / 82 - 0.2 - 0.1)
    assert result.total_pnl == pytest.approx(expected=11000 / 200 - 10000 / 82 - 0.6)


def test_short_cover_and_reversal_allocate_fees_once():
    investments = book()
    investments.record(event=fill(identity="short", quantity=-100, price=100))
    investments.record(event=fill(identity="reverse", quantity=150, price=110, dt=END))
    investments.record(event=InvestmentEvent(
        id="fee", dt=END, kind="fee", key=KEY, currency="RUB", amount=150, transaction_id="reverse"))
    result = snapshot(investments=investments)
    assert result.positions[0].quantity == 50
    assert result.realized_pnl == pytest.approx(expected=10000 / 82 - 11000 / 200 - 100 / 200)
    assert result.historical_cost == pytest.approx(expected=5500 / 200 + 50 / 200)
    assert result.unrealized_pnl == pytest.approx(expected=-50 / 200)


def test_lots_do_not_cross_account_or_exchange_boundaries():
    investments = book()
    other = ("OTHER", "other-account", SBER)
    investments.record(event=fill(identity="same-id", quantity=100, price=100))
    investments.record(event=fill(identity="same-id", quantity=-100, price=110, dt=END, key=other))
    result = investments.snapshot(currency="USD", dt=END, marks={KEY: (110, END), other: (110, END)})
    assert result.realized_pnl == 0
    assert len(result.positions) == 2
    assert result.positions[0].quantity == 100
    assert result.positions[1].quantity == -100


def test_split_preserves_cost_and_fractional_cash_in_lieu_fails_explicitly():
    investments = book()
    investments.record(event=fill(identity="buy", quantity=100, price=100))
    investments.record(event=InvestmentEvent(id="split", dt=END, kind="split", key=KEY, currency="RUB", amount=0.5))
    result = snapshot(investments=investments, price=55)
    assert result.positions[0].quantity == 200
    assert result.historical_cost == pytest.approx(expected=10000 / 82)
    assert result.market_value == 55
    with pytest.raises(expected_exception=ValueError, match="cash-in-lieu"):
        investments.record(
            event=InvestmentEvent(id="fractional", dt=END, kind="split", key=KEY, currency="RUB", amount=3),
        )
    assert investments.quantities[KEY] == 200


def test_sale_closes_stock_but_retained_cash_continues_to_have_fx_exposure():
    investments = book()
    investments.record_cash(
        event=CashEvent(
            id="funding", dt=START, key=("MOEX", "account", "RUB"), amount=10000, kind=CashEventKind.OPENING,
        ),
    )
    investments.record(event=fill(identity="buy", quantity=100, price=100))
    investments.record(event=fill(identity="sell", quantity=-100, price=110))
    at_sale = investments.snapshot(currency="USD", dt=START, marks={})
    later = investments.snapshot(currency="USD", dt=END, marks={})
    assert at_sale.realized_pnl == pytest.approx(expected=1000 / 82)
    assert later.realized_pnl == at_sale.realized_pnl
    assert investments.cash_value(currency="USD", dt=START) == pytest.approx(expected=11000 / 82)
    assert investments.cash_value(currency="USD", dt=END) == 55


def test_income_and_fees_use_their_posting_rates_and_history_round_trips():
    investments = book()
    investments.record_cash(
        event=CashEvent(
            id="opening", dt=START, key=("MOEX", "account", "RUB"), amount=10000, kind=CashEventKind.OPENING,
        ),
    )
    investments.record(event=fill(identity="buy", quantity=100, price=100))
    investments.record(event=InvestmentEvent(
        id="fee", dt=START, kind="fee", key=KEY, currency="RUB", amount=82, transaction_id="buy"))
    investments.record(event=InvestmentEvent(id="dividend", dt=END, kind="income", key=KEY, currency="RUB", amount=200))
    result = snapshot(investments=investments)
    assert result.income == 1
    assert result.fees == 1
    assert result.total_pnl == pytest.approx(expected=55 - 10000 / 82)
    restored = InvestmentBook.from_history(valuation=investments.valuation, history=investments.history())
    assert snapshot(investments=restored) == result
    assert restored.cash_balances(dt=END) == investments.cash_balances(dt=END)
    assert investments.record(event=fill(identity="buy", quantity=100, price=100)) is False
    with pytest.raises(expected_exception=ValueError, match="Conflicting"):
        investments.record(event=fill(identity="buy", quantity=101, price=100))


def test_historical_snapshots_do_not_include_future_trades_or_future_marks():
    investments = book()
    investments.record(event=fill(identity="buy", quantity=100, price=100))
    investments.record(event=fill(identity="sell", quantity=-100, price=110, dt=END))
    assert snapshot(investments=investments, price=100, dt=START).unrealized_pnl == 0
    with pytest.raises(expected_exception=ValueError, match="future price"):
        investments.snapshot(currency="USD", dt=START, marks={KEY: (110, END)})
    with pytest.raises(expected_exception=ValueError, match="market mark"):
        investments.snapshot(currency="USD", dt=START, marks={})


def test_mixed_currency_investments_and_native_cash_are_valued_before_aggregation():
    investments = book()
    usd_asset = replace(SBER, quote=currency(name="USD", identity=1), sid=55, symbol="US")
    usd_key = ("NYSE", "usd-account", usd_asset)
    investments.record(event=fill(identity="sber", quantity=100, price=100))
    investments.record(event=InvestmentEvent(
        id="us", dt=START, kind="trade", key=usd_key, currency="USD", quantity=5, unit_value=30))
    result = investments.snapshot(currency="USD", dt=END, marks={KEY: (110, END), usd_key: (30, END)})
    assert result.market_value == 205
    assert result.historical_cost == pytest.approx(expected=10000 / 82 + 150)
    assert investments.cash_balances(dt=END) == {
        ("MOEX", "account", "RUB"): -10000, ("NYSE", "usd-account", "USD"): -150}


def test_fx_observation_and_current_order_values_are_not_historical_costs():
    service = book().valuation
    first = service.value(asset=SBER, quantity=100, price=100, price_at=START, dt=START, currency="USD")
    later = service.value(asset=SBER, quantity=100, price=110, price_at=END, dt=END, currency="USD")
    assert first.value == pytest.approx(expected=10000 / 82)
    assert later.value == 55
    assert later.fx.observed_at == END
    assert later.fx.listing_sid == 3


def test_explicit_fx_execution_moves_balances_without_changing_security_pnl():
    investments = book()
    investments.record_cash(
        event=CashEvent(
            id="opening", dt=START, key=("MOEX", "account", "RUB"), amount=10000, kind=CashEventKind.OPENING,
        ),
    )
    assert investments.exchange_cash(
        identity="fx",
        dt=START,
        exchange="MOEX",
        account="account",
        source_currency="RUB",
        currency="USD",
        sold=8200,
        received=100,
    )
    assert investments.cash_balances(dt=START) == {
        ("MOEX", "account", "RUB"): 1800, ("MOEX", "account", "USD"): 100}
    assert investments.exchange_cash(
        identity="fx",
        dt=START,
        exchange="MOEX",
        account="account",
        source_currency="RUB",
        currency="USD",
        sold=8200,
        received=100,
    ) is False
    with pytest.raises(expected_exception=ValueError, match="Conflicting"):
        investments.exchange_cash(
            identity="fx",
            dt=START,
            exchange="MOEX",
            account="account",
            source_currency="RUB",
            currency="USD",
            sold=8200,
            received=101,
        )
    assert investments.snapshot(currency="USD", dt=END, marks={}).total_pnl == 0
    assert investments.cash_value(currency="USD", dt=END) == 109


def test_option_premiums_and_bond_quotes_are_converted_as_monetary_values():
    investments = book()
    option = make_option(sid=77, strike=100.0, option_type=OptionType.CALL)
    bond = make_zero_coupon_bond()
    for listing, quantity, price, expected in ((option, 2, 1.5, 300), (bond, 3, 98, 2940)):
        valuation = investments.valuation.value(
            asset=listing,
            quantity=quantity,
            price=price,
            price_at=START,
            dt=START,
            currency="RUB",
        )
        assert valuation.native_value == expected
        assert valuation.value == expected * 82


def test_fee_after_split_keeps_historical_costs_and_disposal_allocation():
    investments = book()
    investments.record(event=fill(identity="buy", quantity=100, price=100))
    investments.record(event=InvestmentEvent(id="split", dt=START, kind="split", key=KEY, currency="RUB", amount=0.5))
    investments.record(event=fill(identity="sell", quantity=-100, price=55, dt=END))
    investments.record(
        event=InvestmentEvent(id="fee", dt=END, kind="fee", key=KEY, currency="RUB", amount=200, transaction_id="buy"),
    )
    result = snapshot(investments=investments, price=55)
    assert result.fees == 1
    assert result.historical_cost == pytest.approx(expected=5000 / 82 + 0.5)
    assert result.realized_pnl == pytest.approx(expected=5500 / 200 - 5000 / 82 - 0.5)


def test_invalid_events_do_not_mutate_native_history():
    investments = book()
    with pytest.raises(expected_exception=ValueError, match="quote currency"):
        investments.record(event=replace(fill(identity="bad", quantity=100, price=100), currency="USD"))
    assert not investments.events
    with pytest.raises(expected_exception=ValueError, match="account scope"):
        investments.exchange_cash(
            identity="fx",
            dt=START,
            exchange="",
            account="account",
            source_currency="USD",
            currency="RUB",
            sold=1,
            received=82,
        )
    assert not investments.cash_events
