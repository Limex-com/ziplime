"""Buy two quote currencies, compare their orders in USD, then separate security PNL from cash FX."""
from ziplime.domain.bar_data import BarData
from ziplime.finance.domain.order_quantity_basis import OrderQuantityBasis
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm


async def initialize(context: TradingAlgorithm) -> None:
    """Resolve the two demo listings and initialize ``context.session_number``.

    Parameters
    ----------
    context : TradingAlgorithm
        Investment-enabled simulation context. Adds ``sber`` and ``usd_stock``
        listing attributes plus the session counter used by this five-bar example.
    """
    context.sber = await context.symbol(symbol="SBER", mic="XNYS")
    context.usd_stock = await context.symbol(symbol="USD_STOCK", mic="XNYS")
    context.session_number = 0


async def handle_data(context: TradingAlgorithm, data: BarData) -> None:
    """Demonstrate one valuation/accounting step per synthetic daily session.

    Parameters
    ----------
    context : TradingAlgorithm
        Context initialized above, sharing reporting FX, native cash, and FIFO history.
        Sessions submit/compare orders, inspect the SBER loss in USD, sell SBER,
        inspect retained-cash FX, then post external funding and executed FX legs.
    data : BarData
        Current bar supplied by the engine. Prices are fetched through the
        valuation APIs rather than read directly in this demonstration.

    Notes
    -----
    Submitted/remaining estimates use the same current prices but different
    quantities. Historical-cost snapshots include booked fills only: same-close
    demo orders fill after this callback, not when ``value_orders`` is called.
    """
    context.session_number += 1
    if context.session_number == 1:
        sber_order = await context.order(asset=context.sber, amount=100, style=MarketOrder())
        usd_order = await context.order(asset=context.usd_stock, amount=5, style=MarketOrder())
        if sber_order is None or usd_order is None:
            raise RuntimeError("Both nonzero demo orders must be submitted.")
        orders = [sber_order, usd_order]
        submitted = await context.value_orders(orders=orders, currency="USD")
        remaining = await context.value_orders(
            orders=orders,
            currency="USD",
            quantity_basis=OrderQuantityBasis.REMAINING,
        )
        print("\nSame-bar submitted orders, before fills or performance metrics:")
        for valued in submitted.orders:
            money = valued.monetary
            print(f"  {money.asset.symbol}: {money.quantity:g} shares, "
                  f"{money.native_value:,.2f} {money.native_currency} = USD {money.value:,.4f}; "
                  f"FX observation {money.fx.observed_at.isoformat()}")
        print(f"  Gross: USD {submitted.gross_value:,.4f}; net: USD {submitted.net_value:,.4f}")
        print(f"  Remaining: USD {remaining.gross_value:,.4f} (both orders are still unfilled)")
        print(f"  Stored SBER submission snapshot: {sber_order.submitted_valuations['USD'].monetary.value:.4f} USD")
        context.record(submitted_orders_usd=submitted.gross_value)

    elif context.session_number == 2:
        eur = await context.investment_pnl(currency="EUR")
        usd = await context.value_positions(currency="USD")
        eur_sber = next(position for position in eur.positions if position.key[2] == context.sber)
        usd_sber = next(position for position in usd.positions if position.key[2] == context.sber)
        print("\nSBER after its EUR price rises and EUR weakens:")
        print(f"  EUR cost {eur_sber.historical_cost:,.2f}, value {eur_sber.market_value:,.2f}, "
              f"PNL {eur_sber.unrealized_pnl:+,.2f}")
        print(f"  USD historical cost {usd_sber.historical_cost:,.4f}, value {usd_sber.market_value:,.4f}, "
              f"PNL {usd_sber.unrealized_pnl:+,.4f}")
        context.record(sber_pnl_eur=eur_sber.unrealized_pnl, sber_pnl_usd=usd_sber.unrealized_pnl)

    elif context.session_number == 3:
        await context.order(asset=context.sber, amount=-100, style=MarketOrder())
        print("\nSelling SBER. Realized security PNL freezes at the sale's FX rate.")

    elif context.session_number == 4:
        investment = await context.investment_pnl(currency="USD", trading_account_id="simulation_account")
        print(f"\nAfter EUR weakens further: security PNL stays USD {investment.total_pnl:+.4f}.")
        print("  Retained EUR cash loses another USD 5; see cash_fx_pnl in the result table.")

    elif context.session_number == 5:
        await context.fund_account(
            amount=25,
            currency="USD",
            exchange_name="LIME",
            trading_account_id="simulation_account",
            event_id="demo-deposit",
        )
        # Actual executed amounts, not an inferred conversion or an instruction to a broker.
        await context.exchange_currency(
            source_currency="EUR", currency="USD", sold=2200, received=10,
            exchange_name="LIME", trading_account_id="simulation_account", execution_id="demo-fx")
        print("\nPosted a USD 25 external deposit and exchanged EUR 2,200 for USD 10.")

    # Orders are not holdings until filled; this snapshot reflects only already-booked fills.
    investment = await context.investment_pnl(currency="USD")
    context.record(security_pnl_before_metrics_usd=investment.total_pnl)
