"""Hold an OFZ and short a USD/RUB future against it.

A rouble bond portfolio carries currency risk for anyone whose liabilities are not in roubles. The
textbook hedge is a short dollar future, and it is the case that exercises the two most different
sets of accounting rules ziplime has in one ledger:

* the **bond** settles at its dirty price, ties up cash, and pays coupons into it;
* the **future** ties up no cash at all -- only margin -- and settles its variation daily.

So a correct run shows a bond position with a value, a futures position with none but with
exposure, and margin reported separately from cash. If futures ever showed a position value, or a
bond ever settled by variation margin, this is where it would be obvious.

The result is not a demonstration that hedging neutralises anything. Over this window the rouble
strengthened hard -- SiM5 went from 99 774 to 78 473 by its expiry -- so the short leg made about
850 000 roubles on top of the bond's coupons. A hedge removes exposure to a direction; it does not
remove the P&L that comes from having been on the right side of one.
"""
from ziplime.assets.domain.asset_type import AssetType
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "bonds": ["SU26238RMFS4"],
    "futures": ["SiM5"],
    "description": "Long OFZ, short a USD/RUB future as a currency hedge",
    "margin": "fixed",
}


async def initialize(context: TradingAlgorithm):
    context.bond = await context.bond_symbol("SU26238RMFS4@MISX")
    context.future = await context.symbol("SiM5@RTSX",
                                       asset_type=AssetType.FUTURES_CONTRACT)
    context.built = False
    context.reported = False


async def handle_data(context: TradingAlgorithm, data: BarData):
    # Keyed by sid, never by position -- see x01.
    quotes = await data.current(assets=[context.bond, context.future], fields=["price"])
    prices = dict(zip(quotes["sid"].to_list(), quotes["price"].to_list()))
    bond_quote = prices.get(context.bond.sid)
    future_price = prices.get(context.future.sid)
    if bond_quote is None or future_price is None:
        return

    if not context.built:
        per_bond = await context.bond_dirty_price(context.bond, bond_quote)
        bond_amount = int(context.portfolio.cash * 0.8 / per_bond)
        if bond_amount <= 0:
            return
        bond_notional = bond_amount * per_bond
        # Size the hedge on notional, not on count: one contract is 1000 dollars of exposure.
        contracts = context.contracts_for_notional(
            asset=context.future, notional=bond_notional, price=future_price)
        await context.order(asset=context.bond, amount=bond_amount, style=MarketOrder())
        await context.order(asset=context.future, amount=-contracts, style=MarketOrder())
        print(f"{context.simulation_dt.date()} OFZ {bond_amount} = {bond_notional:,.0f} RUB, "
              f"short {contracts} SiM5 @ {future_price:,.0f}")
        context.built = True
        return

    if not context.reported:
        margin = context.futures_margin_by_currency()
        print(f"{context.simulation_dt.date()} positions value "
              f"{context.portfolio.positions_value:,.0f} (the bond only), exposure "
              f"{context.portfolio.positions_exposure:,.0f}, margin {margin}")
        context.reported = True
