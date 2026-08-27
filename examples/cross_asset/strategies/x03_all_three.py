"""Equities, bonds and futures in one portfolio, rebalanced monthly.

The full cross-asset case: a Sberbank sleeve for growth, an OFZ sleeve for carry, and a short
USD/RUB future sized against the rouble assets. All three share one cash balance, one ledger and
one set of performance figures -- which is the whole claim being tested here.

Each class is sized the way its own arithmetic requires, and getting any of them wrong is a factor
of ten or more:

* an **equity** by its price -- money per share;
* a **bond** by its dirty price -- percent of face, plus accrued interest;
* a **future** by notional -- price times the contract multiplier, with no cash paid at all.
"""
import datetime

from ziplime.assets.domain.asset_type import AssetType
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "equities": ["SBER"],
    "bonds": ["SU26238RMFS4"],
    "futures": ["SiM5", "SiZ5", "SiM6"],
    "description": "Equity, bond and futures sleeves in one portfolio, rebalanced monthly",
    "margin": "fixed",
}

EQUITY_WEIGHT = 0.45
BOND_WEIGHT = 0.45
#: Share of the rouble assets to hedge with a short dollar future.
HEDGE_RATIO = 0.5


async def initialize(context: TradingAlgorithm):
    context.equity = await context.symbol("SBER@MISX")
    context.bond = await context.bond_symbol("SU26238RMFS4@MISX")
    # Held one at a time, in order, as each contract's window comes up.
    context.contracts = [
        await context.symbol(f"{t}@RTSX", asset_type=AssetType.FUTURES_CONTRACT)
        for t in STRATEGY_INFO["futures"]]
    context.hedge = None
    context.last_rebalance = None


def live_contract(context, today: datetime.date):
    """The first hedge contract that is still tradeable today."""
    for contract in context.contracts:
        if contract.start_date <= today <= min(contract.end_date, contract.auto_close_date):
            return contract
    return None


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    if context.last_rebalance is not None and (today - context.last_rebalance).days < 30:
        return

    contract = live_contract(context, today)
    assets = [context.equity, context.bond] + ([contract] if contract else [])
    # Keyed by sid, never by position -- see x01.
    quotes = await data.current(assets=assets, fields=["price"])
    prices = dict(zip(quotes["sid"].to_list(), quotes["price"].to_list()))
    equity_quote = prices.get(context.equity.sid)
    bond_quote = prices.get(context.bond.sid)
    future_price = prices.get(contract.sid) if contract else None
    if equity_quote is None or bond_quote is None:
        return
    if contract is not None and future_price is None:
        contract = None

    value = context.portfolio.portfolio_value

    # --- equity sleeve: money per share ---------------------------------------------------
    target_shares = int(value * EQUITY_WEIGHT / equity_quote)
    held_shares = await context.portfolio.get_asset_positions_amount(context.equity)
    if target_shares != held_shares:
        await context.order(asset=context.equity, amount=target_shares - held_shares,
                            style=MarketOrder())

    # --- bond sleeve: percent of face, plus accrued interest -------------------------------
    per_bond = await context.bond_dirty_price(context.bond, bond_quote)
    target_bonds = int(value * BOND_WEIGHT / per_bond)
    held_bonds = await context.portfolio.get_asset_positions_amount(context.bond)
    if target_bonds != held_bonds:
        await context.order(asset=context.bond, amount=target_bonds - held_bonds,
                            style=MarketOrder())

    # --- futures hedge: notional, no cash paid ---------------------------------------------
    if contract is not None:
        if context.hedge is not None and context.hedge.sid != contract.sid:
            held = await context.portfolio.get_asset_positions_amount(context.hedge)
            if held:
                await context.order(asset=context.hedge, amount=-held, style=MarketOrder())
            context.hedge = None
        target_contracts = -context.contracts_for_notional(
            asset=contract, notional=value * HEDGE_RATIO, price=future_price)
        held_contracts = await context.portfolio.get_asset_positions_amount(contract)
        if target_contracts != held_contracts:
            await context.order(asset=contract, amount=target_contracts - held_contracts,
                                style=MarketOrder())
        context.hedge = contract

    context.last_rebalance = today
