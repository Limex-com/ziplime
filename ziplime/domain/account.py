from dataclasses import dataclass

@dataclass
class Account:
    """
    The account object tracks information about the trading account. The
    values are updated as the algorithm runs and its keys remain unchanged.
    If connected to a exchange, one can update these values with the trading
    account values as reported by the exchange.
    """

    settled_cash: float
    accrued_interest: float
    buying_power: float
    equity_with_loan: float
    total_positions_value: float
    total_positions_exposure: float
    regt_equity: float
    regt_margin: float
    initial_margin_requirement: float
    maintenance_margin_requirement: float
    available_funds: float
    excess_liquidity: float
    cushion: float
    day_trades_remaining: float
    leverage: float
    net_leverage: float
    net_liquidation: float
