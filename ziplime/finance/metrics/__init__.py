import empyrical

from ziplime.finance.metrics.max_leverage import MaxLeverage
from ziplime.finance.metrics.alpha_beta import AlphaBeta
from ziplime.finance.metrics.benchmark_returns_and_volatility import BenchmarkReturnsAndVolatility
from ziplime.finance.metrics.cash_flow import CashFlow
from ziplime.finance.metrics.constant_cumulative_risk_metric import ConstantCumulativeRiskMetric
from ziplime.finance.metrics.daily_ledger_field import DailyLedgerField
from ziplime.finance.metrics.num_trading_days import NumTradingDays
from ziplime.finance.metrics.orders import Orders
from ziplime.finance.metrics.period_label import PeriodLabel
from ziplime.finance.metrics.pnl import PNL
from ziplime.finance.metrics.retruns_statistics import ReturnsStatistic
from ziplime.finance.metrics.simple_ledger_field import SimpleLedgerField
from ziplime.finance.metrics.start_of_period_ledger_field import StartOfPeriodLedgerField
from ziplime.finance.metrics.transactions import Transactions
from ziplime.finance.metrics.returns import Returns


def default_metrics():
    return {
        Returns(),
        ReturnsStatistic(empyrical.annual_volatility, "algo_volatility"),
        BenchmarkReturnsAndVolatility(),
        PNL(),
        CashFlow(),
        Orders(),
        Transactions(),
        SimpleLedgerField("positions"),
        StartOfPeriodLedgerField(
            "portfolio.positions_exposure",
            "starting_exposure",
        ),
        DailyLedgerField(
            "portfolio.positions_exposure",
            "ending_exposure",
        ),
        StartOfPeriodLedgerField("portfolio.positions_value", "starting_value"),
        DailyLedgerField("portfolio.positions_value", "ending_value"),
        StartOfPeriodLedgerField("portfolio.cash", "starting_cash"),
        DailyLedgerField("portfolio.cash", "ending_cash"),
        DailyLedgerField("portfolio.portfolio_value"),
        DailyLedgerField("position_tracker.stats.longs_count"),
        DailyLedgerField("position_tracker.stats.shorts_count"),
        DailyLedgerField("position_tracker.stats.long_value"),
        DailyLedgerField("position_tracker.stats.short_value"),
        DailyLedgerField("position_tracker.stats.long_exposure"),
        DailyLedgerField("position_tracker.stats.short_exposure"),
        DailyLedgerField("account.gross_leverage"),
        DailyLedgerField("account.net_leverage"),
        AlphaBeta(),
        ReturnsStatistic(empyrical.sharpe_ratio, "sharpe"),
        ReturnsStatistic(empyrical.sortino_ratio, "sortino"),
        ReturnsStatistic(empyrical.max_drawdown),
        MaxLeverage(),
        # Please kill these!
        ConstantCumulativeRiskMetric("excess_return", 0.0),
        ConstantCumulativeRiskMetric("treasury_period_return", 0.0),
        NumTradingDays(),
        PeriodLabel(),
    }
