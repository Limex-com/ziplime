from dataclasses import dataclass

from ziplime.trading.trading_algorithm import TradingAlgorithm
from ziplime.trading.trading_algorithm_execution_result import TradingAlgorithmExecutionResult


@dataclass
class TradingAlgorithmExecutionStatus:
    perf: dict
    trading_algorithm: TradingAlgorithm
    daily_perf: dict
    cumulative_perf: dict
    cumulative_risk_metrics: dict
    errors: list[str]
    result: TradingAlgorithmExecutionResult
    progress: float
