from dataclasses import dataclass

import pandas as pd

from ziplime.trading.trading_algorithm import TradingAlgorithm


@dataclass
class TradingAlgorithmExecutionResult:
    trading_algorithm: TradingAlgorithm
    perf: pd.DataFrame
    errors: list[str]

