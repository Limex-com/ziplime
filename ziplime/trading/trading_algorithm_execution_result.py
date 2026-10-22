from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from ziplime.trading.trading_algorithm import TradingAlgorithm


@dataclass
class TradingAlgorithmExecutionResult:
    """Execution results with optional independent currency performance histories.

    Attributes
    ----------
    trading_algorithm : TradingAlgorithm
        Executed strategy/context; vectorized replay supplies its run facade.
    perf : pandas.DataFrame
        Default/base-currency daily performance and native execution records.
    risk_report : pandas.DataFrame
        Default run risk report, retained separately from currency-specific reports.
    errors : list[str]
        Execution errors collected by the runner.
    perf_by_currency : dict[str, pandas.DataFrame]
        Daily performance by reporting ISO code, in configured currency order;
        empty without reporting. Metric names are unchanged and ``pnl`` is the
        period increment. Investment fields, when enabled, are lifetime cumulative
        totals; arbitrary recorded variables and raw execution units stay native.
    risk_report_by_currency : dict[str, dict[str, Any]]
        Independently computed risk packets by reporting currency, or an empty mapping.
    """

    trading_algorithm: TradingAlgorithm
    perf: pd.DataFrame
    risk_report: pd.DataFrame
    errors: list[str]
    perf_by_currency: dict[str, pd.DataFrame] = field(default_factory=dict)
    risk_report_by_currency: dict[str, dict[str, Any]] = field(default_factory=dict)
