"""Result utils for engine."""

import numpy as np
import pandas as pd
from ziplime.sweep.market import Market
from ziplime.vectorized.kernel.models import ExecutionTiming, VectorFill, VectorSimulationResult
from ziplime.sweep.engine.result import Result


def _merge(result: Result, again: Result, rows: np.ndarray) -> Result:
    for name, values in again.metrics.items():
        result.metrics[name][rows] = values
    for name in ("cash", "positions", "fees", "fills", "unsupported"):
        getattr(result, name)[rows] = getattr(again, name)
    if result.value is not None:
        result.value[:, rows] = again.value
    if result.log is not None:  # a recorded run is one combination: the rerun is the run
        result.log, result.cash_log, result.held_log = again.log, again.cash_log, again.held_log
    return result


def as_vector_result(result: Result, market: Market, timing: str,
                     initial_cash: float) -> VectorSimulationResult:
    """A recorded run of one combination as ziplime's vector result, so to_execution_result books
    its trades through ziplime's ledger and metrics. Fills carry their bar's time; the value
    series is the sessions' ends."""
    columns = market.columns
    stamps = [stamp.to_pydatetime() for stamp in market.index]
    index = market.session_index
    fills = []
    for bar, amounts, prices, fees, held_before in result.log:
        for column in np.nonzero(amounts)[0]:
            amount, before = float(amounts[column]), float(held_before[column])
            if amount > 0:
                intent = "open_long" if before >= 0 else "close_short"
            else:
                intent = "close_long" if before > 0 else "open_short"
            signal_bar = bar if timing == "same_close" else max(bar - 1, 0)
            fills.append(VectorFill(
                instrument=columns[column], timestamp=stamps[bar], amount=amount,
                price=float(prices[column]), commission=float(fees[column]),
                sequence=len(fills), intent=intent, signal_timestamp=stamps[signal_bar]))
    return VectorSimulationResult(
        index=index, fills=fills, rejects=[], instruments=columns,
        cash=pd.Series(result.cash_log, index=index, name="cash"),
        portfolio_value=pd.Series(result.value[:, 0], index=index, name="portfolio_value"),
        positions=pd.DataFrame(np.array(result.held_log), index=index, columns=columns),
        initial_cash=float(initial_cash), timing=ExecutionTiming(execute_at=timing))
