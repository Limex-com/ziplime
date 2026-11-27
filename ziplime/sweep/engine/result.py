"""Result for engine."""

import dataclasses
import numpy as np


@dataclasses.dataclass
class Result:
    #: (combinations,) each: Sharpe, total return, max drawdown, final value -- computed on the
    #: device, so a batch's value curves never leave it
    metrics: dict
    value: np.ndarray | None  # (bars, combinations), recorded runs only
    cash: np.ndarray
    positions: np.ndarray
    fees: np.ndarray
    fills: np.ndarray
    unsupported: np.ndarray  # (combinations,) bool
    compiled: bool
    log: list = None
    #: Why the bars were not compiled, when they were not
    compile_reasons: list = None
    cash_log: list = None
    held_log: list = None
