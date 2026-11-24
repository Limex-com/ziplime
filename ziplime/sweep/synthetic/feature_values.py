"""Features for synthetic."""

import dataclasses
import numpy as np


@dataclasses.dataclass
class Features:
    """Each real bar as a path reuses it: (bars, instruments) arrays."""

    #: close-to-close log return; opening gap log(open / previous close)
    returns: np.ndarray
    gap: np.ndarray
    #: log(high / close) >= 0 and log(low / close) <= 0
    high: np.ndarray
    low: np.ndarray
    volume: np.ndarray | None
    #: the close carried over missing rows
    close: np.ndarray
