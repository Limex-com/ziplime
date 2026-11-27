"""Market for market."""

import dataclasses
import datetime
import numpy as np
import pandas as pd


@dataclasses.dataclass
class Market:
    #: Session closes of the run, in the calendar's zone: the engine's bar timestamps
    index: pd.DatetimeIndex
    columns: list[str]
    listings: list
    #: {field: (bars, instruments)} -- the latest row at or before the bar
    spot: dict
    #: The same at before_trading_start
    spot_before: dict
    #: (bars, instruments): price the portfolio is marked at (previous mark where none)
    marks: np.ndarray
    #: (bars, instruments): rows of each instrument before the bar / before before_trading_start
    history_end: np.ndarray
    history_end_before: np.ndarray
    #: {field: (rows, instruments)}: each instrument's own rows, NaN-padded at the end
    rows: dict
    #: (bars, instruments): orders refused -- the session is past the instrument's last bar or
    #: its auto-close date
    stopped: np.ndarray
    #: (bars, instruments): the engine closes the position at this bar's session end
    auto_close: np.ndarray
    #: (instruments,): contract multiplier; futures are valued by their margin flows
    multiplier: np.ndarray
    is_future: np.ndarray
    #: (bars, instruments): split ratio (old/new shares) effective at the bar, 1.0 for none
    splits: np.ndarray
    dividends: list
    #: (bars,): the session's first bar (dividends, splits, before_trading_start come before it)
    #: and its last (auto-close, the cancel policy and the day's portfolio value come after it)
    session_start: np.ndarray = None
    session_end: np.ndarray = None
    #: The emission rate: one day, or a part of one
    rate: datetime.timedelta = datetime.timedelta(days=1)
    #: (rows, instruments): each row's local calendar day, as a day number; -1 past the rows
    row_days: np.ndarray = None

    @property
    def n_bars(self) -> int:
        return len(self.index)

    @property
    def intraday(self) -> bool:
        return self.rate < datetime.timedelta(days=1)

    @property
    def session_index(self) -> pd.DatetimeIndex:
        """The bars that end a session: where the engine records the day's portfolio value."""
        return self.index[self.session_end]

    @property
    def has_splits(self) -> bool:
        return bool((self.splits != 1.0).any())

    @property
    def has_futures(self) -> bool:
        return bool(self.is_future.any())

    @property
    def has_auto_close(self) -> bool:
        return bool(self.auto_close.any())
