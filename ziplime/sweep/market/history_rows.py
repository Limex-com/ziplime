"""History rows for market."""

import numpy as np
from ziplime.sweep.params import SweepError
from ziplime.sweep.market.market import Market
from ziplime.sweep.market.history_utils import _daily_parts


class HistoryRows:
    """The rows ``data.history(assets=[asset], bar_count=n, fields=[field])[field]`` holds at a
    bar: what a strategy's own code reads from history (``.to_numpy()``), for the host."""

    def __init__(self, market: Market) -> None:
        self.market = market
        self._daily = {}

    def window(self, field: str, column: int, t: int, length: int, before: bool,
               daily: bool) -> np.ndarray:
        market = self.market
        if field not in market.rows:
            raise SweepError(f"The bundle has no {field!r} column.")
        length = int(length)
        if length < 1:
            raise SweepError(f"history bar_count has to be at least 1, got {length}.")
        end = int((market.history_end_before if before else market.history_end)[t, column])
        if end <= 0:
            return np.empty(0)
        if not daily:
            return market.rows[field][max(end - length, 0):end, column].copy()
        key = (field, column)
        if key not in self._daily:
            self._daily[key] = _daily_parts(market, field, column)
        daily_rows, partial, day_index = self._daily[key]
        last = end - 1
        d = day_index[last]
        return np.append(daily_rows[max(0, d - length + 1):d], partial[last])
