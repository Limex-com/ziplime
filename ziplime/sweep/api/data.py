"""Data for api."""

import datetime
from ziplime.sweep.all_assets import ALL
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.vectorized_containers import VectorUniverse, Vectorized
from ziplime.sweep.params import SweepError
from ziplime.sweep.backends import Backend
from ziplime.sweep.windows import Windows
from ziplime.sweep.api.column import Column
from ziplime.sweep.api.columns import Columns
from ziplime.sweep.api.frame import Frame


class Data:
    """``data``: current values, history windows and can_trade, for the bar being simulated."""

    def __init__(self, columns: Columns, backend: Backend, windows: Windows | None) -> None:
        self._columns, self._backend, self._windows = columns, backend, windows
        #: {field: (1 × instruments)} at this bar; set by the simulation
        self.now = {}
        #: before_trading_start reads earlier rows
        self.before = False
        self.dt = None
        self.stopped = None

    @property
    def current_dt(self):
        return self.dt

    def current(self, assets=None, fields=None, data_source: str | None = None, **kwargs) -> Frame:
        if fields is None:
            raise NotConvertible("data.current needs fields=[...].")
        if data_source is not None:
            raise NotConvertible("data.current(data_source=...) is not supported in a sweep.")
        fields = [fields] if isinstance(fields, str) else list(fields)
        assets = self._columns.assets(assets)
        source = self.now["spot_before" if self.before else "spot"]
        missing = [field for field in fields if field not in source]
        if missing:
            raise SweepError(f"The bundle has no {missing} column.")
        return Frame({field: Column([self._columns.select(source[field], asset) for asset in assets])
                      for field in fields})

    def history(self, assets=None, bar_count=None, frequency=None, fields=None, data_source: str | None = None,
                since=None, **kwargs):
        from ziplime.sweep.api.window import Window

        if since is not None or data_source is not None:
            raise NotConvertible("data.history(since=...) and data_source are not supported in a "
                                 "sweep; use bar_count.")
        if bar_count is None:
            raise NotConvertible("data.history needs bar_count in a sweep.")
        from ziplime.sweep.frames import History

        # The universe itself (context.universe, its values(), list(...) of them) outside a loop
        # over it is every instrument in one frame; [asset] inside the loop is one per pass.
        everything = isinstance(assets, (VectorUniverse, Vectorized))
        if isinstance(assets, Vectorized) and assets.mask is not True:
            raise NotConvertible("data.history over some of the instruments chosen per "
                                 "combination is not supported in a sweep; ask for the universe.")
        chosen = self._columns.assets(assets)
        fields = None if fields is None else ([fields] if isinstance(fields, str) else list(fields))
        if everything or len(chosen) > 1:
            if not everything and any(asset is ALL for asset in chosen):
                raise NotConvertible("data.history over the loop's instrument and others at once "
                                     "is not supported in a sweep.")
            return History(self, [ALL] if everything else chosen, bar_count, fields, frequency)
        return Window(self, chosen[0], bar_count, fields, frequency)

    def _daily(self, frequency) -> bool:
        """Whether history reads days built from the run's rows (an intraday run asking for days,
        which is history's default) or the rows themselves."""
        from ziplime.sweep.market import parse_rate

        market = self._windows.market
        asked = parse_rate(frequency)
        if not market.intraday:
            if asked != datetime.timedelta(days=1):
                raise NotConvertible("A daily run's history is read in days.")
            return False
        if asked == datetime.timedelta(days=1):
            return True
        if asked == market.rate:
            return False
        raise NotConvertible(f"history(frequency={frequency!r}) on a {market.rate} run: a sweep "
                             f"reads the run's own bars or days.")

    def _window(self, asset, field: str, op: str, length, frequency=None):
        return self._columns.select(
            self._windows.value(field, op, length, self.before, self._daily(frequency)), asset)

    def returns(self, assets=None, bar_count=None, field: str = "close", data_source: str | None = None):
        """BarData.returns: ``{instrument: last / first - 1}`` over its last ``bar_count`` rows,
        absent where it has fewer than two or a non-positive first price."""
        from ziplime.sweep.cross import SweepDict

        if data_source is not None:
            raise NotConvertible("data.returns(data_source=...) is not supported in a sweep.")
        if bar_count is None:
            raise NotConvertible("data.returns needs bar_count.")
        assets = self._columns.assets(assets)
        xp = self._backend.xp
        daily = self._daily(None)
        first = self._windows.value(field, "first", bar_count, self.before, daily)
        last = self._windows.value(field, "last", bar_count, self.before, daily)
        count = self._windows.value(field, "count", bar_count, self.before, daily)
        result = SweepDict(self._runtime)
        result._values = last / first - 1.0
        present = xp.logical_and(count >= 2, first > 0)
        if not assets == [ALL]:
            chosen = None
            for asset in assets:
                here = self._columns.only(asset)
                chosen = here if chosen is None else xp.logical_or(chosen, here)
            present = xp.logical_and(present, chosen)
        result._present = present
        return result

    def can_trade(self, assets=None):
        assets = self._columns.assets(assets)
        tradable = self._backend.xp.logical_not(self.stopped)
        if len(assets) == 1:
            return self._columns.select(tradable, assets[0])
        return [self._columns.select(tradable, asset) for asset in assets]
