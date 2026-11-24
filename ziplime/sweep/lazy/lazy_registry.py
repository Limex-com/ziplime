"""Lazy registry for lazy."""

import pickle
import numpy as np
from ziplime.sweep.errors import NotConvertible, Rediscover
from ziplime.sweep.params import SweepError
from ziplime.sweep.windows import Windows
from ziplime.sweep.market import HistoryRows
from ziplime.sweep.market import Market
from ziplime.sweep.lazy.lazy import Lazy
from ziplime.sweep.lazy.support import PARALLEL_FROM
from ziplime.sweep.lazy.select import Select
from ziplime.sweep.lazy.template import _Template
from ziplime.sweep.lazy.unrunnable import _Unrunnable
from ziplime.sweep.lazy.worker_utils import _replay_job
from ziplime.sweep.lazy.worker_utils import _start_worker
from ziplime.sweep.lazy.replay_utils import compile_plan
from ziplime.sweep.lazy.series_utils import fast_series
from ziplime.sweep.lazy.recording import is_lazy
from ziplime.sweep.lazy.replay_utils import number
from ziplime.sweep.lazy.replay_utils import replay
from ziplime.sweep.lazy.replay_utils import replay_series
from ziplime.sweep.lazy.worker_utils import replay_workers
from ziplime.sweep.lazy.support import where_in_strategy


class LazyRegistry:
    """Every record the strategy turned into a number, and its values at every bar.

    Lives on the run's Windows: ``now`` and ``stacks`` are the windows' own, so the step's inputs
    carry these values as they carry history windows (compiled steps included)."""

    def __init__(self, windows: Windows) -> None:
        self.windows = windows
        #: key -> stack name, record (for replaying), per_column
        self.records = {}
        #: stack name -> sorted parameter values computed
        self.values = {}
        self._cache = {}
        self._probes = {}
        #: Each record's parameter values per combination, as discovery saw them, by the record
        #: and the order it was asked for within the bar: (name, k) -> [tuple per combination]
        self._seen = {}
        self._occurrence = {}
        #: (name, k) -> each combination's row of the record's values, worked out once a batch
        self._rows = {}
        #: Whether the step is being compiled, and the combinations whose parameter was not the
        #: one their row was looked up for (see value())
        self.compiled = False
        self.misses = []
        #: How the records' values were computed: once on the whole history, replayed at every
        #: bar here, or on the CPU's cores
        self.stats = {"whole history": 0, "replayed": 0, "replayed on cores": 0}
        #: key -> how its values were computed; record name -> where the strategy uses it
        self.method, self.where = {}, {}

    def _name(self, template: _Template, value: Lazy) -> str:
        entry = self.records.get(template.key)
        if entry is None:
            entry = self.records[template.key] = (f"lazy{len(self.records)}", value,
                                                  template.per_column)
            #: where the strategy first needed the record as a number, for the report
            self.where[entry[0]] = where_in_strategy()
        return entry[0]

    def how(self, key) -> str:
        """How a record's values were computed, for the report."""
        return self.method.get(key, "not computed")

    def always_nan(self, key) -> bool:
        """Whether a record is NaN at every bar for every set of its parameters."""
        name = self.records[key][0]
        series = [self._cache.get((key, params)) for params in self.values.get(name, [()])]
        series = [values for values in series if values is not None]
        return bool(series) and all(np.isnan(values).all() for values in series)

    def _host(self, params):
        """The parameters' values per combination, on the host; None while a compiled step
        traces them."""
        try:
            return [np.asarray(self.windows.backend.numpy(p), dtype=float).reshape(-1)
                    for p in params]
        except Exception:
            return None

    def begin_step(self) -> None:
        """A bar starts: records are asked for in the same order at every bar."""
        self._occurrence = {}
        self.misses = []

    def begin_discovery(self) -> None:
        """A batch starts: its combinations' parameter values are seen anew."""
        self._seen, self._rows = {}, {}

    def _rows_of(self, params) -> list | None:
        """Each combination's parameter values, as tuples; None while a compiled step traces."""
        host = self._host(params)
        if host is None:
            return None
        length = max(len(h) for h in host)
        host = [np.broadcast_to(h, (length,)) for h in host]
        # Whole numbers stay ints: they are bar counts and slice bounds
        return [tuple(int(v) if float(v).is_integer() else v for v in row)
                for row in zip(*[h.tolist() for h in host])]

    def _request(self, name: str, params, k: int | None = None) -> bool:
        """Note the parameter values seen; True if any were new."""
        known = set(self.values.get(name, ()))
        if not params:
            new = {()}
        else:
            rows = self._rows_of(params)
            if rows is None:
                return False
            if k is not None:
                self._seen[(name, k)] = rows
            new = set(rows)
        if new <= known and name in self.values:
            return False
        self.values[name] = sorted(known | new)
        return True

    def _row_index(self, name: str, k: int, combinations: int):
        """Each combination's row of the record's values for its k-th use in the bar, from the
        values discovery saw -- once a batch, not at every bar; None if discovery did not see it."""
        if (name, k) not in self._rows:
            seen = self._seen.get((name, k))
            position = {row: n for n, row in enumerate(self.values.get(name, ()))}
            if seen is None or len(seen) not in (1, combinations) or \
                    any(row not in position for row in seen):
                self._rows[(name, k)] = None
            else:
                self._rows[(name, k)] = np.array([position[row] for row in seen], dtype=np.int32)
        return self._rows[(name, k)]

    def value(self, value: Lazy):
        windows, xp = self.windows, self.windows.backend.xp
        template = _Template(value)
        name = self._name(template, value)
        k = self._occurrence[name] = self._occurrence.get(name, -1) + 1
        width = len(windows.market.columns) if template.per_column else 1
        if windows.discovering:
            self._request(name, template.params, k)
            return xp.zeros((1, width)) + np.nan
        if not self.compiled and self._request(name, template.params) or name not in windows.now:
            raise Rediscover()
        rows = windows.now[name]  # (distinct parameter values, width)
        if not template.params:
            return rows
        table = self.values[name]
        if self.compiled:
            return self._compiled_value(name, k, rows, table, template.params)
        return xp.take(rows, self._match(table, template.params), axis=0)

    def _compiled_value(self, name: str, k: int, rows, table, params):
        """In a compiled step the parameters cannot be read on the host. Each combination's row
        was found once for the batch from what discovery saw; the step only checks, one
        comparison a combination, that the parameter is still the row's value. Where it is not
        -- a parameter that changed during the run -- the combination is noted, and the batch is
        run again without compiling, where every bar checks on the host."""
        xp, backend = self.windows.backend.xp, self.windows.backend
        combinations = max(int(param.shape[0]) for param in params)
        found = self._row_index(name, k, combinations)
        index = backend.xp.array(found) if found is not None else self._match(table, params)
        same = None
        for p, param in enumerate(params):
            column = backend.array(np.array([row[p] for row in table], dtype=float))
            hit = xp.take(column, index, axis=0)[:, None] == param
            same = hit if same is None else xp.logical_and(same, hit)
        self.misses.append(xp.logical_not(same))
        return xp.take(rows, index, axis=0)

    def _match(self, table, params):
        """Each combination's row of the record's values, looked up on the device."""
        xp, backend = self.windows.backend.xp, self.windows.backend
        index = None
        for p, param in enumerate(params):
            column = backend.array(np.array([row[p] for row in table], dtype=float))
            hit = param == column[None, :]
            index = hit if index is None else xp.logical_and(index, hit)
        return xp.argmax(index.astype(xp.int32), axis=1).astype(xp.int32)

    def is_number(self, value) -> bool:
        """Whether the record ends in a number (or an array): replayed once at the last bar."""
        if isinstance(value, Select):
            side = value.yes if is_lazy(value.yes) else value.no
            return self.is_number(side)
        template = _Template(value)
        if template.key not in self._probes:
            from ziplime.sweep.market import HistoryRows

            market = self.windows.market
            params = tuple(self._first(p) for p in template.params)
            try:
                result = replay(value, HistoryRows(market), market.n_bars - 1, 0, params)
                self._probes[template.key] = np.ndim(result) == 0
            except Exception:
                self._probes[template.key] = True
        return self._probes[template.key]

    def _first(self, param):
        v = float(np.asarray(self.windows.backend.numpy(param)).reshape(-1)[0])
        return int(v) if v.is_integer() else v

    def prepare(self) -> None:
        """Every record's value at every bar, for each distinct set of parameter values: a TA-Lib
        function of one window once on the whole history (fast_series), anything else replayed
        bar by bar -- on the CPU's cores when there is enough of it."""
        from ziplime.sweep.market import HistoryRows

        market = self.windows.market
        rows = HistoryRows(market)
        pending = []
        for key, (name, value, per_column) in self.records.items():
            columns = list(range(len(market.columns))) if per_column else [0]
            for params in self.values.get(name, [()]):
                if (key, params) in self._cache:
                    continue
                fast = fast_series(key, params, market, columns)
                if fast is not None:
                    self._cache[(key, params)] = fast
                    self.stats["whole history"] += 1
                    self.method[key] = "once on the whole history"
                else:
                    pending.append((key, params, columns, value))
        self._replay_pending(pending, rows, market)
        for key, (name, value, per_column) in self.records.items():
            table = self.values.get(name, [()])
            width = len(market.columns) if per_column else 1
            stack = np.full((market.n_bars, len(table), width), np.nan)
            for k, params in enumerate(table):
                stack[:, k, :] = self._cache[(key, params)]
            self.windows.stacks[name] = self.windows.backend.array(stack)

    def _replay_pending(self, pending, rows: HistoryRows, market: Market) -> None:
        work = sum(market.n_bars * len(columns) for _, _, columns, _ in pending)
        jobs, local = [], []
        for key, params, columns, value in pending:
            try:
                compile_plan(key, rows)
                pickle.dumps(key)
                jobs.append((key, params, columns))
            except (_Unrunnable, pickle.PicklingError, TypeError, AttributeError):
                local.append((key, params, columns, value))
        workers = min(replay_workers(), len(jobs))
        if workers > 1 and work >= PARALLEL_FROM:
            import concurrent.futures
            import multiprocessing

            arrays = dict(rows=market.rows, history_end=market.history_end,
                          history_end_before=market.history_end_before, row_days=market.row_days,
                          n_bars=market.n_bars)
            with concurrent.futures.ProcessPoolExecutor(
                    max_workers=workers, mp_context=multiprocessing.get_context("spawn"),
                    initializer=_start_worker, initargs=(arrays,)) as pool:
                chunk = max(1, len(jobs) // (workers * 4))
                for (key, params, _), series in zip(jobs, pool.map(_replay_job, jobs, chunksize=chunk)):
                    self._cache[(key, params)] = series
                    self.method[key] = f"bar by bar, on {workers} of the CPU's cores"
            self.stats["replayed on cores"] += len(jobs)
        else:
            for key, params, columns in jobs:
                plan = compile_plan(key, rows)
                self._cache[(key, params)] = replay_series(plan, market.n_bars, columns, params)
                self.method[key] = "bar by bar"
            self.stats["replayed"] += len(jobs)
        for key, params, columns, value in local:
            series = np.full((market.n_bars, len(columns)), np.nan)
            for j, column in enumerate(columns):
                for t in range(market.n_bars):
                    series[t, j] = self._replay(value, rows, t, column, params)
            self._cache[(key, params)] = series
            self.stats["replayed"] += 1
            self.method[key] = "bar by bar"

    @staticmethod
    def _replay(value, rows: HistoryRows, t: int, column: int, params):
        try:
            result = replay(value, rows, t, column, params)
        except NotConvertible:
            raise
        except Exception:
            # The engine would raise at this bar -- but only where the strategy takes this
            # branch, which a sweep, running every branch, cannot know here
            return np.nan
        try:
            return number(result)
        except TypeError as error:
            raise SweepError(f"A computation on history is used as a number but gives {error}: "
                             f"index it ([-1]) or reduce it (np.mean, .max()) first.") from None
