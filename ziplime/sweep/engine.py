"""The engine's daily loop, run for every combination at once.

Bars still go one at a time -- a decision at bar t depends on the portfolio after bar t-1 -- and the
parallelism comes from the combinations: every value is an array (combinations × instruments). Each
session does what TradingAlgorithm.transform does, in the same order:

    session start        dividends earned (ex date) and paid (pay date); splits
    before_trading_start the strategy's hook, reading rows before 08:45; no orders
    bar                  marks synced to the close; futures settled
                         same_close: handle_data, scheduled functions, then fills at this bar
                         next_*:     fills of earlier orders, then handle_data, scheduled functions
    session end          auto-close past the last bar / auto-close date; EODCancel; marks

Fills follow the blotter: an order fills only on a bar with a price and volume; triggers of limit
and stop orders are tested against that price and, once met, stay met; FixedBasisPointsSlippage
lets through at most its share of the bar's volume and the rest waits; commissions are charged per
fill with the order's own minimum. Like the blotter, the sweep keeps every order separately -- up to
ORDER_SLOTS open orders per instrument, walked oldest first against the bar's volume allowance; a
combination that needs more is flagged ``unsupported`` instead of silently diverging.
"""
import dataclasses
import os

import numpy as np
import pandas as pd

from ziplime.sweep import api
from ziplime.sweep.backends import Backend
from ziplime.sweep.convert import Converted, NotConvertible, is_array
from ziplime.sweep.market import WINDOW_OPS, Market, daily_window_values, window_values
from ziplime.sweep.params import SweepError
from ziplime.vectorized.kernel.models import ExecutionTiming, VectorFill, VectorSimulationResult

#: How many bars MLX may queue before the host waits for them.
EVALUATE_EVERY = 32

#: Open orders an instrument can hold at once in a sweep. A run starts with one slot per
#: instrument -- one order at a time is the common case, and every slot costs operations on every
#: bar -- and is repeated with more only when a combination needed them. A combination that needs
#: more than ORDER_SLOTS is flagged ``unsupported`` and left out of the ranking.
ORDER_SLOTS = 4
SLOT_STEPS = (1, 2, 4)


class RunPlain(Exception):
    """A compiled run met what only a plain one checks (a computation on history whose parameter
    changed during the run): the batch is run again without compiling."""


class Rediscover(Exception):
    """A history window was asked for that the run had not prepared."""


# -- costs ----------------------------------------------------------------------------------------

@dataclasses.dataclass
class Costs:
    """Commission and slippage per instrument, as the engine's models compute them."""

    #: Per-unit models (PerShare, PerContract): cost per unit, minimum per order, exchange fee
    per_unit: np.ndarray
    min_trade: np.ndarray
    initial: np.ndarray
    unit_model: np.ndarray
    per_dollar: np.ndarray
    per_trade: np.ndarray
    #: FixedBasisPointsSlippage: share of price and of volume; FixedSlippage: spread
    percentage: np.ndarray
    spread: np.ndarray
    volume_limit: np.ndarray
    #: 0 none, 1 fixed basis points, 2 fixed spread
    slippage_kind: np.ndarray

    def on(self, backend: Backend) -> "Costs":
        return dataclasses.replace(self, **{
            field.name: backend.array(getattr(self, field.name)[None, :])
            for field in dataclasses.fields(self)})

    @property
    def order_state(self) -> bool:
        """Whether a fill's commission depends on earlier fills of its order."""
        return bool(np.any(np.asarray(self.min_trade) > 0) or np.any(np.asarray(self.initial) > 0)
                    or np.any(np.asarray(self.per_trade) > 0))

    host = None

    @property
    def capped_limit(self):
        """The volume share where a model caps by volume, 0 elsewhere (no inf × 0)."""
        return self.volume_limit * (self.slippage_kind == 1)

    def require_percent_sizing(self):
        kind = (self.host or self).slippage_kind
        if np.any(np.asarray(kind) == 2):
            raise SweepError("order_percent / order_target_percent with fixed_spread slippage fail "
                             "in the engine (FixedSlippage has no order_target_percentage_maximum_"
                             "quantity); use fixed_bps or none.")

    def projected(self, xp, quantity, value):
        """commission.calculate_for_asset: what sizing expects to pay."""
        unit = xp.maximum(self.min_trade, abs(quantity * self.per_unit) + self.initial)
        return xp.where(self.unit_model, unit, 0.0) + value * self.per_dollar + self.per_trade

    def maximum_quantity(self, xp, cash, now):
        """slippage.order_target_percentage_maximum_quantity: shares the cash buys at the close,
        priced as a buy, capped by what the bar's volume still allows."""
        close = now["spot_close"]
        price = close * (1 + self.percentage)
        capacity = xp.maximum(0.0, xp.floor(self.capped_limit * now["volume"]) - now["filled"])
        bps = xp.minimum(abs(cash / price), capacity)
        return xp.where(self.slippage_kind == 1, bps, abs(cash / close))


def resolve_costs(params, market: Market) -> Costs:
    columns = len(market.listings)

    def per(field, equity, future):
        return np.where(market.is_future, future, equity).astype(float)

    def commission_parts(model):
        kind = model.type
        return {
            "per_unit": model.cost if kind in ("per_share", "per_contract") else 0.0,
            "min_trade": model.min_trade_cost if kind in ("per_share", "per_contract") else 0.0,
            "initial": model.exchange_fee if kind == "per_contract" else 0.0,
            "unit_model": 1.0 if kind in ("per_share", "per_contract") else 0.0,
            "per_dollar": model.cost if kind == "per_dollar" else 0.0,
            "per_trade": model.cost if kind == "per_trade" else 0.0,
        }

    def slippage_parts(model):
        return {
            "percentage": model.basis_points / 10_000 if model.type == "fixed_bps" else 0.0,
            "spread": model.spread if model.type == "fixed_spread" else 0.0,
            "volume_limit": model.volume_limit if model.type == "fixed_bps" else 0.0,
            "slippage_kind": {"none": 0, "fixed_bps": 1, "fixed_spread": 2}[model.type],
        }

    equity = {**commission_parts(params.equity_commission), **slippage_parts(params.equity_slippage)}
    future = {**commission_parts(params.future_commission), **slippage_parts(params.future_slippage)}
    return Costs(**{name: per(name, np.full(columns, equity[name]), np.full(columns, future[name]))
                    for name in equity})


# -- history windows --------------------------------------------------------------------------------

class Windows:
    """data.history statistics: computed on the host per distinct bar_count, read per bar."""

    def __init__(self, market: Market, backend: Backend):
        self.market, self.backend = market, backend
        #: (field, op, before) -> sorted distinct lengths
        self.lengths = {}
        self.stacks = {}
        self.discovering = False
        self.now = {}
        from ziplime.sweep.lazy import LazyRegistry
        #: The strategy's own computations on history rows, replayed on the host (see lazy.py)
        self.lazy = LazyRegistry(self)
        #: What happened to a compiled run, for the report (explain.py)
        self.notes = set()

    def request(self, field, op, before, lengths, daily=False):
        key = (field, op, before, daily)
        known = set(self.lengths.get(key, ()))
        if not set(lengths) <= known:
            self.lengths[key] = sorted(known | set(lengths))
            return True
        return False

    def prepare(self):
        """Host arrays (bars, lengths, instruments) on the device."""
        cache = self.__dict__.setdefault("_host", {})
        for key, lengths in self.lengths.items():
            field, op, before, daily = key
            compute = daily_window_values if daily else window_values
            for length in lengths:
                if (key, length) not in cache:
                    cache[(key, length)] = compute(self.market, field, op, length, before)
            self.stacks[key] = self.backend.array(
                np.stack([cache[(key, length)] for length in lengths], axis=1))
        self.lazy.prepare()

    def at(self, bar: int) -> dict:
        return {key: stack[bar] for key, stack in self.stacks.items()}

    def value(self, field, op, length, before, daily=False):
        if op not in WINDOW_OPS:
            raise NotConvertible(f"history(...).{op}() is not supported in a sweep.")
        key, xp = (field, op, before, daily), self.backend.xp
        if self.discovering:
            values = (np.unique(self.backend.numpy(length)) if is_array(length)
                      else [length])
            self.request(field, op, before, [int(v) for v in values], daily)
            return xp.zeros((1, len(self.market.columns))) + np.nan
        lengths = self.lengths.get(key)
        if lengths is None:
            self.request(field, op, before, [length] if not is_array(length) else [], daily)
            raise Rediscover()
        rows = self.now[key]  # (lengths, instruments)
        if not is_array(length):
            if length not in lengths:
                self.request(field, op, before, [length], daily)
                raise Rediscover()
            k = lengths.index(int(length))
            return rows[k:k + 1]
        known = self.backend.array(np.array(lengths, dtype=float))
        index = xp.sum(length > known[None, :], axis=1).astype(xp.int32)  # (combinations,)
        return xp.take(rows, index, axis=0)


# -- the strategy's hooks ---------------------------------------------------------------------------

@dataclasses.dataclass
class Hooks:
    handle_data: Converted
    before_trading_start: Converted | None
    #: Scheduled functions, in the order initialize scheduled them
    scheduled: list

    def all(self):
        return [self.handle_data] + ([self.before_trading_start] if self.before_trading_start
                                     else []) + list(self.scheduled)


# -- the simulation ---------------------------------------------------------------------------------

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


def run(*, market: Market, hooks: Hooks, configs: list, states: list, signals: dict,
        signal_index, timing: str, initial_cash: float, costs: Costs, backend: str,
        cancel_at_session_end=False, compile: bool | None = None,
        record: bool = False, windows: Windows | None = None,
        periods_per_year: int = 252, schedule: list = (), order_slots: int = SLOT_STEPS[0],
        rerun: bool = True) -> Result:
    """Every combination of ``configs`` over the market's bars.

    ``schedule``: per scheduled function, the bars it runs at -- (bars,) when every combination
    agrees, (bars, combinations) when its rule depends on the config. ``cancel_at_session_end``:
    EODCancel -- a bool, or (combinations,) when set_cancel_policy depends on the config.

    Runs with ``order_slots`` open orders per instrument; the few combinations that needed more are
    run again with more, on their own, and their results put in place -- unless ``rerun`` is off,
    for a caller that collects them across batches and reruns them once.
    """
    arguments = dict(market=market, hooks=hooks, configs=configs, states=states, signals=signals,
                     signal_index=signal_index, timing=timing, initial_cash=initial_cash,
                     costs=costs, backend=backend, cancel_at_session_end=cancel_at_session_end,
                     compile=compile, record=record, windows=windows,
                     periods_per_year=periods_per_year, schedule=schedule)
    result = _run(order_slots=order_slots, **arguments)
    more = [steps for steps in SLOT_STEPS if steps > order_slots]
    if not rerun or not result.unsupported.any() or not more:
        return result
    needed = np.nonzero(result.unsupported)[0]
    again = run(order_slots=more[0], **_subset(arguments, needed))
    return _merge(result, again, needed)


def _subset(arguments: dict, rows: np.ndarray) -> dict:
    """The run's arguments for some of its combinations only."""
    picked = dict(arguments)
    picked["configs"] = [arguments["configs"][k] for k in rows]
    picked["states"] = [arguments["states"][k] for k in rows]
    index = arguments["signal_index"]
    if isinstance(index, dict):
        picked["signal_index"] = {name: values[rows] for name, values in index.items()}
    elif index is not None:
        picked["signal_index"] = np.asarray(index)[rows]
    else:  # one signal set per combination, in order
        picked["signals"] = {name: stack[rows] for name, stack in arguments["signals"].items()}
    picked["schedule"] = [flags[:, rows] if np.ndim(flags) == 2 else flags
                          for flags in arguments["schedule"]]
    cancel = arguments["cancel_at_session_end"]
    if isinstance(cancel, np.ndarray):
        picked["cancel_at_session_end"] = cancel[rows]
    return picked


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


def _run(*, market, hooks, configs, states, signals, signal_index, timing, initial_cash, costs,
         backend, cancel_at_session_end, compile, record, windows, periods_per_year, schedule,
         order_slots) -> Result:
    b = Backend(backend)
    windows = windows or Windows(market, b)
    windows.backend = b

    def simulation(on=None):
        on = on or b
        windows.backend = on
        made = _Simulation(market, hooks, configs, states, signals, signal_index, timing,
                           initial_cash, costs, on, cancel_at_session_end, record, windows)
        made.periods_per_year = periods_per_year
        made.set_schedule(schedule)
        made.slots = order_slots
        return made

    plain_only = False
    for attempt in range(8):
        try:
            current = simulation()
            if attempt == 0 and any("history" in hook.source or "returns" in hook.source
                                    for hook in hooks.all()):
                current.discover()
            windows.prepare()
            use_compile = (b.lazy and not record) if compile is None else compile
            if use_compile and not plain_only and current.compilable():
                try:
                    return current.go(compiled=True)
                except RunPlain:
                    plain_only = True
                    windows.notes.add("a parameter of a computation on history changed during the "
                                      "run, so the bars ran without compiling")
                    current = simulation()
                except RuntimeError:
                    # The backend itself failed (MLX could not build a kernel). What it computes
                    # afterwards in the same process cannot be trusted, so this batch goes to the
                    # CPU in float64 -- slower, and right.
                    current = simulation(Backend("numpy"))
                    windows.prepare()
                except (NotConvertible, TypeError, ValueError, AttributeError) as error:
                    windows.notes.add(f"compiling the step failed ({type(error).__name__}: "
                                      f"{str(error)[:120]}), so the bars ran without compiling")
                    current = simulation()
            return current.go(compiled=False)
        except Rediscover:
            continue
    raise SweepError("data.history kept asking for windows the sweep had not prepared: its "
                     "bar_count, and the numbers a computation on history reads (a slice's "
                     "bounds, a period), have to depend on the config only.")


class _Simulation:
    def __init__(self, market, hooks, configs, states, signals, signal_index, timing, initial_cash,
                 costs, backend, cancel_at_session_end, record, windows):
        self.m, self.hooks, self.b, self.xp = market, hooks, backend, backend.xp
        self.timing, self.record, self.windows = timing, record, windows
        self.initial_cash, self._states = initial_cash, states
        self.periods_per_year = 252
        self.slots = ORDER_SLOTS

        if isinstance(cancel_at_session_end, np.ndarray):
            self.cancel_at_session_end = (bool(cancel_at_session_end[0])
                                          if (cancel_at_session_end == cancel_at_session_end[0]).all()
                                          else backend.array(cancel_at_session_end[:, None]))
        else:
            self.cancel_at_session_end = bool(cancel_at_session_end)
        self.n_combos, self.n_columns = len(configs), len(market.columns)
        self.costs = costs.on(backend)
        self.costs.host = costs
        self.order_state = costs.order_state
        self.priced = any(hook.priced_orders for hook in hooks.all())
        b, xp = backend, backend.xp

        execution = "open" if timing == "next_open" else "close"
        if execution not in market.spot:
            raise SweepError(f"The bundle has no {execution!r} column to fill orders at.")
        price, volume = market.spot[execution], market.spot.get("volume")
        if volume is None:
            raise SweepError("The bundle has no volume column; the engine fills nothing without it.")
        can_fill = np.isfinite(price) & np.isfinite(volume) & (volume > 0)
        sizing = market.spot.get("price", market.spot["close"])
        self.bars = {
            "exec": b.array(np.where(can_fill, price, 0.0)[:, None, :]),
            "can_fill": b.array(can_fill[:, None, :]),
            "volume": b.array(np.where(np.isfinite(volume), volume, 0.0)[:, None, :]),
            "marks": b.array(np.where(np.isfinite(market.marks), market.marks, 0.0)[:, None, :]),
            "sizing_price": b.array(sizing[:, None, :]),
            "spot_close": b.array(market.spot["close"][:, None, :]),
            "stopped": b.array(market.stopped[:, None, :]),
        }
        self.spot = {field: b.array(values[:, None, :]) for field, values in market.spot.items()}
        self.spot_before = {field: b.array(values[:, None, :])
                            for field, values in market.spot_before.items()}
        self.multiplier = b.array(market.multiplier[None, :])
        self.nan = b.array(np.array(np.nan))
        self.inf = b.array(np.array(np.inf))
        self.is_future = b.array(market.is_future[None, :])
        if market.has_splits:
            self.bars["split"] = b.array(market.splits[:, None, :])
            self.split_bars = set(np.nonzero((market.splits != 1.0).any(axis=1))[0].tolist())
        else:
            self.split_bars = set()
        if market.has_auto_close:
            self.bars["auto_close"] = b.array(market.auto_close[:, None, :])
        self.close_bars = set(np.nonzero(market.auto_close.any(axis=1))[0].tolist())
        # Dividends: earned at the ex date into a ring of pay dates, paid on the pay date
        self.dividend_bars = set()
        if market.dividends:
            span = max(d.pay_bar - d.ex_bar for d in market.dividends) + 1
            self.ring = span
            earn = np.zeros((market.n_bars, self.n_columns, span))
            pay = np.zeros((market.n_bars, span), dtype=bool)
            for d in market.dividends:
                earn[d.ex_bar, d.column, d.pay_bar % span] += d.amount
                pay[d.pay_bar, d.pay_bar % span] = True
                self.dividend_bars.update((d.ex_bar, d.pay_bar))
            self.bars["earn"] = b.array(earn)
            self.bars["pay"] = b.array(pay[:, None, :])
        self.signals = {name: b.array(np.ascontiguousarray(np.swapaxes(stack, 0, 1)))
                        for name, stack in signals.items()}
        if signal_index is None or isinstance(signal_index, dict):
            self.variants = {name: xp.array(index) for name, index in (signal_index or {}).items()}
        else:
            self.variants = {name: xp.array(signal_index) for name in signals}

        self.columns = api.Columns(dict(zip(market.columns, market.listings)), b)
        self.state_names, state = _stack_states(states, b)
        self.context = api.Context(
            config=_Config.stack(configs, b), signals=api.Signals(b, self.columns),
            portfolio=api.Portfolio(self.columns), data=api.Data(self.columns, b, windows),
            backend=b, universe=dict(zip(market.columns, market.listings)), columns=self.columns,
            costs=self.costs, state=state)
        self.calls = {id(hook): hook.bind(xp) for hook in hooks.all()}
        for call in self.calls.values():
            call.namespace.update(api.STYLES)
        self.log, self.cash_log, self.held_log = [], [], []

    def set_schedule(self, schedule):
        self.schedule = [np.asarray(flags[:, 0] if flags.ndim == 2 and (flags == flags[:, :1]).all()
                                    else flags) for flags in schedule]
        self.schedule_device = [self.b.array(flags[:, :, None]) if flags.ndim == 2 else None
                                for flags in self.schedule]

    # -- what may be compiled
    def compilable(self) -> bool:
        reasons = []
        for hook in self.hooks.all():
            reasons += hook.compile_blockers
        written = set().union(*(hook.writes for hook in self.hooks.all()))
        for name in written:
            value = getattr(self.context, name, None)
            if not api.numeric(value):
                reasons.append(f"context.{name} is not a number")
        for name in self.maps:
            if not isinstance(getattr(self.context, name, None), api.AssetState):
                reasons.append(f"context.{name} is changed in place")
        self.compile_reasons = reasons
        return not reasons and bool(self.b.lazy)

    # -- one bar's inputs
    def now(self, t: int) -> dict:
        xp = self.xp
        now = {name: values[t] for name, values in self.bars.items()}
        now["spot"] = {field: values[t] for field, values in self.spot.items()}
        now["spot_before"] = {field: values[t] for field, values in self.spot_before.items()}
        now["signals"] = {name: xp.take(stack[t], self.variants[name], axis=0)
                          if name in self.variants else stack[t]
                          for name, stack in self.signals.items()}
        now["history"] = self.windows.at(t) if self.windows.stacks else {}
        now["scheduled"] = [bool(flags[t]) if device is None else device[t]
                            for flags, device in zip(self.schedule, self.schedule_device)]
        # NaN as an input, never a constant: MLX writes a NaN constant into a fused Metal kernel
        # as an undeclared `nan` and the compile fails
        now["nan"], now["inf"] = self.nan, self.inf
        now["session_start"] = bool(self.m.session_start[t])
        now["session_end"] = bool(self.m.session_end[t])
        return now

    def initial_state(self, compiled: bool) -> dict:
        b, c, n = self.b, self.n_combos, self.n_columns
        zeros = lambda: b.array(np.zeros((c, n)))
        state = {
            "cash": b.array(np.full((c, 1), self.initial_cash)),
            "held": zeros(),
            "fees": b.array(np.zeros((c, 1))), "fills": b.array(np.zeros((c, 1))),
            "conflict": b.array(np.zeros((c, 1), dtype=bool)),
            # Positions opened by a fill are marked at the fill price until the next sync
            "mark": zeros(),
            "slots": [self._empty_slot(zeros) for _ in range(self.slots)],
        }
        if self.split_bars:
            state["cost_basis"] = zeros()
        if self.m.has_futures:
            state["reference"] = zeros()
        if self.dividend_bars:
            state["owed"] = b.array(np.zeros((c, self.ring)))
        if compiled and self.maps:
            like = zeros()
            state["maps"] = {name: getattr(self.context, name).dump(like) for name in self.maps}
        if compiled and self.windows.lazy.records:
            state["lazy_miss"] = b.array(np.zeros((c, 1), dtype=bool))
        if compiled:
            state["user"] = {name: b.scalar(getattr(self.context, name))
                             if not is_array(getattr(self.context, name))
                             else getattr(self.context, name)
                             for name in self.writes}
        return state

    def _empty_slot(self, zeros) -> dict:
        slot = {"amount": zeros()}
        if self.priced:
            slot["limit"], slot["stop"] = zeros() + np.nan, zeros() + np.nan
        if self.order_state:
            slot["filled"], slot["commission"] = zeros(), zeros()
        return slot

    @property
    def writes(self):
        return sorted(set().union(*(hook.writes for hook in self.hooks.all())))

    @property
    def maps(self):
        """context attributes changed in place: per-instrument dicts, carried as arrays."""
        return sorted(set().union(*(hook.in_place for hook in self.hooks.all())))

    # -- discovering which history windows the strategy reads
    def discover(self):
        self.windows.discovering = True
        self.windows.lazy.begin_discovery()
        try:
            state = self.initial_state(compiled=False)
            self.step(state, self.now(0), 0, plain=True)
        except (NotConvertible, SweepError):
            raise
        except Exception:
            pass  # a discovery pass runs on placeholder windows; the real run reports errors
        finally:
            self.windows.discovering = False
        # The pass changed the context's state (context.i += 1); start from initialize's again
        _, fresh = _stack_states(self._states, self.b)
        for name, value in fresh.items():
            self.context.set_state(name, value)

    # -- the session
    def step(self, state: dict, now: dict, t: int, plain: bool):
        xp, ctx = self.xp, self.context
        self.windows.lazy.begin_step()
        state = dict(state)
        state["slots"] = [dict(slot) for slot in state["slots"]]
        self.nan_now = now["nan"]
        if "user" in state:
            for name, value in state["user"].items():
                setattr(ctx, name, value)
        for name, (values, present) in state.get("maps", {}).items():
            getattr(ctx, name).load(values, present)

        # Session start: dividends, then splits
        if "earn" in now and (not plain or t in self.dividend_bars):
            state["owed"] = state["owed"] + xp.sum(state["held"][:, :, None] * now["earn"][None],
                                                   axis=1)
            paid = xp.sum(xp.where(now["pay"], state["owed"], 0.0), axis=1, keepdims=True)
            state["cash"] = state["cash"] + xp.where(paid > 0, paid, 0.0)
            state["owed"] = xp.where(now["pay"], 0.0, state["owed"])
        if "split" in now and (not plain or t in self.split_bars):
            self._split(state, now["split"])

        # before_trading_start: the ledger's portfolio as the previous session left it
        if self.hooks.before_trading_start is not None and now["session_start"] is not False:
            self._snapshot(state, state["mark"])
            self._hook(self.hooks.before_trading_start, now, before=True,
                       mask=now["session_start"])

        # Bar: marks synced to the close; futures settled against them
        marks = now["marks"]
        state["mark"] = xp.where(state["held"] != 0, marks, state["mark"])
        if self.m.has_futures:
            self._settle(state, marks)
        filled = xp.zeros_like(state["held"])
        if self.timing != "same_close":
            filled = self._fill(state, now)
        self._snapshot(state, state["mark"])
        ctx.reset_orders()
        now["filled"] = filled
        now["pending_now"] = self._open(state)
        self._hook(self.hooks.handle_data, now, before=False)
        # Scheduled functions after handle_data, in the order they were scheduled
        for hook, runs in zip(self.hooks.scheduled, now["scheduled"]):
            if runs is not False:
                self._hook(hook, now, before=False, mask=runs)
        self._queue(state)
        if self.timing == "same_close":
            self._fill(state, now)

        # Session end: auto-close, cancel policy, marks back to the close
        state["mark"] = xp.where(state["held"] != 0, marks, state["mark"])
        if self.m.has_futures:
            self._settle(state, marks)
        if "auto_close" in now and (not plain or t in self.close_bars):
            closing = now["auto_close"] & (state["held"] != 0)
            proceeds = xp.where(closing & ~self.is_future, state["held"] * marks, 0.0)
            state["cash"] = state["cash"] + xp.sum(proceeds, axis=1, keepdims=True)
            state["held"] = xp.where(closing, 0.0, state["held"])
            for slot in state["slots"]:
                self._clear(slot, now["auto_close"])
        cancel, end = self.cancel_at_session_end, now["session_end"]
        if cancel is not False and end is not False:
            where = cancel if end is True else (end if cancel is True else xp.logical_and(cancel, end))
            for slot in state["slots"]:
                self._clear(slot, where)
        value = state["cash"] + xp.sum(xp.where(self.is_future, 0.0, state["held"] * marks),
                                       axis=1, keepdims=True)
        if "user" in state:
            state["user"] = {name: getattr(ctx, name) for name in state["user"]}
        if "maps" in state:
            like = xp.zeros_like(state["held"])
            state["maps"] = {name: getattr(ctx, name).dump(like) for name in state["maps"]}
        if "lazy_miss" in state:
            for miss in self.windows.lazy.misses:
                state["lazy_miss"] = xp.logical_or(state["lazy_miss"], miss)
        return state, value

    def _snapshot(self, state, marks):
        self.context.portfolio.update(self.xp, state["cash"], state["held"], marks,
                                      self.multiplier, self.is_future)

    def _hook(self, hook, now, before, mask=True):
        ctx, data = self.context, self.context._data
        data.before = before
        data.now = {"spot": now["spot"], "spot_before": now["spot_before"],
                    "sizing_price": now["sizing_price"], "spot_close": now["spot_close"],
                    "volume": now["volume"], "filled": now.get("filled", 0.0),
                    "multiplier": self.multiplier, "pending": now.get("pending_now", 0.0),
                    "nan": now["nan"]}
        data.stopped = now["stopped"]
        data.dt = now.get("dt")
        self.windows.now = now["history"]
        ctx.signals.now = now["signals"]
        ctx._in_before_trading_start = before
        call = self.calls[id(hook)]
        ctx._runtime = data._runtime = call.runtime
        call.runtime.nan, call.runtime.inf = now["nan"], now["inf"]
        from ziplime.sweep import convert
        convert._RUNTIME_INF[0] = now["inf"]
        call(ctx, data, mask)
        ctx._in_before_trading_start = False

    def _open(self, state):
        """Where an instrument has an open order."""
        xp = self.xp
        result = state["slots"][0]["amount"] != 0
        for slot in state["slots"][1:]:
            result = xp.logical_or(result, slot["amount"] != 0)
        return result

    def _clear(self, slot, where):
        """Empty a slot where ``where`` holds (a filled, cancelled or closed order)."""
        xp = self.xp
        for name, value in slot.items():
            empty = self.nan_now if name in ("limit", "stop") else 0.0
            slot[name] = xp.where(where, empty, value) if where is not True else \
                xp.zeros_like(value) + empty

    def _compact(self, state):
        """Open orders to the front, in the order they were placed: the blotter walks an
        instrument's orders oldest first."""
        xp, slots = self.xp, state["slots"]
        for last in range(len(slots) - 1, 0, -1):
            for k in range(last):
                move = (slots[k]["amount"] == 0) & (slots[k + 1]["amount"] != 0)
                for name in slots[k]:
                    here, there = slots[k][name], slots[k + 1][name]
                    slots[k][name] = xp.where(move, there, here)
                    slots[k + 1][name] = xp.where(move, here, there)

    def _queue(self, state):
        """This bar's orders behind the instrument's open ones, each in its own slot."""
        xp, ctx = self.xp, self.context
        if not ctx.new_orders:
            return
        self._compact(state)
        slots = state["slots"]
        for amount, limit, stop in ctx.new_orders:
            placed = amount != 0
            count = xp.zeros_like(amount)
            for slot in slots:
                count = count + (slot["amount"] != 0)
            for k, slot in enumerate(slots):
                put = placed & (count == k)
                slot["amount"] = xp.where(put, amount, slot["amount"])
                if self.priced:
                    slot["limit"] = xp.where(put, limit, slot["limit"])
                    slot["stop"] = xp.where(put, stop, slot["stop"])
            overflow = xp.any(placed & (count >= len(slots)), axis=1, keepdims=True)
            state["conflict"] = xp.logical_or(state["conflict"], overflow)
        ctx.reset_orders()

    def _fill(self, state, now):
        """The blotter's fill step: each instrument's open orders oldest first, sharing the bar's
        volume allowance; an order the allowance cannot serve stops the walk for that bar."""
        xp, c = self.xp, self.costs
        price = now["exec"]
        bps = c.slippage_kind == 1
        remaining = xp.floor(c.capped_limit * now["volume"])
        stopped = xp.zeros_like(state["held"]) != 0
        total = xp.zeros_like(state["held"])
        for slot in state["slots"]:
            pending = slot["amount"]
            can = now["can_fill"] & (pending != 0) & ~stopped
            buy = pending > 0
            if self.priced:
                limit, stop = slot["limit"], slot["stop"]
                stop_hit = xp.where(buy, price >= stop, price <= stop) & ~xp.isnan(stop)
                waiting = ~xp.isnan(stop) & ~stop_hit
                limit_hit = xp.where(buy, price <= limit, price >= limit)
                triggered = can & ~waiting & (xp.isnan(limit) | limit_hit)
                slot["stop"] = xp.where(can & stop_hit, now["nan"], stop)
                slot["limit"] = xp.where(triggered, now["nan"], limit)
            else:
                triggered = can
            direction = xp.sign(pending)
            shares = xp.where(bps, xp.minimum(abs(pending), remaining), abs(pending))
            exhausted = triggered & bps & (shares <= 0)
            stopped = xp.logical_or(stopped, exhausted)
            amount = xp.where(triggered & ~exhausted, shares, 0.0) * direction
            remaining = remaining - abs(amount)
            total = total + abs(amount)
            self._book(state, slot, amount, price, direction, now)
            slot["amount"] = pending - amount
            self._clear(slot, slot["amount"] == 0)
        return total

    def _book(self, state, slot, amount, price, direction, now):
        """One transaction per instrument: cash, position, commission, cost basis, marks."""
        xp, c = self.xp, self.costs
        fill_price = price + price * c.percentage * direction + c.spread / 2.0 * direction
        traded = amount != 0
        before = state["held"]

        # Commission per fill, with the order's own minimum and fee charged once
        dollar = abs(amount) * fill_price * c.per_dollar
        additional = abs(amount) * c.per_unit
        if self.order_state:
            first = slot["commission"] == 0
            total = abs(slot["filled"]) * c.per_unit + additional + c.initial
            unit = xp.where(first, xp.maximum(c.min_trade, additional + c.initial),
                            xp.where(total < c.min_trade, 0.0, total - slot["commission"]))
            trade = xp.where(first, c.per_trade, 0.0)
        else:
            unit = xp.maximum(c.min_trade, additional + c.initial)
            trade = c.per_trade
        fee = xp.where(traded, xp.where(c.unit_model, unit, 0.0) + dollar + trade, 0.0)
        fee = xp.where(fee > 0, fee, 0.0)

        # Cash: equities pay their value; futures settle against their reference price
        cash_flow = xp.where(self.is_future, 0.0, -amount * fill_price)
        if self.m.has_futures:
            reference = state["reference"]
            payout = xp.where(self.is_future & traded & (before != 0),
                              (fill_price - reference) * self.multiplier * before, 0.0)
            cash_flow = cash_flow + payout
            closed = (before + amount) == 0
            state["reference"] = xp.where(self.is_future & traded,
                                          xp.where(closed, 0.0, fill_price), reference)
        if self.split_bars:
            state["cost_basis"] = _cost_basis(xp, state["cost_basis"], before, amount, fill_price,
                                              traded, fee, self.multiplier)
        state["cash"] = state["cash"] + xp.sum(cash_flow - fee, axis=1, keepdims=True)
        state["held"] = before + amount
        state["fees"] = state["fees"] + xp.sum(fee, axis=1, keepdims=True)
        state["fills"] = state["fills"] + xp.sum(traded, axis=1, keepdims=True)
        state["mark"] = xp.where((before == 0) & traded, fill_price, state["mark"])
        if self.order_state:
            slot["filled"] = slot["filled"] + amount
            slot["commission"] = slot["commission"] + fee
        if self.record:
            self.log.append((self.bar, self.b.numpy(amount)[0], self.b.numpy(fill_price)[0],
                             self.b.numpy(fee)[0], self.b.numpy(before)[0]))

    def _settle(self, state, marks):
        """Futures variation margin in update_portfolio: held × (mark − reference) × multiplier."""
        xp = self.xp
        held, reference = state["held"], state["reference"]
        live = self.is_future & (held != 0)
        payout = xp.where(live, (marks - reference) * self.multiplier * held, 0.0)
        state["cash"] = state["cash"] + xp.sum(payout, axis=1, keepdims=True)
        state["reference"] = xp.where(live, marks, reference)

    def _split(self, state, ratio):
        """position_tracker.handle_splits and Order.handle_split."""
        xp = self.xp
        held = state["held"]
        splitting = (ratio != 1.0) & (held != 0)
        raw = held / ratio
        whole = xp.floor(raw)
        fraction = raw - whole
        basis = _round_cents(xp, state["cost_basis"] * ratio)
        cash = xp.where(splitting, _round_cents(xp, fraction * basis), 0.0)
        leftover = xp.sum(cash, axis=1, keepdims=True)
        state["cash"] = state["cash"] + xp.where(leftover > 0, leftover, 0.0)
        state["held"] = xp.where(splitting, whole, held)
        state["cost_basis"] = xp.where(splitting, basis, state["cost_basis"])
        for slot in state["slots"]:
            moving = (ratio != 1.0) & (slot["amount"] != 0)
            scaled = slot["amount"] / ratio
            slot["amount"] = xp.where(moving, xp.sign(scaled) * xp.floor(abs(scaled)), slot["amount"])
            if self.priced:
                slot["limit"] = xp.where(moving, xp.round(slot["limit"] * ratio, 2), slot["limit"])
                slot["stop"] = xp.where(moving, xp.round(slot["stop"] * ratio, 2), slot["stop"])

    # -- the run
    def go(self, compiled: bool) -> Result:
        b, xp = self.b, self.xp
        state = self.initial_state(compiled)
        values = []
        self.evaluate_every = _evaluate_every(b, self.n_combos * self.n_columns)

        def block(state, nows):
            """Several bars in one compiled call: Python calls it once per block, not per bar."""
            out = []
            for now in nows:
                state, value = self.step(state, now, -1, plain=False)
                out.append(value)
            return state, out

        compiled_block = b.compile(block) if compiled else None
        self.windows.lazy.compiled = compiled
        span = _bars_per_call() if compiled else 1
        since_evaluation, t, waiting = 0, 0, []
        while t < self.m.n_bars:
            bars = range(t, min(t + span, self.m.n_bars))
            self.bar = bars[-1]
            if compiled:
                nows = []
                for u in bars:
                    now = self.now(u)
                    # A compiled step sees arrays only: rules and session edges become masks
                    now["scheduled"] = [xp.array(flag) if isinstance(flag, bool) else flag
                                        for flag in now["scheduled"]]
                    now["session_start"] = xp.array(now["session_start"])
                    now["session_end"] = xp.array(now["session_end"])
                    nows.append(now)
                state, block_values = compiled_block(state, nows)
                # The day's value is what the engine records, at the session's end
                kept = [value for u, value in zip(bars, block_values) if self.m.session_end[u]]
            else:
                now = self.now(t)
                now["dt"] = self.m.index[t]
                state, value = self.step(state, now, t, plain=True)
                kept = [value] if self.m.session_end[t] else []
            values.extend(kept)
            waiting.extend(kept)
            t = bars[-1] + 1
            since_evaluation += len(bars)
            if b.lazy and (since_evaluation >= self.evaluate_every or t == self.m.n_bars):
                b.evaluate(state["cash"], state["held"], *waiting)
                since_evaluation, waiting = 0, []
            if self.record and kept:
                self.cash_log.append(float(b.numpy(state["cash"])[0, 0]))
                self.held_log.append(b.numpy(state["held"])[0])
        self.windows.lazy.compiled = False
        if "lazy_miss" in state and bool(b.numpy(state["lazy_miss"]).any()):
            raise RunPlain()
        curves = xp.concatenate(values, axis=1)  # (combinations, bars)
        return Result(
            metrics=_metrics(b, curves, self.initial_cash, self.periods_per_year),
            value=b.numpy(curves).T.astype(float) if self.record else None,
            cash=b.numpy(state["cash"])[:, 0].astype(float),
            positions=b.numpy(state["held"]).astype(float),
            fees=b.numpy(state["fees"])[:, 0].astype(float),
            fills=b.numpy(state["fills"])[:, 0].astype(int),
            unsupported=b.numpy(state["conflict"])[:, 0].astype(bool),
            compiled=compiled, log=self.log if self.record else None,
            compile_reasons=None if compiled else list(getattr(self, "compile_reasons", None) or []),
            cash_log=self.cash_log if self.record else None,
            held_log=self.held_log if self.record else None)


def _metrics(b: Backend, curves, initial_cash: float, periods_per_year: int) -> dict:
    """Sharpe (returns' mean over their sample deviation, annualised), total return, max drawdown
    and final value per combination, as report.metrics computes them with pandas."""
    xp = b.xp
    returns = curves[:, 1:] / curves[:, :-1] - 1.0
    count = returns.shape[1]
    mean = xp.mean(returns, axis=1)
    deviation = xp.sqrt(xp.sum((returns - mean[:, None]) ** 2, axis=1) / max(count - 1, 1))
    sharpe = xp.where(deviation > 0, mean / deviation * float(np.sqrt(periods_per_year)), np.nan) \
        if b.name != "mlx" else mean / deviation * float(np.sqrt(periods_per_year))
    peak = _running_max(b, curves)
    drawdown = xp.min(curves / peak - 1.0, axis=1)
    result = {"sharpe": b.numpy(sharpe).astype(float),
              "total_return": b.numpy(curves[:, -1] / initial_cash - 1.0).astype(float),
              "max_drawdown": b.numpy(drawdown).astype(float),
              "final_value": b.numpy(curves[:, -1]).astype(float)}
    result["sharpe"][~(b.numpy(deviation) > 0)] = np.nan
    return result


#: Rough device memory one bar's intermediates take per element (combinations × instruments):
#: a few hundred float32 operations.
BYTES_PER_ELEMENT_BAR = 1_600
#: What MLX on CUDA may hold in queued bars before evaluating them.
QUEUE_BUDGET = 4e9


#: Bars per compiled call. One call per bar leaves the GPU waiting on Python between bars; a
#: block of bars is one call. ZIPLIME_SWEEP_BARS_PER_CALL to experiment.
BARS_PER_CALL = 16


def _bars_per_call() -> int:
    return max(1, int(os.environ.get("ZIPLIME_SWEEP_BARS_PER_CALL", BARS_PER_CALL)))


def _evaluate_every(backend: Backend, elements: int) -> int:
    """How many bars MLX queues before evaluating. On Apple silicon the graph's intermediates go to
    unified memory and 32 bars is cheap; on CUDA they fill the card's own memory, so the queue is
    sized to a budget."""
    override = os.environ.get("ZIPLIME_SWEEP_EVALUATE_EVERY")
    if override:
        return max(1, int(override))
    if backend.metal or not backend.lazy:
        return EVALUATE_EVERY
    return int(min(EVALUATE_EVERY, max(1, QUEUE_BUDGET // (BYTES_PER_ELEMENT_BAR * max(1, elements)))))


def _running_max(b: Backend, curves):
    """The running maximum along the bars. CuPy has no maximum.accumulate; doubling the shift
    gets there in log2(bars) elementwise passes on any backend."""
    xp = b.xp
    if b.name == "mlx":
        return xp.cummax(curves, axis=1)
    if b.name == "numpy":
        return np.maximum.accumulate(curves, axis=1)
    peak, shift, bars = curves, 1, curves.shape[1]
    while shift < bars:
        shifted = xp.concatenate([peak[:, :shift], peak[:, :-shift]], axis=1)
        peak = xp.maximum(peak, shifted)
        shift *= 2
    return peak


def _round_cents(xp, value):
    """Python's round(value, 2): the nearer of the two cents around the exact value. numpy's round
    scales by 100 first, and that product can land on the other side of a half cent."""
    low = xp.floor(value * 100.0) / 100.0
    high = (xp.floor(value * 100.0) + 1.0) / 100.0
    return xp.where(value - low < high - value, low, high)


def _cost_basis(xp, basis, held, amount, price, traded, fee, multiplier):
    """Position._update_position and adjust_commission_cost_basis."""
    total = held + amount
    same = (xp.sign(held) == xp.sign(amount)) | (held == 0)
    crossed = ~same & (abs(amount) > abs(held))
    averaged = (basis * held + amount * price) / xp.where(total == 0, 1.0, total)
    updated = xp.where(total == 0, 0.0, xp.where(same, averaged, xp.where(crossed, price, basis)))
    with_fee = xp.where(total != 0, (updated * total + fee / multiplier) / xp.where(total == 0, 1.0, total),
                        updated)
    return xp.where(traded, with_fee, basis)


# -- configs and state as arrays ----------------------------------------------------------------------

class _Config:
    """context.algorithm.config for every combination, shaped like the config itself: a numeric
    field is a (combinations × 1) array, a nested model the same object, a list a list of them, a
    string shared by all combinations the string."""

    def __init__(self, fields: dict):
        self._fields = fields

    def __getattr__(self, name):
        try:
            return self.__dict__["_fields"][name]
        except KeyError:
            raise AttributeError(f"The config has no field {name!r}.") from None

    @classmethod
    def stack(cls, values: list, backend: Backend):
        from pydantic import BaseModel

        first = values[0]
        if all(isinstance(value, (bool, int, float)) for value in values):
            if all(value == first for value in values):
                return first
            return backend.array(np.array(values, dtype=float)[:, None])
        if isinstance(first, BaseModel):
            return cls({name: cls.stack([getattr(value, name) for value in values], backend)
                        for name in type(first).model_fields})
        if isinstance(first, (list, tuple)) and len({len(value) for value in values}) == 1:
            if all(value == first for value in values):
                return first
            return [cls.stack([value[k] for value in values], backend) for k in range(len(first))]
        if all(value == first for value in values):
            return first
        return _Varying()


class _Varying:
    """A non-numeric config field that differs between combinations: no array can hold it."""

    def __getattr__(self, name):
        raise SweepError("This config field is not numeric and differs between combinations; a "
                         "sweep cannot vary it.")


def _stack_states(states: list, backend: Backend):
    """What initialize put on the context, per combination: shared values as they are, numbers that
    differ as (combinations × 1) arrays."""
    import copy

    if not states:
        return [], {}
    from ziplime.sweep.lazy import Objects, is_object_state

    names = list(states[0])
    result = {}
    for name in names:
        values = [state.get(name) for state in states]
        if any(is_object_state(value) for value in values):
            # A fitted model and the like: one per distinct run of initialize, chosen by position
            distinct = list({id(value): value for value in values}.values())
            if len(distinct) == 1:
                result[name] = Objects(distinct)
            else:
                position = {id(value): k for k, value in enumerate(distinct)}
                result[name] = Objects(distinct, backend.array(
                    np.array([position[id(value)] for value in values], dtype=float)[:, None]))
            continue
        try:
            same = all(value == values[0] for value in values)
        except Exception:
            same = all(value is values[0] for value in values)
        if same:
            result[name] = copy.deepcopy(values[0])
        else:
            result[name] = _Config.stack(values, backend)
    return names, result


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
