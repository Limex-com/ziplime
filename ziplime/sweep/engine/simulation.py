"""Simulation for engine."""

from collections.abc import Sequence
import numpy as np
from ziplime.sweep import api
from ziplime.sweep.backends import Backend
from ziplime.sweep.convert import Converted
from ziplime.sweep.array_utils import is_array
from ziplime.sweep.costs import Costs
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.market import Market
from ziplime.sweep.windows import Windows
from ziplime.sweep.params import SweepError
from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.sweep.engine.hooks import Hooks
from ziplime.sweep.engine.support import ORDER_SLOTS
from ziplime.sweep.engine.result import Result
from ziplime.sweep.engine.run_plain import RunPlain
from ziplime.sweep.engine.config import _Config
from ziplime.sweep.engine.scheduling_utils import _bars_per_call
from ziplime.sweep.engine.accounting_utils import _cost_basis
from ziplime.sweep.engine.scheduling_utils import _evaluate_every
from ziplime.sweep.engine.metric_utils import _metrics
from ziplime.sweep.engine.accounting_utils import _round_cents
from ziplime.sweep.engine.config_utils import _stack_states


class _Simulation:
    def __init__(
        self,
        market: Market,
        hooks: Hooks,
        configs: list[BaseAlgorithmConfig],
        states,
        signals: dict[str, np.ndarray],
        signal_index: np.ndarray | dict[str, np.ndarray] | None,
        timing: str,
        initial_cash: float,
        costs: Costs,
        backend: Backend,
        cancel_at_session_end: bool | np.ndarray,
        record: bool,
        windows: Windows,
    ) -> None:
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

    def set_schedule(self, schedule: Sequence[np.ndarray]) -> None:
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
    def discover(self) -> None:
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

    def _snapshot(self, state, marks) -> None:
        self.context.portfolio.update(self.xp, state["cash"], state["held"], marks,
                                      self.multiplier, self.is_future)

    def _hook(self, hook: Converted, now, before: bool, mask=True) -> None:
        from ziplime.sweep.convert.support import _RUNTIME_INF
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
        _RUNTIME_INF[0] = now["inf"]
        call(ctx, data, mask)
        ctx._in_before_trading_start = False

    def _open(self, state):
        """Where an instrument has an open order."""
        xp = self.xp
        result = state["slots"][0]["amount"] != 0
        for slot in state["slots"][1:]:
            result = xp.logical_or(result, slot["amount"] != 0)
        return result

    def _clear(self, slot, where) -> None:
        """Empty a slot where ``where`` holds (a filled, cancelled or closed order)."""
        xp = self.xp
        for name, value in slot.items():
            empty = self.nan_now if name in ("limit", "stop") else 0.0
            slot[name] = xp.where(where, empty, value) if where is not True else \
                xp.zeros_like(value) + empty

    def _compact(self, state) -> None:
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

    def _queue(self, state) -> None:
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

    def _book(self, state, slot, amount, price, direction, now) -> None:
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

    def _settle(self, state, marks) -> None:
        """Futures variation margin in update_portfolio: held × (mark − reference) × multiplier."""
        xp = self.xp
        held, reference = state["held"], state["reference"]
        live = self.is_future & (held != 0)
        payout = xp.where(live, (marks - reference) * self.multiplier * held, 0.0)
        state["cash"] = state["cash"] + xp.sum(payout, axis=1, keepdims=True)
        state["reference"] = xp.where(live, marks, reference)

    def _split(self, state, ratio) -> None:
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
