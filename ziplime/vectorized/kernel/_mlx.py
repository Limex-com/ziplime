"""The batch kernel on the Apple GPU, through MLX: one thread per signal set.

This is the second spelling of one set of decisions, not a second set. `_core.run_events` is the
reference; the Metal source below walks the same events in the same order and takes the same
decisions with the same codes, because neither Python nor Numba can run on the GPU. What keeps
the two from drifting is `tests/vectorized/test_vector_kernel_batch.py`, which runs both on the
same randomised inputs and requires the same fills, rejections and no-ops.

Three things differ from `_core`, and the hardware decided each of them:

**float32.** Metal has no double precision. Prices, sizes and cash are seven significant digits
here -- a cent or so on a six-figure account -- which is why a batch result ranks and
:meth:`~ziplime.vectorized.kernel.batch.BatchSimulationResult.rerun` reports.

**NaN is decided on the CPU.** Metal compiles with fast math, under which `isfinite` is allowed
to fold to `true`. So the host turns every price, size and volume into a finite number plus a
flag bit before the launch, and the GPU never sees a NaN to misjudge.

**The combination is the innermost axis.** A SIMD group's 32 threads run in lockstep, each a
different combination at the same bar and instrument. Laid out as (bar, instrument, combination),
their 32 reads of a signal are 32 adjacent bytes -- one memory transaction rather than 32.

Events are not sorted here as `simulation` sorts them. A thread walks every (bar, instrument)
cell in the order the sort key defines -- every instrument's closings, then every instrument's
openings -- and skips cells with no signal. On level signals, where nearly every cell carries
one, that is the same work, and it needs no per-combination event list.
"""
from __future__ import annotations

import numpy as np

from ziplime.vectorized.kernel import _core
from ziplime.vectorized.kernel.costs import ResolvedCommission, ResolvedSlippage
from ziplime.vectorized.kernel.models import VectorKernelError

try:  # pragma: no cover - depends on the platform
    import mlx.core as mx

    HAVE_MLX = True
except ImportError:  # pragma: no cover
    mx = None
    HAVE_MLX = False

#: Per-cell flag bits, set on the host where NaN can still be seen.
PRICE_OK = 1
SIZE_OK = 2
VOLUME_OK = 4

#: Rejection codes the loop can produce, which is the width of its counter row. Two more columns
#: follow them in the output: fills, then no-ops.
N_CODES = _core.R_INSUFFICIENT_CASH + 1

#: Threads per threadgroup. Each thread is one long sequential loop, so small groups spread a
#: short sweep across every core of the GPU rather than stacking it onto one.
THREADGROUP = 32

_HEADER = f"""
constant int CLOSE_LONG = {_core.CLOSE_LONG};
constant int CLOSE_SHORT = {_core.CLOSE_SHORT};
constant int OPEN_LONG = {_core.OPEN_LONG};
constant int OPEN_SHORT = {_core.OPEN_SHORT};
constant int R_SUPERSEDED = {_core.R_SUPERSEDED};
constant int R_NAN_PRICE = {_core.R_NAN_PRICE};
constant int R_NOTHING_TO_CLOSE = {_core.R_NOTHING_TO_CLOSE};
constant int R_NAN_SIZE = {_core.R_NAN_SIZE};
constant int R_ZERO_SIZE = {_core.R_ZERO_SIZE};
constant int R_ZERO_SIZE_VOLUME = {_core.R_ZERO_SIZE_VOLUME};
constant int R_INSUFFICIENT_CASH = {_core.R_INSUFFICIENT_CASH};
constant int N_CODES = {N_CODES};
constant uint PRICE_OK = {PRICE_OK};
constant uint SIZE_OK = {SIZE_OK};
constant uint VOLUME_OK = {VOLUME_OK};
"""

# The body of `_core.run_events`, decision for decision. Comments there explain the order; the
# only additions here are the flag tests that stand in for `np.isfinite`.
_SOURCE = """
    const uint combo = thread_position_in_grid.x;
    const int n_combos = ints[0];
    if ((int)combo >= n_combos) {
        return;
    }
    const int n_bars = ints[1];
    const int n_columns = ints[2];
    const int bar_offset = ints[3];
    const bool accumulate = ints[4] != 0;
    const bool capped_by_volume = ints[5] != 0;

    const float slip_percentage = floats[0];
    const float slip_spread = floats[1];
    const float slip_volume_limit = floats[2];
    const float com_per_unit = floats[3];
    const float com_per_dollar = floats[4];
    const float com_fixed = floats[5];
    const float com_min_trade = floats[6];
    float cash = floats[7];

    for (int column = 0; column < n_columns; ++column) {
        held[(ulong)column * n_combos + combo] = 0.0f;
    }
    int rejects[N_CODES];
    for (int code = 0; code < N_CODES; ++code) {
        rejects[code] = 0;
    }
    int fills = 0;
    int no_ops = 0;
    float fees_paid = 0.0f;

    for (int bar = 0; bar < n_bars; ++bar) {
        const int signal_bar = bar - bar_offset;
        for (int phase = 0; signal_bar >= 0 && phase < 2; ++phase) {
            for (int column = 0; column < n_columns; ++column) {
                const uint bits = signals[((ulong)signal_bar * n_columns + column) * n_combos + combo];
                if (bits == 0) {
                    continue;
                }
                const ulong cell = (ulong)bar * n_columns + column;
                const ulong slot = (ulong)column * n_combos + combo;
                for (int k = 0; k < 2; ++k) {
                    const int intent = phase * 2 + k;
                    if ((bits & (1u << intent)) == 0) {
                        continue;
                    }
                    const float position = held[slot];

                    if (!accumulate) {
                        if (intent == OPEN_LONG && position > 0.0f) { no_ops++; continue; }
                        if (intent == OPEN_SHORT && position < 0.0f) { no_ops++; continue; }
                    }
                    if (position == 0.0f && (intent == CLOSE_LONG || intent == CLOSE_SHORT)) {
                        no_ops++;
                        continue;
                    }
                    if (intent == OPEN_LONG && (bits & (1u << CLOSE_LONG)) != 0) {
                        rejects[R_SUPERSEDED]++;
                        continue;
                    }
                    if (intent == OPEN_SHORT && (bits & (1u << CLOSE_SHORT)) != 0) {
                        rejects[R_SUPERSEDED]++;
                        continue;
                    }

                    const uint ok = flags[cell];
                    if ((ok & PRICE_OK) == 0) {
                        rejects[R_NAN_PRICE]++;
                        continue;
                    }
                    const float price = execution[cell];

                    float wanted;
                    if (intent == OPEN_LONG || intent == OPEN_SHORT) {
                        if ((ok & SIZE_OK) == 0) {
                            rejects[R_NAN_SIZE]++;
                            continue;
                        }
                        wanted = intent == OPEN_LONG ? metal::fabs(sizes[cell])
                                                     : -metal::fabs(sizes[cell]);
                    } else {
                        if ((intent == CLOSE_LONG && position <= 0.0f) ||
                            (intent == CLOSE_SHORT && position >= 0.0f)) {
                            rejects[R_NOTHING_TO_CLOSE]++;
                            continue;
                        }
                        wanted = -position;
                    }
                    if (wanted == 0.0f) {
                        rejects[R_ZERO_SIZE]++;
                        continue;
                    }

                    const float direction = wanted > 0.0f ? 1.0f : -1.0f;
                    const float filled_price = price + price * slip_percentage * direction +
                                               slip_spread / 2.0f * direction;

                    float capped = metal::fabs(wanted);
                    if (capped_by_volume && (ok & VOLUME_OK) != 0) {
                        const float room = metal::max(volumes[cell] * slip_volume_limit, 0.0f);
                        capped = metal::min(capped, room);
                    }
                    if (capped <= 0.0f) {
                        rejects[R_ZERO_SIZE_VOLUME]++;
                        continue;
                    }

                    const float amount = direction * capped;
                    const float value = amount * filled_price;
                    float fee = com_per_unit * metal::fabs(amount) +
                                com_per_dollar * metal::fabs(value) + com_fixed;
                    if (fee < com_min_trade) {
                        fee = com_min_trade;
                    }

                    const float cost = value + fee;
                    if (cost > 0.0f && cost > cash + 1e-9f) {
                        rejects[R_INSUFFICIENT_CASH]++;
                        continue;
                    }

                    cash -= cost;
                    held[slot] = position + amount;
                    fills++;
                    fees_paid += fee;
                }
            }
        }

        float marked = cash;
        for (int column = 0; column < n_columns; ++column) {
            marked += held[(ulong)column * n_combos + combo] * marks[(ulong)bar * n_columns + column];
        }
        values[(ulong)bar * n_combos + combo] = marked;
    }

    const ulong row = (ulong)combo * (N_CODES + 2);
    for (int code = 0; code < N_CODES; ++code) {
        counts[row + code] = rejects[code];
    }
    counts[row + N_CODES] = fills;
    counts[row + N_CODES + 1] = no_ops;
    money[(ulong)combo * 2] = cash;
    money[(ulong)combo * 2 + 1] = fees_paid;
"""

_kernel = None


def require() -> None:
    """Refuse, by name, a GPU run on a machine that cannot do one."""
    if not HAVE_MLX:
        raise VectorKernelError(
            "backend='mlx' needs the `mlx` package, which is not installed here. It exists for "
            "Apple silicon Macs only: `pip install mlx`. On any other machine use "
            "backend='exact', which gives the same decisions in float64, one combination at a "
            "time.")


def _compiled():
    global _kernel
    if _kernel is None:
        _kernel = mx.fast.metal_kernel(
            name="ziplime_vector_batch",
            input_names=["signals", "flags", "execution", "marks", "sizes", "volumes", "ints",
                         "floats"],
            output_names=["values", "held", "counts", "money"],
            header=_HEADER,
            source=_SOURCE,
        )
    return _kernel


def _packed_signals(stacks: dict[str, np.ndarray]):
    """Four boolean ``(combinations, T, N)`` stacks as one byte per cell, combination innermost.

    Bit ``intent`` of the byte is that intent's signal, so the kernel tests ``1 << intent``
    against the same codes `_core` numbers them by.
    """
    packed = mx.zeros(stacks["entries"].shape, dtype=mx.uint8)
    for name, intent in (("exits", _core.CLOSE_LONG), ("short_exits", _core.CLOSE_SHORT),
                         ("entries", _core.OPEN_LONG), ("short_entries", _core.OPEN_SHORT)):
        # A long-only sweep leaves both short stacks empty; copying them over is most of the
        # transfer for nothing.
        if stacks[name].any():
            packed = packed | (mx.array(stacks[name]).astype(mx.uint8) * (1 << intent))
    return mx.transpose(packed, (1, 2, 0))


def run_batch(stacks: dict[str, np.ndarray], execution_values: np.ndarray,
              close_values: np.ndarray, size_values: np.ndarray,
              volume_values: np.ndarray | None, slippage: ResolvedSlippage,
              commission: ResolvedCommission, initial_cash: float, accumulate: bool,
              bar_offset: int) -> dict[str, np.ndarray]:
    """Every combination through the kernel at once.

    Returns:
        ``value`` (T x C), ``cash`` (C), ``positions`` (C x N), ``fees`` (C), ``fills`` (C),
        ``no_ops`` (C) and ``codes`` (C x N_CODES, counted by `_core` rejection code).
    """
    require()
    n_combos, n_bars, n_columns = stacks["entries"].shape

    flags = np.where(np.isfinite(execution_values), PRICE_OK, 0)
    flags |= np.where(np.isfinite(size_values), SIZE_OK, 0)
    capped_by_volume = slippage.volume_limit is not None and volume_values is not None
    if capped_by_volume:
        # NaN volume is "no opinion", as in `_core`, where `room` comes out NaN and caps nothing.
        # Infinities stay ordered, so -inf still leaves no room and +inf never binds.
        flags |= np.where(np.isnan(volume_values), 0, VOLUME_OK)
        volumes = mx.array(np.nan_to_num(volume_values, nan=0.0, posinf=3.0e38, neginf=-3.0e38),
                           dtype=mx.float32)
    else:
        volumes = mx.zeros((1,), dtype=mx.float32)

    def finite(values):
        return mx.array(np.where(np.isfinite(values), values, 0.0), dtype=mx.float32)

    ints = mx.array([n_combos, n_bars, n_columns, bar_offset, int(accumulate),
                     int(capped_by_volume)], dtype=mx.int32)
    floats = mx.array([slippage.percentage, slippage.spread, slippage.volume_limit or 0.0,
                       commission.per_unit, commission.per_dollar, commission.fixed,
                       commission.min_trade, initial_cash], dtype=mx.float32)

    values, held, counts, money = _compiled()(
        inputs=[_packed_signals(stacks), mx.array(flags.astype(np.uint8)),
                finite(execution_values), finite(close_values), finite(size_values), volumes,
                ints, floats],
        grid=(n_combos, 1, 1),
        threadgroup=(min(THREADGROUP, n_combos), 1, 1),
        output_shapes=[(n_bars, n_combos), (n_columns, n_combos), (n_combos, N_CODES + 2),
                       (n_combos, 2)],
        output_dtypes=[mx.float32, mx.float32, mx.int32, mx.float32],
    )
    mx.eval(values, held, counts, money)

    counts = np.array(counts, dtype=np.int64)
    money = np.array(money, dtype=np.float64)
    return {
        "value": np.array(values, dtype=np.float64),
        "cash": money[:, 0],
        "positions": np.array(held, dtype=np.float64).T,
        "fees": money[:, 1],
        "fills": counts[:, N_CODES],
        "no_ops": counts[:, N_CODES + 1],
        "codes": counts[:, :N_CODES],
    }
