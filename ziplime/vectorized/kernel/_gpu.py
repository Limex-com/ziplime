"""The batch kernel's GPU source, one thread per signal set, shared by Metal, CUDA and OpenCL.

This is the second spelling of one set of decisions, not a second set. `_core.run_events` is the
reference; the source below walks the same events in the same order and takes the same decisions
with the same codes, because neither Python nor Numba can run on every GPU. It is written once in
the C that Metal, CUDA and OpenCL share, and `_mlx` (Apple, through MLX), `_cupy` (NVIDIA,
through CuPy) and `_opencl` (Intel Arc, AMD, through PyOpenCL) each add only the line that says
which thread this is and the types and math functions their compiler spells differently. What
keeps it from drifting from `_core` is `tests/vectorized/test_vector_kernel_batch.py`, which runs
every backend on the same randomised inputs and requires the same fills, rejections and no-ops.

Three things differ from `_core`, and the hardware decided each of them:

**float32.** Metal has no double precision, nor do many OpenCL GPUs, and CUDA runs the same
float32 source so that every GPU ranks a sweep the same way. Prices, sizes and cash are seven
significant digits here -- a cent or so on a six-figure account -- which is why a batch result
ranks and :meth:`~ziplime.vectorized.kernel.batch.BatchSimulationResult.rerun` reports.

**NaN is decided on the CPU.** Metal compiles with fast math, under which `isfinite` is allowed
to fold to `true`. So the host turns every price, size and volume into a finite number plus a
flag bit before the launch, and the GPU never sees a NaN to misjudge.

**The combination is the innermost axis.** A SIMD group's (or warp's) 32 threads run in lockstep,
each a different combination at the same bar and instrument. Laid out as (bar, instrument,
combination), their 32 reads of a signal are 32 adjacent bytes -- one memory transaction rather
than 32.

Events are not sorted here as `simulation` sorts them. A thread walks every (bar, instrument)
cell in the order the sort key defines -- every instrument's closings, then every instrument's
openings -- and skips cells with no signal. On level signals, where nearly every cell carries
one, that is the same work, and it needs no per-combination event list.
"""
from __future__ import annotations

import numpy as np

from ziplime.vectorized.kernel import _core
from ziplime.vectorized.kernel.costs import ResolvedCommission, ResolvedSlippage

#: Per-cell flag bits, set on the host where NaN can still be seen.
PRICE_OK = 1
SIZE_OK = 2
VOLUME_OK = 4

#: Rejection codes the loop can produce, which is the width of its counter row. Two more columns
#: follow them in the output: fills, then no-ops.
N_CODES = _core.R_INSUFFICIENT_CASH + 1

#: Threads per threadgroup (Metal), block (CUDA) or work-group (OpenCL). Each thread is one long
#: sequential loop, so small groups spread a short sweep across every core of the GPU rather than
#: stacking it onto one.
THREADGROUP = 32

#: Which bit of the packed signal byte each stack sets: bit ``intent``, so the kernel tests
#: ``1 << intent`` against the same codes `_core` numbers them by.
SIGNAL_BITS = (("exits", _core.CLOSE_LONG), ("short_exits", _core.CLOSE_SHORT),
               ("entries", _core.OPEN_LONG), ("short_entries", _core.OPEN_SHORT))

#: The kernel's arguments, in order. Every input is read-only; ``held`` is an output only so the
#: thread has somewhere to keep its book.
INPUT_NAMES = ("signals", "flags", "execution", "marks", "sizes", "volumes", "ints", "floats")
OUTPUT_NAMES = ("values", "held", "counts", "money")

#: Constants for both compilers. Each backend's own header adds ``u32``, ``u64`` and ``ZL_ABS``,
#: ``ZL_MAX``, ``ZL_MIN``.
CONSTANTS = f"""
#define CLOSE_LONG {_core.CLOSE_LONG}
#define CLOSE_SHORT {_core.CLOSE_SHORT}
#define OPEN_LONG {_core.OPEN_LONG}
#define OPEN_SHORT {_core.OPEN_SHORT}
#define R_SUPERSEDED {_core.R_SUPERSEDED}
#define R_NAN_PRICE {_core.R_NAN_PRICE}
#define R_NOTHING_TO_CLOSE {_core.R_NOTHING_TO_CLOSE}
#define R_NAN_SIZE {_core.R_NAN_SIZE}
#define R_ZERO_SIZE {_core.R_ZERO_SIZE}
#define R_ZERO_SIZE_VOLUME {_core.R_ZERO_SIZE_VOLUME}
#define R_INSUFFICIENT_CASH {_core.R_INSUFFICIENT_CASH}
#define N_CODES {N_CODES}
#define PRICE_OK {PRICE_OK}u
#define SIZE_OK {SIZE_OK}u
#define VOLUME_OK {VOLUME_OK}u
"""

# The body of `_core.run_events`, decision for decision. Comments there explain the order; the
# only additions here are the flag tests that stand in for `np.isfinite`. It starts with `combo`,
# the thread's index, already defined by the backend.
BODY = """
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
        held[(u64)column * n_combos + combo] = 0.0f;
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
                const u32 bits = signals[((u64)signal_bar * n_columns + column) * n_combos + combo];
                if (bits == 0) {
                    continue;
                }
                const u64 cell = (u64)bar * n_columns + column;
                const u64 slot = (u64)column * n_combos + combo;
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

                    const u32 ok = flags[cell];
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
                        wanted = intent == OPEN_LONG ? ZL_ABS(sizes[cell]) : -ZL_ABS(sizes[cell]);
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

                    float capped = ZL_ABS(wanted);
                    if (capped_by_volume && (ok & VOLUME_OK) != 0) {
                        const float room = ZL_MAX(volumes[cell] * slip_volume_limit, 0.0f);
                        capped = ZL_MIN(capped, room);
                    }
                    if (capped <= 0.0f) {
                        rejects[R_ZERO_SIZE_VOLUME]++;
                        continue;
                    }

                    const float amount = direction * capped;
                    const float value = amount * filled_price;
                    float fee = com_per_unit * ZL_ABS(amount) +
                                com_per_dollar * ZL_ABS(value) + com_fixed;
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
            marked += held[(u64)column * n_combos + combo] * marks[(u64)bar * n_columns + column];
        }
        values[(u64)bar * n_combos + combo] = marked;
    }

    const u64 row = (u64)combo * (N_CODES + 2);
    for (int code = 0; code < N_CODES; ++code) {
        counts[row + code] = rejects[code];
    }
    counts[row + N_CODES] = fills;
    counts[row + N_CODES + 1] = no_ops;
    money[(u64)combo * 2] = cash;
    money[(u64)combo * 2 + 1] = fees_paid;
"""


def host_inputs(execution_values: np.ndarray, close_values: np.ndarray, size_values: np.ndarray,
                volume_values: np.ndarray | None, slippage: ResolvedSlippage,
                commission: ResolvedCommission, initial_cash: float, accumulate: bool,
                bar_offset: int, n_combos: int) -> dict[str, np.ndarray]:
    """Every input but the signals, as the host arrays the kernel reads: NaN already decided."""
    n_bars, n_columns = execution_values.shape
    flags = np.where(np.isfinite(execution_values), PRICE_OK, 0)
    flags |= np.where(np.isfinite(size_values), SIZE_OK, 0)
    capped_by_volume = slippage.volume_limit is not None and volume_values is not None
    if capped_by_volume:
        # NaN volume is "no opinion", as in `_core`, where `room` comes out NaN and caps nothing.
        # Infinities stay ordered, so -inf still leaves no room and +inf never binds.
        flags |= np.where(np.isnan(volume_values), 0, VOLUME_OK)
        volumes = np.nan_to_num(volume_values, nan=0.0, posinf=3.0e38, neginf=-3.0e38)
    else:
        volumes = np.zeros(1)

    def finite(values):
        return np.where(np.isfinite(values), values, 0.0).astype(np.float32)

    return {
        "flags": flags.astype(np.uint8),
        "execution": finite(execution_values),
        "marks": finite(close_values),
        "sizes": finite(size_values),
        "volumes": volumes.astype(np.float32),
        "ints": np.array([n_combos, n_bars, n_columns, bar_offset, int(accumulate),
                          int(capped_by_volume)], dtype=np.int32),
        "floats": np.array([slippage.percentage, slippage.spread, slippage.volume_limit or 0.0,
                            commission.per_unit, commission.per_dollar, commission.fixed,
                            commission.min_trade, initial_cash], dtype=np.float32),
    }


def output_shapes(n_combos: int, n_bars: int, n_columns: int) -> list[tuple[int, int]]:
    """Shapes of ``values``, ``held``, ``counts`` and ``money``; their dtypes are float32, float32,
    int32 and float32."""
    return [(n_bars, n_combos), (n_columns, n_combos), (n_combos, N_CODES + 2), (n_combos, 2)]


def summary(values: np.ndarray, held: np.ndarray, counts: np.ndarray,
            money: np.ndarray) -> dict[str, np.ndarray]:
    """The kernel's outputs, copied back to the host, as `run_batch` returns them.

    Returns:
        ``value`` (T x C), ``cash`` (C), ``positions`` (C x N), ``fees`` (C), ``fills`` (C),
        ``no_ops`` (C) and ``codes`` (C x N_CODES, counted by `_core` rejection code).
    """
    counts = np.asarray(counts, dtype=np.int64)
    money = np.asarray(money, dtype=np.float64)
    return {
        "value": np.asarray(values, dtype=np.float64),
        "cash": money[:, 0],
        "positions": np.asarray(held, dtype=np.float64).T,
        "fees": money[:, 1],
        "fills": counts[:, N_CODES],
        "no_ops": counts[:, N_CODES + 1],
        "codes": counts[:, :N_CODES],
    }
