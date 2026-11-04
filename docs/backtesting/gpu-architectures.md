# GPUs and architectures

A sweep runs on Apple, NVIDIA and AMD GPUs, and on any CPU. The same strategy file gives the same
ranking on each one. This page covers which backend runs where, what was checked on real
hardware, and how fast each machine was. It was measured in November 2026 with ziplime 2.12.0.

## What runs where

A sweep reaches the GPU in one of two ways, depending on the strategy:

* **`handle_data` strategies.** The strategy is converted to array operations and runs on an
  array library: MLX, CuPy or dpnp.
* **entries/exits strategies.** `compute_signals` returns `entries` and `exits`, and the sweep
  runs ziplime's vector kernel. One kernel source is compiled for Metal, CUDA, HIP and OpenCL.

| GPU | `handle_data` strategies | entries/exits strategies | Checked on |
|---|---|---|---|
| Apple silicon | `mlx` | `mlx`, `opencl` | Apple M3 |
| NVIDIA | `cupy`, `mlx` (`mlx[cuda]`, Linux) | `cupy`, `opencl` | RTX 5090 (Blackwell, sm_120), RTX 3090 (Ampere, sm_86); earlier RTX 4090 (Ada) |
| AMD | `cupy` (CuPy built for ROCm, Linux) | `cupy`, `opencl` | Instinct MI300X (CDNA3, gfx942) |
| Intel Arc | `dpnp` | `opencl` (also under `dpnp`) | not yet on an Arc GPU (see below) |
| no GPU | `cpu`, `exact` | `exact` | every machine |

Every GPU computes in float32. The best combinations are then recomputed in float64 on the CPU,
and the best one is run through `run_simulation`, so the result you report never comes from the
GPU's arithmetic.

## The results agree

Each machine ran the same three sweeps of `handle_data` strategies, all on the same cached daily
bars: 12 US ETFs, 2,723 sessions from 2016 to 2026.

* **momentum:** `momentum_brake.py`, 1,080 combinations;
* **rotation:** `sector_rotation.py`, 360 combinations;
* **rotation + synthetic:** the same, plus 6 synthetic paths (block bootstrap and GARCH).

Every run was compared, combination by combination, with the run on the Apple M3:

| Machine | Backend | Same number of fills | Same best combination | Best one against `run_simulation` |
|---|---|---|---|---|
| RTX 5090 | `cupy` | 100% in all three | yes | trades agree, $0.00 gap |
| RTX 5090 | `mlx` | 100% / 99.4% / 99.4% | yes | trades agree, $0.00 gap |
| RTX 3090 | `cupy` | 100% in all three | yes | trades agree, $0.00 gap |
| RTX 3090 | `mlx` | 100% / 99.4% / 99.4% | yes | trades agree, $0.00 gap |
| MI300X | `cupy` | 100% / 99.2% / 99.2% | yes | trades agree, $0.00 gap |

Where the counts differ, two or three combinations out of 360 are involved. In each one an order's
cost lands within float32 rounding of the cash left, so the order fits on one GPU and not on the
other. Sharpe ratios differ by less than 2·10⁻⁴. The synthetic-path sweep chose the same
combination on every machine. The test suites passed on each GPU: the kernel's parity tests (every
fill, rejection and no-op against float64) and the sweep's GPU tests. The RTX 5090 also ran the
whole sweep suite (87 passed).

## Speed

### `handle_data` strategies

GPU time of the sweep in seconds. This is the "simulation" stage, with the synthetic paths
separate:

| Machine | Backend | momentum (1,080) | rotation (360) | 6 synthetic paths |
|---|---|---:|---:|---:|
| Apple M3, MacBook Air | `mlx` | **1.3** | **1.0** | 6.0 |
| RTX 5090 | `mlx`, kernels compiled | — | 1.2 | **3.5** |
| RTX 5090 | `mlx`, first run of a strategy | 24.9 | 14.4 | — |
| RTX 5090 | `cupy` | 3.5 | 2.9 | 16.1 |
| RTX 3090 | `mlx`, kernels compiled | — | 4.2 | 32.2 |
| RTX 3090 | `mlx`, first run of a strategy | 70.3 | 37.5 | — |
| RTX 3090 | `cupy` | 11.2 | 10.0 | 54.3 |
| MI300X | `cupy` | 8.3 | 7.2 | 36.1 |

End to end, a sweep also spends about 7–8 s on the CPU checking its best combination with
`run_simulation`. That makes a whole run 11–14 s on the M3 and the RTX 5090.

What these numbers say:

* **These grids are small for a GPU.** 360–1,080 combinations of 12 instruments are 4,000–13,000
  numbers per bar. At that size the time goes to launching operations, not to computing them, so
  a GPU with more cores does not help.
* **The CPU matters as much as the GPU.** CuPy launches one kernel per operation from Python. The
  RTX 3090 pod had a slow server CPU (EPYC 7H12), which is most of why it was three times slower
  than the RTX 5090 pod.
* **MLX on NVIDIA compiles each strategy on its first run.** That takes seconds to minutes, and
  MLX then keeps the kernels. Once they are compiled, `mlx` was the fastest backend on NVIDIA:
  4.6 times faster than `cupy` on the synthetic paths of the RTX 5090. `cupy` needs no warm-up.
* **Apple silicon was fastest on these grids.** It has unified memory and no copies between CPU
  and GPU, and MLX compiles each bar into a few kernels.

These grids do not show where a large GPU pulls ahead. That takes hundreds of thousands of
combinations or many instruments. See [Sweep performance](sweep-performance.md) for measurements
at that scale.

### entries/exits strategies (the vector kernel)

`examples/vectorized/sweep_on_gpu.py`: 2,508 moving-average pairs, 50 instruments, 1,000
sessions, all in one kernel launch:

| Machine | Backend | 2,508 combinations | Fills against the float64 kernel |
|---|---|---:|---|
| MI300X | `cupy` (ROCm) | **0.08 s** | identical |
| Apple M3 | `mlx` | 0.13 s | identical |
| RTX 5090 | `cupy` | 0.14 s | identical |
| Apple M3 | `opencl` | 0.36 s | identical |
| RTX 3090 | `cupy` | 0.39 s | identical |
| any CPU core | `exact` | about 52 s | (the reference) |

Here each combination is a GPU thread looping over every bar, so the work grows with the GPU and
not with the number of operations. The MI300X, the largest GPU here, was the fastest.

## Not checked yet

* **Intel Arc.** No cloud offered one. The `dpnp` backend passed the sweep's GPU tests on dpnp
  0.21 against Intel's OpenCL CPU device (x86 Linux in Docker). That checks the numbers, but not
  the speed or the GPU driver.
* **AMD Radeon.** Only the MI300X was checked. ROCm supports some Radeon models, and on those CuPy
  is expected to work the same way.
* **Volta (V100) and Turing GPUs.** None were available at the time.
* **The `opencl` backend on NVIDIA.** It ran on Apple and on the MI300X.

## Running the same check

The software was Python 3.12, numpy 2.5 and pandas 2.3 on every machine. The GPU libraries were
MLX 0.32.4 (0.31.2 on the Mac) and CuPy 14.2.0, for CUDA 12 on NVIDIA driver 595.91 and built
against ROCm 6.4.4 on the MI300X. [Sweep performance](sweep-performance.md) has the setup steps:
a RunPod pod for NVIDIA, the ROCm build of CuPy for AMD, and dpnp for Intel. On any of them, run
the checks:

```bash
ziplime sweep doctor                     # which backends this machine has, and on which GPU
python -m pytest tests/vectorized/test_vector_kernel_batch.py -rs
ZIPLIME_SWEEP_TEST_GPU=cupy python -m pytest tests/sweep -q    # or mlx, dpnp
python -m examples.vectorized.sweep_on_gpu
```
