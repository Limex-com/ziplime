# Sweep performance

What a sweep costs in time, which backend to use, and how to run one on a rented NVIDIA GPU. Every
number on this page was measured; the hardware and the data are stated with it. How the sweep
works is on [How a sweep works](sweep-internals.md). Which GPUs a sweep runs on, and how they compare
on the same sweeps, is on [GPUs and architectures](gpu-architectures.md).

## What the time goes to

A sweep prints where its time went (`Stages:`). Four parts matter:

| Part | Runs on | Grows with |
|---|---|---|
| preparation: data, `initialize`, indicators | CPU | instruments × bars; distinct configs `initialize` reads |
| computations on history (`.to_numpy()`, TA-Lib, models) | CPU | distinct parameter values × bars × instruments — **not** combinations |
| the bar loop | GPU (or CPU) | combinations × instruments × bars |
| refine, recorded best run, verification | CPU | `refine_top`; one run_simulation |

The GPU only speeds up the bar loop. A sweep of a few hundred combinations is dominated by the
rest, and the CPU backend can be faster; the GPU wins from tens of thousands of combinations up.

## Measured

### Apple M3 (8 cores), MLX

| Strategy and data | Combinations | Time |
|---|---:|---:|
| rotation across 12 ETFs, 2,723 sessions | 128,520 | 20–25 s (simulation) |
| trend on 4 MOEX shares, 188 sessions | 998,400 | 27 s end to end, 1.6 GB |
| pair switched by TA-Lib ROC on history arrays, 2 instruments, 1,363 sessions | 24,900 | **1.7 s** end to end |
| the same with TA-Lib RSI (replayed bar by bar) | 24,900 | 3.2 s end to end |
| SMA crossover, 1 instrument, 1,363 sessions | 399 | 3.7 s (CPU `exact`: 2.7 s) |
| intraday, 2 instruments, 23,000 one-minute bars | 300 | 14 s end to end |

How the TA-Lib pair got from 64 s to 1.7 s, all on the M3 with the same 24,900 combinations:

| | Time |
|---|---:|
| CPU, `exact` (float64, one process) | 64 s |
| MLX, bars not compiled | 32 s |
| MLX, bars compiled | 5.6 s |
| MLX, compiled, ROC computed once on the whole history | **1.7 s** |

The TA-Lib part alone went from 3.07 s (replayed at every bar) to 0.02 s (once on the whole
history). Replayed computations on the CPU's cores: 0.7 million evaluations took 1.8 s in one
process and 3.4 s on 8 cores (starting the processes costs about 2.5 s); 3 million took 8.0 s and
4.6 s. Below two million the sweep stays in one process.

### NVIDIA RTX 4090 (RunPod, 96 vCPU)

| Strategy and data | Backend | Combinations | Time |
|---|---|---:|---:|
| rotation across 12 ETFs, 2,723 sessions | `mlx` (`mlx[cuda]`) | 128,520 | 10 s (simulation) |
| the same | `cupy` | 128,520 | 11 s (simulation) |
| TA-Lib ROC pair, 1,363 sessions | `cupy` | 24,900 | 60.9 s end to end |
| the same | `mlx`, first run on the machine | 24,900 | 23.0 s end to end |
| the same | `mlx`, kernels already compiled | 24,900 | **4.9 s** end to end (1.6 s simulation) |

* The first MLX run of a strategy on a machine compiles its kernels — here about 18 s — and MLX
  keeps them for the runs after it.
* CuPy runs every operation as its own kernel; on a small array (24,900 × 2) that launch cost is
  the whole time. MLX joins a bar's operations, and 16 bars, into one call.
* On this problem the M3 (1.7 s) beat the 4090 (4.9 s): 24,900 combinations of 2 instruments are
  50,000 numbers a bar, too few to keep a large GPU busy, and the CPU parts run faster on the M3.
  A GPU's size pays from hundreds of thousands of combinations or many instruments.

### One CPU core, `exact`

Rotation across 12 ETFs, 2,723 sessions: 8,568 combinations in 11.9 s.

## Choosing a backend

| Backend | When |
|---|---|
| `mlx` | Apple silicon; NVIDIA on Linux with `mlx[cuda]`. Compiles each bar; the fastest wherever it runs |
| `cupy` | NVIDIA when MLX's CUDA backend is not available; slower than `mlx` on small arrays. AMD on Linux with CuPy built for ROCm (see below) |
| `dpnp` | Intel Arc: `handle_data` on dpnp, entries/exits through PyOpenCL (see below) |
| `opencl` | Intel Arc, AMD — any GPU with an OpenCL driver (`ziplime[opencl]`). Strategies that return entries/exits from `compute_signals` only; one swept through `handle_data` is refused |
| `cpu` | no GPU: NumPy in float64 across `workers` processes |
| `exact` | one process, float64 — small grids, `ziplime sweep check`, reference numbers |

`ziplime sweep doctor` says what this machine has. Other knobs:

| Setting | |
|---|---|
| `"compile": false` / `--no-compile` | do not compile the step (MLX) — only to look into a problem |
| `"batch_size"` | combinations per GPU call; `auto` sizes it to about 500,000 numbers (2,000,000 on CuPy) |
| `"refine_top"` | how many of the best are recomputed in float64; 0 to skip |
| `ZIPLIME_SWEEP_REPLAY_WORKERS` | processes for computations on history; 1 to keep them in one |
| `ZIPLIME_SWEEP_BARS_PER_CALL` | bars per compiled call (16) |
| `ZIPLIME_SWEEP_CACHE_LIMIT` | bytes MLX keeps cached on CUDA (2 GB) |

## A sweep on a rented NVIDIA GPU (RunPod)

How the measurements above were taken; the same works on any Linux machine with an NVIDIA GPU.

**1. A pod.** Image `runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04`, one GPU, 30 GB
container disk, port `22/tcp` with a public IP, and your SSH public key in the `PUBLIC_KEY`
environment variable. Through the REST API (`POST https://rest.runpod.io/v1/pods`); "community
cloud" is cheaper but often has none free, "secure cloud" usually does. SSH answered 20–30 s after
the request.

**2. Code and environment** (about a minute):

```bash
pip install uv
uv venv -p 3.12 /root/venv && . /root/venv/bin/activate
uv pip install "./ziplime[cuda]" "mlx[cuda]" "nvidia-cuda-runtime-cu12>=12.8" \
    "pandas==2.3.*" ta-lib scikit-learn        # ./ziplime: a checkout copied to the pod;
                                               # ta-lib, scikit-learn: if the strategy uses them
# MLX on CUDA needs CUDA headers 12.8 or later: point it at the wheel's, not the image's 12.4
mv /usr/local/cuda /usr/local/cuda-12.4-hidden
export CUDA_HOME=$(python -c "import nvidia.cuda_runtime as m; print(m.__path__[0])")
export CUDA_PATH=$CUDA_HOME PYTHONUTF8=1 LC_ALL=C.UTF-8
ziplime sweep doctor
```

ziplime needs pandas 2.x — pandas 3 changes the results. TA-Lib 0.6.4 has no Linux wheel; a later
version has (`ta-lib` 0.8.3 was used, with the same results).

**3. Check both GPU paths** before measuring anything. A sweep reaches the GPU two ways: a strategy
swept through its `handle_data` runs on `mlx` or `cupy` as array operations; a strategy that
returns entries/exits runs `simulate_signals_batch`, a kernel of its own — Metal for `mlx`, CUDA for
`cupy`. Each needs its own check:

```bash
cd ziplime
python -m pytest tests/vectorized/test_vector_kernel_batch.py -rs    # CudaAgreesWithExactTests:
                                                                     # passed, not skipped
ZIPLIME_SWEEP_TEST_GPU=cupy python -m pytest tests/sweep -q
ZIPLIME_SWEEP_TEST_GPU=mlx python -m pytest tests/sweep -q           # with mlx[cuda]
python -m examples.vectorized.sweep_on_gpu                           # prints "GPU (cupy)"
```

With `mlx[cuda]` the entries/exits test expects a refusal that points at `cupy` — MLX on CUDA has no
Metal for the kernel — and passes on it.

**4. Run** with `ziplime sweep run sweep.json` (`--backend mlx`). The first run on a fresh machine is
slow: MLX compiles its kernels and Python its bytecode — the sweep's own test suite took 19 minutes
cold on the 4090 and its GPU tests 16 s warm.

**5. Delete the pod** when done (`DELETE https://rest.runpod.io/v1/pods/{id}`) and check that none is
left (`GET https://rest.runpod.io/v1/pods` returns `[]`). Measured cost: 31 minutes on an RTX 4090 in secure cloud — setting up, the whole
test suite on two backends and the benchmarks — $0.46.

### Which GPU

RunPod's prices per hour on 2026-11-01:

| GPU | Memory | Memory bandwidth (spec) | Secure | Community |
|---|---:|---:|---:|---:|
| RTX 2000 Ada | 16 GB | 224 GB/s | $0.24 | $0.50 |
| RTX A4000 | 16 GB | 448 GB/s | $0.25 | $0.17 |
| RTX A4500 | 20 GB | 640 GB/s | $0.25 | $0.19 |
| RTX A5000 | 24 GB | 768 GB/s | $0.27 | $0.16 |
| RTX 4000 Ada | 20 GB | 360 GB/s | $0.28 | $0.20 |
| RTX 4090 | 24 GB | 1,008 GB/s | $0.89 | $0.34 |

Only the RTX 4090 was measured. What follows is an expectation, not a measurement: a sweep's
step is many small elementwise operations, so on small and medium grids the GPU waits on kernel
launches and on the CPU, and a cheaper card should be nearly as fast; on large grids memory
bandwidth decides, which puts an A5000 at roughly 1.3× and an A4000 at 2.2× the 4090's time for
under a third of its price. 16 GB is enough: MLX on CUDA keeps at most 2 GB cached, and a batch is
about 500,000 numbers. Prefer a pod with many CPU cores — computations on history, refine and
verification run on them.

## A sweep on an Intel GPU (Arc)

`backend: dpnp` runs a strategy swept through `handle_data` on dpnp, Intel's NumPy for SYCL; a
strategy that returns entries/exits runs the vector kernel through PyOpenCL on the same GPU. Both in
float32 — Arc GPUs have no double precision. Linux or Windows; dpnp comes from Intel's own index:

```bash
pip install --index-url https://software.repos.intel.com/python/pypi \
    --extra-index-url https://pypi.org/simple dpnp
pip install "./ziplime[opencl]"               # PyOpenCL, for entries/exits strategies
sudo apt install intel-opencl-icd clinfo      # Linux: the GPU's OpenCL driver; clinfo lists it
ziplime sweep doctor                          # backend dpnp: "... on Intel(R) Arc(TM) B580 (gpu ...)"
```

dpnp picks the GPU by itself; `ONEAPI_DEVICE_SELECTOR=level_zero:0` names one. Checks:

```bash
cd ziplime
python -m pytest tests/vectorized/test_vector_kernel_batch.py -rs    # OpenClAgreesWithExactTests
ZIPLIME_SWEEP_TEST_GPU=dpnp python -m pytest tests/sweep -q
python -m examples.vectorized.sweep_on_gpu                           # prints "GPU (opencl)"
```

The sweep's GPU tests have run on dpnp 0.21 against Intel's OpenCL CPU device (x86 Linux in
Docker), which checks that the numbers are right; no Arc GPU has run them yet, so nothing here
says how fast it is.

## A sweep on an AMD GPU (ROCm)

Checked on an AMD Instinct MI300X (RunPod, image `rocm/dev-ubuntu-24.04:6.4.4-complete`): CuPy
14.2.0 built from source in 6 minutes with the commands below. The kernel's parity tests and the
sweep's GPU tests passed, and the `handle_data` sweeps agreed with the other GPUs — see
[GPUs and architectures](gpu-architectures.md). No Radeon card has run it yet.

CuPy built for ROCm is the same `cupy` backend: the `handle_data` path runs on its arrays, and the
entries/exits kernel compiles through HIP, which reads the CUDA source as it is (in blocks of 64
threads, a whole wavefront). Linux only, and on the GPUs ROCm supports — not every Radeon.

Prebuilt CuPy wheels for ROCm on PyPI stop at ROCm 5.0, so build CuPy against the installed ROCm,
and install ziplime without the `cuda` extra, which would bring CuPy for CUDA:

```bash
rocminfo | grep -m1 -o "gfx[0-9a-f]*"        # the GPU's architecture, e.g. gfx1100
export CUPY_INSTALL_USE_HIP=1 ROCM_HOME=/opt/rocm HCC_AMDGPU_TARGET=gfx1100
pip install cupy                              # compiles; takes a while
pip install ./ziplime
ziplime sweep doctor                          # backend cupy: "... (ROCm) on <the GPU>"
```

Then the same checks as on NVIDIA:

```bash
cd ziplime
python -m pytest tests/vectorized/test_vector_kernel_batch.py -rs    # CudaAgreesWithExactTests:
                                                                     # passed, not skipped
ZIPLIME_SWEEP_TEST_GPU=cupy python -m pytest tests/sweep -q
python -m examples.vectorized.sweep_on_gpu                           # prints "GPU (cupy)"
```

Where CuPy for ROCm will not build or the GPU is not supported, `backend: opencl` still runs the
entries/exits kernel through AMD's OpenCL driver — on Windows as well — and a strategy swept
through `handle_data` runs on `cpu`.
