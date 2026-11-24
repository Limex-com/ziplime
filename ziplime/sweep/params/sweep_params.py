"""Sweep params for params."""

import datetime
from pathlib import Path
from typing import Any, Literal
from pydantic import Field
from ziplime.sweep.params.commission import Commission
from ziplime.sweep.params.support import GridValue
from ziplime.sweep.params.slippage import Slippage
from ziplime.sweep.params.strict import Strict
from ziplime.sweep.params.synthetic import Synthetic


class SweepParams(Strict):
    # What to run -- as in run_simulation
    algorithm_file: str
    config_file: str | None = None
    #: Overrides to ``config_file`` shared by every combination
    config: dict[str, Any] = {}
    start_date: datetime.date
    end_date: datetime.date = Field(
        default_factory=lambda: datetime.date.today() - datetime.timedelta(days=1))
    trading_calendar: str = "XNYS"
    #: Bar frequency, as run_simulation's emission_rate: "1d", or intraday "1m", "5m", "1h". An
    #: intraday run needs a bundle of the same frequency.
    emission_rate: str = "1d"
    total_cash: float = 100_000.0
    equity_commission: Commission = Commission()
    equity_slippage: Slippage = Slippage()
    #: For futures in the universe: commission (per_contract) and slippage
    future_commission: Commission = Commission()
    future_slippage: Slippage = Slippage()
    #: When an order placed on a bar is filled. ``same_close`` is run_simulation's
    #: ``same_bar_execution=True``: the decision and its fill share a close, which is look-ahead.
    timing: Literal["same_close", "next_close", "next_open"] = "next_close"
    #: Shares per entry for strategies that trade on ``entries``/``exits`` signals; otherwise the
    #: ``size`` field of AlgorithmConfig.
    size: float | None = None

    # What to vary
    grid: dict[str, GridValue]
    where: str | None = None

    # How to compute and what to show
    #: ``mlx`` -- Apple GPU (and NVIDIA through mlx[cuda]); ``cupy`` -- NVIDIA GPU;
    #: ``dpnp`` -- Intel GPU (Arc) through SYCL; ``opencl`` -- any OpenCL GPU (Intel Arc, AMD),
    #: for strategies whose decisions are entries/exits signals only;
    #: ``cpu`` -- numpy in float64 across ``workers`` processes;
    #: ``exact`` -- the same in one process.
    backend: Literal["mlx", "cupy", "dpnp", "opencl", "cpu", "exact"] = "mlx"
    #: Processes for ``backend: cpu`` and for engine runs; null -- one per core.
    workers: int | None = None
    #: A strategy the sweep cannot convert: ``engine`` -- run every combination through
    #: run_simulation on the CPU's cores (slower, and still every combination); ``refuse`` -- stop
    #: and say why.
    on_unsupported: Literal["engine", "refuse"] = "engine"
    #: Compile each bar into a few GPU kernels (MLX only); null -- yes. The first run on a machine
    #: compiles for a few seconds; MLX keeps the kernels for the runs after it.
    compile: bool | None = None
    sort_by: Literal["sharpe", "total_return", "final_value", "max_drawdown"] = "sharpe"
    top: int = 10
    #: Combinations with fewer fills stay in the CSV but out of the ranking.
    min_fills: int = 0
    #: How many of the best GPU (float32) results to recompute exactly (float64) before the final
    #: ranking. 0 -- none.
    refine_top: int = 50
    #: Combinations per GPU call; ``auto`` sizes it from the number of instruments.
    batch_size: int | Literal["auto"] = "auto"
    periods_per_year: int = 252
    #: Run the best combination through run_simulation and compare the trades.
    verify: bool = True
    #: Also run every combination on synthetic price paths and compare the choices made on them
    #: with the real data's; see docs/backtesting/synthetic-data.md
    synthetic: Synthetic | None = None
    #: Asset database; null -- ``~/.ziplime/assets.sqlite``, as for the command line.
    asset_db: str | None = None
    #: Bundle to read bars from -- the market_data_source run_simulation would get. null --
    #: download daily bars from Yahoo.
    bundle: str | None = None
    #: Bundle version; null -- the latest.
    bundle_version: str | None = None
    #: Where bundles are stored; null -- ``~/.ziplime/data``.
    bundle_storage: str | None = None
    #: Where to cache bars downloaded from Yahoo; null -- download every time.
    cache_dir: str | None = "data_cache"
    output_dir: str = "results"

    def resolve(self, base: Path, name: str | None) -> Path | None:
        """A path from the sweep file, relative to the file's own folder."""
        if name is None:
            return None
        path = Path(name).expanduser()
        return path if path.is_absolute() else base / path
