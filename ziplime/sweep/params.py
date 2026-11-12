"""The sweep file: what to run, what to vary, and how to compute it.

Field names follow :func:`ziplime.core.run_simulation.run_simulation` wherever the two mean the
same thing, so a sweep file reads like the arguments of the run it stands for::

    {
      "algorithm_file": "strategies/moex_trend.py",
      "bundle": "grpc_daily_data",
      "start_date": "2025-04-07", "end_date": "2025-12-30",
      "trading_calendar": "XMOS",
      "total_cash": 1000000,
      "grid": {"fast": {"from": 2, "to": 20}, "slow": [20, 40, 60]},
      "where": "fast < slow",
      "backend": "mlx"
    }

Each key of ``grid`` is a field of the algorithm's ``AlgorithmConfig`` -- what a ``config_file``
would set for a single run -- or, for a nested field, a short name with an explicit ``path``.
"""
import copy
import datetime
import itertools
import math
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.commission.per_contract import PerContract
from ziplime.finance.commission.per_dolar import PerDollar
from ziplime.finance.commission.per_share import PerShare
from ziplime.finance.commission.per_trade import PerTrade
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
from ziplime.finance.slippage.fixed_slippage import FixedSlippage
from ziplime.finance.slippage.no_slippage import NoSlippage

#: Where bundles and the asset database live unless the sweep file says otherwise -- the same
#: defaults as the ``ziplime`` command line.
DEFAULT_STORAGE = Path(Path.home(), ".ziplime", "data")
DEFAULT_ASSET_DB = Path(Path.home(), ".ziplime", "assets.sqlite")


class SweepError(Exception):
    """The sweep cannot run as asked. The message says why and what to change."""


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Range(Strict):
    """``{"from": 5, "to": 50, "step": 5}`` -- 5, 10, ..., 50, ``to`` included."""

    start: float = Field(alias="from")
    stop: float = Field(alias="to")
    step: float = 1

    @model_validator(mode="after")
    def check(self):
        if self.step <= 0:
            raise ValueError("step has to be positive")
        if self.stop < self.start:
            raise ValueError("to has to be at least from")
        return self

    def values(self) -> list:
        count = int(math.floor((self.stop - self.start) / self.step + 1e-9)) + 1
        values = [self.start + k * self.step for k in range(count)]
        if all(float(v).is_integer() for v in (self.start, self.stop, self.step)):
            return [int(round(v)) for v in values]
        return [round(v, 10) for v in values]


class Commission(Strict):
    type: Literal["none", "per_share", "per_trade", "per_dollar", "per_contract"] = "none"
    cost: float = 0.0
    min_trade_cost: float = 0.0
    #: per_contract only: a flat exchange fee per order
    exchange_fee: float = 0.0

    def model(self):
        return {
            "none": lambda: NoCommission(),
            "per_share": lambda: PerShare(cost=self.cost, min_trade_cost=self.min_trade_cost),
            "per_trade": lambda: PerTrade(cost=self.cost),
            "per_dollar": lambda: PerDollar(cost=self.cost),
            "per_contract": lambda: PerContract(cost=self.cost, exchange_fee=self.exchange_fee,
                                                min_trade_cost=self.min_trade_cost),
        }[self.type]()

    @classmethod
    def of(cls, model) -> "Commission":
        """The sweep's description of a ziplime commission model (set_commission in initialize)."""
        if model is None or isinstance(model, NoCommission):
            return cls()
        if isinstance(model, PerShare):
            return cls(type="per_share", cost=model.cost_per_share, min_trade_cost=model.min_trade_cost)
        if isinstance(model, PerDollar):
            return cls(type="per_dollar", cost=model.cost_per_dollar)
        if isinstance(model, PerTrade):
            return cls(type="per_trade", cost=model.cost)
        raise SweepError(f"Commission model {type(model).__name__} is not supported in a sweep.")


class Slippage(Strict):
    type: Literal["none", "fixed_bps", "fixed_spread"] = "none"
    basis_points: float = 0.0
    spread: float = 0.0
    #: Share of a bar's volume an order may take (``fixed_bps`` only).
    volume_limit: float = 0.1

    def model(self):
        if self.type == "fixed_bps":
            return FixedBasisPointsSlippage(basis_points=self.basis_points,
                                            volume_limit=self.volume_limit)
        if self.type == "fixed_spread":
            return FixedSlippage(spread=self.spread)
        return NoSlippage()

    @classmethod
    def of(cls, model) -> "Slippage":
        """The sweep's description of a ziplime slippage model (set_slippage in initialize)."""
        if model is None or isinstance(model, NoSlippage):
            return cls()
        if isinstance(model, FixedBasisPointsSlippage):
            return cls(type="fixed_bps", basis_points=model.percentage * 10_000,
                       volume_limit=model.volume_limit)
        if isinstance(model, FixedSlippage):
            return cls(type="fixed_spread", spread=model.spread)
        raise SweepError(f"Slippage model {type(model).__name__} is not supported in a sweep.")


class ConfigPath(Strict):
    """A parameter deeper than the config's first level::

        "target": {"path": "equities_to_trade.0.target_percentage", "values": [10, 20]}

    The key (``target``) is how the parameter is named in ``where`` and in the results."""

    path: str
    values: list[bool | int | float | str] | Range


GridValue = list[bool | int | float | str] | Range | ConfigPath | bool | int | float | str

#: How a family of synthetic paths is made from the real bars; see ``ziplime/sweep/synthetic.py``.
Method = Literal["iid", "shuffle", "block", "circular", "stationary", "meboot", "gbm", "student_t",
                 "garch"]


class Family(Strict):
    """One family of synthetic paths: ``{"method": "stationary", "block": 20}``."""

    method: Method
    #: Shown in the report and the files; null -- the method and its settings
    name: str | None = None
    #: ``block``, ``circular``: bars per block; ``stationary``: the mean block length
    block: int = 20
    #: ``historical`` -- the paths drift as the real bars did; ``zero`` -- each instrument's mean
    #: log return is taken out, so a strategy can only earn from the bars' shape, not the trend
    drift: Literal["historical", "zero"] = "historical"
    #: ``student_t``: degrees of freedom of the shocks (more than 2)
    df: float = 4.0
    #: ``meboot``: trimming of the mean absolute change that sets the tails; widen each
    #: replicate whose spread came out narrower than the data's (R meboot's expand.sd)
    trim: float = 0.10
    expand_sd: bool = True

    @model_validator(mode="after")
    def check(self):
        if self.block < 1:
            raise ValueError("block has to be at least 1")
        if self.df <= 2:
            raise ValueError("df has to be more than 2")
        if not 0 <= self.trim < 0.5:
            raise ValueError("trim has to be in [0, 0.5)")
        return self

    @property
    def label(self) -> str:
        if self.name:
            return self.name
        settings = {"block": f"({self.block})", "circular": f"({self.block})",
                    "stationary": f"({self.block})", "student_t": f"(df={self.df:g})"}
        return self.method + settings.get(self.method, "") + \
            (", zero drift" if self.drift == "zero" else "")


class Synthetic(Strict):
    """The sweep run again on synthetic price paths made from the real bars::

        "synthetic": {"paths": 200, "families": [{"method": "stationary", "block": 20},
                                                 {"method": "meboot"}, {"method": "garch"}]}

    Every family's paths start from the real bars at ``start_date`` (the warm-up before it stays
    real); every combination runs on every path."""

    paths: int = 100
    seed: int = 0
    families: list[Family] = [Family(method="stationary")]
    #: The bars a path is resampled or fitted from: ``history`` -- everything loaded, warm-up
    #: included; ``run`` -- from ``start_date`` on
    source: Literal["history", "run"] = "history"
    #: How a combination's paths are summed up into one number to choose by: ``median``,
    #: ``mean``, ``min``, or a percentile ``p5``, ``p10``, ``p25``...
    select_by: str = "median"

    @model_validator(mode="after")
    def check(self):
        if self.paths < 1:
            raise ValueError("paths has to be at least 1")
        if not self.families:
            raise ValueError("families is empty")
        labels = [family.label for family in self.families]
        if len(set(labels)) != len(labels):
            raise ValueError(f"two families share a name: {labels}; give them names")
        statistic = self.select_by
        if statistic not in ("median", "mean", "min") and not (
                statistic.startswith("p") and statistic[1:].replace(".", "", 1).isdigit()
                and 0 <= float(statistic[1:]) <= 100):
            raise ValueError("select_by is median, mean, min or a percentile such as p10")
        return self


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
    #: for strategies whose decisions are entries/exits signals only; ``cpu`` -- numpy in float64 across ``workers`` processes;
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


def config_paths(grid: dict) -> dict[str, str]:
    """``{grid name: path in the config}``."""
    return {name: spec.path if isinstance(spec, ConfigPath) else name
            for name, spec in grid.items()}


def config_for(shared: dict, combo: dict, paths: dict[str, str]) -> dict:
    """One combination's config: the shared part with the combination's values written in."""
    data = copy.deepcopy(shared)
    for name, value in combo.items():
        *parents, last = paths[name].split(".")
        node = data
        try:
            for part in parents:
                node = node[int(part)] if isinstance(node, list) else node[part]
            if isinstance(node, list):
                node[int(last)] = value
            else:
                node[last] = value
        except (KeyError, IndexError, ValueError, TypeError):
            raise SweepError(
                f'Path "{paths[name]}" (parameter "{name}") is not in the config. Nested values '
                f"come from config_file -- check that it has them.") from None
    return data


def expand_grid(grid: dict, where: str | None) -> list[dict]:
    """Every combination of the grid's values that passes ``where``."""
    axes = {}
    for name, spec in grid.items():
        if isinstance(spec, ConfigPath):
            spec = spec.values
        values = spec.values() if isinstance(spec, Range) else spec
        axes[name] = values if isinstance(values, list) else [values]
        if not axes[name]:
            raise SweepError(f'Grid parameter "{name}" has no values.')
    combos = (dict(zip(axes, values)) for values in itertools.product(*axes.values()))
    if where:
        try:
            test = compile(where, "<where>", "eval")
            combos = [c for c in combos if eval(test, {"__builtins__": {}}, c)]
        except NameError as error:
            raise SweepError(f"Unknown name in where: {error}. The grid has: {list(grid)}") from None
    else:
        combos = list(combos)
    if not combos:
        raise SweepError("No combination passes the where condition.")
    return combos
