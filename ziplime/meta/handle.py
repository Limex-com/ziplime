"""What a metastrategy holds for each strategy it runs, and for a cluster of them.

    context.trend = await context.strategy("trend")
    context.trend.portfolio.cash              # the strategy's own book, as of this bar
    context.trend.performance.sharpe          # since the start
    context.trend.performance.last(60).sharpe # over the last 60 sessions
    context.trend.nav                         # 1.0 at the start, 1 + its return since
    await context.order_target_percent(context.trend, 0.5, style=MarketOrder())

    context.bb = await context.strategy("bollinger", grid={"bb_period": [10, 20, 30],
                                                           "bb_std": [1.5, 2.0, 2.5]})
    best = context.bb.best("sharpe", window=60)
    await context.rebalance({best: 1.0})
"""
from collections.abc import Callable, Iterator, Sequence
from typing import Any

import empyrical
import numpy as np
import pandas as pd

from ziplime.assets.entities.exchange_asset import ExchangeAsset

#: Metrics where smaller is better; every other one is ranked largest first.
_SMALLER_IS_BETTER = {"volatility"}


def _finite(value) -> float:
    value = float(value)
    return value if np.isfinite(value) else float("nan")


class Performance:
    """A strategy's performance as of the current bar, from its own ledger.

    Computed the way the run's report computes it (``empyrical`` over daily returns, today's
    partial return included), so ``performance.sharpe`` on the last bar is the ``sharpe`` the
    strategy's result reports. Every statistic is ``nan`` until there are returns enough to have
    one -- and ``nan > x`` is False, so a comparison simply does not fire yet.
    """

    def __init__(self, algorithm, window: int | None = None) -> None:
        self._algorithm = algorithm
        self._window = window

    def last(self, sessions: int) -> "Performance":
        """The same statistics over the last ``sessions`` sessions only."""
        if sessions < 1:
            raise ValueError("last() needs at least one session.")
        return Performance(self._algorithm, window=sessions)

    @property
    def daily_returns(self) -> np.ndarray:
        algorithm = self._algorithm
        ledger = algorithm._ledger
        done = np.asarray(ledger.daily_returns_series.values[:algorithm._session_count], dtype=float)
        if algorithm._in_session:
            done = np.append(done, ledger.todays_returns)
        return done[-self._window:] if self._window else done

    @property
    def days(self) -> int:
        return len(self.daily_returns)

    @property
    def returns(self) -> float:
        """Total return: since the start, or compounded over the window."""
        if self._window is None:
            return float(self._algorithm.portfolio.returns)
        return _finite(np.prod(1.0 + self.daily_returns) - 1.0) if self.days else 0.0

    @property
    def sharpe(self) -> float:
        return _finite(empyrical.sharpe_ratio(self.daily_returns))

    @property
    def sortino(self) -> float:
        return _finite(empyrical.sortino_ratio(self.daily_returns))

    @property
    def volatility(self) -> float:
        return _finite(empyrical.annual_volatility(self.daily_returns))

    @property
    def max_drawdown(self) -> float:
        """Negative, as in the run's report: -0.12 is a 12% drawdown."""
        returns = self.daily_returns
        return _finite(empyrical.max_drawdown(returns)) if len(returns) else 0.0

    @property
    def annual_return(self) -> float:
        return _finite(empyrical.annual_return(self.daily_returns))

    @property
    def calmar(self) -> float:
        return _finite(empyrical.calmar_ratio(self.daily_returns))

    @property
    def pnl(self) -> float:
        return float(self._algorithm.portfolio.pnl)

    @property
    def portfolio_value(self) -> float:
        return float(self._algorithm.portfolio.portfolio_value)

    def metric(self, name: str) -> float:
        if name.startswith("_") or not isinstance(getattr(type(self), name, None), property):
            raise ValueError(f"Unknown performance metric {name!r}. Known: {', '.join(METRICS)}.")
        return getattr(self, name)

    def as_dict(self) -> dict[str, float]:
        return {name: self.metric(name) for name in METRICS}

    def __repr__(self) -> str:
        window = f", last {self._window}" if self._window else ""
        return (f"Performance(days={self.days}{window}, returns={self.returns:.4f}, "
                f"sharpe={self.sharpe:.3f}, max_drawdown={self.max_drawdown:.4f})")


METRICS = ("returns", "sharpe", "sortino", "volatility", "max_drawdown", "annual_return", "calmar",
           "days")


class StrategyHandle:
    """A strategy run by a metastrategy: its state as of this bar, and the listing it trades as.

    Pass it wherever an order method takes an asset -- ``order``, ``order_value``,
    ``order_target_percent``, ``rebalance`` -- and the metastrategy buys or sells units of it at
    its NAV.
    """

    def __init__(self, *, name: str, label: str, algorithm, listing: ExchangeAsset, owner,
                 params: dict[str, Any], capital: float, commission=None) -> None:
        self.name = name
        #: Charged when the metastrategy trades units of this strategy; None -- free.
        self.commission = commission
        #: Unique within the metastrategy: ``name``, or ``name[bb_period=20, bb_std=2.0]``.
        self.label = label
        #: The combination of grid values this strategy runs with; empty outside a cluster.
        self.params = dict(params)
        self.capital = capital
        #: The strategy's own ``context``: whatever its ``initialize`` and ``handle_data`` set.
        self.context = algorithm
        self.asset = listing
        self._owner = owner
        self.packets: list = []
        self.errors: list = []
        #: The strategy's TradingAlgorithmExecutionResult, once the run has finished.
        self.result = None

    # -- the strategy's own book -----------------------------------------------------------------

    @property
    def config(self):
        return self.context.config

    @property
    def portfolio(self):
        return self.context.portfolio

    @property
    def account(self):
        return self.context.account

    @property
    def positions(self):
        return self.context.portfolio.positions

    @property
    def recorded_vars(self) -> dict:
        return self.context.recorded_vars

    @property
    def performance(self) -> Performance:
        return Performance(self.context)

    @property
    def nav(self) -> float:
        """The price of one unit: 1.0 at the start, ``1 + returns`` since."""
        return 1.0 + float(self.context.portfolio.returns)

    # -- what the metastrategy holds of it -------------------------------------------------------

    @property
    def units(self) -> int:
        position = self._owner.portfolio.positions.get(self.asset)
        return int(position.amount) if position is not None else 0

    @property
    def value(self) -> float:
        return self.units * self.nav

    @property
    def weight(self) -> float:
        """Share of the metastrategy's portfolio value held in this strategy."""
        total = self._owner.portfolio.portfolio_value
        return self.value / total if total else 0.0

    def __hash__(self) -> int:
        return hash(("strategy", self.label))

    def __eq__(self, other) -> bool:
        return other is self

    def __repr__(self) -> str:
        return f"<Strategy {self.label} nav={self.nav:.4f}>"


class StrategyCluster(Sequence):
    """Every combination of a parameter grid, each run as its own strategy, side by side.

    A sequence of :class:`StrategyHandle` -- index it, iterate it, ``len`` it -- with ranking
    on top. ``by`` is a :class:`Performance` metric name or a function of a handle.
    """

    def __init__(self, name: str, members: list[StrategyHandle], grid: dict) -> None:
        self.name = name
        self.grid = grid
        self._members = members

    def __getitem__(self, index):
        return self._members[index]

    def __len__(self) -> int:
        return len(self._members)

    def __iter__(self) -> Iterator[StrategyHandle]:
        return iter(self._members)

    def find(self, **params) -> StrategyHandle:
        """The member running exactly these values: ``cluster.find(bb_period=20, bb_std=2.0)``."""
        found = [member for member in self._members
                 if all(member.params.get(key) == value for key, value in params.items())]
        if len(found) != 1:
            raise KeyError(f"{len(found)} members of {self.name} have {params}.")
        return found[0]

    def ranked(self, by: str | Callable[[StrategyHandle], float] = "sharpe",
               window: int | None = None) -> list[StrategyHandle]:
        """Best first. Members whose metric is not known yet (``nan``) come last."""
        if callable(by):
            score, descending = by, True
        else:
            def score(member):
                performance = member.performance
                return (performance.last(window) if window else performance).metric(by)
            descending = by not in _SMALLER_IS_BETTER
        scored = [(score(member), position, member) for position, member in enumerate(self._members)]
        known = [item for item in scored if np.isfinite(item[0])]
        unknown = [item for item in scored if not np.isfinite(item[0])]
        known.sort(key=lambda item: (-item[0] if descending else item[0], item[1]))
        return [member for _, _, member in known + unknown]

    def best(self, by: str | Callable[[StrategyHandle], float] = "sharpe",
             window: int | None = None) -> StrategyHandle:
        return self.ranked(by, window)[0]

    def top(self, n: int, by: str | Callable[[StrategyHandle], float] = "sharpe",
            window: int | None = None) -> list[StrategyHandle]:
        return self.ranked(by, window)[:n]

    def to_frame(self, window: int | None = None) -> pd.DataFrame:
        """One row per member: its parameters, then every performance metric."""
        rows = []
        for member in self._members:
            performance = member.performance.last(window) if window else member.performance
            rows.append({"label": member.label, **member.params, **performance.as_dict(),
                         "nav": member.nav, "units": member.units})
        return pd.DataFrame(rows).set_index("label")

    def __repr__(self) -> str:
        return f"<StrategyCluster {self.name}: {len(self)} combinations of {list(self.grid)}>"
