"""``context.strategy``: an algorithm that runs other algorithms and trades them as assets.

The strategies a metastrategy runs are ordinary algorithm files -- the ones ``run_simulation``
runs -- each with its own book (cash, positions, ledger, metrics) on the same clock and the same
market data. On every clock event each of them is stepped first and the metastrategy second, so its
``handle_data`` reads their state as of this bar, never a bar ahead.

What the metastrategy owns of a strategy is units of it, priced at the strategy's NAV, on a venue of
their own (see :mod:`ziplime.meta.exchange`). The strategies trade their own capital as if alone --
nothing the metastrategy does changes what they do -- and the metastrategy's exposure to each is
``units x NAV``. That is equity-curve trading: allocating between, switching on and off, or picking
the best of several strategies by how they are doing.
"""
import json
import traceback
import uuid
from collections import defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Any

from ziplime.errors import BarSimulationError
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.commission.commission_model import CommissionModel
from ziplime.finance.domain.order import Order
from ziplime.finance.domain.order_status import OrderStatus
from ziplime.finance.execution import MarketOrder
from ziplime.meta.exchange import StrategyExchange, is_strategy_listing, strategy_listing
from ziplime.meta.handle import StrategyCluster, StrategyHandle
from ziplime.utils.api_support import ZiplineAPI, api_method
from ziplime.utils.math_utils import round_if_near_integer


class StrategiesMixin:
    """The metastrategy half of TradingAlgorithm. Inert until ``context.strategy`` is called."""

    def _init_strategies(self) -> None:
        #: ``{listing sid: handle}`` -- every strategy this one runs, cluster members included.
        self._strategies: dict[int, StrategyHandle] = {}
        self._strategy_venue: StrategyExchange | None = None

    # -- declaring strategies ------------------------------------------------------------------

    @api_method
    async def strategy(self, name: str, *, config: dict[str, Any] | None = None,
                       config_file: str | None = None, grid: dict[str, Any] | None = None,
                       where: str | None = None, capital: float | None = None,
                       label: str | None = None,
                       commission: CommissionModel | None = None
                       ) -> StrategyHandle | StrategyCluster:
        """Run another algorithm alongside this one, and trade it as an asset.

            context.trend = await context.strategy("trend")                  # trend.py, next to this file
            context.mr = await context.strategy("strategies/mean_rev.py", config={"lookback": 20})
            context.bb = await context.strategy("bollinger", grid={"bb_period": [10, 20, 30],
                                                                   "bb_std": {"from": 1.5, "to": 2.5, "step": 0.5}},
                                                where="bb_period >= 20 or bb_std < 2.5")

        Parameters
        ----------
        name
            The strategy's file: a path ending in ``.py`` relative to this algorithm's file, or a
            bare name, looked up as ``<name>.py`` and then ``strategies/<name>.py`` beside it.
        config, config_file
            The strategy's ``AlgorithmConfig``: a JSON file, a dict, or both (the dict wins).
        grid, where
            A parameter cluster -- the sweep file's syntax: each key a field of ``AlgorithmConfig``,
            each value a list, a ``{"from", "to", "step"}`` range or a ``{"path", "values"}`` for
            a nested field; ``where`` a condition the combination has to pass. Every combination
            runs as its own strategy, and a :class:`StrategyCluster` of them is returned.
        capital
            The strategy's own starting cash. Defaults to this algorithm's. Its NAV is a return, so
            this only matters for how its orders round to whole shares.
        label
            Its name in results; defaults to the file's name, with the parameters for a cluster.
        commission
            Charged when *this* algorithm buys or sells units of the strategy. None: free.

        Returns
        -------
        StrategyHandle, or StrategyCluster when ``grid`` is given.
        """
        if self.initialized:
            raise RuntimeError("context.strategy() declares a strategy for the whole run, so it can "
                               "only be called in initialize.")
        if self.currency_reporting is not None:
            raise NotImplementedError("Metastrategies do not support currency reporting yet.")

        path = self._resolve_strategy_file(name)
        name = label or path.stem
        shared = {}
        if config_file is not None:
            shared = json.loads(self._resolve_beside(config_file).read_text(encoding="utf-8"))
        shared.update(config or {})

        if grid:
            from pydantic import TypeAdapter

            from ziplime.sweep.params import GridValue, config_for, config_paths, expand_grid
            spec = TypeAdapter(dict[str, GridValue]).validate_python(grid)
            paths = config_paths(spec)
            combos = [(combo, config_for(shared, combo, paths)) for combo in expand_grid(spec, where)]
        elif where:
            raise ValueError("where= filters the combinations of a grid; there is no grid.")
        else:
            paths, combos = {}, [({}, shared)]

        if capital is None:
            capital = (await self.exchange_repository.get_default_exchange()).get_start_cash_balance()

        members = []
        for combo, combo_config in combos:
            member_label = name if not grid else (
                f"{name}[{', '.join(f'{key}={value}' for key, value in combo.items())}]")
            if any(handle.label == member_label for handle in self._strategies.values()):
                raise ValueError(f"Two strategies are both called {member_label!r}; give one a label=.")
            algorithm = await self._build_strategy(path=path, config=combo_config, capital=capital)
            if not members:
                self._check_config_names(path, algorithm.algorithm.config_class,
                                         names=list(shared) + [p.split(".")[0] for p in paths.values()])
            sid = -(len(self._strategies) + 1)
            handle = StrategyHandle(name=name, label=member_label, algorithm=algorithm,
                                    listing=strategy_listing(sid=sid, label=member_label), owner=self,
                                    params=combo, capital=capital, commission=commission)
            self._strategies[sid] = handle
            members.append(handle)

        await self._strategy_exchange()
        if grid:
            return StrategyCluster(name=name, members=members, grid=grid)
        return members[0]

    def strategy_results(self) -> dict:
        """``{label: TradingAlgorithmExecutionResult}`` of every strategy run, once the run is over."""
        return {handle.label: handle.result for handle in self._strategies.values()}

    def _resolve_beside(self, name: str) -> Path:
        path = Path(name).expanduser()
        if path.is_absolute():
            return path
        script = getattr(self.algorithm, "algorithm_file", None)
        base = Path(script).resolve().parent if script else Path.cwd()
        return base / path

    def _resolve_strategy_file(self, name: str) -> Path:
        if Path(name).suffix == ".py":
            candidates = [self._resolve_beside(name)]
        else:
            candidates = [self._resolve_beside(f"{name}.py"),
                          self._resolve_beside(str(Path("strategies") / f"{name}.py"))]
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        raise FileNotFoundError(
            f"Strategy {name!r} not found; looked for {', '.join(str(c) for c in candidates)}.")

    @staticmethod
    def _check_config_names(path: Path, config_class, names: list[str]) -> None:
        """Fail on a config or grid key the strategy does not have.

        pydantic ignores unknown fields, so a misspelt grid key would run every combination with
        the default value and report a cluster of identical strategies as if it were a sweep.
        """
        if not names:
            return
        if config_class is None:
            raise ValueError(f"{path.name} defines no AlgorithmConfig class, so config= and grid= "
                             f"have nothing to set ({', '.join(names)}).")
        unknown = sorted(set(names) - set(config_class.model_fields))
        if unknown:
            raise ValueError(f"{path.name}: {config_class.__name__} has no field "
                             f"{', '.join(unknown)}. Fields: {', '.join(config_class.model_fields)}.")

    async def _build_strategy(self, path: Path, config: dict, capital: float):
        """One strategy as run_simulation would build it: same data, clock and costs, own book."""
        from ziplime.core.algorithm_file import AlgorithmFile
        from ziplime.exchanges.repositories.in_memory_exchange_repository import InMemoryExchangeRepository
        from ziplime.exchanges.simulation_exchange import SimulationExchange
        from ziplime.finance.blotter.in_memory_blotter import InMemoryBlotter

        parent = await self.exchange_repository.get_default_exchange()
        if not isinstance(parent, SimulationExchange) or parent.live:
            raise NotImplementedError("Metastrategies run in simulation only for now: each strategy "
                                      "needs a simulated book of its own.")
        exchange = SimulationExchange(
            name=parent.name, country_code=parent.country_code,
            trading_calendar=parent.trading_calendar, clock=parent.clock, cash_balance=capital,
            equity_slippage=None, future_slippage=None, equity_commission=None,
            future_commission=None, account_id=parent.account_id, is_default=True,
            data_source=parent.data_source,
            price_used_in_order_execution=parent.price_used_in_order_execution,
            automatic_currency_exchange=parent.automatic_currency_exchange)
        # Own copies: a slippage model counts the volume it has filled in a bar, and two
        # strategies sharing one would take each other's share of it.
        exchange.slippage_models = deepcopy(parent.slippage_models)
        exchange.commission_models = deepcopy(parent.commission_models)
        repository = InMemoryExchangeRepository()
        await repository.add_exchange(exchange)

        child = type(self)(
            algorithm=AlgorithmFile(algorithm_file=str(path), config=config),
            asset_service=self.asset_service,
            exchange_repository=repository,
            metrics_set=deepcopy(self._metrics_set),
            blotter=InMemoryBlotter(exchanges=[exchange], cancel_policy=self.blotter.cancel_policy),
            benchmark_source=self.benchmark_source,
            clock=self.clock,
            custom_data_sources=[exchange] + [source for source in self._custom_data_sources
                                              if not isinstance(source, Exchange)],
            get_pipeline_loader=self._get_pipeline_loader,
            stop_on_error=self.stop_on_error,
            same_bar_execution=self.same_bar_execution,
            futures_margin_model=deepcopy(self._futures_margin_model),
            intraday_metrics=self.intraday_metrics,
        )
        child._ledger.synchronize_exchange_portfolio(portfolio=await exchange.get_portfolio())
        for control in self.account_controls:
            child.register_account_control(control=deepcopy(control))
        # Builds its metrics tracker and runs its initialize; the loop itself is this algorithm's.
        with ZiplineAPI(child):
            generator = await child.get_generator()
        await generator.aclose()
        return child

    async def _strategy_exchange(self) -> StrategyExchange:
        if self._strategy_venue is None:
            venue = StrategyExchange(
                clock=self.clock, trading_calendar=self.clock.trading_calendar,
                nav=lambda sid: self._strategies[sid].nav,
                commission=lambda sid: self._strategies[sid].commission)
            await self.exchange_repository.add_exchange(venue)
            # The blotter keeps orders by venue and was built before this one existed.
            self.blotter.open_orders.setdefault(venue.name, defaultdict(dict))
            self.blotter.orders.setdefault(venue.name, {})
            self._strategy_venue = venue
        return self._strategy_venue

    # -- running them --------------------------------------------------------------------------

    async def _step_strategies(self, dt, action) -> None:
        """Every strategy through this clock event, before this algorithm handles it.

        A strategy that raises is recorded and carries on, as a run does on its own; it does not
        take the metastrategy's bar down with it, unless the run stops on errors.
        """
        if not self._strategies:
            return
        for handle in self._strategies.values():
            child = handle.context
            try:
                with ZiplineAPI(child):
                    async for packet in child.process_event(dt=dt, action=action):
                        handle.packets.append(packet)
                    # Same-bar fills land after its handle_data and are not in its portfolio until
                    # the next refresh; its NAV has to include them before anyone reads it.
                    await child._ledger.update_portfolio()
                    child._ledger.update_account()
            except Exception as error:
                handle.errors.append(BarSimulationError(
                    trace=traceback.format_exc(),
                    message=f"Strategy {handle.label}: exception at dt={dt}: {error}",
                    simulation_dt=dt))
                child._logger.error(f"Strategy {handle.label}: simulation error on dt={dt}")
                if self.stop_on_error:
                    raise

    async def _finish_strategies(self) -> None:
        if not self._strategies:
            return
        from ziplime.trading.currency_results import currency_performance
        from ziplime.trading.trading_algorithm_execution_result import TradingAlgorithmExecutionResult
        from ziplime.trading.trading_algorithm_executor import TradingAlgorithmExecutor

        for handle in self._strategies.values():
            child = handle.context
            # A strategy may itself be a metastrategy: its own strategies finish first.
            await child._finish_strategies()
            errors = handle.errors + child._end_of_run_warnings()
            packets = handle.packets + [child.metrics_tracker.handle_simulation_end()]
            perf, risk_report = TradingAlgorithmExecutor()._create_daily_stats(packets)
            handle.result = TradingAlgorithmExecutionResult(
                trading_algorithm=child, perf=perf, risk_report=risk_report, errors=errors,
                perf_by_currency=currency_performance(packets=packets),
                risk_report_by_currency=risk_report.get("risk_report_by_currency", {}),
                strategies=child.strategy_results())
            handle.packets = []

    # -- trading them --------------------------------------------------------------------------

    def _strategy_of(self, target) -> StrategyHandle | None:
        """The strategy an order names: a handle, its listing, or its ``.portfolio``."""
        if isinstance(target, StrategyHandle):
            if target._owner is not self:
                raise ValueError(f"{target.label} is run by another algorithm.")
            return target
        # getattr: also called on algorithms built without __init__, which run no strategies.
        strategies = getattr(self, "_strategies", None)
        if not strategies:
            return None
        if is_strategy_listing(target):
            return strategies.get(target.sid)
        for handle in strategies.values():
            if target is handle.context.portfolio:
                return handle
        return None

    @staticmethod
    def _is_strategy_listing(asset) -> bool:
        return is_strategy_listing(asset)

    async def _order_strategy(self, handle: StrategyHandle, *, amount: float | None = None,
                              value: float | None = None, percent: float | None = None,
                              target: float | None = None, target_value: float | None = None,
                              target_percent: float | None = None) -> Order | None:
        """Units of a strategy, sized at its NAV, filled whole at the NAV of the bar they execute."""
        if not self.initialized:
            raise RuntimeError("Strategies can only be ordered from handle_data or a scheduled "
                               "function, not from initialize.")
        nav = handle.nav
        if amount is not None:
            units = int(round_if_near_integer(amount))
        else:
            if nav <= 0:
                self._logger.warning("Strategy NAV is not positive; not ordering it",
                                     strategy=handle.label, nav=nav)
                return None
            total = self.portfolio.portfolio_value
            if value is not None:
                units = int(round_if_near_integer(value / nav))
            elif percent is not None:
                units = int(round_if_near_integer(percent * total / nav))
            else:
                if target is None:
                    target = (target_value if target_value is not None
                              else target_percent * total) / nav
                units = int(round_if_near_integer(target)) - handle.units
        if units == 0:
            return None

        venue = await self._strategy_exchange()
        order = Order(id=uuid.uuid4().hex[:20], dt=self.simulation_dt, asset=handle.asset,
                      amount=units, filled=0, commission=0.0, execution_style=MarketOrder(),
                      status=OrderStatus.OPEN, exchange_name=venue.name,
                      trading_account_id=venue.account_id)
        return self.new_order_submitted(await venue.submit_order(order))

    async def _rebalance_strategies(self, weights: dict) -> dict:
        """Take the strategies out of ``rebalance``'s weights and trade them; return the rest."""
        listings, named = {}, {}
        for key, weight in weights.items():
            handle = self._strategy_of(key)
            if handle is None:
                listings[key] = weight
            else:
                named[handle] = weight
        for handle in self._strategies.values():
            if handle not in named and handle.units:
                named[handle] = 0.0
        # Reductions first, so a same-bar fill frees the cash the increases use.
        for handle, weight in sorted(named.items(), key=lambda item: item[1] - item[0].weight):
            await self._order_strategy(handle, target_percent=weight)
        return listings
