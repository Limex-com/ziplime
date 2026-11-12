"""The algorithm file, its config for each combination, and what its ``initialize`` declares.

The sweep runs the same :class:`AlgorithmFile` that run_simulation runs. ``initialize`` is called
with :class:`SweepContext` standing in for TradingAlgorithm: it can look instruments up, declare
indicators and set attributes on the context, which is everything ``initialize`` does before the
first bar.
"""
import ast
import inspect
import json
import tempfile
import textwrap
from pathlib import Path

from pydantic import ValidationError

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.core.algorithm_file import AlgorithmFile
from ziplime.sweep.params import SweepError, SweepParams, config_for, config_paths


class SweepContext:
    """TradingAlgorithm as ``initialize`` sees it: the config, instrument lookup, indicators."""

    #: The context's own attributes; everything else on it was set by the algorithm's initialize
    OWN = {"algorithm", "_asset_service", "_found", "requested", "_indicators", "_scheduled",
           "_commission", "_slippage", "_cancel_policy", "universe", "datasets"}

    def __init__(self, algorithm: AlgorithmFile, asset_service, found: dict | None = None):
        self.algorithm = algorithm
        self._asset_service = asset_service
        #: Instruments already looked up: initialize runs once per distinct config
        self._found = {} if found is None else found
        #: Every instrument the algorithm asked for, in order
        self.requested = []
        self._indicators = {}
        #: ``schedule_function`` calls, in order: (function, date_rule, time_rule, half_days)
        self._scheduled = []
        #: What set_commission / set_slippage / set_cancel_policy chose
        self._commission, self._slippage, self._cancel_policy = {}, {}, None

    async def symbol(self, symbol: str, mic: str = None, asset_type: AssetType = AssetType.EQUITY,
                     country_code: str | None = None):
        if "@" in symbol:
            symbol, mic = symbol.split("@")
        key = (symbol, mic, asset_type)
        if key not in self._found:
            self._found[key] = await self._asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=symbol, mic=mic), asset_type=asset_type)
        asset = self._found[key]
        if asset is None:
            raise SweepError(f"{symbol} (mic={mic}) is not in the asset database.")
        if asset not in self.requested:
            self.requested.append(asset)
        return asset

    async def symbols(self, *symbols, **kwargs):
        return [await self.symbol(symbol, **kwargs) for symbol in symbols]

    def indicators(self, **expressions):
        """As TradingAlgorithm.indicators: the expressions are kept, not computed."""
        self._indicators.update(expressions)

    def schedule_function(self, func, date_rule=None, time_rule=None, half_days=True,
                          calendar=None):
        self._scheduled.append((func, date_rule, time_rule, half_days))

    def set_commission(self, us_equities=None, us_futures=None, bonds=None):
        if bonds is not None:
            raise SweepError("Bonds are not supported in a sweep.")
        self._commission.update({k: v for k, v in (("equities", us_equities),
                                                    ("futures", us_futures)) if v is not None})

    def set_slippage(self, us_equities=None, us_futures=None, bonds=None):
        if bonds is not None:
            raise SweepError("Bonds are not supported in a sweep.")
        self._slippage.update({k: v for k, v in (("equities", us_equities),
                                                  ("futures", us_futures)) if v is not None})

    def set_cancel_policy(self, cancel_policy):
        self._cancel_policy = cancel_policy

    def state(self) -> dict:
        """What initialize put on the context (``context.i = 0``, ...)."""
        return {name: value for name, value in vars(self).items() if name not in self.OWN}


class Combinations:
    """The grid's combinations and their configs, built when needed.

    A million validated configs held at once cost gigabytes; a combination is kept as the few grid
    values that make it, and its config is built for the batch that needs it.
    """

    def __init__(self, combos: list[dict], shared: dict, paths: dict[str, str], config_class):
        self.combos, self.shared, self.paths, self.config_class = combos, shared, paths, config_class

    def __len__(self):
        return len(self.combos)

    def config(self, index: int):
        try:
            return self.config_class.model_validate(
                config_for(self.shared, self.combos[index], self.paths))
        except ValidationError as error:
            raise SweepError(f"Grid values {self.combos[index]} do not fit "
                             f"{self.config_class.__name__}:\n{error}") from None

    def configs(self, indices) -> list:
        return [self.config(index) for index in indices]

    def key(self, index: int, fields: set[str]) -> tuple:
        """The combination's values of the grid parameters under ``fields`` of the config."""
        combo = self.combos[index]
        return tuple(combo[name] for name, path in self.paths.items()
                     if path.split(".")[0] in fields)


def load_algorithm(params: SweepParams, base: Path, combos: list[dict]):
    """The AlgorithmFile, its config class, the shared part of the config, and the combinations."""
    shared = {}
    if params.config_file:
        shared.update(json.loads(params.resolve(base, params.config_file).read_text(encoding="utf-8")))
    shared.update(params.config)
    paths = config_paths(params.grid)

    # AlgorithmFile builds its config only from a file, so it gets one
    with tempfile.TemporaryDirectory() as folder:
        seed = Path(folder) / "config.json"
        seed.write_text(json.dumps(config_for(shared, combos[0], paths)), encoding="utf-8")
        algorithm = AlgorithmFile(algorithm_file=str(params.resolve(base, params.algorithm_file)),
                                  algorithm_config_file=str(seed))

    config_class = type(algorithm.config)
    if config_class is BaseAlgorithmConfig:
        raise SweepError(
            f"{params.algorithm_file} has no AlgorithmConfig(BaseAlgorithmConfig) class. The swept "
            f"parameters are its fields, as for a config_file in run_simulation.")
    unknown = sorted({path.split(".")[0] for path in paths.values()} - set(config_class.model_fields))
    if unknown:
        raise SweepError(f"{unknown} are not fields of {config_class.__name__}: "
                         f"{list(config_class.model_fields)}.")
    combinations = Combinations(combos, shared, paths, config_class)
    combinations.config(0)  # fail early on a grid that does not fit the config
    return algorithm, combinations


class _ReadTracker:
    """A config that records which of its fields were read."""

    def __init__(self, config):
        object.__setattr__(self, "_config", config)
        object.__setattr__(self, "read", set())

    def __getattr__(self, name):
        if name in type(self._config).model_fields:
            self.read.add(name)
        return getattr(self._config, name)


class Initialized:
    """What one run of initialize declared."""

    def __init__(self, context: SweepContext):
        self.indicators = dict(context._indicators)
        self.state = context.state()
        self.scheduled = list(context._scheduled)
        self.commission, self.slippage = dict(context._commission), dict(context._slippage)
        self.cancel_policy = context._cancel_policy
        universe = getattr(context, "universe", None) or context.requested
        if isinstance(universe, dict):
            self.universe = universe
        else:
            self.universe = {listing.symbol: listing for listing in universe}
        self.datasets = getattr(context, "datasets", None)


async def run_initialize(algorithm: AlgorithmFile, asset_service, config, found: dict) -> Initialized:
    algorithm.config = config
    context = SweepContext(algorithm, asset_service, found)
    try:
        await algorithm.initialize(context)
    except AttributeError as error:
        raise SweepError(f"initialize() uses something a sweep does not provide: {error}.") from None
    if not getattr(context, "universe", None) and not context.requested:
        # The strategy looks its instruments up in handle_data: the universe is what it names
        for function in (algorithm.handle_data, getattr(algorithm, "before_trading_start", None)):
            for symbol, mic in symbols_in(function):
                await context.symbol(symbol, mic)
    return Initialized(context)


def symbols_in(function) -> list[tuple[str, str | None]]:
    """The tickers a hook passes to context.symbol / context.symbols: string constants, and the
    items of a list of them the call's argument loops over
    (``{s: await context.symbol(s) for s in ["SOXL", "TQQQ"]}``)."""
    try:
        definition = ast.parse(textwrap.dedent(inspect.getsource(function))).body[0]
    except (OSError, TypeError, SyntaxError, IndexError):
        return []
    if not isinstance(definition, (ast.FunctionDef, ast.AsyncFunctionDef)) or not definition.args.args:
        return []
    context = definition.args.args[0].arg
    lists = {}
    for node in ast.walk(definition):
        if isinstance(node, ast.Assign) and isinstance(node.value, (ast.List, ast.Tuple)) and \
                all(isinstance(e, ast.Constant) and isinstance(e.value, str) for e in node.value.elts):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    lists[target.id] = [e.value for e in node.value.elts]

    def items(node):
        if isinstance(node, (ast.List, ast.Tuple)):
            return [e.value for e in node.elts if isinstance(e, ast.Constant) and isinstance(e.value, str)]
        if isinstance(node, ast.Name):
            return lists.get(node.id, [])
        return []

    loops = {}  # loop variable -> the tickers it takes
    for node in ast.walk(definition):
        if isinstance(node, (ast.For, ast.comprehension)) and isinstance(node.target, ast.Name):
            loops.setdefault(node.target.id, []).extend(items(node.iter))
    found = []
    for node in ast.walk(definition):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr in ("symbol", "symbols") and isinstance(node.func.value, ast.Name)
                and node.func.value.id == context):
            continue
        mic = next((k.value.value for k in node.keywords
                    if k.arg == "mic" and isinstance(k.value, ast.Constant)), None)
        for arg in (node.args if node.func.attr == "symbols" else node.args[:1]):
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                tickers = [arg.value]
            elif isinstance(arg, ast.Name):
                tickers = loops.get(arg.id, [])
            else:
                tickers = []
            found += [(ticker, mic) for ticker in tickers if (ticker, mic) not in found]
    return found


async def initialize_each(algorithm: AlgorithmFile, asset_service, combinations: Combinations,
                          found: dict) -> list[Initialized]:
    """initialize for every combination -- once per distinct value of the fields it reads.

    initialize usually reads a few fields of the config (moex_trend reads fast and slow); every
    combination that agrees on those gets the same result. Which fields matter is seen from what
    initialize read, so a million combinations of moex_trend cost 1,248 calls.
    """
    fields, cache, results = None, {}, []
    for index in range(len(combinations)):
        if fields is not None:
            key = combinations.key(index, fields)
            if key in cache:
                results.append(cache[key])
                continue
        tracker = _ReadTracker(combinations.config(index))
        result = await run_initialize(algorithm, asset_service, tracker, found)
        if fields is None or not tracker.read <= fields:
            # A newly read field changes the key; earlier results stay right, only the cache resets
            fields = (fields or set()) | tracker.read
            cache.clear()
        cache[combinations.key(index, fields)] = result
        results.append(result)
    algorithm.config = combinations.config(0)
    names = list(results[0].universe)
    distinct = {id(result): result for result in results}.values()
    if any(list(result.universe) != names for result in distinct):
        raise SweepError("context.universe differs between combinations -- a sweep runs every "
                         "combination on one set of instruments.")
    return results
