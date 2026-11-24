"""Utils for initialize."""

import json
import tempfile
from pathlib import Path
from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.core.algorithm_file import AlgorithmFile
from ziplime.sweep.params import SweepError, SweepParams, config_for, config_paths
from ziplime.assets.services.asset_service import AssetService
from ziplime.sweep.initialize.combinations import Combinations
from ziplime.sweep.initialize.initialized import Initialized
from ziplime.sweep.initialize.sweep_context import SweepContext
from ziplime.sweep.initialize.read_tracker import _ReadTracker
from ziplime.sweep.initialize.symbol_utils import symbols_in


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


async def run_initialize(
    algorithm: AlgorithmFile,
    asset_service: AssetService,
    config: BaseAlgorithmConfig,
    found: dict,
) -> Initialized:
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


async def initialize_each(algorithm: AlgorithmFile, asset_service: AssetService, combinations: Combinations,
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
