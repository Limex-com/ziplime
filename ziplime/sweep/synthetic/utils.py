"""Utils for synthetic."""

import json
import time
from pathlib import Path
from typing import Callable
import numpy as np
import pandas as pd
from exchange_calendars import ExchangeCalendar
from ziplime.sweep.params import SweepError, Synthetic
from ziplime.assets.services.asset_service import AssetService
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.sweep.params import SweepParams
from ziplime.sweep.synthetic.support import METRICS
from ziplime.sweep.synthetic.bar_utils import as_bundle
from ziplime.sweep.synthetic.bar_utils import build
from ziplime.sweep.synthetic.feature_utils import features
from ziplime.sweep.synthetic.generator_utils import generator
from ziplime.sweep.synthetic.report_utils import long_format
from ziplime.sweep.synthetic.logging_utils import quiet_logs
from ziplime.sweep.synthetic.validation import refuse_splits
from ziplime.sweep.synthetic.bar_utils import side_by_side
from ziplime.sweep.synthetic.report_utils import summarize


async def run(
    *,
    params: SweepParams,
    say: Callable[[str], None],
    bundle: DataBundle,
    universe: dict,
    calendar: ExchangeCalendar,
    asset_service: AssetService,
    once: Callable,
    config_of: Callable,
    output: Path,
) -> pd.DataFrame:
    """The sweep on the real bars, then on every path of every family; the comparison."""
    synthetic: Synthetic = params.synthetic
    if params.emission_rate not in ("1d", "1D"):
        raise SweepError("Synthetic paths are built from daily bars; set emission_rate to 1d.")
    last = min(params.end_date, bundle.data["date"].max().date())
    await refuse_splits(asset_service, universe, calendar, params.start_date, last)
    bars = side_by_side(bundle.data, universe, params.start_date, calendar.tz)
    feats = features(bars)
    rows, columns = bars.shape
    pool = np.arange(1 if synthetic.source == "history" else bars.start, rows)
    if len(pool) < 2:
        raise SweepError("Fewer than 2 bars to resample from.")

    real = await once(bundle, False)
    say(f"\nSynthetic paths: {len(synthetic.families)} "
        f"famil{'y' if len(synthetic.families) == 1 else 'ies'} × {synthetic.paths} paths of "
        f"{rows - bars.start} bars, from {len(pool)} real bars ({synthetic.source}); every "
        f"combination on each")
    seeds = np.random.SeedSequence(synthetic.seed).spawn(len(synthetic.families))
    n_combos = len(real)
    results = {}
    for family, seed in zip(synthetic.families, seeds):
        started = time.perf_counter()
        made = generator(family, feats, pool, bars.start)
        rngs = [np.random.default_rng(s) for s in seed.spawn(synthetic.paths)]
        values = {metric: np.full((synthetic.paths, n_combos), np.nan) for metric in METRICS}
        unsupported = np.zeros(n_combos, dtype=bool)
        for k, rng in enumerate(rngs):
            source, returns = made.path(rng)
            with quiet_logs():
                table = await once(as_bundle(bundle, bars, build(bars, feats, source, returns)),
                                   True)
            table = table.reindex(range(n_combos))
            for metric in METRICS:
                values[metric][k] = table[metric].to_numpy(dtype=float)
            unsupported |= table["unsupported"].fillna(False).to_numpy(dtype=bool)
        results[family.label] = (values, unsupported)
        say(f"  {family.label}: {synthetic.paths} paths in {time.perf_counter() - started:.1f} s")

    summary, text, best = summarize(real, results, synthetic, params.sort_by, params.min_fills)
    say("\n" + text)
    output.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output / "synthetic_summary.csv")
    long_format(results).write_parquet(output / "synthetic_paths.parquet")
    (output / "best_synthetic_config.json").write_text(
        json.dumps(config_of(best).model_dump(mode="json"), indent=2, default=str) + "\n",
        encoding="utf-8")
    say(f"Written: {output}/synthetic_summary.csv, synthetic_paths.parquet, "
        f"best_synthetic_config.json")
    real.attrs["synthetic"] = summary
    real.attrs["synthetic_best"] = best
    return real
