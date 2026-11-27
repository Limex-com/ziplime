"""Report utils for synthetic."""

import math
import numpy as np
import pandas as pd
import polars as pl
from ziplime.sweep.params import SweepError, Synthetic
from ziplime.sweep.synthetic.support import METRICS


def long_format(results: dict) -> pl.DataFrame:
    parts = []
    for label, (values, _) in results.items():
        paths, combos = values["sharpe"].shape
        parts.append(pl.DataFrame({
            "family": [label] * (paths * combos),
            "path": np.repeat(np.arange(paths), combos),
            "combo": np.tile(np.arange(combos), paths),
            **{metric: values[metric].ravel() for metric in METRICS}}))
    return pl.concat(parts)


def statistic(values: np.ndarray, name: str) -> np.ndarray:
    """Over the paths (axis 0) of each combination."""
    if name == "median":
        return np.nanmedian(values, axis=0)
    if name == "mean":
        return np.nanmean(values, axis=0)
    if name == "min":
        return np.nanmin(values, axis=0)
    return np.nanpercentile(values, float(name[1:]), axis=0)


def scores(values: dict, sort_by: str) -> np.ndarray:
    """The ranked metric per path and combination, higher better; a run without a single return
    has no Sharpe ratio -- 0, as earning nothing."""
    found = values[sort_by]
    return np.where(np.isnan(found), 0.0, found) if sort_by == "sharpe" else found


def ranks(score: np.ndarray, eligible: np.ndarray) -> np.ndarray:
    """1 for the best eligible combination; NaN for the others."""
    out = np.full(len(score), np.nan)
    order = np.flatnonzero(eligible)[np.argsort(-score[eligible], kind="stable")]
    out[order] = np.arange(1, len(order) + 1)
    return out


def spearman(a: np.ndarray, b: np.ndarray) -> float:
    keep = np.isfinite(a) & np.isfinite(b)
    if keep.sum() < 3:
        return float("nan")
    ra = pd.Series(a[keep]).rank().to_numpy()
    rb = pd.Series(b[keep]).rank().to_numpy()
    if ra.std() == 0 or rb.std() == 0:
        return float("nan")
    return float(np.corrcoef(ra, rb)[0, 1])


def summarize(real: pd.DataFrame, results: dict, synthetic: Synthetic, sort_by: str,
              min_fills: int) -> tuple[pd.DataFrame, str, int]:
    real = real.sort_index()
    n = len(real)
    parameters = [c for c in real.columns if c not in (
        "sharpe", "total_return", "max_drawdown", "final_value", "fills", "fees", "rejects",
        "unsupported", "precision")]
    real_score = real[sort_by].to_numpy(dtype=float)
    if sort_by == "sharpe":
        real_score = np.where(np.isnan(real_score), 0.0, real_score)
    real_ok = (real["fills"].to_numpy() >= min_fills) & ~real["unsupported"].astype(bool).to_numpy()
    summary = real[parameters].copy()
    summary[f"real {sort_by}"] = real_score
    summary["real rank"] = ranks(real_score, real_ok)

    select = synthetic.select_by
    pooled, pooled_ok, per_family = [], np.ones(n, dtype=bool), {}
    for label, (values, unsupported) in results.items():
        score = scores(values, sort_by)
        ok = (np.nanmedian(values["fills"], axis=0) >= min_fills) & ~unsupported
        chosen = statistic(score, select)
        summary[f"{label} {select}"] = chosen
        for name in ("p5", "median", "p95"):
            if name != select:
                summary[f"{label} {name}"] = statistic(score, name)
        summary[f"{label} P(gain)"] = (values["total_return"] > 0).mean(axis=0)
        summary[f"{label} rank"] = ranks(chosen, ok)
        # Where the real result falls among the combination's own paths
        summary[f"{label} real percentile"] = (score < real_score[None, :]).mean(axis=0) * 100
        pooled.append(score)
        pooled_ok &= ok
        per_family[label] = (score, chosen, ok)
    every = np.vstack(pooled)
    overall = statistic(every, select)
    summary[f"all {select}"] = overall
    summary["all rank"] = ranks(overall, pooled_ok)
    if not pooled_ok.any():
        raise SweepError(f"No combination made {min_fills} fills on the median path; lower "
                         f"min_fills.")
    best = int(np.flatnonzero(summary["all rank"].to_numpy() == 1)[0])
    real_best = int(np.flatnonzero(summary["real rank"].to_numpy() == 1)[0]) \
        if real_ok.any() else None

    def combo(k: int):
        return {name: (v.item() if hasattr(v, "item") else v)
                for name, v in real.loc[k, parameters].items()}

    def place(rank: int, total: int):
        if np.isnan(rank):
            return "not ranked"
        return f"{int(rank):,} of {int(total):,} (top {math.ceil(rank / total * 100):.0f}%)" \
            if rank > 1 else f"1 of {int(total):,}"

    width = max(14, *(len(label) for label in results))
    head = (f"  {'family':<{width}} {select:>8} {'p5':>8} {'p95':>8} {'P(gain)':>8}   rank")
    lines = [f"Chosen on the synthetic paths ({select} {sort_by} over all "
             f"{every.shape[0]:,} paths): {combo(best)}", head]
    for label, (score, chosen, ok) in per_family.items():
        values = score[:, best]
        lines.append(f"  {label:<{width}} {chosen[best]:8.2f} {np.percentile(values, 5):8.2f} "
                     f"{np.percentile(values, 95):8.2f} "
                     f"{summary.loc[real.index[best], f'{label} P(gain)'] * 100:7.0f}%   "
                     f"{place(summary[f'{label} rank'].iloc[best], ok.sum())}")
    lines.append(f"  {'real bars':<{width}} {real_score[best]:8.2f} {'':>8} {'':>8} {'':>8}   "
                 f"{place(summary['real rank'].iloc[best], real_ok.sum())}")
    if real_best is not None:
        lines += ["", f"Chosen on the real bars: {combo(real_best)} -- {sort_by} "
                      f"{real_score[real_best]:.2f}",
                  head + "                        real beats"]
        for label, (score, chosen, ok) in per_family.items():
            values = score[:, real_best]
            percentile = summary[f"{label} real percentile"].iloc[real_best]
            lines.append(
                f"  {label:<{width}} {chosen[real_best]:8.2f} {np.percentile(values, 5):8.2f} "
                f"{np.percentile(values, 95):8.2f} "
                f"{summary[f'{label} P(gain)'].iloc[real_best] * 100:7.0f}%   "
                f"{place(summary[f'{label} rank'].iloc[real_best], ok.sum()):<24}"
                f"  {percentile:.0f}% of its paths")
    lines += ["", "Best on each family, and on the real bars:"]
    for label, (score, chosen, ok) in per_family.items():
        if not ok.any():
            continue
        k = int(np.flatnonzero(summary[f"{label} rank"].to_numpy() == 1)[0])
        lines.append(f"  {label:<{width}} {combo(k)}: {select} {chosen[k]:.2f}; real "
                     f"{real_score[k]:.2f}, {place(summary['real rank'].iloc[k], real_ok.sum())}")
    agreement = " · ".join(f"{label} {spearman(real_score, chosen):+.2f}"
                           for label, (_, chosen, _) in per_family.items())
    lines += ["", f"Rankings agree (Spearman, real {sort_by} against {select} of the paths): "
                  f"{agreement}"]
    summary.index.name = "combo"
    return summary, "\n".join(lines), int(real.index[best])
