"""Synthetic price paths made from the real bars, and the sweep run on every one of them.

A sweep chooses the combination that did best on one history -- the one that happened. Run on
paths that could have happened instead, the same grid shows which choices hold up: the
combination chosen on the paths can be looked at on the real bars, and the one chosen on the real
bars on the paths, family by family.

A path is a whole set of bars -- open, high, low, close, volume for every instrument -- from
``start_date`` on; the warm-up before it stays real, so every path starts where the real run
starts. Each bar of a path is built from one real bar: its close-to-close log return, its
opening gap, its high and low relative to its close, its volume. Methods that pick real bars pick
the same bar for every instrument, so the instruments move together as they did:

* ``iid`` -- bars drawn with replacement; ``shuffle`` -- without (a permutation)
* ``block`` -- blocks of ``block`` consecutive bars (moving block bootstrap, Künsch 1989);
  ``circular`` -- the same, wrapping around the end (Politis & Romano 1992); ``stationary`` --
  blocks of random length, ``block`` on average (Politis & Romano 1994). Blocks keep what a
  strategy may live on: trends, volatility clusters, mean reversion within the block
* ``meboot`` -- the maximum entropy bootstrap (Vinod 2004, as R's meboot): each instrument's log
  price redrawn from a density around its own values, in their order -- paths that keep the real
  path's shape, with its turns where they were
* ``gbm`` -- normal log returns with the real means and covariance; ``student_t`` -- the same with
  fat tails (a multivariate t with ``df`` degrees of freedom); the bars' shape and volume from
  real bars drawn at random
* ``garch`` -- GARCH(1,1) per instrument, fitted by maximum likelihood, driven by the real
  standardised residuals of a date drawn at random for all instruments at once (filtered
  historical simulation, Barone-Adesi et al. 1999): volatility clusters, fat tails and the
  correlation of the shocks, starting from the volatility at ``start_date``

Every combination then runs on every path the way it ran on the real bars -- the same converted
strategy, the same backend, every combination of a path in one call.
"""
import contextlib
import copy
import dataclasses
import datetime
import json
import logging
import math
import time
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
import polars as pl
import structlog

from ziplime.sweep.params import Family, SweepError, Synthetic

#: The fields a path rewrites; ``price`` follows ``close``.
FIELDS = ("open", "high", "low", "close", "volume")
#: What each path's run leaves for the summary.
METRICS = ("sharpe", "total_return", "max_drawdown", "final_value", "fills")


# -- the real bars --------------------------------------------------------------------------------

@dataclasses.dataclass
class Bars:
    """The universe's bars side by side, as the paths are built from them."""

    #: {field: (bars, instruments)}, NaN where an instrument has no row
    fields: dict
    #: The bundle's rows of the universe, and each one's bar and instrument
    rows: np.ndarray
    bar: np.ndarray
    column: np.ndarray
    #: The run's first bar: paths are synthetic from here on
    start: int

    @property
    def shape(self) -> tuple[int, int]:
        return self.fields["close"].shape


def side_by_side(data: pl.DataFrame, universe: dict, start: datetime.date, tz) -> Bars:
    sids = [listing.sid for listing in universe.values()]
    position = {sid: k for k, sid in enumerate(sids)}
    frame = data.with_row_index("_row").filter(pl.col("sid").is_in(sids))
    if frame.is_empty():
        raise SweepError("No bars of the universe to build synthetic paths from.")
    dates = frame["date"]
    if getattr(dates.dtype, "time_zone", None):
        dates = dates.dt.convert_time_zone("UTC")
    stamps = dates.dt.timestamp("ns").to_numpy()
    moments = np.unique(stamps)
    bar = np.searchsorted(moments, stamps)
    column = np.array([position[sid] for sid in frame["sid"].to_list()])
    fields = {}
    for field in FIELDS:
        name = field if field in frame.columns else "price" if field == "close" else None
        if name is None or name not in frame.columns:
            continue
        values = np.full((len(moments), len(sids)), np.nan)
        values[bar, column] = frame[name].cast(pl.Float64).to_numpy()
        fields[field] = values
    if "close" not in fields:
        raise SweepError("The bundle has neither close nor price: no synthetic paths without them.")
    local = pd.DatetimeIndex(pd.to_datetime(moments, utc=True)).tz_convert(tz).date
    after = np.nonzero(local >= start)[0]
    if len(after) == 0:
        raise SweepError(f"No bars from {start} on to build synthetic paths for.")
    # A path continues from a real close: the first bar of the run needs one before it
    return Bars(fields=fields, rows=frame["_row"].to_numpy(), bar=bar, column=column,
                start=max(1, int(after[0])))


@dataclasses.dataclass
class Features:
    """Each real bar as a path reuses it: (bars, instruments) arrays."""

    #: close-to-close log return; opening gap log(open / previous close)
    returns: np.ndarray
    gap: np.ndarray
    #: log(high / close) >= 0 and log(low / close) <= 0
    high: np.ndarray
    low: np.ndarray
    volume: np.ndarray | None
    #: the close carried over missing rows
    close: np.ndarray


def features(bars: Bars) -> Features:
    close = pd.DataFrame(bars.fields["close"]).ffill().to_numpy()
    previous = np.vstack([np.full((1, close.shape[1]), np.nan), close[:-1]])
    with np.errstate(divide="ignore", invalid="ignore"):
        returns = np.log(close / previous)
        opens = bars.fields.get("open")
        gap = np.log(opens / previous) if opens is not None else returns.copy()
        high = np.log(bars.fields["high"] / bars.fields["close"]) if "high" in bars.fields \
            else np.zeros_like(close)
        low = np.log(bars.fields["low"] / bars.fields["close"]) if "low" in bars.fields \
            else np.zeros_like(close)
    returns = np.where(np.isfinite(returns), returns, 0.0)
    gap = np.where(np.isfinite(gap), gap, returns)
    high = np.where(np.isfinite(high), np.maximum(high, 0.0), 0.0)
    low = np.where(np.isfinite(low), np.minimum(low, 0.0), 0.0)
    return Features(returns=returns, gap=gap, high=high, low=low,
                    volume=bars.fields.get("volume"), close=close)


# -- the methods ----------------------------------------------------------------------------------

class Generator:
    """Draws paths of one family: for each path, the real bar each synthetic bar takes its shape
    and volume from, and the log returns ((bars of the run, instruments))."""

    def __init__(self, family: Family, feats: Features, pool: np.ndarray, start: int):
        self.family, self.feats, self.pool, self.start = family, feats, pool, start
        self.length = feats.returns.shape[0] - start
        self.pooled = feats.returns[pool]
        #: each instrument's mean log return over the pool -- what zero drift takes out
        self.drift = self.pooled.mean(axis=0)

    def path(self, rng: np.random.Generator) -> tuple[np.ndarray, np.ndarray]:
        raise NotImplementedError


class Resampled(Generator):
    """Real bars, picked by position in the pool -- the same bar for every instrument."""

    def positions(self, rng) -> np.ndarray:
        raise NotImplementedError

    def path(self, rng):
        source = self.pool[self.positions(rng)]
        returns = self.feats.returns[source]
        if self.family.drift == "zero":
            returns = returns - self.drift
        return source, returns


class IID(Resampled):
    def positions(self, rng):
        return rng.integers(0, len(self.pool), self.length)


class Shuffle(Resampled):
    def positions(self, rng):
        rounds = -(-self.length // len(self.pool))
        return np.concatenate([rng.permutation(len(self.pool)) for _ in range(rounds)])[:self.length]


class Blocks(Resampled):
    """Moving blocks of ``block`` bars; ``circular`` lets a block run past the end into the
    beginning, so every bar is as likely to be drawn."""

    def __init__(self, *args, circular: bool):
        super().__init__(*args)
        self.circular = circular

    def positions(self, rng):
        size = len(self.pool)
        block = min(self.family.block, size)
        count = -(-self.length // block)
        starts = rng.integers(0, size if self.circular else size - block + 1, count)
        return ((starts[:, None] + np.arange(block)).ravel() % size)[:self.length]


class Stationary(Resampled):
    """Blocks of geometric length, ``block`` bars on average, wrapping around the end."""

    def positions(self, rng):
        size = len(self.pool)
        new = rng.random(self.length) < 1.0 / self.family.block
        new[0] = True
        firsts = np.flatnonzero(new)
        group = np.cumsum(new) - 1
        starts = rng.integers(0, size, len(firsts))
        return (starts[group] + np.arange(self.length) - firsts[group]) % size


class Meboot(Generator):
    """Vinod's maximum entropy bootstrap of each instrument's log close over the run, anchored at
    the last real close before it."""

    def __init__(self, *args):
        super().__init__(*args)
        close = self.feats.close
        self.levels = []
        for column in range(close.shape[1]):
            series = np.log(close[self.start - 1:, column])
            first = int(np.argmax(np.isfinite(series))) if np.isfinite(series).any() else None
            self.levels.append((first, series[first:] if first is not None else None))

    def path(self, rng):
        returns = np.zeros((self.length, len(self.levels)))
        for column, (first, levels) in enumerate(self.levels):
            if levels is None or len(levels) < 3:
                continue
            replicate = meboot(levels, rng, trim=self.family.trim,
                               expand_sd=self.family.expand_sd)[0]
            steps = np.diff(replicate - replicate[0] + levels[0])
            if self.family.drift == "zero":
                steps = steps - np.diff(levels).mean()
            returns[first:, column] = steps
        return np.arange(self.start, self.start + self.length), returns


class Gaussian(Generator):
    """Log returns with the pool's means and covariance; ``df`` makes them a multivariate t."""

    def __init__(self, *args, df: float | None):
        super().__init__(*args)
        self.df = df
        covariance = np.atleast_2d(np.cov(self.pooled, rowvar=False))
        jitter = 1e-12 * max(1.0, float(np.trace(covariance)))
        self.factor = np.linalg.cholesky(covariance + jitter * np.eye(len(covariance)))
        self.mean = np.zeros_like(self.drift) if self.family.drift == "zero" else self.drift

    def path(self, rng):
        shocks = rng.standard_normal((self.length, len(self.mean)))
        if self.df is not None:
            # One mixing draw per bar for all instruments: a multivariate t of unit variance
            shocks *= np.sqrt((self.df - 2) / rng.chisquare(self.df, (self.length, 1)))
        source = self.pool[rng.integers(0, len(self.pool), self.length)]
        return source, self.mean + shocks @ self.factor.T


class Garch(Generator):
    """GARCH(1,1) per instrument with variance targeting; the shocks are the real standardised
    residuals of one pooled date for all instruments at once."""

    def __init__(self, *args):
        super().__init__(*args)
        self.mean = np.zeros_like(self.drift) if self.family.drift == "zero" else self.drift
        residuals = self.feats.returns - self.drift
        columns = residuals.shape[1]
        self.params = np.zeros((columns, 3))
        variance = np.zeros_like(residuals)
        for column in range(columns):
            omega, alpha, beta = fit_garch(residuals[self.pool, column])
            self.params[column] = omega, alpha, beta
            variance[:, column] = garch_variance(residuals[:, column], omega, alpha, beta,
                                                 float(np.var(residuals[self.pool, column])))
        sigma = np.sqrt(np.maximum(variance, 1e-300))
        self.standardised = np.where(sigma > 0, residuals / sigma, 0.0)[self.pool]
        # The state at the run's start: the variance the real bars left
        omega, alpha, beta = self.params.T
        self.variance0 = omega + alpha * residuals[self.start - 1] ** 2 + \
            beta * variance[self.start - 1]
        self.omega, self.alpha, self.beta = omega, alpha, beta

    def path(self, rng):
        draws = rng.integers(0, len(self.pool), self.length)
        shocks = self.standardised[draws]
        returns = np.empty((self.length, len(self.mean)))
        variance = self.variance0.copy()
        for t in range(self.length):
            residual = np.sqrt(variance) * shocks[t]
            returns[t] = self.mean + residual
            variance = self.omega + self.alpha * residual ** 2 + self.beta * variance
        return self.pool[draws], returns


def generator(family: Family, feats: Features, pool: np.ndarray, start: int) -> Generator:
    arguments = (family, feats, pool, start)
    return {
        "iid": lambda: IID(*arguments),
        "shuffle": lambda: Shuffle(*arguments),
        "block": lambda: Blocks(*arguments, circular=False),
        "circular": lambda: Blocks(*arguments, circular=True),
        "stationary": lambda: Stationary(*arguments),
        "meboot": lambda: Meboot(*arguments),
        "gbm": lambda: Gaussian(*arguments, df=None),
        "student_t": lambda: Gaussian(*arguments, df=family.df),
        "garch": lambda: Garch(*arguments),
    }[family.method]()


# -- the maximum entropy bootstrap ----------------------------------------------------------------

def meboot(x: np.ndarray, rng: np.random.Generator, reps: int = 1, *, trim: float = 0.10,
           reachbnd: bool = True, expand_sd: bool = True, fiv: float = 5.0) -> np.ndarray:
    """``reps`` replicates of the series ``x`` ((reps, len(x))), as R's ``meboot(x, reps, trim,
    reachbnd, expand.sd, force.clt=FALSE)``.

    The sorted values split the line into intervals at their midpoints, with tails as wide as the
    trimmed mean of the absolute changes; each interval carries probability 1/n and, inside, the
    mean the mean-preserving constraint gives it (Vinod 2004, step 4). Uniform draws, sorted, are
    read off that density and put back in the original order of ranks: every replicate has the
    same ranks as ``x``. ``expand_sd`` widens a replicate whose standard deviation came out
    smaller than the data's to the data's times U(1, 1 + fiv/100) -- around its own mean, where
    R scales the values themselves (the same for the deviations; for log prices R's way would
    move the level). ``force.clt`` only shifts each replicate as a whole, which a path anchored at
    a real close undoes, so there is none.
    """
    x = np.asarray(x, dtype=float)
    n = len(x)
    if n < 3:
        raise SweepError("meboot needs at least 3 values.")
    order = np.argsort(x, kind="stable")
    xx = x[order]
    z = (xx[:-1] + xx[1:]) / 2
    changes = np.sort(np.abs(np.diff(x)))
    cut = int(math.floor(len(changes) * trim))
    trimmed = changes[cut:len(changes) - cut].mean() if len(changes) > 2 * cut else changes.mean()
    xmin, xmax = xx[0] - trimmed, xx[-1] + trimmed
    # The mean of each interval (Vinod 2004, eq. 2): its own value with half the weight, its
    # neighbours with a quarter each; the tails three quarters and a quarter
    desired = np.empty(n)
    desired[0] = 0.75 * xx[0] + 0.25 * xx[1]
    desired[1:-1] = 0.25 * xx[:-2] + 0.5 * xx[1:-1] + 0.25 * xx[2:]
    desired[-1] = 0.25 * xx[-2] + 0.75 * xx[-1]
    lower = np.concatenate([[xmin], z])
    upper = np.concatenate([z, [xmax]])

    p = rng.random((reps, n))
    interval = np.clip(np.ceil(p * n).astype(int) - 1, 0, n - 1)
    fraction = p * n - interval
    q = lower[interval] + fraction * (upper[interval] - lower[interval])
    shift = desired - (lower + upper) / 2
    if reachbnd:
        shift[0] = shift[-1] = 0.0
    q += shift[interval]
    replicates = np.empty_like(q)
    replicates[:, order] = np.sort(q, axis=1)

    if expand_sd:
        sd = x.std(ddof=1)
        spread = replicates.std(axis=1, ddof=1)
        narrow = spread < sd
        if narrow.any():
            target = sd * rng.uniform(1.0, 1.0 + fiv / 100, narrow.sum())
            centre = replicates[narrow].mean(axis=1, keepdims=True)
            spread = spread[narrow]
            factor = np.divide(target, spread, out=np.ones_like(spread), where=spread > 0)
            replicates[narrow] = centre + (replicates[narrow] - centre) * factor[:, None]
    return replicates


# -- GARCH(1,1) -----------------------------------------------------------------------------------

def garch_variance(residuals: np.ndarray, omega: float, alpha: float, beta: float,
                   initial: float) -> np.ndarray:
    """σ²[0] = initial; σ²[t] = ω + α e²[t-1] + β σ²[t-1]."""
    from scipy.signal import lfilter

    variance = np.empty(len(residuals))
    variance[0] = initial
    if len(residuals) > 1:
        drive = omega + alpha * residuals[:-1] ** 2
        variance[1:] = lfilter([1.0], [1.0, -beta], drive, zi=[beta * initial])[0]
    return variance


def fit_garch(residuals: np.ndarray) -> tuple[float, float, float]:
    """(ω, α, β) by Gaussian maximum likelihood, ω = var · (1 - α - β) (variance targeting)."""
    try:
        from scipy.optimize import minimize
    except ImportError:
        raise SweepError("The garch family needs scipy (pip install scipy).") from None
    variance = float(np.var(residuals))
    if variance <= 0 or len(residuals) < 30:
        return variance, 0.0, 0.0

    def loss(parameters):
        alpha, beta = parameters
        if alpha < 0 or beta < 0 or alpha + beta >= 0.999:
            return 1e300
        sigma2 = garch_variance(residuals, variance * (1 - alpha - beta), alpha, beta, variance)
        sigma2 = np.maximum(sigma2, 1e-300)
        return 0.5 * float(np.sum(np.log(sigma2) + residuals ** 2 / sigma2))

    best = minimize(loss, x0=[0.08, 0.88], method="Nelder-Mead",
                    options={"xatol": 1e-5, "fatol": 1e-6, "maxiter": 800})
    alpha, beta = (float(v) for v in best.x)
    if not best.success and loss(best.x) >= loss([0.0, 0.0]):
        alpha, beta = 0.0, 0.0
    return variance * (1 - alpha - beta), alpha, beta


# -- a path as a bundle ---------------------------------------------------------------------------

def build(bars: Bars, feats: Features, source: np.ndarray, returns: np.ndarray) -> dict:
    """The path's bars ((bars, instruments) per field): real before the run, then each bar from
    its log return and the shape of the real bar ``source`` names."""
    start = bars.start
    close_real = feats.close
    base = close_real[start - 1].copy()
    returns = returns.copy()
    # An instrument whose rows begin inside the run starts at its own first close
    for column in np.flatnonzero(~np.isfinite(base)):
        finite = np.flatnonzero(np.isfinite(close_real[start:, column]))
        if len(finite):
            base[column] = close_real[start + finite[0], column]
            returns[:finite[0] + 1, column] = 0.0
    close = base * np.exp(np.cumsum(returns, axis=0))
    previous = np.vstack([base[None, :], close[:-1]])
    opened = previous * np.exp(feats.gap[source])
    high = np.maximum.reduce([close * np.exp(feats.high[source]), opened, close])
    low = np.minimum.reduce([close * np.exp(feats.low[source]), opened, close])
    made = {"close": close, "open": opened, "high": high, "low": low}
    if feats.volume is not None:
        drawn = feats.volume[source]
        made["volume"] = np.where(np.isfinite(drawn), drawn, feats.volume[start:])
    out = {}
    for field, real in bars.fields.items():
        values = real.copy()
        values[start:] = made[field]
        out[field] = values
    return out


def as_bundle(bundle, bars: Bars, path: dict):
    """The real bundle with the universe's rows rewritten to the path: the same rows, the same
    stamps -- only the numbers differ."""
    data = bundle.data
    columns = []
    for field in (*FIELDS, "price"):
        if field not in data.columns:
            continue
        values = path["close" if field == "price" else field]
        if field == "price" and "close" not in path:
            continue
        series = data[field]
        full = series.cast(pl.Float64).to_numpy().copy()
        written = values[bars.bar, bars.column]
        present = np.isfinite(full[bars.rows])
        full[bars.rows] = np.where(present, written, full[bars.rows])
        if series.dtype.is_integer():
            full = np.round(full)
        columns.append(pl.Series(field, full).cast(series.dtype))
    made = copy.copy(bundle)
    made.data = data.with_columns(columns)
    made._last_bars = {}
    made._roll_finders = {}
    return made


# -- the sweep over every path --------------------------------------------------------------------

async def refuse_splits(asset_service, universe: dict, calendar, start, end):
    """A split happens on its date in the asset database, the price jump wherever the path put
    that bar: the two would not meet."""
    from ziplime.sweep.market import corporate_actions, run_sessions

    sessions = run_sessions(calendar, start, end)
    splits, _ = await corporate_actions(asset_service, list(universe.values()), sessions)
    found = [name for column, name in enumerate(universe) if (splits[:, column] != 1.0).any()]
    if found:
        raise SweepError(f"{', '.join(found)} split during the run: a synthetic path moves the "
                         f"split's price jump to another day, and the split itself stays on its "
                         f"own. Sweep on synthetic paths over bars adjusted for splits.")


async def run(*, params, say: Callable[[str], None], bundle, universe: dict, calendar,
              asset_service, once, config_of, output: Path) -> pd.DataFrame:
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


@contextlib.contextmanager
def quiet_logs():
    """The kernel logs every batch it runs; once per path is noise."""
    saved = structlog.get_config()
    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING))
    try:
        yield
    finally:
        structlog.configure(**saved)


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


# -- the comparison -------------------------------------------------------------------------------

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

    def combo(k):
        return {name: (v.item() if hasattr(v, "item") else v)
                for name, v in real.loc[k, parameters].items()}

    def place(rank, total):
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
