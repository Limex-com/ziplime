"""Sampling utils for synthetic."""

import math
import numpy as np
from ziplime.sweep.params import SweepError


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
