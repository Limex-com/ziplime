"""Python constructs the sweep converts, each a hook writing its answer to context.out."""
import math
import statistics

import numpy as np
import pandas as pd
import polars as pl
from numpy import log

import sweep_helpers_lib
from sweep_helpers_lib import bounded_score


def clipped(value, low):
    if value < low:
        return low
    return value


async def branches(context, data):
    if context.x > context.y:
        out = 1.0
    elif context.x == context.y:
        out = 2.0
    else:
        out = 3.0
    context.out = out


async def early_return(context, data):
    context.out = 0.0
    if context.x < 0:
        return
    context.out = context.x * 2


async def logic(context, data):
    context.out = 1.0 if (context.x > 0 and not context.y > 0) or context.x > 5 else -1.0


async def assign_in_branch(context, data):
    total = context.x
    if context.y > 1:
        total += context.y
        total = total * 2
    context.out = total


async def helper_with_return(context, data):
    context.out = clipped(context.x, context.y) + 1


async def builtins_and_math(context, data):
    value = max(context.x, context.y) - min(context.x, 0.5)
    if math.isnan(context.y) or abs(context.x) > 3:
        value = int(context.x)
    context.out = float(value)


async def chained(context, data):
    context.out = 1.0 if 0 < context.x < context.y else 0.0


async def loop_with_continue(context, data):
    total = 0.0
    for step in [1.0, 2.0, 3.0]:
        if context.x > step:
            continue
        total = total + step
    context.out = total


async def uniform_while(context, data):
    k = 0
    total = context.x * 0
    while k < 3:
        total = total + context.x
        k += 1
    context.out = total


async def carried_over_instruments(context, data):
    budget = 100.0
    for name, asset in context.universe.items():
        if context.x > 0:
            budget -= 10.0
    context.out = budget


async def uses_try(context, data):
    try:
        context.out = context.x
    except ValueError:
        context.out = 0.0


async def break_on_data(context, data):
    for step in [1.0, 2.0]:
        if context.x > step:
            break
    context.out = 0.0


async def numpy_elementwise(context, data):
    value = np.where(context.x > 0, np.log(np.abs(context.x) + 1), np.clip(context.x, -1, 1))
    context.out = float(value) + log(2.0) + np.sqrt(np.square(context.y))


async def numpy_reductions(context, data):
    context.out = (np.mean([context.x, context.y, 3.0]) + np.std([context.x, 2.0])
                   + np.max([context.x, 0.5]) + np.nanmean([context.x, context.y]) + np.sum(context.x))


async def numpy_with_axis(context, data):
    context.out = np.mean(context.x, axis=0)


async def numpy_unsupported(context, data):
    context.out = np.cumsum([context.x, context.y])[0]


async def polars_col(context, data):
    frame = await data.history(assets=list(context.universe.values()), bar_count=5,
                               fields=["close"])
    context.out = frame.group_by("sid").agg(pl.col("close").mean())


async def polars_other(context, data):
    frame = await data.history(assets=list(context.universe.values()), bar_count=5,
                               fields=["close"])
    context.out = frame.with_columns(pl.lit(1.0))


async def uses_pandas(context, data):
    context.out = 1.0 if pd.isna(context.x) else 0.0


async def uses_a_model(context, data):
    context.out = context.model.cdf(context.x)


async def uses_statistics(context, data):
    context.out = statistics.mean([context.x, context.y])


async def own_module_helpers(context, data):
    context.out = bounded_score(context.x, context.y) + sweep_helpers_lib.bounded_score(context.y, 0.0)


async def cross_sections(context, data):
    scores = {}
    for name, asset in context.universe.items():
        if context.x > -1 or name == "A":
            scores[name] = context.x + (1.0 if name == "A" else 2.0)
    if any(score != score for score in scores.values()):
        context.out = -1.0
        return
    best = sorted(scores, key=scores.get, reverse=True)[:1]
    context.out = sum(scores[name] for name in best) + len([n for n in scores if scores[n] > 0]) \
        + max(scores.values()) + min(s for s in scores.values())
