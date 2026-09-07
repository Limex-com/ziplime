"""The parts every strategy repeats, so that a strategy is its idea and not its plumbing.

Zipline algorithms are short because two things are given to them: `schedule_function` decides
when the logic runs, and a pipeline turns a column of numbers into a ranked selection. Neither
exists in this fork, so nineteen strategies here each hand-rolled both -- the same five-line
"has it been ninety days yet" gate in twenty files out of twenty, the same loop skipping rows
whose inputs are missing, the same rank-and-equal-weight.

None of that is the strategy. What follows takes it out of the way. A rule that says *hold the
companies whose earnings are cash rather than accruals* should read as roughly that sentence, and
after this it does:

    @every(days=90)
    async def handle_data(context, data):
        rows = await data.current(assets=context.universe, fields=FIELDS,
                                  data_source=context.source)
        scores = factor(fresh(rows, context, MAX_AGE),
                        lambda r: -(r["net_income"] - r["operating_cash_flow"]) / r["total_assets"])
        await hold_top(context, data, scores, keep=40)

These belong in the engine rather than in an examples directory -- `every` in particular is
`schedule_function` wearing a smaller hat. They are here because that is where they can be written
today.
"""
import datetime
import functools

import polars as pl

from portfolio import priced, rebalance

#: Skip a row rather than fail when its inputs are missing. Sparse fundamentals make this the
#: normal case: a company that did not report gross profit is not an error, it is a company that
#: did not report gross profit. Only the two errors that *mean* "missing" are caught -- arithmetic
#: on ``None`` and division by a zero denominator. Anything else is a bug and still raises.
_MISSING = (TypeError, ZeroDivisionError)


def every(days: int):
    """Run the decorated ``handle_data`` at most once every ``days`` calendar days.

    The gate every strategy was writing by hand, with the state kept on the algorithm rather than
    in a module global so two strategies in one process cannot interfere.

    Counting calendar days rather than sessions is deliberate here: these datasets publish on
    filing schedules, not trading ones, and "once a quarter" means ninety days whatever the
    exchange was doing.
    """
    def decorate(handler):
        @functools.wraps(handler)
        async def gated(context, data):
            today = context.simulation_dt.date()
            last = getattr(context, "_last_run", None)
            if last is not None and (today - last).days < days:
                return
            context._last_run = today
            return await handler(context, data)
        return gated
    return decorate


async def equities(context, pairs: list[tuple[str, str]]) -> list:
    """Resolve ``(ticker, MIC)`` pairs to listings.

    The MIC is required rather than optional: a bare ticker is not unique in an asset database,
    and resolving one silently picks a venue that may not be the one the price bundle holds.
    """
    return [await context.symbol(ticker, mic=mic) for ticker, mic in pairs]


def fresh(rows: pl.DataFrame, context, max_age: datetime.timedelta) -> pl.DataFrame:
    """Drop rows whose newest value is older than ``max_age``.

    ``data.current`` returns the freshest thing a source knows, however old that is. For a company
    that stopped filing in 2015 that is a 2015 statement, and without this it stays in the book
    forever.
    """
    if rows.is_empty():
        return rows
    return rows.filter(pl.col("date") >= context.simulation_dt - max_age)


def factor(rows: pl.DataFrame, score) -> dict[int, float]:
    """Score each row, keeping only the instruments the score could be computed for.

    Args:
        rows: A frame carrying ``sid`` and the columns ``score`` reads.
        score: Called with one row as a dict; returns a number, or ``None`` to skip.

    Returns:
        ``{sid: score}``. Rows whose inputs are missing are absent rather than zero -- a company
        that did not report is not a company that reported nothing.
    """
    scores: dict[int, float] = {}
    for row in rows.iter_rows(named=True):
        try:
            value = score(row)
        except _MISSING:
            continue
        if value is None:
            continue
        scores[row["sid"]] = float(value)
    return scores


async def hold_top(context, data, scores: dict[int, float], keep: int,
                   tolerance: float = 0.25) -> dict[int, float]:
    """Hold the ``keep`` highest-scoring instruments, equally weighted.

    Args:
        tolerance: Rebalance band, as a fraction of the target weight. A fraction rather than a
            flat percentage because the target shrinks as the book widens: with 230 names each
            target is 0.43%, so a flat 1% band is wider than the position and nothing ever trades.

    Returns:
        The weights ordered, so a strategy can print what it did.
    """
    ranked = sorted(scores.items(), key=lambda kv: -kv[1])[:keep]
    if not ranked:
        return {}
    weight = 1.0 / len(ranked)
    targets = {sid: weight for sid, _ in ranked}
    await rebalance(context, data, targets, tolerance=weight * tolerance)
    return targets


async def hold(context, data, sids, rebalance_drift: bool = False,
               tolerance: float = 0.0) -> dict[int, float]:
    """Hold a set of instruments equally weighted, trading only when the set changes.

    A signal that says *hold these names* is answered by the membership of the set, not by the
    weights drifting apart. Left to rebalance on drift, the rule quietly becomes "hold these names
    and also sell whichever of them went up", which is a different strategy and a worse one here:
    it cost 62 percentage points over ten years on the insider cluster rule.

    Args:
        rebalance_drift: Also trim back to equal weight between changes. Off by default, because
            the equal weighting is a way of expressing "no view on which of these is better", not
            a target to be defended against the market.
        tolerance: Band as a fraction of the target weight, applied when the set does change.
            Zero by default: the set changing *is* the reason to trade, and a band there means
            some names keep last period's weight for no stated reason. `hold_top` bands instead,
            because a ranking shuffles constantly and re-sorting on every wobble is pure cost.

    Returns:
        The weights held.
    """
    sids = frozenset(sids)
    weight = (1.0 / len(sids)) if sids else 0.0
    targets = {sid: weight for sid in sids}

    unchanged = getattr(context, "_held", None) == sids
    if unchanged and not rebalance_drift:
        return targets
    context._held = sids
    await rebalance(context, data, targets, tolerance=weight * tolerance if weight else 0.0)
    return targets


def any_of(rows: pl.DataFrame, predicate) -> set[int]:
    """The instruments any of whose rows satisfy ``predicate``.

    For signals that fire on an event rather than on a ranking -- a cluster of insider buys, a
    committee agreeing -- where the question is whether it happened in the window at all.
    """
    hits: set[int] = set()
    for row in rows.iter_rows(named=True):
        try:
            if predicate(row):
                hits.add(row["sid"])
        except _MISSING:
            continue
    return hits


def show_once(context, heading: str, lines) -> None:
    """Print a block the first time there is something to say, and never again.

    Every strategy here reports its first book so a reader can see the rule bite. Doing it once
    keeps a ten-year run readable.
    """
    if getattr(context, "_shown", False):
        return
    rendered = list(lines)
    if not rendered:
        return
    context._shown = True
    print(f"{context.simulation_dt.date()} {heading}")
    for line in rendered:
        print(f"    {line}")


__all__ = ["every", "equities", "fresh", "factor", "hold_top", "hold", "any_of", "show_once",
           "priced", "rebalance"]
