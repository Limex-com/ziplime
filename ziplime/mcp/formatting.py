"""The engine's output, reduced to what an assistant can read back to a person.

A performance frame is one row per bar with dozens of columns. Handing that to
a model wastes its context and buries the four numbers anybody asked for, so
the summary comes first and the rows are optional, thinned, and never silently
truncated.
"""
from __future__ import annotations

import math

#: Columns worth reporting from a run, in the order they answer questions.
_HEADLINE = (
    "portfolio_value", "returns", "algorithm_period_return",
    "benchmark_period_return", "sharpe", "sortino", "max_drawdown",
    "volatility", "alpha", "beta",
)


def _finite(value):
    """A JSON-safe number, or None. NaN and inf are not values, they are gaps."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(number) or math.isinf(number) else round(number, 6)


def summarise(perf) -> dict:
    """The end state of a run: what it returned, how bumpy, how much it traded.

    Read off the **last row**, which is how these cumulative columns are
    defined. A column the engine did not produce is absent rather than zero —
    reporting a Sharpe of 0.0 for a run that computed none is a lie a model
    will repeat.
    """
    if perf is None or getattr(perf, "empty", True):
        return {"bars": 0, "note": "The run produced no performance rows."}

    last = perf.iloc[-1]
    summary: dict = {"bars": int(len(perf))}
    for column in _HEADLINE:
        if column in perf.columns:
            value = _finite(last.get(column))
            if value is not None:
                summary[column] = value

    if "portfolio_value" in perf.columns:
        start = _finite(perf.iloc[0].get("portfolio_value"))
        end = _finite(last.get("portfolio_value"))
        if start and end:
            summary["starting_portfolio_value"] = start
            summary["ending_portfolio_value"] = end
            summary["total_return_percent"] = round((end / start - 1) * 100, 4)

    if "transactions" in perf.columns:
        summary["transactions"] = int(
            sum(len(row) if isinstance(row, (list, tuple)) else 0 for row in perf["transactions"])
        )
    if "orders" in perf.columns:
        summary["orders"] = int(
            sum(len(row) if isinstance(row, (list, tuple)) else 0 for row in perf["orders"])
        )
    return summary


def equity_curve(perf, limit: int = 100) -> dict:
    """The portfolio value over time, thinned evenly so its shape survives.

    Every k-th row, not the first k — a head-truncated curve of a three-year
    run is a picture of its first fortnight.
    """
    if perf is None or getattr(perf, "empty", True) or "portfolio_value" not in perf.columns:
        return {"rows": [], "returned": 0, "row_count": 0, "truncated": False}

    total = len(perf)
    step = max(1, math.ceil(total / max(1, limit)))
    rows = []
    for position in range(0, total, step):
        row = perf.iloc[position]
        rows.append({
            "date": str(perf.index[position])[:19],
            "portfolio_value": _finite(row.get("portfolio_value")),
        })
    # The last bar is the one anybody checks; evenly stepping can skip it.
    if rows and rows[-1]["date"] != str(perf.index[-1])[:19]:
        rows.append({
            "date": str(perf.index[-1])[:19],
            "portfolio_value": _finite(perf.iloc[-1].get("portfolio_value")),
        })
    return {
        "rows": rows,
        "returned": len(rows),
        "row_count": total,
        "truncated": len(rows) < total,
        "thinning": f"every {step} bar(s)" if step > 1 else "every bar",
    }


def compare(runs: list[dict]) -> dict:
    """Several runs side by side on the fields they share."""
    fields = ("total_return_percent", "sharpe", "max_drawdown", "volatility",
              "transactions", "bars")
    table = []
    for run in runs:
        summary = run.get("summary") or {}
        row = {"backtest_id": run.get("backtest_id"), "strategy": run.get("strategy")}
        row.update({field: summary.get(field) for field in fields})
        table.append(row)

    best = None
    scored = [row for row in table if isinstance(row.get("sharpe"), (int, float))]
    if scored:
        best = max(scored, key=lambda row: row["sharpe"])["backtest_id"]
    return {
        "runs": table,
        "highest_sharpe": best,
        "note": (
            "Different date ranges, capital or bundles make these numbers "
            "incomparable. Check the parameters before reading the ranking."
        ),
    }
