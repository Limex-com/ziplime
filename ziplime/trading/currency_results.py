from collections.abc import Iterable
from typing import Any

import pandas as pd


def currency_performance(packets: Iterable[dict[str, Any]]) -> dict[str, pd.DataFrame]:
    """Collect currency-specific daily packets into execution-result tables.

    Parameters
    ----------
    packets : Iterable[dict[str, Any]]
        Performance packets containing optional ``metrics_by_currency`` mappings.
        Currency packets without ``daily_perf`` are skipped.

    Returns
    -------
    dict[str, pandas.DataFrame]
        Tables keyed in first-seen currency order and indexed by ``period_close``.
        Rows merge daily performance, recorded variables, and cumulative risk
        metrics. Raw recorded variables are not currency-converted. Missing
        currency packets produce an empty mapping, not synthetic native reports.
    """
    rows: dict[str, list[dict[str, Any]]] = {}
    for packet in packets:
        for currency, converted in packet.get("metrics_by_currency", {}).items():
            if "daily_perf" not in converted:
                continue
            row = dict(converted["daily_perf"])
            row.update(row.pop("recorded_vars", {}))
            row.update(converted["cumulative_risk_metrics"])
            rows.setdefault(currency, []).append(row)
    return {
        currency: pd.DataFrame(data=daily, index=pd.DatetimeIndex(data=[row["period_close"] for row in daily]))
        for currency, daily in rows.items()
    }
