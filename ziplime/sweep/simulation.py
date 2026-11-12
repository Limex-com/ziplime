"""A sweep called the way run_simulation is called: the same arguments, and the grid.

    result = await run_simulation(algorithm_file="trend.py", config_file="trend.json", ...)
    table = await sweep_simulation(algorithm_file="trend.py", config_file="trend.json", ...,
                                   grid={"fast": [5, 10, 20], "slow": {"from": 30, "to": 90}})

Every argument means what it means for run_simulation -- the dates, the calendar, the cash, the
bundle as ``market_data_source``, the asset service, the cost models, ``same_bar_execution`` and
``price_used_in_order_execution`` -- and has the same default. ``grid`` and the rest of the sweep
file's fields (``where``, ``backend``, ``refine_top``, ``verify``...) are the sweep's own; see
:class:`~ziplime.sweep.params.SweepParams`. The best combination's config is written to
``output_dir/best_config.json``, a config file run_simulation takes as it is.
"""
import datetime
from pathlib import Path
from typing import Any, Callable, Literal

import pandas as pd

from ziplime.sweep.params import Commission, Slippage, SweepError, SweepParams


async def sweep_simulation(
        *,
        grid: dict[str, Any],
        start_date: datetime.date | datetime.datetime,
        end_date: datetime.date | datetime.datetime,
        algorithm_file: str | Path,
        market_data_source=None,
        asset_service=None,
        custom_data_sources: list | None = None,
        trading_calendar: str = "XNYS",
        emission_rate: datetime.timedelta | str = datetime.timedelta(days=1),
        total_cash: float = 100_000.0,
        config_file: str | Path | None = None,
        equity_commission=None,
        equity_slippage=None,
        future_commission=None,
        future_slippage=None,
        same_bar_execution: bool = True,
        price_used_in_order_execution: Literal["open", "close"] = "close",
        say: Callable[[str], None] = print,
        **sweep: Any,
) -> pd.DataFrame:
    """Every combination of ``grid`` through the strategy, as run_simulation runs one.

    ``sweep``: any other field of a sweep file -- ``where``, ``backend``, ``output_dir``,
    ``verify``, ``refine_top``, ``sort_by``, ``min_fills``, ``compile``, ``batch_size``,
    ``workers``, ``on_unsupported``, ``config`` (overrides of ``config_file`` for every
    combination), or ``bundle`` / ``asset_db`` when ``market_data_source`` / ``asset_service``
    are not given. Returns every combination's metrics, best first; whether the best one's trades
    agree with run_simulation is in ``table.attrs["verified"]``.
    """
    algorithm_file = Path(algorithm_file).resolve()
    fields = {
        "algorithm_file": str(algorithm_file),
        "config_file": str(Path(config_file).resolve()) if config_file else None,
        "start_date": _date(start_date),
        "end_date": _date(end_date),
        "trading_calendar": trading_calendar,
        "emission_rate": _rate(emission_rate),
        "total_cash": total_cash,
        "timing": _timing(same_bar_execution, price_used_in_order_execution),
        "grid": grid,
        **sweep,
    }
    for name, model, spec in (("equity_commission", equity_commission, Commission),
                               ("future_commission", future_commission, Commission),
                               ("equity_slippage", equity_slippage, Slippage),
                               ("future_slippage", future_slippage, Slippage)):
        if model is not None:
            fields[name] = spec.of(model)
    params = SweepParams.model_validate(fields)
    from ziplime.sweep.runner import run_sweep
    return await run_sweep(params, algorithm_file.parent, say=say, market_data=market_data_source,
                           asset_service=asset_service, custom_data_sources=custom_data_sources)


def _date(value) -> datetime.date:
    return value.date() if isinstance(value, datetime.datetime) else value


def _rate(value) -> str:
    """run_simulation's emission_rate as a sweep file writes it: 1d, 1h, 5m."""
    if isinstance(value, str):
        return value
    seconds = int(value.total_seconds())
    if seconds == 86_400:
        return "1d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    raise SweepError(f"emission_rate {value} is not a whole number of minutes.")


def _timing(same_bar_execution: bool, price: str) -> str:
    """run_simulation's execution settings as the sweep's ``timing``."""
    if same_bar_execution:
        if price != "close":
            raise SweepError("A sweep fills on the same bar at its close only "
                             "(price_used_in_order_execution='close').")
        return "same_close"
    if price in ("close", "open"):
        return f"next_{price}"
    raise SweepError(f"A sweep fills at the next bar's close or open, not its {price}.")
