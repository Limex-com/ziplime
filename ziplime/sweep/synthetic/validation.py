"""Validation for synthetic."""

import datetime
from exchange_calendars import ExchangeCalendar
from ziplime.sweep.params import SweepError
from ziplime.assets.services.asset_service import AssetService


async def refuse_splits(
    asset_service: AssetService,
    universe: dict,
    calendar: ExchangeCalendar,
    start: datetime.date,
    end: datetime.date,
) -> None:
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
