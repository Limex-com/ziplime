import datetime

import polars as pl

from ziplime.constants.data_type import DataType
from ziplime.data.services.data_source import DataSource
from ziplime.gens.domain.simulation_clock import SimulationClock
from ziplime.utils.calendar_utils import get_calendar


def test_simulation_clock_normalizes_naive_bounds_and_handles_dst():
    calendar = get_calendar("XNYS")
    clock = SimulationClock(
        start_date=datetime.datetime(2024, 3, 8, 9, 30),
        end_date=datetime.datetime(2024, 3, 11, 16, 0),
        trading_calendar=calendar,
        emission_rate=datetime.timedelta(days=1),
    )

    assert clock.start_date.tzinfo is not None
    assert clock.end_date.tzinfo is not None
    assert list(clock.sessions) == [
        datetime.date(2024, 3, 8),
        datetime.date(2024, 3, 11),
    ]
    assert clock.market_opens[0].utcoffset() != clock.market_opens[1].utcoffset()


def test_data_source_normalizes_naive_query_bounds_to_source_timezone():
    calendar = get_calendar("XNYS")
    source = DataSource(
        name="bars",
        start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
        end_date=datetime.datetime(2024, 1, 3, tzinfo=datetime.timezone.utc),
        trading_calendar=calendar,
        frequency=datetime.timedelta(days=1),
        original_frequency=datetime.timedelta(days=1),
        data_type=DataType.CUSTOM,
    )
    source.data = pl.DataFrame(
        {
            "date": [
                datetime.datetime(2024, 1, 1, tzinfo=calendar.tz),
                datetime.datetime(2024, 1, 2, tzinfo=calendar.tz),
            ],
            "sid": [1, 1],
            "value": [10.0, 20.0],
        }
    )

    result = source.get_data_by_date(
        fields=frozenset({"value"}),
        from_date=datetime.datetime(2024, 1, 1),
        to_date=datetime.datetime(2024, 1, 2),
        frequency=datetime.timedelta(days=1),
        assets=frozenset({type("Asset", (), {"sid": 1})()}),
        include_bounds=True,
    )

    assert result["value"].to_list() == [[10.0, 20.0]]
