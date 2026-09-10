"""Reading a trailing window by calendar time rather than by row count.

``data.history(bar_count=30)`` asks for thirty rows. On bar data that is thirty sessions, which is
what a strategy means. On **event data** it is not: a source that emits a row only when something
was disclosed can put thirty rows across four years for one instrument and across four months for
another, so the same call means a different span per asset -- and a strategy asking for "the last
quarter" gets an answer about how often that company's insiders file.

``since=timedelta(days=90)`` asks the question the strategy actually has. These tests pin the two
apart on a source built to make the difference visible: one instrument filing weekly, another
filing twice a decade.
"""
import datetime
import unittest

import polars as pl

from ziplime.constants.data_type import DataType
from ziplime.data.services.data_source import DataSource

NOW = datetime.datetime(2024, 6, 30, tzinfo=datetime.timezone.utc)


class Asset:
    """The only thing these reads use of an asset is its sid."""

    def __init__(self, sid: int):
        self.sid = sid

    def __hash__(self):
        return hash(self.sid)


FREQUENT = Asset(1)   # files every week
RARE = Asset(2)       # files twice in ten years


def make_source() -> DataSource:
    """A source whose two instruments disclose on wildly different rhythms."""
    rows = []
    # 200 weekly rows for the frequent filer, ending a week before NOW.
    for week in range(200):
        rows.append({"date": NOW - datetime.timedelta(weeks=week + 1), "sid": FREQUENT.sid,
                     "value": float(week)})
    # Two rows for the rare filer: one last month, one nine years ago.
    rows.append({"date": NOW - datetime.timedelta(days=30), "sid": RARE.sid, "value": 1.0})
    rows.append({"date": NOW - datetime.timedelta(days=9 * 365), "sid": RARE.sid, "value": 2.0})

    source = DataSource(name="events", start_date=datetime.date(2000, 1, 1),
                        end_date=datetime.date(2030, 1, 1),
                        frequency=datetime.timedelta(days=1),
                        original_frequency=datetime.timedelta(days=1),
                        data_type=DataType.CUSTOM)
    source.data = pl.DataFrame(rows).sort("date")
    return source


class WindowTests(unittest.IsolatedAsyncioTestCase):
    async def read(self, **kwargs):
        defaults = dict(fields=frozenset({"value"}), end_date=NOW,
                        frequency=datetime.timedelta(days=1),
                        assets=frozenset({FREQUENT, RARE}), include_end_date=False)
        return await make_source().get_data_by_window(**{**defaults, **kwargs})

    async def test_a_quarter_returns_a_quarter_for_both_instruments(self):
        rows = await self.read(since=datetime.timedelta(days=90))
        oldest = rows["date"].min()

        self.assertGreater(oldest, NOW - datetime.timedelta(days=91))
        # 13 weekly rows for the frequent filer, 1 for the rare one -- and crucially the rare
        # filer's nine-year-old row is not in it.
        self.assertEqual(rows.filter(pl.col("sid") == RARE.sid).height, 1)
        self.assertGreater(rows.filter(pl.col("sid") == FREQUENT.sid).height, 10)

    async def test_the_rare_filer_gets_no_stale_row_smuggled_in(self):
        # This is the whole point. `bar_count=30` on the rare filer reaches back nine years,
        # because thirty rows is all it has; `since` cannot.
        by_count = await make_source().get_data_by_limit(
            fields=frozenset({"value"}), limit=30, end_date=NOW,
            frequency=datetime.timedelta(days=1), assets=frozenset({RARE}),
            include_end_date=False)
        by_time = await self.read(since=datetime.timedelta(days=90), assets=frozenset({RARE}))

        self.assertEqual(by_count.height, 2)
        self.assertLess(by_count["date"].min(), NOW - datetime.timedelta(days=8 * 365))
        self.assertEqual(by_time.height, 1)
        self.assertGreater(by_time["date"].min(), NOW - datetime.timedelta(days=91))

    async def test_a_row_at_the_near_edge_is_excluded_by_default(self):
        source = make_source()
        source.data = pl.concat([
            source.data,
            pl.DataFrame([{"date": NOW, "sid": FREQUENT.sid, "value": 99.0}]),
        ]).sort("date")

        excluded = await source.get_data_by_window(
            fields=frozenset({"value"}), since=datetime.timedelta(days=7), end_date=NOW,
            frequency=datetime.timedelta(days=1), assets=frozenset({FREQUENT}),
            include_end_date=False)
        included = await source.get_data_by_window(
            fields=frozenset({"value"}), since=datetime.timedelta(days=7), end_date=NOW,
            frequency=datetime.timedelta(days=1), assets=frozenset({FREQUENT}),
            include_end_date=True)

        self.assertNotIn(99.0, excluded["value"].to_list())
        self.assertIn(99.0, included["value"].to_list())

    async def test_it_returns_the_same_shape_as_the_count_based_read(self):
        # The two must be interchangeable at the call site, or the choice becomes a rewrite.
        by_time = await self.read(since=datetime.timedelta(days=90))
        by_count = await make_source().get_data_by_limit(
            fields=frozenset({"value"}), limit=13, end_date=NOW,
            frequency=datetime.timedelta(days=1), assets=frozenset({FREQUENT, RARE}),
            include_end_date=False)

        self.assertEqual(sorted(by_time.columns), sorted(by_count.columns))
        self.assertEqual(by_time["date"].dtype, by_count["date"].dtype)

    async def test_rows_come_back_in_date_order(self):
        rows = await self.read(since=datetime.timedelta(days=365))
        self.assertEqual(rows["date"].to_list(), sorted(rows["date"].to_list()))

    async def test_only_the_requested_instruments_come_back(self):
        rows = await self.read(since=datetime.timedelta(days=365), assets=frozenset({RARE}))
        self.assertEqual(set(rows["sid"].to_list()), {RARE.sid})

    async def test_an_empty_window_is_empty_rather_than_an_error(self):
        rows = await self.read(since=datetime.timedelta(days=1), assets=frozenset({RARE}))
        self.assertTrue(rows.is_empty())

    async def test_a_zero_or_negative_span_is_refused(self):
        for span in (datetime.timedelta(0), datetime.timedelta(days=-30)):
            with self.assertRaises(ValueError):
                await self.read(since=span)


class HistoryArgumentTests(unittest.IsolatedAsyncioTestCase):
    """`history` takes one of the two, and says so rather than guessing."""

    def make_bar_data(self):
        from unittest.mock import Mock
        from ziplime.domain.bar_data import BarData
        from ziplime.finance.asset_restrictions import NoRestrictions

        source = make_source()
        source.name = "events"
        calendar = Mock()
        return BarData(data_sources={"events": source}, simulation_dt_func=lambda: NOW,
                       trading_calendar=calendar, restrictions=NoRestrictions())

    async def test_passing_neither_is_refused(self):
        with self.assertRaises(ValueError) as raised:
            await self.make_bar_data().history(assets=[FREQUENT], fields=["value"])
        self.assertIn("bar_count", str(raised.exception))

    async def test_passing_both_is_refused(self):
        with self.assertRaises(ValueError):
            await self.make_bar_data().history(
                assets=[FREQUENT], bar_count=10, since=datetime.timedelta(days=90),
                fields=["value"])

    async def test_since_reaches_the_window_read(self):
        rows = await self.make_bar_data().history(
            assets=[FREQUENT, RARE], since=datetime.timedelta(days=90), fields=["value"])
        self.assertGreater(rows["date"].min(), NOW - datetime.timedelta(days=91))


if __name__ == "__main__":
    unittest.main(verbosity=2)
