import datetime
import unittest

import polars as pl

from ziplime.sources.benchmark_source import BenchmarkSource


def make_source(bars: dict[datetime.date, float]) -> BenchmarkSource:
    series = pl.DataFrame({
        "date": [datetime.datetime.combine(day, datetime.time()) for day in bars],
        "close": list(bars.values()),
    })
    return BenchmarkSource(
        asset_service=None,
        trading_calendar=None,
        sessions=None,
        exchange=None,
        emission_rate=datetime.timedelta(days=1),
        benchmark_fields=frozenset({"close"}),
        precalculated_series=series,
    )


class DailyReturnsBySessionTests(unittest.TestCase):
    sessions = [datetime.date(2026, 9, day) for day in (21, 22, 23, 24)]

    def test_one_row_per_session_in_session_order(self):
        # The index skips the 23rd and carries a bar on Saturday the 26th: positions
        # must still be sessions, or every metric read after the gap is misaligned.
        source = make_source({
            datetime.date(2026, 9, 21): 100.0,
            datetime.date(2026, 9, 22): 110.0,
            datetime.date(2026, 9, 24): 121.0,
            datetime.date(2026, 9, 26): 130.0,
        })

        returns = source.daily_returns_by_session(self.sessions)

        self.assertEqual(returns["date"].to_list(), self.sessions)
        pct = returns["pct_change"].to_list()
        self.assertAlmostEqual(pct[1], 0.10)
        self.assertEqual(pct[2], 0.0, "a session the benchmark did not trade is a flat day")
        self.assertAlmostEqual(pct[3], 0.10)

    def test_no_benchmark_bars_is_flat_rather_than_empty(self):
        source = make_source({datetime.date(2026, 1, 5): 100.0})

        returns = source.daily_returns_by_session(self.sessions)

        self.assertEqual(returns.height, len(self.sessions))
        self.assertEqual(returns["pct_change"].to_list(), [0.0] * len(self.sessions))

    def test_accepts_session_datetimes(self):
        source = make_source({datetime.date(2026, 9, 21): 100.0, datetime.date(2026, 9, 22): 101.0})
        stamps = [datetime.datetime.combine(day, datetime.time()) for day in self.sessions[:2]]

        returns = source.daily_returns_by_session(stamps)

        self.assertEqual(returns["date"].to_list(), self.sessions[:2])


if __name__ == "__main__":
    unittest.main()
