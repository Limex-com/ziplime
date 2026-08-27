"""Futures acceptance suite: calendars, sessions and bar semantics (#20-#24).

Numbered to match the acceptance checklist.
"""
import datetime
import unittest

import pandas as pd
import polars as pl

from futures_fixtures import make_bundle, make_future, session

from ziplime.assets.domain.roll_finder import CalendarRollFinder
from ziplime.utils.calendar_utils import get_calendar


class SessionVersusCalendarDayTests(unittest.IsolatedAsyncioTestCase):
    """#22: a roll offset must state whether it counts calendar days or trading sessions."""

    def _chain(self, auto_close: datetime.date):
        return [
            make_future(sid=1, symbol="CLM23", root_symbol="CL",
                        start=datetime.date(2022, 1, 3),
                        expiration=auto_close - datetime.timedelta(days=1)),
            make_future(sid=2, symbol="CLU23", root_symbol="CL",
                        start=datetime.date(2022, 1, 3), expiration=datetime.date(2023, 9, 21)),
        ]

    async def test_calendar_day_and_session_offsets_disagree(self):
        # The contract auto-closes on Tuesday 2023-06-20. A three-session offset spans a weekend
        # and the Juneteenth holiday, so it reaches further back than three calendar days:
        #   from Wed 06-14: +3 days -> 06-17 (still front), +3 sessions -> 06-20 (rolled)
        #   from Fri 06-16: +3 days -> 06-19 (still front), +3 sessions -> 06-22 (rolled)
        auto_close = datetime.date(2023, 6, 20)
        front, back = self._chain(auto_close)
        bundle = make_bundle({c: [(datetime.date(2023, 6, 12), 70.0, 100.0)] for c in (front, back)})
        calendar = get_calendar("XNYS")

        by_days = CalendarRollFinder(asset_service=bundle.asset_service, roll_offset_days=3)
        by_sessions = CalendarRollFinder(asset_service=bundle.asset_service,
                                         roll_offset_sessions=3, trading_calendar=calendar)

        for day in (datetime.date(2023, 6, 14), datetime.date(2023, 6, 16)):
            self.assertEqual((await by_days.get_contract_center("CL", day, 0)).sid, front.sid,
                             f"{day}: a calendar-day offset has not reached auto close yet")
            self.assertEqual((await by_sessions.get_contract_center("CL", day, 0)).sid, back.sid,
                             f"{day}: a session offset has, because weekends and holidays count")

    def test_the_two_offsets_cannot_be_combined(self):
        with self.assertRaises(ValueError):
            CalendarRollFinder(asset_service=None, roll_offset_days=2, roll_offset_sessions=2,
                               trading_calendar=get_calendar("XNYS"))

    def test_session_offset_requires_a_calendar(self):
        with self.assertRaises(ValueError):
            CalendarRollFinder(asset_service=None, roll_offset_sessions=2)


class HolidayTests(unittest.TestCase):
    """#22: sessions to expiration is not the same number as calendar days to expiration."""

    def test_a_holiday_weekend_shows_the_difference(self):
        calendar = get_calendar("XNYS")
        friday = datetime.date(2023, 6, 30)
        following_wednesday = datetime.date(2023, 7, 5)   # 4 July is a holiday
        calendar_days = (following_wednesday - friday).days
        sessions = len(calendar.sessions_in_range(pd.Timestamp(friday),
                                                  pd.Timestamp(following_wednesday))) - 1
        self.assertEqual(calendar_days, 5)
        self.assertEqual(sessions, 2, "Saturday, Sunday and Independence Day are not sessions")
        self.assertNotEqual(calendar_days, sessions)


class DaylightSavingTests(unittest.TestCase):
    """#21: session boundaries must stay correct across a DST transition."""

    def test_session_opens_and_closes_survive_a_dst_change(self):
        calendar = get_calendar("XNYS")
        before = pd.Timestamp("2023-03-10")   # Friday before US DST starts
        after = pd.Timestamp("2023-03-13")    # Monday after
        for day in (before, after):
            opens = calendar.session_open(day)
            closes = calendar.session_close(day)
            local_open = opens.tz_convert(calendar.tz)
            local_close = closes.tz_convert(calendar.tz)
            self.assertEqual((local_open.hour, local_open.minute), (9, 30))
            self.assertEqual((local_close.hour, local_close.minute), (16, 0))
        # The wall-clock session is unchanged while its UTC offset moves, which is the point.
        self.assertNotEqual(calendar.session_open(before).tz_convert(calendar.tz).utcoffset(),
                            calendar.session_open(after).tz_convert(calendar.tz).utcoffset())

    def test_moscow_has_no_dst_so_sessions_are_stable(self):
        calendar = get_calendar("XMOS")
        for day in (pd.Timestamp("2023-03-24"), pd.Timestamp("2023-03-27"),
                    pd.Timestamp("2023-10-27"), pd.Timestamp("2023-10-30")):
            local_open = calendar.session_open(day).tz_convert(calendar.tz)
            self.assertEqual((local_open.hour, local_open.minute), (10, 0))
            self.assertEqual(local_open.utcoffset(), datetime.timedelta(hours=3),
                             "Moscow has had no DST since 2014")


class BarSemanticsTests(unittest.IsolatedAsyncioTestCase):
    """#20 and #23: bars are labelled by session, and `close` has a stated meaning."""

    async def test_daily_bars_are_labelled_by_session_start_in_exchange_time(self):
        contract = make_future(sid=1, expiration=datetime.date(2023, 9, 21))
        days = [datetime.date(2023, 6, d) for d in (12, 13, 14)]
        bundle = make_bundle({contract: [(d, 70.0, 100.0) for d in days]})
        labelled = bundle.get_dataframe().with_columns(
            pl.col("date").dt.convert_time_zone(str(bundle.trading_calendar.tz)).alias("local"))
        for value, expected in zip(labelled["local"], days):
            self.assertEqual(value.date(), expected)
            self.assertEqual((value.hour, value.minute), (0, 0),
                             "a daily bar is stamped at the start of its session")

    async def test_the_close_field_is_documented(self):
        # #23: whatever `close` means, it must be written down rather than assumed.
        from ziplime.data.data_sources.finam import finam_data_source
        source = finam_data_source.__doc__ or ""
        module = finam_data_source.FinamDataSource.__doc__ or ""
        self.assertTrue("session" in module.lower() or "session" in source.lower(),
                        "the connector must state how its bars are dated")

    async def test_a_bar_never_lands_on_the_wrong_side_of_midnight(self):
        # #20: converting a UTC-stored bar to the exchange's day must not shift it.
        contract = make_future(sid=1, expiration=datetime.date(2023, 9, 21))
        day = datetime.date(2023, 6, 12)
        bundle = make_bundle({contract: [(day, 70.0, 100.0)]})
        stored = bundle.get_dataframe()["date"][0]
        self.assertEqual(stored.astimezone(bundle.trading_calendar.tz).date(), day)
        self.assertEqual(session(bundle, day), stored)


class SameBarExecutionDisclosureTests(unittest.TestCase):
    """#24: filling on the bar that produced the decision is disclosed as look-ahead."""

    def test_the_warning_names_look_ahead(self):
        import inspect
        from ziplime.trading.trading_algorithm import TradingAlgorithm
        source = inspect.getsource(TradingAlgorithm.__init__)
        self.assertIn("LOOK-AHEAD", source)
        self.assertIn("same_bar_execution=False", source,
                      "the warning must say how to turn it off")


if __name__ == "__main__":
    unittest.main()
