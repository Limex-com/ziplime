"""Futures acceptance suite: look-ahead invariants (#11, #12, #16).

Look-ahead is the failure mode that makes a backtest look good and be worthless, so these are
checked directly rather than inferred from results.
"""
import datetime
import unittest

from futures_fixtures import make_bundle, make_future, session

from ziplime.assets.domain.roll_finder import CalendarRollFinder, VolumeRollFinder

SESSIONS = [datetime.date(2023, 6, d) for d in (12, 13, 14, 15, 16, 20, 21, 22)]


def chain():
    return [make_future(sid=1, symbol="CLM23", root_symbol="CL",
                        start=datetime.date(2022, 1, 3), expiration=datetime.date(2023, 6, 30)),
            make_future(sid=2, symbol="CLU23", root_symbol="CL",
                        start=datetime.date(2022, 1, 3), expiration=datetime.date(2023, 9, 30))]


class VolumeRollLookAheadTests(unittest.IsolatedAsyncioTestCase):
    """#11: the roll decision must not use volume the market has not printed yet."""

    def _bundle(self, crossover: datetime.date):
        front, back = chain()
        return front, back, make_bundle({
            front: [(d, 70.0, 100.0 if d < crossover else 10.0) for d in SESSIONS],
            back: [(d, 72.0, 10.0 if d < crossover else 900.0) for d in SESSIONS],
        })

    async def test_decision_uses_only_completed_sessions(self):
        crossover = datetime.date(2023, 6, 15)
        front, back, bundle = self._bundle(crossover)
        finder = VolumeRollFinder(asset_service=bundle.asset_service, data_source=bundle,
                                  grace_period_days=0)

        # On the crossover session itself, that session's volume is still being formed. A decision
        # made during it may only use the previous completed session, which still favoured `front`.
        on_crossover = await finder.get_contract_center("CL", crossover, 0)
        self.assertEqual(on_crossover.sid, front.sid,
                         "rolling on the same session uses volume that is not known yet")

        # The next session may act on it.
        next_session = datetime.date(2023, 6, 16)
        self.assertEqual((await finder.get_contract_center("CL", next_session, 0)).sid, back.sid)

    async def test_roll_schedule_is_stable_when_future_data_is_appended(self):
        # #16 in miniature: extending the data must not change earlier roll decisions.
        crossover = datetime.date(2023, 6, 15)
        front, back, _ = self._bundle(crossover)

        short_sessions = SESSIONS[:5]
        long_sessions = SESSIONS

        def build(sessions):
            return make_bundle({
                front: [(d, 70.0, 100.0 if d < crossover else 10.0) for d in sessions],
                back: [(d, 72.0, 10.0 if d < crossover else 900.0) for d in sessions],
            })

        short_finder = VolumeRollFinder(asset_service=build(short_sessions).asset_service,
                                        data_source=build(short_sessions), grace_period_days=0)
        long_finder = VolumeRollFinder(asset_service=build(long_sessions).asset_service,
                                       data_source=build(long_sessions), grace_period_days=0)

        for day in short_sessions:
            short = await short_finder.get_contract_center("CL", day, 0)
            long = await long_finder.get_contract_center("CL", day, 0)
            self.assertEqual(short.sid, long.sid,
                             f"{day}: the past changed when future data was added")


class CalendarRollSessionTests(unittest.IsolatedAsyncioTestCase):
    """#12: the calendar roll lands on a defined session relative to auto-close."""

    async def test_roll_offset_is_measured_in_days_before_auto_close(self):
        front, back = chain()
        bundle = make_bundle({c: [(d, 70.0, 100.0) for d in SESSIONS] for c in (front, back)})
        auto_close = front.auto_close_date  # 2023-06-30

        for offset in (0, 1, 3):
            finder = CalendarRollFinder(asset_service=bundle.asset_service,
                                        roll_offset_days=offset)
            last_day_on_front = auto_close - datetime.timedelta(days=offset + 1)
            first_day_on_back = auto_close - datetime.timedelta(days=offset)
            self.assertEqual(
                (await finder.get_contract_center("CL", last_day_on_front, 0)).sid, front.sid,
                f"offset={offset}: rolled a session too early")
            self.assertEqual(
                (await finder.get_contract_center("CL", first_day_on_back, 0)).sid, back.sid,
                f"offset={offset}: rolled a session too late")

    async def test_a_contract_is_never_active_after_its_auto_close(self):
        front, back = chain()
        bundle = make_bundle({c: [(d, 70.0, 100.0) for d in SESSIONS] for c in (front, back)})
        finder = CalendarRollFinder(asset_service=bundle.asset_service, roll_offset_days=0)
        after = front.auto_close_date + datetime.timedelta(days=1)
        self.assertEqual((await finder.get_contract_center("CL", after, 0)).sid, back.sid)


if __name__ == "__main__":
    unittest.main()
