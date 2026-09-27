"""Non-blocking clock that drives exactly ONE bar of a live run and stops.

A scheduled live deployment runs one bar per invocation: a scheduler (a queue
message, a cron entry) starts a fresh process, which runs a single
``handle_data`` against the venue and exits. ``RealtimeClock`` cannot do that
-- its ``__iter__`` is a blocking loop that sleeps until the next bar and lives
for the whole session.

``SingleTickClock`` reuses all of ``RealtimeClock``'s calendar and session
bookkeeping (``sessions``, ``start_session``, ``market_opens`` ..., which
``run_algo._prepare_algorithm`` and the metrics tracker read) but replaces the
blocking iteration with a single-shot emission of the current session's bar::

    (session_date, SESSION_START)            -> once_a_day: start of session, splits
    (bts_minute,   BEFORE_TRADING_START_BAR) -> the strategy's before_trading_start
    (tick_dt,      BAR)                      -> every_bar: handle_data + order execution
    (tick_dt,      EMISSION_RATE_END)           intraday emission: the minute packet
    (tick_dt,      SESSION_END)                 daily emission: the daily packet

The tick runs at the strategy's own emission rate, as its backtest does -- a
daily strategy gets daily bars, and so does its ``compute_signals``, which is
computed at the emission rate. The closing event is what makes the engine hand
back a performance packet and so lets the run finish: at a daily rate that
packet is built on SESSION_END, intraday on EMISSION_RATE_END. It is the
sequence :class:`~ziplime.gens.domain.single_execution_clock.SingleExecutionClock`
uses.

No strategy state lives in the clock. A fresh process seeds its ledger from the
venue's portfolio before the loop (``synchronize_exchange_portfolio``) and
re-runs ``initialize``, so one tick is self-contained.
"""
import datetime

from ziplime.gens.domain.realtime_clock import RealtimeClock
from ziplime.trading.enums.simulation_event import SimulationEvent


class SingleTickClock(RealtimeClock):
    def __init__(self,
                 trading_calendar,
                 emission_rate: datetime.timedelta,
                 tick_dt: datetime.datetime | None = None,
                 timedelta_diff_from_current_time: datetime.timedelta | None = None,
                 session_window_days: int = 7):
        now = tick_dt or datetime.datetime.now(tz=trading_calendar.tz)
        if now.tzinfo is None:
            now = now.replace(tzinfo=trading_calendar.tz)

        # RealtimeClock validates a >= 1-session [start, end] window and builds its
        # session tables from it. On a session day the window is exactly the tick's
        # session: the engine reads ``clock.sessions`` as the run's length, and a
        # longer window makes every tick "a record covering 1 of N sessions" and
        # points compute_signals' data window into the future. Off-session there is
        # no bar to emit; a forward window keeps the tables valid.
        on_session = trading_calendar.is_session(now.date())
        super().__init__(
            trading_calendar=trading_calendar,
            emission_rate=emission_rate,
            start_date=now,
            end_date=now + (datetime.timedelta(minutes=1) if on_session
                            else datetime.timedelta(days=session_window_days)),
            timedelta_diff_from_current_time=timedelta_diff_from_current_time,
        )
        self._tick_dt = now

    @property
    def tick_dt(self) -> datetime.datetime:
        return self._tick_dt

    def _sleep_and_increase_time(self, sleep_seconds: int) -> datetime.datetime:
        # A single-tick clock must never block. Nothing in the single-shot __iter__
        # calls this; the override makes an accidental call a no-op rather than a
        # session-long sleep.
        return datetime.datetime.now(tz=self.trading_calendar.tz) + self.timedelta_diff_from_current_time

    def __iter__(self):
        now = self._tick_dt
        session_date = now.date()
        session_index = self.sessions.index_of(session_date)

        if session_index is None:
            # Fired outside a trading session (holiday, weekend): emit nothing, so the
            # caller can tell an empty tick from one that ran.
            self._logger.warning("SingleTickClock: %s is not a trading session, emitting no bar",
                                 session_date)
            return

        yield session_date, SimulationEvent.SESSION_START
        # Every tick is a fresh process that re-runs initialize, so it also gets the
        # session's before_trading_start: a backtest calls it before the first bar
        # of each session, and state set there would otherwise be missing.
        bts_minute = self.before_trading_start_minutes[session_index]
        yield min(bts_minute, now), SimulationEvent.BEFORE_TRADING_START_BAR
        # BAR and its closing event share one instant (unlike RealtimeClock, where
        # wall-clock time advances between them), which keeps the tick
        # deterministic and inside the data window.
        yield now, SimulationEvent.BAR
        if self.emission_rate < datetime.timedelta(days=1):
            yield now, SimulationEvent.EMISSION_RATE_END
        else:
            yield now, SimulationEvent.SESSION_END
