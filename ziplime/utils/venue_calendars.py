"""Which schedule each venue keeps, and which calendar a mixed run has to be clocked on.

A simulation runs on one calendar, and until this module every listing was judged by it: a
listing carries its venue but not that venue's hours, so `can_trade` answered with the
simulation's own schedule and an order at a minute the listing's market was shut failed deep
inside `_calculate_order_value_amount` with `CannotOrderDelistedAsset` -- an asset that is
neither delisted nor, at that minute, orderable.

That is survivable while a run stays on one market. It is not survivable for the strategies this
exists for: a Moscow-versus-CME gas spread trades two venues whose sessions overlap for part of
the day and miss each other for the rest, and the whole point of the strategy is the part where
both are open.

So a listing answers for its own venue here, and the simulation is clocked on something wide
enough to visit every minute any of its venues trades. The two are different questions and this
module answers both: :func:`calendar_for_mic` for the listing, :func:`clock_calendar_name` for
the run.

The table here covers the venues `exchange_calendars` already describes. A market that needs more
-- a calendar of its own, or venues ziplime does not ship -- comes as an extension: it registers
its venues with :func:`register_venue_calendars`, or advertises them under the
``ziplime.venue_calendars`` entry point group so that installing it is enough. `zipfinam` does
this for Moscow: MISX on XMOS, and RTSX on the FORTS calendar it defines.
"""

import os
from collections.abc import Iterable, Mapping
from functools import lru_cache

import structlog
from exchange_calendars import ExchangeCalendar

from ziplime.utils.calendar_utils import get_calendar

_logger = structlog.get_logger(__name__)


#: The calendar each venue keeps, by MIC.
#:
#: Keyed on MIC and nothing else. `exchanges.country_code` is `US` for every row in both asset
#: databases -- including MISX and RTSX -- so reading the region off an exchange would put Moscow
#: on the American session.
#:
#: The American equity venues all keep the NYSE session and share its holidays, so they share its
#: calendar rather than the near-identical per-venue ones `exchange_calendars` also ships: one
#: schedule that is right is easier to reason about than eight that agree.
VENUE_CALENDARS: dict[str, str] = {
    # United States, equities and the ETFs filed among them.
    "ARCX": "XNYS",
    "BATS": "XNYS",
    "IEXG": "XNYS",
    "PINX": "XNYS",
    "XASE": "XNYS",
    "XNCM": "XNYS",
    "XNGS": "XNYS",
    "XNMS": "XNYS",
    "XNYS": "XNYS",
    # United States, futures. CME Globex runs nearly around the clock, which is why a spread
    # against it needs a clock wider than either equity session.
    "XCBT": "CMES",
    "XCEC": "CMES",
    "XCME": "CMES",
    "XNYM": "NYMEX",
    "XCBF": "XCBF",
    # Moscow is not here: zipfinam registers MISX (XMOS) and RTSX (its FORTS calendar).
    # ICE.
    "IFEU": "IEPA",
    "IFLL": "IEPA",
    "IFUS": "IEPA",
    # Crypto. No weekend, no holiday, no close -- and both asset databases already carry these:
    # Kraken 1,464 listings, Binance spot 1,394, Binance perpetuals 1,798. Their bars are served
    # for every day of the week, so a weekday calendar does not merely mis-time them, it puts two
    # sevenths of the history somewhere the simulation clock can never reach.
    "KRME": "24/7",
    "_BNCC": "24/7",
    "_BNCF": "24/7",
}

#: Environment override, ``MIC=CALENDAR`` separated by commas, e.g. ``"MEXC=24/7,XHKG=XHKG"``.
#:
#: A venue added here needs no new wheel, which is the point: the table above ships inside
#: ziplime, and getting a one-line change into a running container means rebuilding the core,
#: the deps image and the service image. A new exchange -- and new crypto venues appear
#: constantly -- should not cost that. Entries here win over the table and over extensions.
VENUE_CALENDARS_ENV = "ZIPLIME_VENUE_CALENDARS"

#: Entry point group an extension advertises its venues under. Each entry point is loaded once,
#: on the first lookup, and must be a ``{MIC: calendar name}`` mapping or a callable returning
#: one. Loading it is also where an extension registers a calendar type `exchange_calendars` does
#: not ship, so that the name it maps to can be resolved.
ENTRY_POINT_GROUP = "ziplime.venue_calendars"

#: Venues registered by extensions, laid over :data:`VENUE_CALENDARS`.
_extension_venues: dict[str, str] = {}
_entry_points_loaded = False

#: What a run spanning venues that keep different *weekday* hours is clocked on.
#:
#: Every weekday, around the clock. Not a union of the venues' sessions, which would be the exact
#: answer and is not expressible as an `exchange_calendars` calendar: a union has to keep a
#: session whenever *either* market trades, and the holidays of the two markets do not line up --
#: 9 May is a Moscow holiday and an American trading day, the fourth Thursday of November the
#: reverse. Taking either market's calendar as the union therefore silently deletes the other
#: market's trading days.
#:
#: `24/5` keeps every weekday and every hour instead, and leaves the question of whether a
#: particular venue is open at a particular minute to :func:`calendar_for_mic`, which can answer
#: it. A minute no venue trades in simply has no bars, and a strategy reading prices there sees
#: nothing -- the same thing it sees in a half-hour lunch break.
MIXED_VENUE_CLOCK = "24/5"

#: The same, once one of the venues never closes.
#:
#: A weekday clock cannot hold a crypto venue at all: Saturday and Sunday are not sessions, so
#: two sevenths of the history are not merely mis-timed but unreachable -- the simulation clock
#: visits only calendar minutes, and those days have none. Measured on the vendor: BTCUSDT@_BNCC
#: returns 2,001 daily bars of which 572 fall at a weekend, and 24 bars a day hourly.
#:
#: The cost is paid by the other venues, which gain sessions they are shut for; each is skipped
#: by the per-listing check, and a weekend session for an equity simply holds no bars.
CONTINUOUS_CLOCK = "24/7"


def register_venue_calendars(venues: Mapping[str, str]) -> None:
    """Add venues to the table, or change the calendar of ones it has.

    For extensions that cover a market ziplime does not: ``{"MISX": "XMOS", "RTSX": "FORTS"}``.
    A calendar the mapping names that `exchange_calendars` does not ship must be registered with
    it (``exchange_calendars.register_calendar_type``) before a run looks the venue up; until it
    is, the venue resolves to no calendar and is judged by the simulation's, as before.

    Registered venues win over the shipped table; the environment override still wins over them.
    """
    for mic, calendar in venues.items():
        mic, calendar = str(mic or "").strip().upper(), str(calendar or "").strip()
        if mic and calendar:
            _extension_venues[mic] = calendar
    _clear_caches()


def unregister_venue_calendars(mics: Iterable[str]) -> None:
    """Take back venues :func:`register_venue_calendars` added. Unknown ones are ignored."""
    for mic in mics:
        _extension_venues.pop(str(mic or "").strip().upper(), None)
    _clear_caches()


def _clear_caches() -> None:
    _venue_table.cache_clear()
    _calendar_name.cache_clear()
    _calendar.cache_clear()


def _load_extensions() -> None:
    """Register what installed extensions advertise under :data:`ENTRY_POINT_GROUP`, once.

    A broken extension is logged and skipped: a venue it would have mapped falls back to the
    simulation calendar, which is wrong-but-working, while raising would take down runs that
    never touch it.
    """
    global _entry_points_loaded
    if _entry_points_loaded:
        return
    _entry_points_loaded = True
    try:
        from importlib.metadata import entry_points
        advertised = list(entry_points(group=ENTRY_POINT_GROUP))
    except Exception as error:  # importlib.metadata problems must not break ziplime
        _logger.debug("Could not read venue calendar entry points", error=str(error))
        return
    for entry_point in advertised:
        try:
            provided = entry_point.load()
            venues = provided() if callable(provided) else provided
            register_venue_calendars(dict(venues))
        except Exception as error:
            _logger.warning("Venue calendar extension failed to load",
                            entry_point=entry_point.name, error=str(error))


def _overrides() -> str:
    """The raw override string, read per call so a deployment can change it without a restart."""
    return os.environ.get(VENUE_CALENDARS_ENV, "").strip()


@lru_cache(maxsize=None)
def _venue_table(overrides: str) -> dict[str, str]:
    """:data:`VENUE_CALENDARS`, then the extensions' venues, then the environment's entries.

    Cached on the override string rather than on nothing, so changing the environment changes the
    answer -- which is what makes the override worth having, and what a plain `lru_cache` on the
    lookup would have quietly prevented. Registering venues clears the cache.
    """
    _load_extensions()
    table = dict(VENUE_CALENDARS)
    table.update(_extension_venues)
    for entry in overrides.split(","):
        mic, separator, calendar = entry.partition("=")
        if not separator:
            continue
        mic, calendar = mic.strip().upper(), calendar.strip()
        if mic and calendar:
            table[mic] = calendar
    return table


@lru_cache(maxsize=None)
def _calendar_name(mic: str, overrides: str) -> str | None:
    return _venue_table(overrides).get(mic.strip().upper())


def calendar_name_for_mic(mic: str | None) -> str | None:
    """The calendar name a venue keeps, or ``None`` for a venue nothing is known about."""
    if not mic:
        return None
    return _calendar_name(mic, _overrides())


@lru_cache(maxsize=None)
def _calendar(mic: str, overrides: str) -> ExchangeCalendar | None:
    name = _calendar_name(mic, overrides)
    if name is None:
        return None
    try:
        return get_calendar(name)
    except Exception:
        # A calendar the installed `exchange_calendars` does not carry -- including a typo in the
        # environment override. Falling back to the simulation calendar is wrong-but-working;
        # raising here would take down runs that never touch the venue.
        return None


def calendar_for_mic(mic: str | None) -> ExchangeCalendar | None:
    """The calendar a venue keeps, or ``None`` when it is not known.

    ``None`` is an answer, not a failure: callers fall back to the simulation's own calendar,
    which is what every caller did before this module existed. A venue missing from the table
    therefore behaves exactly as it used to rather than raising in the middle of a run.

    Called once per listing per bar, so the lookup is cached; the cache is keyed on the override
    string as well, which is what lets the environment still be read.
    """
    if not mic:
        return None
    return _calendar(mic, _overrides())


@lru_cache(maxsize=None)
def trades_every_day(calendar_name: str) -> bool:
    """Whether this calendar keeps a seven-day week.

    Read off the calendar's own week mask rather than from a list of names here, so a venue added
    through the environment -- or a calendar a later `exchange_calendars` ships -- is classified
    correctly without this module knowing about it.
    """
    try:
        weekmask = getattr(get_calendar(calendar_name), "weekmask", "1111100")
    except Exception:
        return False
    return "0" not in str(weekmask)


@lru_cache(maxsize=None)
def covers(wide: str, narrow: str) -> bool:
    """Whether every minute ``narrow`` trades is a minute ``wide`` trades too.

    Moscow's equity and derivatives calendars are the case this exists for: FORTS (registered by
    zipfinam) runs 09:00-23:50 and XMOS 10:00-18:45, same zone, same holidays, one inside the other.
    A strategy holding both a Moscow share and a Moscow future has no reason to be clocked on `24/5`
    -- FORTS already visits every minute either of them trades, and `24/5` would add fifteen hours
    of nothing to every session.

    Measured rather than declared: same zone, same session days, and on every session sampled the
    wider one opens no later and closes no earlier. A sample is enough because these are fixed
    schedules; a calendar that changed its hours mid-history would only lose the shortcut and fall
    back to the generic clock, which is the safe direction.
    """
    if wide == narrow:
        return True
    try:
        a, b = get_calendar(wide), get_calendar(narrow)
    except Exception:
        return False
    if str(a.tz) != str(b.tz):
        return False

    sessions = b.sessions[-250:]
    if not len(sessions):
        return False
    for session in sessions[::10]:
        if not a.is_session(session):
            return False
        if a.session_open(session) > b.session_open(session):
            return False
        if a.session_close(session) < b.session_close(session):
            return False
    return True


def clock_calendar_name(mics) -> str | None:
    """What to clock a run on, given the venues its instruments listed on.

    One venue, or several that keep the same schedule, answers with that schedule -- a Moscow
    equity run stays on XMOS and costs exactly what it costs today. So does a set where one
    venue's calendar already contains the others': Moscow shares beside Moscow futures are
    clocked on FORTS, not on a 24-hour calendar, because FORTS visits every minute either trades.

    Venues that genuinely miss each other need a clock wide enough for all of them:
    :data:`CONTINUOUS_CLOCK` when any of them never closes, :data:`MIXED_VENUE_CLOCK` otherwise.
    The distinction is the week, not the day: both keep all 24 hours, and only one keeps Saturday
    and Sunday. Handing a crypto venue a weekday clock does not shift its bars, it deletes them.

    The caller is expected to leave the benchmark's own listing out of ``mics``, and leaving it
    in matters more than it sounds. The benchmark is not something the strategy trades; it joins
    every run's symbols so its series can be read. On the Moscow deployment it is `IMOEX@RTSX`, an
    index on the derivatives venue -- so with it counted, *every* Moscow equity backtest looked
    like a run spanning XMOS and FORTS and was clocked on `24/5`, sessions and all. Dropping it by
    venue would be wrong in the other direction: a strategy that does trade Moscow futures must
    keep RTSX. It is the listing that comes out, not the MIC.

    An empty or wholly unknown set answers ``None``: the caller keeps whatever it was going to
    use, which is the deployment's configured calendar.
    """
    names = {calendar_name_for_mic(mic) for mic in mics or ()}
    names.discard(None)
    if not names:
        return None
    if len(names) == 1:
        return names.pop()

    for candidate in sorted(names):
        if all(covers(candidate, other) for other in names):
            return candidate

    return CONTINUOUS_CLOCK if any(trades_every_day(name) for name in names) else MIXED_VENUE_CLOCK
