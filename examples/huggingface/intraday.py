"""Intraday bars, and the alignment question a daily bundle never has to answer.

Everything else in this directory runs on daily bars, and the strategies read datasets whose
knowledge times are recorded to the second. Those two facts do not sit well together. A Form 4 is
accepted by EDGAR at 16:54 on a Tuesday; a daily simulation can act on Wednesday's close, forty-
eight hours of trading later, and there is no way to say anything more precise than that. All the
work that went into keeping ``knowledge_date`` to the second buys nothing at that resolution.

Measured on the 309 011 Form 4 point-in-time rows filed in 2026, by New York time:

    00:00-09:30  pre-market       11 159    3.6%
    09:30-16:00  market open      39 535   12.8%
    16:00-20:00  after the close 220 129   71.2%
    20:00-24:00  late             38 188   12.4%
                 median 16:54

**Five filings in six arrive after the close.** For those the earliest honest fill is the next
session's open, which is a statement a daily backtest cannot make and an intraday one can. The
other one in six lands while the market is trading, and can be acted on in the next bar.

## Aligning a vendor's bars to the simulation's clock

Yahoo labels an intraday bar with the time it starts and the clock emits the minute a bar ends, so
the two have to be reconciled before a bundle is assembled. Getting it backwards is a silent
one-bar look-ahead; :mod:`ziplime.data.services.bar_alignment` does it and explains why.
"""
import datetime

import polars as pl

from ziplime.constants.data_type import DataType
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services import frame_cache
from ziplime.data.services.bar_alignment import align_to_clock, clock_minutes
from ziplime.utils.bundle_utils import get_market_data_source
from ziplime.utils.calendar_utils import get_calendar

#: Yahoo serves intraday history for a short window and no further back, per interval. These are
#: the limits it actually honoured when this was written, not the ones its documentation states.
INTRADAY_HORIZON = {
    datetime.timedelta(minutes=1): datetime.timedelta(days=7),
    datetime.timedelta(minutes=5): datetime.timedelta(days=59),
    datetime.timedelta(minutes=15): datetime.timedelta(days=59),
    datetime.timedelta(minutes=30): datetime.timedelta(days=59),
    datetime.timedelta(minutes=60): datetime.timedelta(days=729),
}

#: Liquid names with recent Form 4 activity, each with a MIC. The MIC is not decoration: ``LIFE``
#: resolves to a Moscow listing in this asset database, and a bare ticker would have picked it.
INTRADAY_UNIVERSE = [
    ("AMD", "XNGS"), ("ABNB", "XNGS"), ("ANET", "XNYS"), ("AXON", "XNGS"),
    ("CRWD", "XNGS"), ("CRWV", "XNGS"), ("DDOG", "XNGS"), ("HSY", "XNYS"),
    ("MCHP", "XNGS"), ("META", "XNGS"), ("NET", "XNYS"), ("NOW", "XNYS"),
    ("PCTY", "XNGS"), ("PG", "XNYS"), ("SE", "XNYS"), ("STX", "XNGS"),
    ("TMO", "XNYS"), ("UTHR", "XNGS"), ("WEN", "XNGS"),
]


async def build_intraday_bundle(listings, start: datetime.date, end: datetime.date,
                                emission_rate: datetime.timedelta, asset_service,
                                calendar_name: str) -> DataBundle:
    """Yahoo intraday bars for ``listings``, stamped on the simulation clock's own grid."""
    horizon = INTRADAY_HORIZON.get(emission_rate)
    if horizon is None:
        raise SystemExit(f"Yahoo serves no intraday interval of {emission_rate}.")
    oldest = datetime.date.today() - horizon
    if start < oldest:
        raise SystemExit(
            f"Yahoo keeps only {horizon.days} days of {emission_rate} bars, so a run cannot start "
            f"before {oldest}. Asked for {start}. Use a coarser interval or a later start.")

    calendar = get_calendar(calendar_name)
    sids = tuple(sorted(listing.sid for listing in listings))
    # Intraday bars are cached for an hour, not a day: unlike a decade of daily history, the most
    # recent session is still being written to while a strategy is being edited against it.
    disk_key = frame_cache.cache_key("yahoo-intraday-bars", sids, start, end, str(emission_rate))
    data = frame_cache.load(disk_key, max_age=datetime.timedelta(hours=1))
    if data is None:
        source = get_market_data_source("yahoo", assets=listings)
        frame = await source.get_data(
            symbols=[listing.symbol for listing in listings],
            frequency=emission_rate,
            date_from=datetime.datetime.combine(start, datetime.time.min, tzinfo=calendar.tz),
            date_to=datetime.datetime.combine(end, datetime.time.max, tzinfo=calendar.tz))
        if frame.is_empty():
            raise SystemExit("Yahoo returned no intraday bars for the requested equities.")

        sid_by_symbol = {listing.symbol: listing.sid for listing in listings}
        frame = frame.with_columns(
            pl.col("symbol").replace_strict(sid_by_symbol, return_dtype=pl.Int64).alias("sid"),
            pl.col("date").dt.convert_time_zone(str(calendar.tz)),
        ).sort("date")
        grid = clock_minutes(calendar, start, end, emission_rate)
        frame = align_to_clock(frame, grid, emission_rate)
        # No split adjustment to undo over a window this short, so the two prices are the same one.
        # The column exists because the shared strategy helpers read it.
        frame = frame.with_columns(pl.col("close").alias("unadjusted_close"))
        data = frame.select(["date", "sid", "symbol", "mic", "open", "high", "low", "close",
                             "price", "volume", "unadjusted_close"]).sort(["sid", "date"])
        frame_cache.store(disk_key, data)

    indexes = data.with_row_index().group_by("sid", maintain_order=True).agg([
        pl.col("index").first().alias("start"), pl.col("index").last().alias("end")])
    return DataBundle(
        name="hf_equities_intraday", version="1",
        start_date=data["date"].min(), end_date=data["date"].max(),
        trading_calendar=calendar, frequency=emission_rate,
        original_frequency=emission_rate, data_type=DataType.MARKET_DATA,
        timestamp=data["date"].max(), data=data,
        sid_indexes={r["sid"]: (r["start"], r["end"] + 1) for r in indexes.iter_rows(named=True)},
        asset_service=asset_service)


def recent_window(emission_rate: datetime.timedelta, sessions: int,
                  calendar_name: str = "XNYS") -> tuple[datetime.date, datetime.date]:
    """The last ``sessions`` complete sessions that Yahoo still serves at this interval.

    An intraday example cannot be pinned to fixed dates the way the daily ones are. Yahoo keeps
    seven days of one-minute bars and deletes what falls off the back, so a window written down
    today returns nothing next month. The window moves with the calendar instead, which means
    these examples do not reproduce a number twice -- they demonstrate a mechanism, and the
    docstrings say so.
    """
    calendar = get_calendar(calendar_name)
    horizon = INTRADAY_HORIZON[emission_rate]
    today = datetime.date.today()
    available = calendar.sessions_in_range(today - horizon + datetime.timedelta(days=1), today)
    # The session in progress has no complete bars yet, and Yahoo's last one is partial.
    complete = [s.date() for s in available if s.date() < today]
    if len(complete) < sessions:
        raise SystemExit(f"Only {len(complete)} complete sessions available at {emission_rate}.")
    return complete[-sessions], complete[-1]


#: Form 4 code for an open-market sale. `A` is a grant and `F` is shares withheld for tax --
#: neither is somebody choosing to sell, and pooling them dilutes the signal with payroll.
SALE = "S"

#: How long a name stays out of the book after a disclosed sale.
BLOCK_FOR = datetime.timedelta(days=5)


async def mount_sales(context, name: str = "insider:pit"):
    """The Form 4 point-in-time table, whose knowledge dates are stamped to the second."""
    return await context.huggingface_dataset(
        "ZipLime/insider-trading", config="pit",
        fields=["ticker", "transaction_code"], name=name)


async def note_sales(context, data) -> int:
    """Record which names have a disclosed insider sale published since the last look.

    Reads from the previous call rather than a fixed window. A fixed one-bar window looks
    reasonable and quietly discards five filings in six: 16:00 to 09:31 is eighteen hours with no
    bar in it, and that is when most Form 4s are accepted.

    Returns:
        How many names this call newly blocked.
    """
    now = context.simulation_dt
    since = (now - context.last_look) if getattr(context, "last_look", None) else datetime.timedelta(days=1)
    context.last_look = now
    fresh = await data.history(assets=context.universe, since=since,
                               fields=["transaction_code"], data_source=context.source)
    added = 0
    for row in fresh.iter_rows(named=True):
        if row["transaction_code"] == SALE and row["sid"] not in context.blocked:
            added += 1
        if row["transaction_code"] == SALE:
            context.blocked[row["sid"]] = now
    context.blocked = {sid: at for sid, at in context.blocked.items() if now - at < BLOCK_FOR}
    return added


def unblocked(context) -> list[int]:
    """The universe minus the names currently sitting out."""
    return [asset.sid for asset in context.universe if asset.sid not in context.blocked]
