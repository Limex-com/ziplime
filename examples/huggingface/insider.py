"""Mounting SEC Form 4 insider disclosures for the `i00`-`i10` strategy suite.

The dataset's ``features`` config is already the right shape: one row per issuer and knowledge
day, carrying daily counts and trailing 7/30/90-day aggregates, all computed by knowledge time.
Nothing needs joining, unlike the congressional trades.

Two things to know before reading any result off these strategies.

**The disclosure lag is two days, not forty-five.** Section 16(a) requires a Form 4 within two
business days of the transaction, and the median lag in this data is two days. That is the reason
this suite exists: the congressional strategies were all reading month-old news, and no rule on
month-old news beat a passive control. Here the news is fresh, which makes the question worth
asking again.

**The universe survives, and that flatters everything.** See ``INSIDER10_UNIVERSE`` in
``hf_config`` for the numbers: 126 of 251 insider-active candidates are gone from the price source
entirely. Absolute returns here mean nothing. The control (`i00`) holds the same 125 names and
carries the same bias, so only the comparison against it is worth reading.

The knowledge column is ``feature_available_at``, chosen automatically by the adapter: it is
midnight New York at the start of the day *after* the events it aggregates, which is the first
moment the row could have been read. ``knowledge_day`` would be up to a day early.
"""
import datetime

from ziplime.data.data_sources.huggingface.huggingface_data_source import HuggingFaceDataSource

DATASET = "ZipLime/insider-trading"

#: Columns every strategy in the suite may read. Named explicitly so the mount keeps eight of the
#: forty columns rather than all of them, on a config of 6.6 million rows.
SIGNAL_FIELDS = [
    "ticker",
    "n_open_market_buys", "n_open_market_sells",
    "n_buyers", "n_sellers",
    "buy_notional_usd", "sell_notional_usd", "net_notional_usd",
    "n_ceo_buys", "n_cfo_buys", "n_director_buys",
    "n_unique_buyers_30d", "n_open_market_buys_30d", "buy_notional_usd_30d",
    "max_holding_change_pct",
    "is_cluster_buy", "cluster_buyers_14d",
]


async def mount_features(context, fields: list[str] | None = None,
                         name: str = "insider:features") -> HuggingFaceDataSource:
    """Mount the ``features`` config over the simulation's own window.

    Args:
        context: The running algorithm, for its asset database, clock and calendar.
        fields: Columns to keep besides ``date`` and ``sid``. Defaults to :data:`SIGNAL_FIELDS`.
        name: What the strategy calls the source.
    """
    return await context.huggingface_dataset(
        DATASET, config="features",
        fields=fields if fields is not None else SIGNAL_FIELDS,
        name=name)


def newest_per_sid(frame, columns: list[str]) -> dict[int, dict]:
    """The most recent row per instrument in a ``data.history`` result.

    ``history`` returns a trailing window per sid, and most of these rules want the latest
    observation rather than the window. Sorting by date and taking the last row per sid is the
    whole of it, but doing it in three places invited three subtly different versions.

    Its companion ``window_sums`` is gone: it summed a frame after re-filtering it by date, which
    is what ``data.history(since=...)`` now does at the source. ``priced`` and ``rebalance`` moved
    to ``portfolio``, where they always belonged.
    """
    latest: dict[int, dict] = {}
    for row in frame.sort("date").iter_rows(named=True):
        latest[row["sid"]] = row
    return {sid: {c: row.get(c) for c in columns} for sid, row in latest.items()}
