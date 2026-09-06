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
    is what ``data.history(since=...)`` now does at the source.
    """
    latest: dict[int, dict] = {}
    for row in frame.sort("date").iter_rows(named=True):
        latest[row["sid"]] = row
    return {sid: {c: row.get(c) for c in columns} for sid, row in latest.items()}


async def priced(context, data) -> dict[int, float]:
    """Instruments with a live quote today, keyed by sid.

    Necessary because a target of zero is still an order. Several names in this universe list
    part-way through the window -- BATRA began trading in 2016 -- and calling
    ``order_target_percent(asset, 0.0)`` on one that has no price raises
    ``CannotOrderDelistedAsset`` rather than doing nothing. So the rebalance skips them entirely.
    """
    quotes = await data.current(assets=context.universe, fields=["price"])
    return {sid: price for sid, price
            in zip(quotes["sid"].to_list(), quotes["price"].to_list()) if price and price > 0}


async def rebalance(context, data, targets: dict[int, float], tolerance: float = 0.0) -> bool:
    """Move the book to ``targets``, touching only what can actually be traded today.

    Args:
        targets: Desired weight per sid. Anything omitted is targeted at zero.
        tolerance: Leave a position alone while it is within this much of its target. Zero
            rebalances on any difference; the weekly strategies pass a band to avoid trading
            against nothing but price drift.

    Returns:
        Whether anything was ordered.
    """
    from ziplime.finance.execution import MarketOrder

    live = await priced(context, data)
    if not live:
        return False
    value = context.portfolio.portfolio_value
    traded = False
    for asset in context.universe:
        if asset.sid not in live:
            continue
        # `order_target_percent` does not account for open orders -- its own docstring says two
        # calls allocate twice. These are thin micro caps, so an order is often still working a
        # week later when the next rebalance comes round, and re-targeting stacks on top of it.
        # An earlier version without this guard ran the book to 15x leverage and a short exposure
        # of ten billion on a one-million-dollar account.
        if context.get_open_orders(asset):
            continue
        target = targets.get(asset.sid, 0.0)
        if tolerance:
            held = await context.portfolio.get_asset_positions_value(asset)
            if abs(target - (held / value if value else 0.0)) < tolerance:
                continue
        await context.order_target_percent(asset=asset, target=target, style=MarketOrder())
        traded = True
    return traded
