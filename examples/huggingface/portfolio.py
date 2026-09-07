"""Turning a set of target weights into orders. Shared by every strategy here, whatever it reads.

These are not data helpers. They belong with the portfolio, not with congressional disclosures or
SEC filings, and they used to live in ``insider.py`` purely because that is where they were first
written -- which left the fundamentals strategies importing ``from insider import rebalance``.

Each guard here was paid for once:

* **A target of zero is still an order.** Several names in these universes list part-way through
  the window, and ``order_target_percent(asset, 0.0)`` on one that has no price raises rather than
  doing nothing.
* **`order_target_percent` does not see open orders.** Its own docstring says two calls allocate
  twice. On thin instruments an order is still working a week later when the next rebalance comes
  round, and re-targeting stacks on top of it -- which ran a book to 15x leverage and a short
  exposure of ten billion on a one-million-dollar account before the guard existed.
* **A tolerance band has to be smaller than the position.** With 230 names each target is 0.43%,
  so a flat 1% band is wider than the position itself and nothing ever trades. Pass a fraction of
  the target, not a constant.
"""
import datetime  # noqa: F401  (kept for callers that pass timedeltas through)

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


async def rebalance_to(context, data, targets: dict[int, float], tolerance: float = 0.0) -> bool:
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
