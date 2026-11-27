"""Initialized for initialize."""

from ziplime.sweep.initialize.sweep_context import SweepContext


class Initialized:
    """What one run of initialize declared."""

    def __init__(self, context: SweepContext) -> None:
        self.indicators = dict(context._indicators)
        self.state = context.state()
        self.scheduled = list(context._scheduled)
        self.commission, self.slippage = dict(context._commission), dict(context._slippage)
        self.cancel_policy = context._cancel_policy
        universe = getattr(context, "universe", None) or context.requested
        if isinstance(universe, dict):
            self.universe = universe
        else:
            self.universe = {listing.symbol: listing for listing in universe}
        self.datasets = getattr(context, "datasets", None)
