"""Portfolio collection helpers."""

from ziplime.assets.domain.continuous_future import ContinuousFuture
from ziplime.domain.position import Position


def _in_chain(position: Position, continuous_future: ContinuousFuture) -> bool:
    """Whether ``position`` is a contract of ``continuous_future``'s chain, on its exchange."""
    asset = position.asset
    return (getattr(asset.asset, "root_symbol", None) == continuous_future.root_symbol
            and asset.mic == continuous_future.mic)
