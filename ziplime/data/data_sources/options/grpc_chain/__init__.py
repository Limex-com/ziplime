"""Real option chains, from the gRPC reference and candle services.

The first :class:`~ziplime.data.data_sources.options.source.OptionChainSource` that describes a
market that existed. Its synthetic sibling generates a chain from a volatility surface and marks
itself ``is_real_market_data = False`` so nothing reports performance on it; this one carries the
opposite flag and means it.

**Two calls make a chain.** ``GetOptionFamilies`` answers "which expiries does this underlying
have listed", and ``GetOptions`` answers "which contracts are in that expiry" -- 32 families for
SPY, 316 contracts in the December series, measured. Neither is filtered by date: the feed
describes the chain as it stands *now*, which is the single most important thing to understand
about this source and the reason for :meth:`GrpcOptionChainSource.contracts`'s ``sessions``
argument being a filter rather than a query. See the note there.

**The contract's own dates come from the feed, not from a convention.** ``trade_first_day`` is
when a contract was listed, and a backtest that assumes otherwise will trade instruments months
before they existed. A LEAPS listed in January 2025 and expiring in January 2027 is two years of
real bars; a weekly listed three days before it expires is three. Both are normal and the
difference is only knowable from the feed.
"""

from ziplime.data.data_sources.options.grpc_chain.grpc_chain_error import GrpcChainError  # noqa: F401
from ziplime.data.data_sources.options.grpc_chain.grpc_option_chain_source import (  # noqa: F401
    GrpcOptionChainSource,
)
from ziplime.data.data_sources.options.grpc_chain.expired_aware_feed import (  # noqa: F401
    ExpiredAwareFeed,
)


__all__ = [
    'GrpcChainError',
    'GrpcOptionChainSource',
    'ExpiredAwareFeed',
]
