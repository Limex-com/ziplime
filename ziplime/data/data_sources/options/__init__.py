"""Option chains: the interface a feed implements, and two feeds that implement it.

:mod:`.source` is the seam. :mod:`.grpc_chain` and :mod:`.yahoo_chain` are real ones --
contracts and bars as a venue lists them, marked ``is_real_market_data = True``, the first over a
paid gRPC feed and the second over Yahoo Finance, which needs no credentials and reaches back
days rather than years; :mod:`.synthetic` generates a 0DTE chain from a
volatility surface and marks itself False so nothing reports performance on a model. The two are
interchangeable behind the seam, and :mod:`.ingest` turns whatever either reports into stored
contracts, listings and a bundle.

The synthetic prices in this package never traded. See
:data:`ziplime.data.data_sources.options.synthetic.SYNTHETIC_DATA_WARNING`.
"""
from ziplime.data.data_sources.options.grpc_chain import GrpcChainError, GrpcOptionChainSource
from ziplime.data.data_sources.options.yahoo_chain import (
    YahooChainError, YahooOptionChainSource,
)
from ziplime.data.data_sources.options.ingest import build_option_bundle, register_contracts
from ziplime.data.data_sources.options.source import (
    BAR_COLUMNS, ContractSpec, OptionChainSource,
)
from ziplime.data.data_sources.options.surface import AtmVolatilityModel, VolatilitySurface
from ziplime.data.data_sources.options.venues import (
    OPRA, VENUES, OptionVenue, get_venue, register_venue,
)
from ziplime.data.data_sources.options.synthetic import (
    SYNTHETIC_DATA_WARNING, ChainSpec, LiquidityModel, SyntheticDataPerformanceClaim,
    SyntheticOptionChainSource, refuse_performance_claims,
)

__all__ = [
    "BAR_COLUMNS",
    "AtmVolatilityModel",
    "OPRA",
    "OptionVenue",
    "VENUES",
    "GrpcChainError",
    "GrpcOptionChainSource",
    "YahooChainError",
    "YahooOptionChainSource",
    "get_venue",
    "register_venue",
    "build_option_bundle",
    "ChainSpec",
    "ContractSpec",
    "LiquidityModel",
    "OptionChainSource",
    "register_contracts",
    "SYNTHETIC_DATA_WARNING",
    "SyntheticDataPerformanceClaim",
    "SyntheticOptionChainSource",
    "VolatilitySurface",
    "refuse_performance_claims",
]
