"""Validation for synthetic."""

from ziplime.data.data_sources.options.source import OptionChainSource
from ziplime.data.data_sources.options.synthetic.synthetic_data_performance_claim import SyntheticDataPerformanceClaim


def refuse_performance_claims(source: OptionChainSource) -> None:
    """Raise if ``source`` is synthetic. Call it where a return would be reported.

    This is meant to sit in the one place a harness turns a run into a performance number, so that
    the guard is structural rather than a comment somebody has to read. A real feed passes through
    and costs nothing.
    """
    if not source.is_real_market_data:
        raise SyntheticDataPerformanceClaim(source.name)
