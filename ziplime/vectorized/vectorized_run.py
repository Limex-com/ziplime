"""The ledger-backed result of a vectorized run."""

import dataclasses
from ziplime.finance.domain.ledger import Ledger


@dataclasses.dataclass
class VectorizedRun:
    """Stands where a :class:`TradingAlgorithm` would, for a run that never had one.

    A vectorised run has no algorithm object -- no ``initialize``, no ``handle_data``, no context.
    What it does have is the ledger the replay built and the portfolio vectorbt produced, and this
    carries both so that code reading ``result.trading_algorithm.portfolio`` keeps working.
    """

    ledger: Ledger
    #: The ``vectorbt.Portfolio`` this was replayed from, kept so the vectorised side stays
    #: reachable: its parameter sweep, its trade records, its plots.
    source: object = None

    @property
    def portfolio(self):
        return self.ledger.portfolio

    @property
    def _ledger(self) -> Ledger:
        """The ledger, under the name :class:`TradingAlgorithm` exposes it by."""
        return self.ledger
