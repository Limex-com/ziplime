"""Synthetic data performance claim for synthetic."""

from ziplime.data.data_sources.options.synthetic.support import SYNTHETIC_DATA_WARNING


class SyntheticDataPerformanceClaim(RuntimeError):
    """Raised when performance figures are asked for from synthetic prices."""

    def __init__(self, source_name: str) -> None:
        super().__init__(
            f"{SYNTHETIC_DATA_WARNING}\n"
            f"The prices came from {source_name!r}. Report what the run *did* -- contracts traded, "
            f"structures opened, settlement at expiry, Greeks carried -- or re-run against a real "
            f"option feed before quoting a return.")
        self.source_name = source_name
