"""Errors for manifest."""


class ManifestError(RuntimeError):
    """The dataset does not describe itself well enough to be mounted."""


class NoKnowledgeDateError(ManifestError):
    """The dataset offers no knowledge date, so it cannot be mounted point-in-time.

    Raised rather than falling back to an event date. A backtest that indexes congressional
    disclosures by transaction date is not slightly optimistic; it trades on information that was
    45 days away from being public.
    """
