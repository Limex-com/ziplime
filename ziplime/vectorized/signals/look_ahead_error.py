"""Look ahead error for signals."""


class LookAheadError(ValueError):
    """A signal computation read data it could not have had at the time.

    Raised before the simulation places an order, because the alternative is a backtest whose
    every number is inflated by an amount nobody can estimate after the fact.
    """
