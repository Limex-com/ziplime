"""When an option may be exercised."""
import enum


class ExerciseStyle(enum.Enum):
    """European (expiry only) or American (any time up to expiry).

    For the 0DTE contracts this package is built around the distinction is nearly moot -- there is
    no "before expiry" left to exercise early in -- but it is not decorative, and it is not
    something to assume. American exercise on a dividend-paying underlying makes an in-the-money
    call worth exercising the day before an ex-date, and index options are European while options
    on the ETF tracking the same index are American. Storing it per contract keeps the two apart
    when a run holds both.

    Early exercise is **not modelled** by the engine: a position is settled at expiry. The field
    records what the instrument is, so a strategy can refuse to trade what the engine cannot price.
    """

    EUROPEAN = "EUROPEAN"
    AMERICAN = "AMERICAN"

    @property
    def allows_early_exercise(self) -> bool:
        return self is ExerciseStyle.AMERICAN
