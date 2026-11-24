"""Varying for engine."""

from ziplime.sweep.params import SweepError


class _Varying:
    """A non-numeric config field that differs between combinations: no array can hold it."""

    def __getattr__(self, name: str):
        raise SweepError("This config field is not numeric and differs between combinations; a "
                         "sweep cannot vary it.")
