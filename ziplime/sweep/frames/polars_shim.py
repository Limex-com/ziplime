"""Polars shim for frames."""

from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.frames.col import Col
from ziplime.sweep.frames.validation import _refuse


class PolarsShim:
    """``pl`` inside a converted strategy."""

    def col(self, name: str) -> 'Col':
        if not isinstance(name, str):
            raise NotConvertible("pl.col takes one column name in a sweep.")
        return Col(name)

    def __getattr__(self, name: str):
        if name.startswith("__"):
            raise AttributeError(name)
        _refuse(f"pl.{name}")
