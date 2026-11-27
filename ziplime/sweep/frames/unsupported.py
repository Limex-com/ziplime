"""Unsupported for frames."""

from ziplime.sweep.errors import NotConvertible


class Unsupported(NotConvertible, AttributeError):
    """An attribute a sweep does not provide: refused, and still False for hasattr()."""
