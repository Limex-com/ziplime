"""Unrunnable for lazy."""


class _Unrunnable(Exception):
    """A record that holds an object only this process has (a dict, a lambda): replayed walking
    the record itself."""
