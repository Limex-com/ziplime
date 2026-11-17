"""Errors shared by sweep execution, its API, and conversion machinery."""


class NotConvertible(Exception):
    """The function cannot run for every combination at once."""


class Rediscover(Exception):
    """A history window was asked for that the run had not prepared."""
