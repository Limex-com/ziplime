import functools
from collections import namedtuple  # noqa: compatibility with python 3.11
from contextlib import contextmanager, ExitStack
from html import escape as escape_html
from math import ceil
from types import MappingProxyType as mappingproxy


def consistent_round(val):
    if (val % 1) >= 0.5:
        return ceil(val)
    else:
        return round(val)


wraps = functools.wraps

unicode = type("")

__all__ = [
    "ExitStack",
    "consistent_round",
    "contextmanager",
    "escape_html",
    "mappingproxy",
    "unicode",
    "wraps",
]
