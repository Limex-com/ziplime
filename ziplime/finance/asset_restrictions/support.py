"""Support for asset_restrictions."""

from collections import namedtuple
from enum import IntEnum


Restriction = namedtuple("Restriction", ["asset", "effective_date", "state"])


RESTRICTION_STATES = IntEnum(
    "RESTRICTION_STATES",
    [
        "ALLOWED",
        "FROZEN",
    ],
    start=0,
)
