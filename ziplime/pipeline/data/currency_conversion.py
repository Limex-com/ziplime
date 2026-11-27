"""Currency-conversion requests shared by datasets and bound columns."""

from collections import namedtuple


CurrencyConversion = namedtuple(
    "CurrencyConversion",
    ["currency", "field"],
    module=__name__,
)
