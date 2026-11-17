"""Currency-conversion requests shared by datasets and bound columns."""

from collections import namedtuple


CurrencyConversion = namedtuple(
    "CurrencyConversion",
    ["currency", "field"],
    module="ziplime.pipeline.data.dataset",
)
