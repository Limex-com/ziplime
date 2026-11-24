"""Value utils for grpc_chain."""

from __future__ import annotations
import datetime


def _decimal(value) -> float:
    """The feed's fixed-point decimal as a float.

    ``num`` scaled by ``10 ** scale``. Guarded because a zero scale is both the common case and
    the representation of a whole number, and reading it as a divisor would turn 100 into 1.
    """
    scale = getattr(value, "scale", 0) or 0
    return float(value.num) / (10 ** scale) if scale else float(value.num)


def _date(value) -> datetime.date | None:
    """A feed date, or None when the field is simply absent.

    An unset protobuf message reads as year 0, which `datetime.date` refuses. That is a fact
    about the field being optional, not an error.
    """
    if not value.year:
        return None
    return datetime.date(value.year, value.month, value.day)
