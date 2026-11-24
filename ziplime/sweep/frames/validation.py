"""Validation for frames."""

from ziplime.sweep.frames.support import HINT
from ziplime.sweep.frames.unsupported import Unsupported


def _refuse(what: str):
    raise Unsupported(f"{what} is not supported in a sweep; {HINT}.")
