"""Foreign for convert."""

from types import ModuleType


class _Foreign:
    """A module of another library (TA-Lib, SciPy...) inside converted code: its functions on
    history rows are recorded (lazy.py); on values that differ between combinations they cannot
    run, and a call with plain numbers runs as written."""

    def __init__(self, module: ModuleType) -> None:
        self._module = module

    def __getattr__(self, name: str):
        from ziplime.sweep.convert.foreign_utils import foreign

        return foreign(getattr(self._module, name), f"{self._module.__name__}.{name}")
