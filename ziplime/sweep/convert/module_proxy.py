"""Module proxy for convert."""

from types import ModuleType


class _ModuleProxy:
    """``import mylib`` in the strategy: ``mylib.helper`` is the converted helper."""

    def __init__(self, module: ModuleType, namespace) -> None:
        self._module, self._namespace = module, namespace

    def __getattr__(self, name: str):
        if name in self._namespace:
            return self._namespace[name]
        return getattr(self._module, name)
