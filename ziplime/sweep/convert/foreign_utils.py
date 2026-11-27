"""Foreign utils for convert."""

import os
import sys
import types
from types import ModuleType
from ziplime.sweep.all_assets import ALL
from ziplime.sweep.array_utils import is_array
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.convert.numpy_shim import _HistoryAware


def foreign(value, name: str):
    from ziplime.sweep.convert.foreign_value import _Foreign

    import types as _types

    if isinstance(value, _types.ModuleType):
        return _Foreign(value)
    if not callable(value) or isinstance(value, type) and issubclass(value, BaseException):
        return value

    def plain(*args, **kwargs):
        from ziplime.sweep.lazy import where_in_strategy
        if any(is_array(a) or a is ALL or hasattr(a, "sweep_len")
               for a in (*args, *kwargs.values())):
            raise NotConvertible(f"{where_in_strategy()}: {name}(...) gets values that differ "
                                 f"between combinations; it computes on the CPU number by number. "
                                 f"On history rows (data.history(...)['close'].to_numpy()) it is "
                                 f"computed ahead of the run; otherwise compute it in "
                                 f"compute_signals.")
        return value(*args, **kwargs)
    return _HistoryAware(value, plain, name)


def user_module(module: ModuleType | str | None) -> bool:
    """A module of the strategy's own code: not the standard library, an installed package or
    ziplime. Its functions are converted like the strategy's."""
    import sysconfig

    if isinstance(module, str):
        module = sys.modules.get(module)
    path = getattr(module, "__file__", None)
    if not path:
        return False
    path = os.path.realpath(path)
    if module.__name__ == "ziplime.ziplime_algorithm":
        return True  # the strategy file, loaded by AlgorithmFile under this name
    roots = {sysconfig.get_paths()[key] for key in ("stdlib", "platstdlib", "purelib", "platlib")}
    roots.add(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))  # ziplime itself
    if any(path.startswith(os.path.realpath(root) + os.sep) for root in roots if root):
        return False
    return "site-packages" not in path and "dist-packages" not in path


def _is_foreign(value) -> bool:
    """A module or function of another library: not Python's, NumPy, polars or ziplime."""
    from ziplime.sweep.check import ALLOWED_MODULES

    if isinstance(value, types.ModuleType):
        module = value.__name__
    elif callable(value) and not isinstance(value, type):
        module = getattr(value, "__module__", None) or ""
    else:
        return False
    top = module.split(".")[0]
    return bool(top) and top not in ALLOWED_MODULES and top not in ("numpy", "polars", "math") \
        and not user_module(module)
