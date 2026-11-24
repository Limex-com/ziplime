"""Context-manager adaptation for pipeline instrumentation."""

from collections.abc import Callable
from ziplime.utils.compat import contextmanager as _contextmanager

# Track the hook methods wrapped as context managers.
PIPELINE_HOOKS_CONTEXT_MANAGERS = set()


def contextmanager(f: Callable):
    """
    Wrapper for contextlib.contextmanager that tracks which methods of
    PipelineHooks are contextmanagers in CONTEXT_MANAGER_METHODS.
    """
    PIPELINE_HOOKS_CONTEXT_MANAGERS.add(f.__name__)
    return _contextmanager(f)
