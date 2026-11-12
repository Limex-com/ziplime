"""Data sources a strategy names by address, ``scheme://...``, supplied by extensions.

A strategy can read a dataset without registering it first:

    df = await data.history(assets=[sber], bar_count=20, data_source="algopack://eq/tradestats")

ziplime itself understands ``hf://`` (Hugging Face, :mod:`ziplime.data.data_sources.huggingface`).
Any other scheme belongs to an extension, which provides a *mount*: a callable that turns the
address into a :class:`~ziplime.data.services.data_source.DataSource` for the running simulation.
`zipfinam` provides ``algopack://`` (MOEX AlgoPack) this way.

An extension makes its scheme known in either of two ways:

* by advertising it under the ``ziplime.address_schemes`` entry point group, the entry point's
  name being the scheme and its value the mount -- installing the extension is then enough::

      [project.entry-points."ziplime.address_schemes"]
      algopack = "zipfinam.algopack:mount_address"

* or by calling :func:`register_address_scheme` when it is imported.

A mount is called as ``mount(address, *, asset_service, start_date, end_date, trading_calendar)``
-- the asset database, the simulation's window and its calendar, which is everything a dataset
needs to map its rows onto listings and onto the simulation clock -- and returns the source, or an
awaitable of it. It should accept and ignore keyword arguments it does not use, so that ziplime
can pass more in later without breaking it.

Every mount also receives the run's *address context* as keyword arguments: whatever the hosting
runtime passed as ``run_simulation(address_context={...})`` -- the requesting user, for one, which
``dataset://`` (private user datasets, served by ziplime itself) needs. See
:func:`address_context`.
"""

from collections.abc import Callable, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

import structlog

_logger = structlog.get_logger(__name__)

#: Entry point group an extension advertises its schemes under.
ENTRY_POINT_GROUP = "ziplime.address_schemes"

Mount = Callable[..., Any]

_mounts: dict[str, Mount] = {}
_entry_points_loaded = False


def scheme_of(address: str) -> str | None:
    """``"algopack"`` for ``"algopack://eq/tradestats"``; ``None`` for a plain name."""
    if not isinstance(address, str):
        return None
    scheme, separator, _ = address.partition("://")
    if not separator or not scheme or not scheme.replace("-", "").replace("_", "").isalnum():
        return None
    return scheme.lower()


def register_address_scheme(scheme: str, mount: Mount) -> None:
    """Make ``scheme://...`` addresses resolvable by ``mount``. A later registration wins."""
    scheme = (scheme or "").strip().lower().removesuffix("://")
    if not scheme:
        raise ValueError("An address scheme needs a name.")
    if not callable(mount):
        raise TypeError(f"The mount for {scheme}:// must be callable, got {mount!r}.")
    _mounts[scheme] = mount


def unregister_address_scheme(scheme: str) -> None:
    """Forget a scheme :func:`register_address_scheme` added. Unknown ones are ignored."""
    _mounts.pop((scheme or "").strip().lower().removesuffix("://"), None)


def _load_extensions() -> None:
    """Register what installed extensions advertise under :data:`ENTRY_POINT_GROUP`, once.

    A broken extension is logged and skipped; a strategy naming its scheme then gets the ordinary
    "no data source named ..." error, which says what is missing.
    """
    global _entry_points_loaded
    if _entry_points_loaded:
        return
    _entry_points_loaded = True
    try:
        from importlib.metadata import entry_points
        advertised = list(entry_points(group=ENTRY_POINT_GROUP))
    except Exception as error:  # importlib.metadata problems must not break ziplime
        _logger.debug("Could not read address scheme entry points", error=str(error))
        return
    for entry_point in advertised:
        if entry_point.name.lower() in _mounts:
            continue  # registered on import already; the explicit registration wins
        try:
            register_address_scheme(entry_point.name, entry_point.load())
        except Exception as error:
            _logger.warning("Address scheme extension failed to load",
                            entry_point=entry_point.name, error=str(error))


def _builtin_mount(scheme: str) -> Mount | None:
    """Schemes ziplime serves itself, imported only when a strategy names one."""
    if scheme == "dataset":
        from ziplime.data.data_sources.user_datasets import mount_user_dataset
        return mount_user_dataset
    return None


def mount_for(address: str) -> Mount | None:
    """The mount registered for ``address``'s scheme, or ``None``."""
    scheme = scheme_of(address)
    if scheme is None:
        return None
    if scheme not in _mounts:
        _load_extensions()
    return _mounts.get(scheme) or _builtin_mount(scheme)


# ---------------------------------------------------------------------------- the run's context

#: What the hosting runtime knows about *this* run and a mount may need -- the requesting user for
#: ``dataset://``, for one. Set by ``run_simulation(address_context=...)`` for the duration of the
#: run and copied into the algorithm when it is built, so it travels with the run and never through
#: a process-wide registration: a worker that runs one user's job after another's cannot hand the
#: second the first's context.
_run_context: ContextVar[Mapping[str, Any] | None] = ContextVar("ziplime_address_context", default=None)


def current_address_context() -> dict[str, Any]:
    """The address context of the run being built, or an empty one."""
    return dict(_run_context.get() or {})


@contextmanager
def address_context(values: Mapping[str, Any] | None):
    """Make ``values`` the address context while the block runs; restore the previous one after."""
    token = _run_context.set(dict(values) if values else None)
    try:
        yield
    finally:
        _run_context.reset(token)


def registered_schemes() -> list[str]:
    """Every scheme an extension has made available, for error messages."""
    _load_extensions()
    return sorted(_mounts)
