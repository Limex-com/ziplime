"""Registry of data-source connectors.

A connector bundles the two ways ziplime reads a market: an
:class:`~ziplime.data.data_sources.asset_data_source.AssetDataSource` that describes instruments,
and a :class:`~ziplime.data.services.data_bundle_source.DataBundleSource` that fetches their bars.
Registering them here means code and CLI options refer to a vendor by name rather than importing it.

Connectors are **independent of each other and of the core**. Nothing outside a connector's own
package may import it, and discovery tolerates a connector that is not installed: deleting a
connector's package leaves a build that still ships the others.
Third parties can add their own by advertising a ``ziplime.data_providers`` entry point.
"""
import importlib
import importlib.util
import sys

import structlog
from ziplime.data.data_sources.provider_errors import UnknownDataProvider
from ziplime.data.data_sources.data_provider import DataProvider


_logger = structlog.get_logger(__name__)

#: Connectors shipped with ziplime. Each module registers itself on import; one that is absent is
#: skipped, which is what makes a connector removable.
BUILTIN_PROVIDER_MODULES: tuple[str, ...] = (
    "ziplime.data.data_sources.yahoo.provider",
)

#: Entry point group third-party connectors advertise themselves under.
ENTRY_POINT_GROUP = "ziplime.data_providers"


_PROVIDERS: dict[str, DataProvider] = {}
_LOADED = False


def register_provider(provider: DataProvider) -> DataProvider:
    """Register a connector, replacing any previous registration of the same name."""
    _PROVIDERS[provider.name] = provider
    return provider


def load_providers(reload: bool = False) -> None:
    """Import the built-in connector modules and any advertised through entry points.

    A connector whose package is absent is skipped silently -- that is how a build ships without it.
    A connector that is present but fails to import is reported, because that is a real problem
    (usually a missing third-party dependency) rather than a deliberate omission.

    Args:
        reload: Re-run discovery even if it has already run. Connectors register themselves as an
            import side effect, so an already-imported module has to be reloaded for its
            registration to happen again.
    """
    global _LOADED
    if _LOADED and not reload:
        return

    for module_name in BUILTIN_PROVIDER_MODULES:
        try:
            if importlib.util.find_spec(module_name) is None:
                _logger.debug("Data provider not installed, skipping", module=module_name)
                continue
        except (ImportError, ValueError):
            _logger.debug("Data provider not importable, skipping", module=module_name)
            continue
        try:
            module = sys.modules.get(module_name)
            if module is not None and reload:
                importlib.reload(module)
            else:
                importlib.import_module(module_name)
        except Exception as error:
            _logger.warning("Data provider failed to load", module=module_name, error=str(error))

    try:
        from importlib.metadata import entry_points
        for entry_point in entry_points(group=ENTRY_POINT_GROUP):
            try:
                entry_point.load()
            except Exception as error:
                _logger.warning("Third-party data provider failed to load",
                                entry_point=entry_point.name, error=str(error))
    except Exception as error:  # importlib.metadata problems must not break ziplime
        _logger.debug("Could not read data provider entry points", error=str(error))

    _LOADED = True


def get_provider(name: str) -> DataProvider:
    """Return the connector registered under ``name``.

    Raises:
        UnknownDataProvider: if nothing is registered under that name.
    """
    load_providers()
    try:
        return _PROVIDERS[name]
    except KeyError:
        raise UnknownDataProvider(name, sorted(_PROVIDERS)) from None


def list_providers(configured_only: bool = False) -> list[DataProvider]:
    """Return every registered connector, sorted by name.

    Args:
        configured_only: Return only connectors whose required environment variables are set.
    """
    load_providers()
    providers = sorted(_PROVIDERS.values(), key=lambda p: p.name)
    return [p for p in providers if p.is_configured] if configured_only else providers


def provider_names(configured_only: bool = False) -> list[str]:
    """Return the names of registered connectors, for CLI choices and error messages."""
    return [provider.name for provider in list_providers(configured_only=configured_only)]
