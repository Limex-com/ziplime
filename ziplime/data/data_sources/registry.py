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
import dataclasses
import importlib
import importlib.util
import os
import sys
from typing import Any, Callable

import structlog

from ziplime.data.data_sources.asset_data_source import AssetDataSource
from ziplime.data.services.data_bundle_source import DataBundleSource

_logger = structlog.get_logger(__name__)

#: Connectors shipped with ziplime. Each module registers itself on import; one that is absent is
#: skipped, which is what makes a connector removable.
BUILTIN_PROVIDER_MODULES: tuple[str, ...] = (
    "ziplime.data.data_sources.yahoo.provider",
)

#: Entry point group third-party connectors advertise themselves under.
ENTRY_POINT_GROUP = "ziplime.data_providers"


class UnknownDataProvider(LookupError):
    """Raised when a provider name is not registered."""

    def __init__(self, name: str, known: list[str]):
        super().__init__(
            f"Unknown data provider {name!r}. Registered providers: {known or 'none'}. "
            f"A provider is missing if its package was removed or its dependencies are not installed."
        )
        self.name = name
        self.known = known


class MissingProviderCredentials(RuntimeError):
    """Raised when a provider is asked for but its credentials are not configured."""

    def __init__(self, provider: str, missing: tuple[str, ...]):
        super().__init__(
            f"Data provider {provider!r} needs these environment variables: {', '.join(missing)}."
        )
        self.provider = provider
        self.missing = missing


@dataclasses.dataclass(frozen=True)
class DataProvider:
    """A named connector to a market data vendor.

    Attributes:
        name: Identifier used in code, CLI options and configuration, e.g. ``"yahoo"``.
        description: One line describing what the connector covers.
        asset_data_source_factory: Builds the reference-data source, or ``None`` if the vendor
            offers no instrument metadata.
        market_data_source_factory: Builds the bar source, or ``None`` if the vendor offers no bars.
        required_env: Environment variables that must be set before the connector can be built.
        default_mic: Exchange this connector serves by default, when it serves exactly one.
        default_calendar: ``exchange_calendars`` name matching ``default_mic``.
        asset_types: Asset types the connector can describe, as
            :class:`~ziplime.assets.domain.asset_type.AssetType` values.
    """

    name: str
    description: str
    asset_data_source_factory: Callable[..., AssetDataSource] | None = None
    market_data_source_factory: Callable[..., DataBundleSource] | None = None
    required_env: tuple[str, ...] = ()
    default_mic: str | None = None
    default_calendar: str | None = None
    asset_types: tuple[str, ...] = ()

    @property
    def supports_assets(self) -> bool:
        return self.asset_data_source_factory is not None

    @property
    def supports_market_data(self) -> bool:
        return self.market_data_source_factory is not None

    def missing_env(self) -> tuple[str, ...]:
        """Return the required environment variables that are not set."""
        return tuple(name for name in self.required_env if not os.environ.get(name))

    @property
    def is_configured(self) -> bool:
        return not self.missing_env()

    def _check_configured(self) -> None:
        missing = self.missing_env()
        if missing:
            raise MissingProviderCredentials(self.name, missing)

    def create_asset_data_source(self, **kwargs: Any) -> AssetDataSource:
        """Build this connector's reference-data source."""
        if self.asset_data_source_factory is None:
            raise NotImplementedError(f"Provider {self.name!r} has no asset data source.")
        self._check_configured()
        return self.asset_data_source_factory(**kwargs)

    def create_market_data_source(self, **kwargs: Any) -> DataBundleSource:
        """Build this connector's bar source.

        Every connector accepts an optional ``assets=[ExchangeAsset, ...]``: the listings that are
        about to be ingested. Connectors use it to avoid per-symbol metadata lookups and to keep
        requests inside each listing's lifetime.
        """
        if self.market_data_source_factory is None:
            raise NotImplementedError(f"Provider {self.name!r} has no market data source.")
        self._check_configured()
        return self.market_data_source_factory(**kwargs)


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
