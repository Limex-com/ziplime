"""The configured data provider contract."""

import dataclasses
import os
from typing import Any, Callable
from ziplime.data.data_sources.asset_data_source import AssetDataSource
from ziplime.data.services.data_bundle_source import DataBundleSource
from ziplime.data.data_sources.provider_errors import MissingProviderCredentials


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
