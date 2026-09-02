"""Tests for the data-source connector registry.

The point of the registry is that connectors are independent: a build can ship without one, and
asking for it then fails with a clear message instead of an ImportError somewhere in the core.
"""
import os
import pathlib
import unittest
from unittest import mock

from ziplime.data.data_sources import registry
from ziplime.data.data_sources.registry import (
    DataProvider, MissingProviderCredentials, UnknownDataProvider,
)


class RegistryIsolationMixin:
    """Runs each test against a private copy of the registry state."""

    def setUp(self):
        registry.load_providers()
        self._saved = dict(registry._PROVIDERS)
        self._saved_loaded = registry._LOADED

    def tearDown(self):
        registry._PROVIDERS.clear()
        registry._PROVIDERS.update(self._saved)
        registry._LOADED = self._saved_loaded


class ProviderRegistrationTests(RegistryIsolationMixin, unittest.TestCase):
    def test_registering_and_looking_up(self):
        provider = registry.register_provider(DataProvider(
            name="dummy", description="test",
            market_data_source_factory=lambda **kw: "bars"))
        self.assertIs(registry.get_provider("dummy"), provider)
        self.assertIn("dummy", registry.provider_names())

    def test_unknown_provider_names_the_ones_that_are_there(self):
        with self.assertRaises(UnknownDataProvider) as caught:
            registry.get_provider("does-not-exist")
        message = str(caught.exception)
        self.assertIn("does-not-exist", message)
        self.assertIn("yahoo", message)

    def test_registering_the_same_name_replaces(self):
        registry.register_provider(DataProvider(name="dup", description="first"))
        registry.register_provider(DataProvider(name="dup", description="second"))
        self.assertEqual(registry.get_provider("dup").description, "second")

    def test_missing_credentials_are_reported_before_the_connector_is_built(self):
        built = []
        registry.register_provider(DataProvider(
            name="needs-key", description="test", required_env=("A_KEY_THAT_IS_NOT_SET",),
            asset_data_source_factory=lambda **kw: built.append(1)))
        provider = registry.get_provider("needs-key")
        self.assertFalse(provider.is_configured)
        with self.assertRaises(MissingProviderCredentials) as caught:
            provider.create_asset_data_source()
        self.assertIn("A_KEY_THAT_IS_NOT_SET", str(caught.exception))
        self.assertEqual(built, [], "the factory must not run when credentials are missing")

    def test_configured_when_the_environment_is_set(self):
        registry.register_provider(DataProvider(
            name="needs-key", description="test", required_env=("A_KEY_THAT_IS_NOT_SET",),
            asset_data_source_factory=lambda **kw: "source"))
        with mock.patch.dict(os.environ, {"A_KEY_THAT_IS_NOT_SET": "x"}):
            provider = registry.get_provider("needs-key")
            self.assertTrue(provider.is_configured)
            self.assertEqual(provider.create_asset_data_source(), "source")

    def test_capability_flags(self):
        assets_only = DataProvider(name="a", description="", asset_data_source_factory=lambda **kw: 1)
        self.assertTrue(assets_only.supports_assets)
        self.assertFalse(assets_only.supports_market_data)
        with self.assertRaises(NotImplementedError):
            assets_only.create_market_data_source()


class ProviderDiscoveryTests(RegistryIsolationMixin, unittest.TestCase):
    def test_a_removed_connector_is_skipped_silently(self):
        registry._PROVIDERS.clear()
        with mock.patch.object(registry.importlib.util, "find_spec", return_value=None):
            registry.load_providers(reload=True)
        self.assertEqual(registry._PROVIDERS, {},
                         "no connector should register when none is installed")

    def test_a_connector_whose_dependency_is_missing_is_reported_not_fatal(self):
        # The connector package is present but its vendor SDK is not installed, so importing it
        # raises. Discovery must survive that and simply not register the connector.
        registry._PROVIDERS.clear()
        real_import = registry.importlib.import_module
        missing_sdk = ImportError("No module named 'some_vendor_sdk'")

        def failing_import(name, *args, **kwargs):
            if name.endswith(".provider"):
                raise missing_sdk
            return real_import(name, *args, **kwargs)

        with mock.patch.object(registry.importlib, "import_module", side_effect=failing_import), \
                mock.patch.object(registry.importlib, "reload", side_effect=missing_sdk):
            registry.load_providers(reload=True)  # must not raise
        self.assertEqual(registry._PROVIDERS, {})
        # And the failure surfaces as a normal lookup error for the caller.
        with self.assertRaises(UnknownDataProvider):
            registry.get_provider("yahoo")

    def test_builtin_connectors_register_themselves(self):
        registry.load_providers(reload=True)
        self.assertIn("yahoo", registry.provider_names())

    def test_configured_only_filters_on_credentials(self):
        registry.register_provider(DataProvider(
            name="unconfigured", description="", required_env=("NOT_SET_ANYWHERE",)))
        self.assertIn("unconfigured", registry.provider_names())
        self.assertNotIn("unconfigured", registry.provider_names(configured_only=True))


class ConnectorIndependenceTests(unittest.TestCase):
    """The core must never import a connector."""

    CORE_PACKAGES = ("ziplime.assets", "ziplime.finance", "ziplime.trading",
                     "ziplime.exchanges", "ziplime.core", "ziplime.data.services")

    def test_core_does_not_import_any_connector(self):
        root = pathlib.Path(registry.__file__).parents[3]
        offenders = []
        for package in self.CORE_PACKAGES:
            directory = root / pathlib.Path(package.replace(".", "/"))
            for path in directory.rglob("*.py"):
                text = path.read_text(encoding="utf-8")
                for connector in ("data_sources.yahoo",):
                    if connector in text:
                        offenders.append(f"{path.relative_to(root)} imports {connector}")
        self.assertEqual(offenders, [], "core code must reach connectors through the registry")

    def test_registry_itself_does_not_import_connectors_eagerly(self):
        source = pathlib.Path(registry.__file__).read_text(encoding="utf-8")
        for connector in ("from ziplime.data.data_sources.yahoo",):
            self.assertNotIn(connector, source)


if __name__ == "__main__":
    unittest.main()
