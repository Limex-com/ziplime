"""A strategy names a dataset by ``scheme://...`` and an extension supplies what mounts it.

zipfinam's ``algopack://`` is the case this exists for: before, the only way for an extension to
add a scheme was to wrap ``TradingAlgorithm._resolve_named_data_source`` on import.
"""

import asyncio
import datetime
import types
import unittest
from unittest import mock

from ziplime.data.data_sources import address_schemes
from ziplime.data.data_sources.address_schemes import (
    ENTRY_POINT_GROUP,
    mount_for,
    register_address_scheme,
    registered_schemes,
    scheme_of,
    unregister_address_scheme,
)
from ziplime.trading.trading_algorithm import TradingAlgorithm
from ziplime.utils.calendar_utils import get_calendar


class Recorder:
    """A mount that remembers how it was called and hands back a marker source."""

    def __init__(self, asynchronous: bool = False):
        self.calls = []
        self.asynchronous = asynchronous
        self.source = types.SimpleNamespace(name="mounted")

    def __call__(self, address, **kwargs):
        self.calls.append((address, kwargs))
        if self.asynchronous:
            async def mounted():
                return self.source
            return mounted()
        return self.source


def algorithm():
    """Just what `_resolve_named_data_source` reads off the algorithm."""
    clock = types.SimpleNamespace(start_session=datetime.date(2025, 1, 2),
                                  end_session=datetime.date(2025, 6, 30),
                                  trading_calendar=get_calendar("XNYS"))
    return types.SimpleNamespace(clock=clock, asset_service=object())


def resolve(name):
    return asyncio.run(TradingAlgorithm._resolve_named_data_source(algorithm(), name))


class SchemeTests(unittest.TestCase):

    def test_the_scheme_is_read_off_the_address(self):
        self.assertEqual(scheme_of("algopack://eq/tradestats"), "algopack")
        self.assertEqual(scheme_of("AlgoPack://eq/tradestats"), "algopack")
        self.assertEqual(scheme_of("my-data://x"), "my-data")

    def test_a_plain_name_has_no_scheme(self):
        for name in ("tradestats", "Factors-Q500US", "", "://x", "a b://x", None):
            self.assertIsNone(scheme_of(name), name)


class RegistrationTests(unittest.TestCase):

    def tearDown(self):
        unregister_address_scheme("test")

    def test_a_registered_scheme_is_found(self):
        mount = Recorder()
        register_address_scheme("test", mount)
        self.assertIs(mount_for("test://anything"), mount)
        self.assertIn("test", registered_schemes())

    def test_unregistering_forgets_it(self):
        register_address_scheme("test://", Recorder())
        unregister_address_scheme("test")
        self.assertIsNone(mount_for("test://anything"))

    def test_a_mount_must_be_callable(self):
        with self.assertRaises(TypeError):
            register_address_scheme("test", "not a function")
        with self.assertRaises(ValueError):
            register_address_scheme("", Recorder())


class EntryPointTests(unittest.TestCase):

    def setUp(self):
        self.addCleanup(unregister_address_scheme, "test")
        self.addCleanup(setattr, address_schemes, "_entry_points_loaded", True)

    def advertise(self, *advertised):
        patcher = mock.patch("importlib.metadata.entry_points",
                             lambda group: [ep for ep in advertised if group == ENTRY_POINT_GROUP])
        patcher.start()
        self.addCleanup(patcher.stop)
        address_schemes._entry_points_loaded = False

    def test_an_installed_extension_is_picked_up_without_being_imported(self):
        mount = Recorder()
        self.advertise(types.SimpleNamespace(name="test", load=lambda: mount))
        self.assertIs(mount_for("test://x"), mount)

    def test_a_registration_on_import_wins_over_the_entry_point(self):
        explicit, advertised = Recorder(), Recorder()
        register_address_scheme("test", explicit)
        self.advertise(types.SimpleNamespace(name="test", load=lambda: advertised))
        self.assertIn("test", registered_schemes())
        self.assertIs(mount_for("test://x"), explicit)

    def test_a_broken_extension_is_skipped(self):
        def broken():
            raise ImportError("moexalgo is not installed")

        self.advertise(types.SimpleNamespace(name="test", load=broken))
        self.assertIsNone(mount_for("test://x"))


class ResolutionTests(unittest.TestCase):
    """What `data.history(data_source="scheme://...")` reaches."""

    def tearDown(self):
        unregister_address_scheme("test")

    def test_the_mount_gets_the_simulation_window_and_calendar(self):
        mount = Recorder()
        register_address_scheme("test", mount)

        source = resolve("test://eq/tradestats")

        self.assertIs(source, mount.source)
        address, kwargs = mount.calls[0]
        self.assertEqual(address, "test://eq/tradestats")
        self.assertEqual(kwargs["start_date"], datetime.date(2025, 1, 2))
        self.assertEqual(kwargs["end_date"], datetime.date(2025, 6, 30))
        self.assertEqual(kwargs["trading_calendar"].name, "XNYS")
        self.assertIn("asset_service", kwargs)

    def test_the_runs_address_context_reaches_the_mount_but_cannot_override_the_engine(self):
        mount = Recorder()
        register_address_scheme("test", mount)
        algo = algorithm()
        algo._address_context = {"user_id": "u-1", "start_date": datetime.date(1999, 1, 1)}
        asyncio.run(TradingAlgorithm._resolve_named_data_source(algo, "test://x"))
        _, kwargs = mount.calls[0]
        self.assertEqual(kwargs["user_id"], "u-1")
        self.assertEqual(kwargs["start_date"], datetime.date(2025, 1, 2))

    def test_an_asynchronous_mount_is_awaited(self):
        mount = Recorder(asynchronous=True)
        register_address_scheme("test", mount)
        self.assertIs(resolve("test://x"), mount.source)

    def test_an_unknown_scheme_says_where_schemes_come_from(self):
        with self.assertRaises(KeyError) as raised:
            resolve("algopack://eq/tradestats")
        self.assertIn("zipfinam", str(raised.exception))

    def test_a_plain_name_is_still_an_unknown_source(self):
        with self.assertRaises(KeyError) as raised:
            resolve("tradestats")
        self.assertIn("hf://owner/name/config", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
