"""The command line, which had no tests and had stopped working.

``run`` and ``ingest`` were calling four internal constructors that had all moved on without them
-- ``SimulationExchange``, ``SimulationClock``, ``SimulationParameters`` and ``run_algorithm`` --
so both raised ``TypeError`` on their first line. Nothing noticed, because ``--help`` renders from
the decorators and never executes the body, and that is all anyone had checked.

So the tests here run the commands, not their help. Most use an in-process runner against a
temporary asset database and bundle directory; the one that would need the network is skipped
rather than faked, because faking a vendor proves nothing about whether the command reaches it.

A command line is also an exit code. A failure that prints a red message and exits 0 is worse than
a crash: every script and CI job downstream treats it as success.
"""
import datetime
import shutil
import tempfile
import unittest
from pathlib import Path

import polars as pl
from asyncclick.testing import CliRunner

from ziplime.__main__ import main
from ziplime.constants.data_type import DataType

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = PROJECT_ROOT / "tests" / "fixtures"


class CliTestCase(unittest.IsolatedAsyncioTestCase):
    """A temporary asset database and bundle directory, so nothing touches the real ones."""

    async def asyncSetUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-cli-")
        self.root = Path(self._temp.name)
        self.asset_db = self.root / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", self.asset_db)
        self.runner = CliRunner()

    async def asyncTearDown(self):
        self._temp.cleanup()

    async def invoke(self, *args):
        return await self.runner.invoke(main, list(args), catch_exceptions=False)

    def storage(self):
        """The options every data-touching command takes."""
        return ["--bundle-storage-path", str(self.root), "--asset-db", str(self.asset_db)]

    def bundle_storage(self):
        """Just the bundle path -- `clean` and `bundles` have no asset database to reach."""
        return ["--bundle-storage-path", str(self.root)]


class HelpTests(CliTestCase):
    """Every command describes itself. They used to share the group's docstring.

    ``ziplime --help`` listed four commands and three of them said "Top level ziplime entry
    point", because they had no docstring of their own and click fell back to the group's.
    """

    async def test_every_command_has_its_own_description(self):
        result = await self.invoke("--help")
        self.assertEqual(result.exit_code, 0)
        self.assertNotIn("Top level ziplime entry point", result.output)
        for command in ("bundles", "clean", "ingest", "ingest-assets", "providers", "run"):
            self.assertIn(command, result.output)

    async def test_the_version_is_reported(self):
        result = await self.invoke("--version")
        self.assertEqual(result.exit_code, 0)
        self.assertIn("ziplime", result.output)


class ProvidersTests(CliTestCase):
    async def test_it_lists_the_registered_connectors(self):
        result = await self.invoke("providers")
        self.assertEqual(result.exit_code, 0)
        self.assertIn("yahoo", result.output)

    async def test_it_says_which_are_ready_to_use(self):
        """A connector needing credentials is listed but marked, rather than omitted."""
        result = await self.invoke("providers")
        self.assertIn("ready", result.output)


class FailureTests(CliTestCase):
    """A command that fails must say why, and exit non-zero."""

    async def test_conflicting_clean_selectors_are_refused(self):
        """The help had claimed this since the command was written; nothing enforced it, on a
        command whose only job is deleting data."""
        result = await self.invoke("clean", "-b", "any", "--keep-last", "1",
                                   "--before", "2024-01-01", *self.bundle_storage())
        self.assertEqual(result.exit_code, 1)
        self.assertIn("--keep-last cannot be combined", result.output)

    async def test_clean_with_no_selector_deletes_nothing_and_says_so(self):
        result = await self.invoke("clean", "-b", "any", *self.bundle_storage())
        self.assertEqual(result.exit_code, 1)
        self.assertIn("Nothing selected", result.output)

    async def test_an_unknown_symbol_names_itself(self):
        result = await self.invoke("run", "-f", str(FIXTURES / "buy_two_and_hold.py"),
                                   "-b", "any", "-s", "NOSUCHTICKER@XNGS",
                                   "--start-date", "2024-01-03", "--end-date", "2024-03-27",
                                   *self.storage())
        self.assertEqual(result.exit_code, 1)
        self.assertIn("NOSUCHTICKER@XNGS", result.output)
        self.assertIn("ingest-assets", result.output, "the message says what to do next")

    async def test_an_inverted_date_range_is_refused_before_anything_is_loaded(self):
        result = await self.invoke("run", "-f", str(FIXTURES / "buy_two_and_hold.py"),
                                   "-b", "any", "-s", "JNJ@XNYS",
                                   "--start-date", "2024-03-27", "--end-date", "2024-01-03",
                                   *self.storage())
        self.assertEqual(result.exit_code, 1)
        self.assertIn("not before", result.output)

    async def test_a_missing_bundle_points_at_the_listing_command(self):
        result = await self.invoke("run", "-f", str(FIXTURES / "buy_two_and_hold.py"),
                                   "-b", "no-such-bundle", "-s", "JNJ@XNYS",
                                   "--start-date", "2024-01-03", "--end-date", "2024-03-27",
                                   *self.storage())
        self.assertEqual(result.exit_code, 1)
        self.assertIn("ziplime bundles", result.output)

    async def test_bundles_says_when_there_are_none(self):
        result = await self.invoke("bundles", "--bundle-storage-path", str(self.root))
        self.assertEqual(result.exit_code, 0)
        self.assertIn("No bundles", result.output)


class RunTests(CliTestCase):
    """``run`` end to end, over a bundle written directly rather than downloaded."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self._write_bundle()

    async def _write_bundle(self):
        """Store a two-instrument daily bundle the way ``ingest`` would, without the network."""
        from ziplime.assets.domain.asset_type import AssetType
        from ziplime.assets.entities.asset_symbol import AssetSymbol
        from ziplime.core.ingest_data import get_asset_service
        from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry
        from ziplime.data.services.file_system_delta_lake_bundle_storage import (
            FileSystemDeltaLakeBundleStorage,
        )
        from ziplime.data.domain.data_bundle import DataBundle
        from ziplime.utils.calendar_utils import get_calendar

        asset_service = get_asset_service(db_path=str(self.asset_db))
        calendar = get_calendar("XNYS")
        self.start = datetime.date(2024, 1, 3)
        self.end = datetime.date(2024, 3, 27)
        sessions = calendar.sessions_in_range(self.start, self.end)
        closes = list(calendar.schedule.loc[sessions, "close"].dt.tz_convert(calendar.tz))

        listings = []
        for ticker in ("JNJ", "KO"):
            listings.append(await asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=ticker, mic="XNYS"), asset_type=AssetType.EQUITY))
        rows = [{"date": close, "sid": listing.sid, "symbol": listing.symbol, "mic": listing.mic,
                 "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "price": 100.0,
                 "volume": 1_000_000.0}
                for listing in listings for close in closes]
        data = pl.DataFrame(rows).sort(["sid", "date"])

        registry = FileSystemBundleRegistry(base_data_path=str(self.root))
        storage = FileSystemDeltaLakeBundleStorage(base_data_path=str(self.root),
                                                   compression_level=1)
        bundle = DataBundle(
            name="cli-test", version="1", start_date=data["date"].min(),
            end_date=data["date"].max(), trading_calendar=calendar,
            frequency=datetime.timedelta(days=1), original_frequency=datetime.timedelta(days=1),
            data_type=DataType.MARKET_DATA, timestamp=data["date"].max(), data=data)
        await registry.register_bundle(data_bundle=bundle, bundle_storage=storage, merge=False)
        await storage.store_bundle(data_bundle=bundle, merge=False,
                                   merge_columns=["sid", "date"])
        await asset_service._asset_repository.engine.dispose()

    async def test_a_backtest_runs_and_reports_what_happened(self):
        """`str(result)` used to be the entire output -- a dataclass repr, several screens of
        nested frames, and no answer to what the run did."""
        result = await self.invoke(
            "run", "-f", str(FIXTURES / "buy_two_and_hold.py"), "-b", "cli-test",
            "-s", "JNJ@XNYS,KO@XNYS", "--start-date", "2024-01-03", "--end-date", "2024-03-27",
            *self.storage())
        self.assertEqual(result.exit_code, 0, result.output)
        for line in ("sessions", "return", "max drawdown", "transactions", "final value"):
            self.assertIn(line, result.output)

    async def test_it_writes_a_csv_when_asked(self):
        out = self.root / "perf.csv"
        result = await self.invoke(
            "run", "-f", str(FIXTURES / "buy_two_and_hold.py"), "-b", "cli-test",
            "-s", "JNJ@XNYS,KO@XNYS", "--start-date", "2024-01-03", "--end-date", "2024-03-27",
            "-o", str(out), *self.storage())
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertTrue(out.exists())
        self.assertGreater(len(out.read_text().splitlines()), 10)

    async def test_the_bundle_is_listed(self):
        result = await self.invoke("bundles", "--bundle-storage-path", str(self.root))
        self.assertEqual(result.exit_code, 0)
        self.assertIn("cli-test", result.output)

    async def test_next_bar_execution_is_the_default(self):
        """Same-bar execution fills at a price the market had not printed when the decision was
        taken. It should be asked for, not inherited."""
        result = await self.invoke(
            "run", "-f", str(FIXTURES / "buy_two_and_hold.py"), "-b", "cli-test",
            "-s", "JNJ@XNYS", "--start-date", "2024-01-03", "--end-date", "2024-03-27",
            *self.storage())
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertNotIn("LOOK-AHEAD", result.output)


class SlippageArgumentTests(unittest.TestCase):
    """``--slippage-bps 0`` is a legitimate control run, and used to be refused.

    Worse, it was refused with "volume_limit must be positive" -- a copy-paste in the message that
    sent you to inspect a parameter you had not passed.
    """

    def test_zero_basis_points_is_allowed(self):
        from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage

        model = FixedBasisPointsSlippage(basis_points=0)
        self.assertEqual(model.basis_points, 0)
        self.assertGreater(model.volume_limit, 0, "still capped by volume, unlike NoSlippage")

    def test_a_negative_is_refused_by_its_own_name(self):
        from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage

        with self.assertRaises(ValueError) as raised:
            FixedBasisPointsSlippage(basis_points=-1)
        self.assertIn("basis_points", str(raised.exception))

    def test_a_zero_volume_limit_is_still_refused(self):
        from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage

        with self.assertRaises(ValueError) as raised:
            FixedBasisPointsSlippage(volume_limit=0)
        self.assertIn("volume_limit", str(raised.exception))


if __name__ == "__main__":
    unittest.main(verbosity=2)


class CleanTests(CliTestCase):
    """`clean` deletes what was selected, and only that.

    Worth an end-to-end class rather than an argument test. Until now `BundleService.clean` took
    the four arguments below, ignored every one of them, iterated *all* bundles instead of the one
    named, and called `self._delete_bundle` -- a method that does not exist. It raised
    `AttributeError` on its first version, which is the only reason a command whose selectors were
    ignored never deleted the wrong thing. So both halves need covering: that deletion now happens
    at all, and that it stays inside what was asked for.
    """

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.versions = []
        for index in range(3):
            self.versions.append(await self._write_version("many", str(100 + index)))
        # A second bundle, to prove the named one is the only one touched.
        await self._write_version("other", "1")

    async def _write_version(self, name: str, version: str) -> str:
        from ziplime.assets.domain.asset_type import AssetType
        from ziplime.assets.entities.asset_symbol import AssetSymbol
        from ziplime.core.ingest_data import get_asset_service
        from ziplime.data.domain.data_bundle import DataBundle
        from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry
        from ziplime.data.services.file_system_delta_lake_bundle_storage import (
            FileSystemDeltaLakeBundleStorage,
        )
        from ziplime.utils.calendar_utils import get_calendar

        asset_service = get_asset_service(db_path=str(self.asset_db))
        calendar = get_calendar("XNYS")
        sessions = calendar.sessions_in_range(datetime.date(2024, 1, 3), datetime.date(2024, 1, 31))
        closes = list(calendar.schedule.loc[sessions, "close"].dt.tz_convert(calendar.tz))
        listing = await asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol="JNJ", mic="XNYS"), asset_type=AssetType.EQUITY)
        data = pl.DataFrame(
            [{"date": close, "sid": listing.sid, "symbol": "JNJ", "mic": "XNYS",
              "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "price": 100.0,
              "volume": 1e6} for close in closes]).sort(["sid", "date"])

        registry = FileSystemBundleRegistry(base_data_path=str(self.root))
        storage = FileSystemDeltaLakeBundleStorage(base_data_path=str(self.root),
                                                   compression_level=1)
        bundle = DataBundle(
            name=name, version=version, start_date=data["date"].min(),
            end_date=data["date"].max(), trading_calendar=calendar,
            frequency=datetime.timedelta(days=1), original_frequency=datetime.timedelta(days=1),
            data_type=DataType.MARKET_DATA, timestamp=data["date"].max(), data=data)
        await registry.register_bundle(data_bundle=bundle, bundle_storage=storage, merge=False)
        await storage.store_bundle(data_bundle=bundle, merge=False, merge_columns=["sid", "date"])
        await asset_service._asset_repository.engine.dispose()
        return version

    def stored_versions(self, name: str) -> set[str]:
        """What is actually on disk, which is the only answer that matters for a delete."""
        path = self.root / "data_bundle" / name
        return {child.name for child in path.iterdir()} if path.is_dir() else set()

    def registered_versions(self, name: str) -> set[str]:
        return {path.name.removeprefix(f"{name}_").removesuffix(".json")
                for path in (self.root / "bundle_registry").iterdir()
                if path.name.startswith(f"{name}_")}

    async def test_keep_last_keeps_that_many_and_deletes_the_rest(self):
        result = await self.invoke("clean", "-b", "many", "--keep-last", "1",
                                   *self.bundle_storage())
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(len(self.stored_versions("many")), 1)
        self.assertEqual(self.registered_versions("many"), self.stored_versions("many"))

    async def test_the_data_goes_too_not_just_the_registry_entry(self):
        """Deleting the metadata alone would leave the bars on disk, invisible to `bundles` and
        still taking the space the caller was reclaiming."""
        before = self.stored_versions("many")
        await self.invoke("clean", "-b", "many", "--keep-last", "0", *self.bundle_storage())
        self.assertEqual(len(before), 3)
        self.assertEqual(self.stored_versions("many"), set())
        self.assertEqual(self.registered_versions("many"), set())

    async def test_another_bundle_is_not_touched(self):
        """The old implementation iterated every bundle in the registry, not the one named."""
        await self.invoke("clean", "-b", "many", "--keep-last", "0", *self.bundle_storage())
        self.assertEqual(self.stored_versions("other"), {"1"})
        self.assertEqual(self.registered_versions("other"), {"1"})

    async def test_before_deletes_only_what_predates_it(self):
        result = await self.invoke("clean", "-b", "many", "--before", "2000-01-01",
                                   *self.bundle_storage())
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("nothing was deleted", result.output)
        self.assertEqual(len(self.stored_versions("many")), 3)

    async def test_before_a_future_date_deletes_every_version(self):
        await self.invoke("clean", "-b", "many", "--before", "2099-01-01", *self.bundle_storage())
        self.assertEqual(self.stored_versions("many"), set())

    async def test_it_names_the_versions_it_deleted(self):
        """A command that deleted nothing used to look exactly like one that deleted everything."""
        result = await self.invoke("clean", "-b", "many", "--keep-last", "1",
                                   *self.bundle_storage())
        self.assertIn("Deleted 2 version(s) of many", result.output)

    async def test_an_unknown_bundle_is_refused_rather_than_silently_doing_nothing(self):
        result = await self.invoke("clean", "-b", "nope", "--keep-last", "1",
                                   *self.bundle_storage())
        self.assertEqual(result.exit_code, 1)
        self.assertIn("not found", result.output)
