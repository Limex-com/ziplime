"""Mounting point-in-time datasets from the Hugging Face Hub.

The tests that matter here are the point-in-time ones. A dataset of disclosures carries two dates
-- when the trade happened and when it became public -- and the whole value of the thing depends
on a backtest seeing only the second. A congressional trade surfaces up to 45 days after
execution; a 13F reports a quarter that ended 45 days earlier. Index on the wrong column and the
equity curve is spectacular for a reason that has nothing to do with the strategy, with nothing in
the output to say so.

So :class:`PointInTimeTests` builds a dataset where the gap between the two dates is known exactly
and checks that the gap is respected -- that a row is invisible for as long as it was unpublished,
and visible immediately afterwards.

Nothing here touches the network. The manifests and README front matter are recorded under
``tests/fixtures/huggingface/`` as the real datasets publish them.
"""
import datetime
import unittest
from pathlib import Path

import polars as pl

from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.data.data_sources.huggingface.huggingface_data_source import (
    HuggingFaceDataSource, is_address, parse_address,
)
from ziplime.data.data_sources.huggingface.manifest import (
    EVENT_DATE_COLUMNS, KNOWLEDGE_DATE_COLUMNS, ManifestError, NoKnowledgeDateError,
    _glob_matches, parse_manifest, resolve_entity_column, resolve_knowledge_column,
)

FIXTURES = Path(__file__).parent / "fixtures" / "huggingface"
FAR_PAST = datetime.date(1900, 1, 1)
FAR_FUTURE = datetime.date(2099, 1, 1)
EXCHANGE = ExchangeInfo(mic="XNYS", name="NYSE", canonical_name="NYSE", country_code="US")


def load_fixture(dataset: str):
    """The two documents a dataset describes itself with, as published."""
    manifest = (FIXTURES / f"{dataset}-manifest.json").read_text()
    readme = (FIXTURES / f"{dataset}-README.md").read_text()
    return parse_manifest(f"ZipLime/{dataset}", "0" * 40, manifest, readme)


def make_equity(sid: int, symbol: str) -> ExchangeAsset:
    equity = Equity(id=sid + 10_000, isin=None, asset_name=symbol, start_date=FAR_PAST,
                    end_date=FAR_FUTURE, first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)
    usd = Currency(id=2, isin=None, asset_name="USD", start_date=FAR_PAST, end_date=FAR_FUTURE,
                   first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)
    return ExchangeAsset(sid=sid, symbol=symbol, start_date=FAR_PAST, end_date=FAR_FUTURE,
                         first_traded=FAR_PAST, auto_close_date=FAR_FUTURE, external_id="",
                         exchange=EXCHANGE, asset=equity, quote=usd)


class AddressTests(unittest.TestCase):
    def test_an_address_names_a_repository_and_a_config(self):
        self.assertEqual(parse_address("hf://ZipLime/congress-trading/features"),
                         ("ZipLime/congress-trading", "features"))

    def test_the_config_is_optional(self):
        self.assertEqual(parse_address("hf://ZipLime/congress-trading"),
                         ("ZipLime/congress-trading", None))

    def test_a_nested_config_path_survives(self):
        self.assertEqual(parse_address("hf://owner/name/data/pit"), ("owner/name", "data/pit"))

    def test_things_that_are_not_addresses(self):
        for value in ("ZipLime/congress-trading", "s3://bucket/key", "features", 7, None):
            self.assertFalse(is_address(value), value)

    def test_an_address_without_a_repository_is_refused(self):
        for value in ("hf://", "hf://only"):
            with self.assertRaises(ValueError):
                parse_address(value)


class GlobTests(unittest.TestCase):
    """``*`` must not cross a directory separator, or a config swallows its neighbours."""

    def test_a_star_stays_inside_one_directory(self):
        self.assertTrue(_glob_matches("data/features/*.parquet", "data/features/a.parquet"))
        self.assertFalse(_glob_matches("data/features/*.parquet",
                                       "data/features/knowledge_year=2004/a.parquet"))

    def test_a_double_star_crosses_directories(self):
        self.assertTrue(_glob_matches("data/features/**/*.parquet",
                                      "data/features/knowledge_year=2004/a.parquet"))

    def test_a_double_star_also_matches_no_directory(self):
        # `data/pit/**/*.parquet` has to find `data/pit/part-0.parquet` too.
        self.assertTrue(_glob_matches("data/features/**/*.parquet", "data/features/a.parquet"))

    def test_a_partition_pattern_matches(self):
        self.assertTrue(_glob_matches("knowledge_year=*/*.parquet",
                                      "knowledge_year=2004/part-000.parquet"))

    def test_an_unrelated_path_does_not_match(self):
        self.assertFalse(_glob_matches("data/features/**/*.parquet", "data/trades/a.parquet"))


class ManifestTests(unittest.TestCase):
    """The published datasets must describe themselves well enough to mount, unchanged."""

    def test_the_congress_dataset_declares_its_configs(self):
        # It carries the older manifest, with no `configs` block -- these come from the README
        # front matter, which is the Hub's own standard.
        manifest = load_fixture("congress-trading")
        self.assertEqual(manifest.name, "congress_trading")
        self.assertEqual(manifest.entity_domain, "us_equities")
        self.assertIn("features", manifest.configs)
        self.assertEqual(manifest.config("features").paths,
                         ("data/features/daily-by-ticker.parquet",))

    def test_the_insider_dataset_declares_its_configs_and_coverage(self):
        manifest = load_fixture("insider-trading")
        self.assertEqual(manifest.config("features").paths, ("data/features/**/*.parquet",))
        self.assertEqual(manifest.coverage_start, datetime.date(2004, 1, 2))
        self.assertEqual(manifest.coverage_end, datetime.date(2016, 3, 3))

    def test_the_13f_dataset_declares_a_delta_table(self):
        manifest = load_fixture("institutional-portfolio-13f")
        self.assertEqual(manifest.default_config, "positions")
        self.assertIsNotNone(manifest.config("positions").delta_path)

    def test_features_is_preferred_as_the_default(self):
        for dataset in ("congress-trading", "insider-trading"):
            self.assertEqual(load_fixture(dataset).default_config, "features", dataset)

    def test_an_unknown_config_names_the_ones_that_exist(self):
        manifest = load_fixture("congress-trading")
        with self.assertRaises(ManifestError) as raised:
            manifest.config("nope")
        self.assertIn("trades", str(raised.exception))

    def test_a_dataset_that_describes_nothing_cannot_be_mounted(self):
        manifest = parse_manifest("owner/name", "sha", manifest_json=None, readme=None)
        with self.assertRaises(ManifestError):
            manifest.config(None)

    def test_unreadable_metadata_is_ignored_rather_than_fatal(self):
        manifest = parse_manifest("owner/name", "sha", manifest_json="{not json",
                                  readme="---\n[[[\n---\n")
        self.assertEqual(manifest.configs, {})


class KnowledgeColumnTests(unittest.TestCase):
    """Choosing the column a mount is indexed on. The one decision that must not be wrong."""

    def test_the_congress_features_index_is_its_disclosure_day(self):
        columns = ["ticker", "date", "n_disclosures", "held_as_of_date"]
        self.assertEqual(resolve_knowledge_column(columns, "x", "features"), "date")

    def test_availability_beats_the_day_it_summarises(self):
        # insider `features` carries both. `knowledge_day` is the New York day of the underlying
        # events; the aggregated row is not readable until that day is over, which is what
        # `feature_available_at` records. Taking the earlier one leaks up to a day.
        columns = ["ticker", "knowledge_day", "knowledge_date", "feature_available_at"]
        self.assertEqual(resolve_knowledge_column(columns, "x", "features"),
                         "feature_available_at")

    def test_a_knowledge_date_beats_a_bare_date(self):
        columns = ["ticker", "date", "knowledge_date"]
        self.assertEqual(resolve_knowledge_column(columns, "x", "c"), "knowledge_date")

    def test_an_event_date_is_never_the_index(self):
        # The whole point. A dataset offering only transaction dates cannot be mounted
        # point-in-time, and saying so is better than indexing on it.
        with self.assertRaises(NoKnowledgeDateError):
            resolve_knowledge_column(["ticker", "transaction_date"], "ZipLime/x", "trades")

    def test_the_refusal_names_the_event_columns_it_found(self):
        with self.assertRaises(NoKnowledgeDateError) as raised:
            resolve_knowledge_column(["ticker", "transaction_date", "period_of_report"],
                                     "ZipLime/x", "trades")
        message = str(raised.exception)
        self.assertIn("transaction_date", message)
        self.assertIn("period_of_report", message)

    def test_no_column_is_both_an_event_and_a_knowledge_date(self):
        self.assertEqual(set(KNOWLEDGE_DATE_COLUMNS) & set(EVENT_DATE_COLUMNS), set())

    def test_the_instrument_column_is_found(self):
        self.assertEqual(resolve_entity_column(["ticker", "date"], "x", "c"), "ticker")
        self.assertEqual(resolve_entity_column(["asset_ticker", "date"], "x", "c"),
                         "asset_ticker")

    def test_a_dataset_with_no_instrument_column_is_refused(self):
        with self.assertRaises(ManifestError):
            resolve_entity_column(["date", "value"], "x", "c")


class PointInTimeTests(unittest.IsolatedAsyncioTestCase):
    """A row must be invisible until its knowledge date, and visible right after.

    The dataset here is deliberately tiny and hand-made so the answer is arithmetic rather than a
    matter of opinion: one disclosure, executed on the 1st, published on the 20th. Nineteen days
    of a real backtest must not see it.
    """

    EVENT = datetime.datetime(2024, 3, 1, tzinfo=datetime.timezone.utc)
    KNOWLEDGE = datetime.datetime(2024, 3, 20, tzinfo=datetime.timezone.utc)

    def make_source(self) -> HuggingFaceDataSource:
        asset = make_equity(sid=1, symbol="AAPL")
        source = HuggingFaceDataSource.__new__(HuggingFaceDataSource)
        HuggingFaceDataSource.__init__(
            source, name="hf://test/set/features", revision=None, manifest=None,
            config="features", repo_files=(), knowledge_column="knowledge_date",
            entity_column="ticker", event_column="transaction_date", asset_service=None,
            start_date=datetime.date(2024, 1, 1), end_date=datetime.date(2024, 12, 31))
        source.data = pl.DataFrame({
            "date": [self.KNOWLEDGE],
            "sid": [asset.sid],
            "transaction_date": [self.EVENT],
            "net_notional_usd": [100_000.0],
        }).with_columns(pl.col("date").dt.replace_time_zone("UTC"),
                        pl.col("transaction_date").dt.replace_time_zone("UTC"))
        self.asset = asset
        return source

    async def read_at(self, source, when: datetime.datetime) -> pl.DataFrame:
        return await source.get_data_by_limit(
            fields=frozenset({"net_notional_usd"}), limit=10, end_date=when,
            frequency=datetime.timedelta(days=1), assets=frozenset({self.asset}),
            include_end_date=False)

    async def test_the_row_is_invisible_before_it_was_published(self):
        source = self.make_source()
        for days_after_the_trade in range(0, 19):
            when = self.EVENT + datetime.timedelta(days=days_after_the_trade)
            self.assertTrue((await self.read_at(source, when)).is_empty(),
                            f"a disclosure published on {self.KNOWLEDGE.date()} was visible "
                            f"{days_after_the_trade} days after the trade")

    async def test_the_row_appears_once_it_was_published(self):
        source = self.make_source()
        visible = await self.read_at(source, self.KNOWLEDGE + datetime.timedelta(days=1))
        self.assertEqual(len(visible), 1)
        self.assertAlmostEqual(visible["net_notional_usd"].to_list()[0], 100_000.0)

    async def test_the_event_date_is_carried_but_is_not_the_index(self):
        # The transaction date stays available as a value -- a strategy may legitimately want to
        # know how stale a disclosure is -- but it is not what decides visibility.
        source = self.make_source()
        self.assertIn("transaction_date", source.data.columns)
        on_the_trade_date = await self.read_at(source, self.EVENT + datetime.timedelta(days=1))
        self.assertTrue(on_the_trade_date.is_empty())

    async def test_a_window_past_the_coverage_returns_nothing_rather_than_raising(self):
        # A strategy that merely consults a dataset must not die when the backtest walks past its
        # last row.
        source = self.make_source()
        beyond = datetime.datetime(2030, 1, 1, tzinfo=datetime.timezone.utc)
        self.assertTrue((await self.read_at(source, beyond)).is_empty())


class WindowTests(unittest.TestCase):
    """Only the Parquet parts the window can reach are downloaded."""

    def make_source(self, files, start, end) -> HuggingFaceDataSource:
        source = HuggingFaceDataSource.__new__(HuggingFaceDataSource)
        HuggingFaceDataSource.__init__(
            source, name="test", revision=None, manifest=None, config="features",
            repo_files=tuple(files), knowledge_column="knowledge_date", entity_column="ticker",
            event_column=None, asset_service=None, start_date=start, end_date=end)
        return source

    def test_partitions_outside_the_window_are_skipped(self):
        files = [f"data/features/knowledge_year={year}/part-000.parquet"
                 for year in range(2004, 2017)]
        source = self.make_source(files, datetime.date(2012, 1, 1), datetime.date(2012, 12, 31))
        self.assertEqual(source._parts_in_window(),
                         ["data/features/knowledge_year=2012/part-000.parquet"])

    def test_a_window_spanning_years_keeps_all_of_them(self):
        files = [f"data/features/knowledge_year={year}/part-000.parquet"
                 for year in range(2004, 2017)]
        source = self.make_source(files, datetime.date(2011, 6, 1), datetime.date(2013, 6, 1))
        self.assertEqual(len(source._parts_in_window()), 3)

    def test_an_unpartitioned_part_is_always_read(self):
        # Nothing in its name rules it out, so it cannot be skipped.
        source = self.make_source(["data/features/daily-by-ticker.parquet"],
                                  datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))
        self.assertEqual(source._parts_in_window(), ["data/features/daily-by-ticker.parquet"])

    def test_the_bounds_the_engine_sees_are_instants(self):
        # The base class compares them against the simulation clock, which is tz-aware.
        source = self.make_source([], datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))
        self.assertIsInstance(source.start_date, datetime.datetime)
        self.assertIsNotNone(source.end_date.tzinfo)
        self.assertEqual(source.window_start, datetime.date(2024, 1, 1))


class TimestampNormalisationTests(unittest.TestCase):
    """Whatever a publisher stamped, the mounted column is one comparable instant."""

    def normalise(self, frame: pl.DataFrame, zone: str = "UTC") -> pl.Series:
        from ziplime.data.data_sources.huggingface.huggingface_data_source import (
            _to_session_time,
        )
        return frame.select(_to_session_time("d", frame.schema["d"], zone).alias("x"))["x"]

    def test_a_plain_date_becomes_midnight(self):
        out = self.normalise(pl.DataFrame({"d": [datetime.date(2024, 1, 2)]}))
        self.assertEqual(out[0], datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc))

    def test_a_plain_date_stays_on_its_own_day_in_a_western_session(self):
        # The bug this pins: read as midnight UTC and converted to New York, a date-only column
        # lands at 19:00 the evening BEFORE -- look-ahead, on the wrong session.
        from zoneinfo import ZoneInfo
        out = self.normalise(pl.DataFrame({"d": [datetime.date(2024, 1, 2)]}),
                             zone="America/New_York")
        self.assertEqual(out[0].date(), datetime.date(2024, 1, 2))
        self.assertEqual(out[0],
                         datetime.datetime(2024, 1, 2, tzinfo=ZoneInfo("America/New_York")))

    def test_a_naive_timestamp_is_read_as_utc(self):
        # Guessing a publisher's local timezone would move every row by hours.
        out = self.normalise(pl.DataFrame({"d": [datetime.datetime(2024, 1, 2, 15, 30)]}))
        self.assertEqual(out[0],
                         datetime.datetime(2024, 1, 2, 15, 30, tzinfo=datetime.timezone.utc))

    def test_a_zoned_timestamp_keeps_its_instant(self):
        frame = pl.DataFrame({"d": [datetime.datetime(2024, 1, 2, 15, 30)]}).with_columns(
            pl.col("d").dt.replace_time_zone("America/New_York"))
        self.assertEqual(self.normalise(frame)[0],
                         datetime.datetime(2024, 1, 2, 20, 30, tzinfo=datetime.timezone.utc))

    def test_a_column_that_is_not_a_time_is_refused(self):
        from ziplime.data.data_sources.huggingface.huggingface_data_source import (
            _to_session_time,
        )
        with self.assertRaises(ManifestError):
            _to_session_time("d", pl.Utf8, "UTC")


if __name__ == "__main__":
    unittest.main(verbosity=2)
