"""dataset://<name>: a user's private Parquet file, read only with that user's own run context."""
import datetime
import unittest
from zoneinfo import ZoneInfo

import polars as pl

from ziplime.data.data_sources import address_schemes
from ziplime.data.data_sources.user_datasets import (
    CACHE, UserDatasetError, mount_user_dataset, parse_address,
)
from ziplime.utils.calendar_utils import get_calendar
from tests.data.test_huggingface_datasets import make_equity

USER_A = "01a0343e-217a-7547-9f13-5b4ab9112a63"
USER_B = "9b2c1d4e-0000-4000-8000-000000000001"
BUCKET = "secret-bucket-name"
UTC = datetime.timezone.utc


class FakeStore:
    """users/<user>/<name>.parquet -> (etag, frame); records every key it was asked for."""

    def __init__(self):
        self.objects: dict[str, tuple[str, pl.DataFrame]] = {}
        self.heads: list[str] = []

    def put(self, user, name, frame, etag="v1"):
        self.objects[f"users/{user}/{name}.parquet"] = (etag, frame)

    def head(self, bucket, key):
        assert bucket == BUCKET
        self.heads.append(key)
        found = self.objects.get(key)
        return None if found is None else (found[0], found[1].estimated_size())

    def scan(self, bucket, key):
        return self.objects[key][1].lazy()


class StubAssets:
    def __init__(self, *symbols):
        self.listings = {s: make_equity(sid=i + 1, symbol=s) for i, s in enumerate(symbols)}

    async def get_exchange_assets_by_symbols(self, symbols, asset_type=None):
        return [self.listings.get(s.symbol) for s in symbols]


def fundamentals(value: float, knowledge=datetime.datetime(2024, 3, 20), event=None):
    frame = {"knowledge_date": [knowledge], "ticker": ["AAPL"], "revenue": [value]}
    if event is not None:
        frame["event_date"] = [event]
    return pl.DataFrame(frame)


class UserDatasetTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        CACHE.clear()
        self.store = FakeStore()
        self.assets = StubAssets("AAPL")
        self.aapl = self.assets.listings["AAPL"]

    def mount(self, address, user, **extra):
        return mount_user_dataset(
            address, asset_service=self.assets, start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 12, 31), trading_calendar=get_calendar("XNYS"),
            user_id=user, user_datasets_bucket=BUCKET, user_dataset_store=self.store, **extra)

    async def read(self, source, when, fields=("revenue",)):
        return await source.get_data_by_limit(
            fields=frozenset(fields), limit=10, end_date=when.replace(tzinfo=UTC),
            frequency=datetime.timedelta(days=1), assets=frozenset({self.aapl}),
            include_end_date=False)

    # -- isolation ----------------------------------------------------------------------------

    async def test_the_owner_reads_rows_known_before_the_bar(self):
        self.store.put(USER_A, "company-fundamentals", fundamentals(10.0))
        source = self.mount("dataset://company-fundamentals", USER_A)
        self.assertTrue((await self.read(source, datetime.datetime(2024, 3, 19))).is_empty())
        rows = await self.read(source, datetime.datetime(2024, 3, 22))
        self.assertEqual(rows["revenue"].to_list(), [10.0])

    def test_another_user_is_told_it_does_not_exist(self):
        self.store.put(USER_A, "company-fundamentals", fundamentals(10.0))
        with self.assertRaises(UserDatasetError) as caught:
            self.mount("dataset://company-fundamentals", USER_B)
        self.assertEqual(str(caught.exception),
                         "403 - dataset does not exist: 'company-fundamentals'")
        self.assertEqual(self.store.heads, [f"users/{USER_B}/company-fundamentals.parquet"])

    async def test_a_cached_entry_is_never_served_to_another_user(self):
        self.store.put(USER_A, "x", fundamentals(1.0))
        self.store.put(USER_B, "x", fundamentals(2.0))
        for first, second, expected in ((USER_A, USER_B, 2.0), (USER_B, USER_A, 1.0)):
            CACHE.clear()
            await self.read(self.mount("dataset://x", first), datetime.datetime(2024, 4, 1))
            rows = await self.read(self.mount("dataset://x", second), datetime.datetime(2024, 4, 1))
            self.assertEqual(rows["revenue"].to_list(), [expected])
        self.assertTrue(all(key[0] in (USER_A, USER_B) for key in CACHE.keys()))

    async def test_a_reupload_is_read_again(self):
        self.store.put(USER_A, "x", fundamentals(1.0), etag="v1")
        await self.read(self.mount("dataset://x", USER_A), datetime.datetime(2024, 4, 1))
        self.store.put(USER_A, "x", fundamentals(5.0), etag="v2")
        rows = await self.read(self.mount("dataset://x", USER_A), datetime.datetime(2024, 4, 1))
        self.assertEqual(rows["revenue"].to_list(), [5.0])
        self.assertEqual({k[2] for k in CACHE.keys()}, {"v2"})

    async def test_the_second_run_uses_the_cache(self):
        self.store.put(USER_A, "x", fundamentals(1.0))
        await self.read(self.mount("dataset://x", USER_A), datetime.datetime(2024, 4, 1))
        source = self.mount("dataset://x", USER_A)
        self.store.objects[f"users/{USER_A}/x.parquet"] = ("v1", pl.DataFrame())  # unreadable now
        rows = await self.read(source, datetime.datetime(2024, 4, 1))
        self.assertEqual(rows["revenue"].to_list(), [1.0])
        self.assertTrue(source.mount_report.get("cached"))

    # -- addresses and context ----------------------------------------------------------------

    def test_addresses_that_reach_outside_the_users_prefix_are_refused(self):
        for address in ("dataset://../x", f"dataset://{USER_B}/x", "dataset://X", "dataset://",
                        "dataset://a/b", "dataset://x.parquet", "dataset:// x"):
            with self.subTest(address=address):
                with self.assertRaises(UserDatasetError) as caught:
                    self.mount(address, USER_A)
                self.assertEqual(str(caught.exception), f"invalid dataset address '{address}'")
        self.assertEqual(self.store.heads, [])

    def test_a_local_run_is_told_it_needs_a_hosted_backtest(self):
        for user, bucket in ((None, BUCKET), (USER_A, None), ("not-a-uuid", BUCKET)):
            with self.subTest(user=user, bucket=bucket):
                with self.assertRaises(UserDatasetError) as caught:
                    mount_user_dataset("dataset://x", trading_calendar=get_calendar("XNYS"),
                                       user_id=user, user_datasets_bucket=bucket,
                                       user_dataset_store=self.store)
                self.assertEqual(str(caught.exception),
                                 "dataset:// is only available in hosted backtests")

    def test_the_name_is_the_only_thing_an_address_carries(self):
        self.assertEqual(parse_address("dataset://my-signals_2"), "my-signals_2")

    def test_the_scheme_is_served_by_ziplime_and_the_context_belongs_to_the_run(self):
        self.assertIs(address_schemes.mount_for("dataset://x"), mount_user_dataset)
        self.assertEqual(address_schemes.current_address_context(), {})
        with address_schemes.address_context({"user_id": USER_A}):
            self.assertEqual(address_schemes.current_address_context(), {"user_id": USER_A})
            with address_schemes.address_context({"user_id": USER_B}):
                self.assertEqual(address_schemes.current_address_context()["user_id"], USER_B)
            self.assertEqual(address_schemes.current_address_context()["user_id"], USER_A)
        self.assertEqual(address_schemes.current_address_context(), {})

    # -- point in time, size, errors ----------------------------------------------------------

    async def test_the_event_date_floors_the_knowledge_date(self):
        self.store.put(USER_A, "x", fundamentals(
            3.0, knowledge=datetime.datetime(2024, 1, 2), event=datetime.datetime(2024, 1, 5)))
        source = self.mount("dataset://x", USER_A)
        self.assertTrue((await self.read(source, datetime.datetime(2024, 1, 4))).is_empty())
        self.assertTrue((await self.read(source, datetime.datetime(2024, 1, 5))).is_empty())
        self.assertEqual((await self.read(source, datetime.datetime(2024, 1, 6)))["revenue"].to_list(),
                         [3.0])

    def test_a_file_over_the_limit_is_refused_before_it_is_read(self):
        self.store.put(USER_A, "big", fundamentals(1.0))
        with self.assertRaises(UserDatasetError) as caught:
            self.mount("dataset://big", USER_A, user_dataset_max_bytes=10)
        self.assertRegex(str(caught.exception), r"^dataset 'big' is .+, over the 10 B limit$")

    def test_a_file_without_an_instrument_column_cannot_be_read(self):
        self.store.put(USER_A, "x", pl.DataFrame({"knowledge_date": [datetime.datetime(2024, 1, 2)],
                                                  "value": [1.0]}))
        with self.assertRaises(UserDatasetError) as caught:
            self.mount("dataset://x", USER_A)
        self.assertTrue(str(caught.exception).startswith("dataset 'x' cannot be read: "))

    def test_fields_are_projected_and_unknown_ones_named(self):
        self.store.put(USER_A, "x", fundamentals(1.0))
        source = self.mount("dataset://x", USER_A, fields=["revenue"])
        self.assertEqual(source.requested_fields, frozenset({"revenue"}))
        with self.assertRaises(UserDatasetError) as caught:
            self.mount("dataset://x", USER_A, fields=["nope"])
        self.assertIn("no column 'nope'", str(caught.exception))

    async def test_columns_are_read_on_demand_and_widened_when_asked(self):
        frame = fundamentals(1.0).with_columns(pl.lit(7.0).alias("net_income"),
                                                pl.lit("x").alias("wide_text"))
        self.store.put(USER_A, "x", frame)
        source = self.mount("dataset://x", USER_A)
        await self.read(source, datetime.datetime(2024, 4, 1))
        self.assertNotIn("wide_text", source.data.columns)
        self.assertNotIn("net_income", source.data.columns)
        current = await source.get_spot_value(assets=frozenset({self.aapl}),
                                              fields=frozenset({"net_income"}),
                                              dt=datetime.datetime(2024, 4, 1, tzinfo=ZoneInfo("America/New_York")))
        self.assertEqual(current["net_income"].to_list(), [7.0])
        self.assertIn("revenue", source.data.columns)
        self.assertNotIn("wide_text", source.data.columns)

    def test_no_message_names_the_bucket_or_the_user(self):
        self.store.put(USER_A, "x", pl.DataFrame({"value": [1.0]}))
        messages = []
        for address, user in (("dataset://x", USER_A), ("dataset://y", USER_A),
                              ("dataset://../x", USER_A), ("dataset://x", None)):
            try:
                self.mount(address, user)
            except UserDatasetError as error:
                messages.append(str(error))
        self.assertEqual(len(messages), 4)
        for message in messages:
            self.assertNotIn(BUCKET, message)
            self.assertNotIn(USER_A, message)
            self.assertNotIn("\n", message)
            self.assertNotIn("Traceback", message)


if __name__ == "__main__":
    unittest.main()
