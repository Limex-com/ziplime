"""Fundamentals read vectorised, and the one question that decides whether it is worth anything.

A statement arrives four times a year. Prices arrive every bar. So a dataset of filings cannot be
pivoted onto the bar grid the way a price series can -- what a strategy wants from it at a bar is
the *as-of* view: the newest thing known by then, carried forward. That is exactly what
``data.current`` returns at one bar, and :func:`~ziplime.vectorized.signals.as_of_panel` computes
it for the whole history in one pass.

Which makes the test that matters obvious: **the panel has to agree with `data.current`, bar for
bar**. If it does not, the vectorised path is trading on a different dataset than the event-driven
one, and every number it produces is about something else. :class:`AsOfAgreementTests` walks every
bar of a run and compares.

The rest is about the two ways this goes wrong quietly:

* A filing accepted during a bar must not be visible inside it. The source reads strictly before
  the current moment, and an inclusive join here would hand the strategy a statement hours before
  it could have acted on it -- the kind of bias that improves a backtest rather than breaking it.
* A source that republishes revisions resolves by column, not by row. A later filing restating a
  period repeats fewer line items than the original, so reading the newest row drops most of the
  balance sheet. On the real SEC dataset that is 78% of ``total_assets``.

Nothing here touches the network. The statements are built in the test, with the revision
structure the real dataset has.
"""
import datetime
import shutil
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.constants.data_type import DataType
from ziplime.core.ingest_data import get_asset_service
from ziplime.data.data_sources.huggingface.huggingface_data_source import (
    HuggingFaceDataSource, Resolution,
)
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.utils.calendar_utils import get_calendar
from ziplime.vectorized.signals import as_of_panel

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures"

CALENDAR = "XNYS"
START, END = datetime.date(2024, 2, 1), datetime.date(2024, 9, 30)
BUNDLE_START = datetime.date(2023, 11, 1)
TICKERS = ("JNJ", "KO")
CASH = 100_000.0

#: Statements shaped the way the real dataset is: a period filed once, then repeated in a later
#: filing that carries fewer line items. `total_assets` is the field the second filing omits, which
#: is the case that separates resolving by column from resolving by row.
STATEMENTS = [
    # ticker, knowledge_date, revenue, gross_profit, total_assets
    ("JNJ", datetime.date(2023, 12, 15), 1000.0, 400.0, 5000.0),
    ("JNJ", datetime.date(2024, 3, 20), 1100.0, 450.0, 5200.0),
    ("JNJ", datetime.date(2024, 6, 18), 1150.0, 470.0, None),   # revision: no balance sheet
    # The August filing is what makes JNJ the better business on this measure: 700/5600 = 0.125
    # against KO's 0.116. Without a crossover the strategy would buy once and hold, and the test
    # would pass without ever exercising a switch.
    ("JNJ", datetime.date(2024, 8, 14), 1200.0, 700.0, 5600.0),
    ("KO", datetime.date(2023, 12, 20), 800.0, 320.0, 3000.0),
    ("KO", datetime.date(2024, 4, 11), 850.0, 360.0, 3100.0),
    ("KO", datetime.date(2024, 7, 9), 900.0, 360.0, None),      # revision: no balance sheet
]

FIELDS = ["revenue", "gross_profit", "total_assets"]


class FundamentalsTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-fund-")
        db = Path(self._temp.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db)
        self.asset_service = get_asset_service(db_path=str(db))
        self.calendar = get_calendar(CALENDAR)
        sessions = self.calendar.sessions_in_range(BUNDLE_START, END)
        self.index = pd.DatetimeIndex(
            self.calendar.schedule.loc[sessions, "close"].dt.tz_convert(self.calendar.tz))
        steps = np.arange(len(self.index))
        self.prices = pd.DataFrame(
            {"JNJ": 150.0 + np.sin(steps / 13.0) * 5.0,
             "KO": 60.0 + np.cos(steps / 9.0) * 3.0}, index=self.index)
        self.listings = {}
        for ticker in TICKERS:
            self.listings[ticker] = await self.asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=ticker, mic="XNYS"), asset_type=AssetType.EQUITY)

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    def statements_frame(self) -> pl.DataFrame:
        return pl.DataFrame({
            "ticker": [row[0] for row in STATEMENTS],
            "knowledge_date": [
                datetime.datetime.combine(row[1], datetime.time(8, 0), tzinfo=self.calendar.tz)
                for row in STATEMENTS],
            "revenue": [row[2] for row in STATEMENTS],
            "gross_profit": [row[3] for row in STATEMENTS],
            "total_assets": [row[4] for row in STATEMENTS],
        })

    async def source(self, resolution: Resolution = Resolution.COALESCE) -> HuggingFaceDataSource:
        return HuggingFaceDataSource.from_frame(
            frame=self.statements_frame(), name="fundamentals",
            knowledge_column="knowledge_date", entity_column="ticker", event_column=None,
            asset_service=self.asset_service, start_date=BUNDLE_START, end_date=END,
            session_timezone=str(self.calendar.tz), fields=FIELDS, resolution=resolution)

    def bundle(self) -> DataBundle:
        rows = [{"date": stamp, "sid": self.listings[t].sid, "symbol": t, "mic": "XNYS",
                 "open": p, "high": p, "low": p, "close": p, "price": p, "volume": 1e9}
                for t in TICKERS for stamp, p in self.prices[t].items()]
        data = pl.DataFrame(rows).sort(["sid", "date"])
        spans = data.with_row_index().group_by("sid", maintain_order=True).agg(
            [pl.col("index").first().alias("first"), pl.col("index").last().alias("last")])
        return DataBundle(
            name="fund-test", version="1", start_date=data["date"].min(),
            end_date=data["date"].max(), trading_calendar=self.calendar,
            frequency=datetime.timedelta(days=1), original_frequency=datetime.timedelta(days=1),
            data_type=DataType.MARKET_DATA, timestamp=data["date"].max(), data=data,
            sid_indexes={r["sid"]: (r["first"], r["last"] + 1)
                         for r in spans.iter_rows(named=True)})

    async def run_strategy(self, fixture: str):
        from ziplime.core.run_simulation import run_simulation

        return await run_simulation(
            start_date=datetime.datetime.combine(START, datetime.time.min,
                                                 tzinfo=self.calendar.tz),
            end_date=datetime.datetime.combine(END, datetime.time.max, tzinfo=self.calendar.tz),
            trading_calendar=CALENDAR, emission_rate=datetime.timedelta(days=1),
            total_cash=CASH, market_data_source=self.bundle(),
            custom_data_sources=[await self.source()],
            algorithm_file=str(FIXTURES / fixture), stop_on_error=True,
            asset_service=self.asset_service, benchmark_asset_symbol=None,
            benchmark_returns=None, equity_commission=NoCommission(),
            equity_slippage=NoSlippage(), max_leverage=10.0, print_algo=False)


class AsOfAgreementTests(FundamentalsTestCase):
    """The panel and `data.current` have to be the same dataset.

    Walked bar by bar rather than spot-checked: an off-by-one at the join would agree everywhere
    except on the few bars a filing lands, which is exactly where it matters.
    """

    async def panel_and_readings(self, resolution: Resolution):
        source = await self.source(resolution)
        await source.materialize()
        names = {self.listings[t].sid: t for t in TICKERS}
        rows = await source.get_data_by_window(
            fields=None, since=datetime.timedelta(days=3650),
            end_date=self.index[-1].to_pydatetime(),
            frequency=datetime.timedelta(days=1),
            assets=frozenset(self.listings.values()), include_end_date=True)
        panel = as_of_panel(rows, names=names, index=self.index,
                            coalesce=resolution is Resolution.COALESCE)

        readings = []
        for stamp in self.index:
            current = await source.get_spot_value(
                assets=frozenset(self.listings.values()), fields=frozenset(FIELDS),
                dt=stamp.to_pydatetime())
            by_sid = {r["sid"]: r for r in current.iter_rows(named=True)} if not current.is_empty() else {}
            readings.append({names[sid]: by_sid.get(sid, {}) for sid in names})
        return panel, readings

    async def test_it_matches_data_current_on_every_bar_when_coalescing(self):
        panel, readings = await self.panel_and_readings(Resolution.COALESCE)
        mismatches = []
        for position, stamp in enumerate(self.index):
            for ticker in TICKERS:
                for field in FIELDS:
                    from_panel = getattr(panel, field)[ticker].iloc[position]
                    from_current = readings[position][ticker].get(field)
                    if _differs(from_panel, from_current):
                        mismatches.append((str(stamp.date()), ticker, field,
                                           from_panel, from_current))
        self.assertEqual(mismatches[:5], [], f"{len(mismatches)} bars disagree")

    async def test_it_matches_data_current_on_every_bar_when_taking_the_latest_row(self):
        panel, readings = await self.panel_and_readings(Resolution.LATEST_ROW)
        mismatches = []
        for position in range(len(self.index)):
            for ticker in TICKERS:
                for field in FIELDS:
                    from_panel = getattr(panel, field)[ticker].iloc[position]
                    from_current = readings[position][ticker].get(field)
                    if _differs(from_panel, from_current):
                        mismatches.append((position, ticker, field, from_panel, from_current))
        self.assertEqual(mismatches[:5], [], f"{len(mismatches)} bars disagree")

    async def test_the_two_resolutions_actually_differ(self):
        """Otherwise the two tests above would pass against a panel that ignores resolution."""
        coalesced, _ = await self.panel_and_readings(Resolution.COALESCE)
        latest, _ = await self.panel_and_readings(Resolution.LATEST_ROW)
        after_revision = self.index[self.index > pd.Timestamp(
            datetime.datetime.combine(datetime.date(2024, 6, 19), datetime.time(8, 0),
                                      tzinfo=self.calendar.tz))][0]
        position = self.index.get_loc(after_revision)
        self.assertTrue(np.isfinite(float(coalesced.total_assets["JNJ"].iloc[position])),
                        "coalescing should carry the balance sheet the revision omitted")
        self.assertTrue(pd.isna(latest.total_assets["JNJ"].iloc[position]),
                        "the newest row omitted total_assets, so reading by row loses it")


class StalenessTests(FundamentalsTestCase):
    """`known_at` and `age_days`: when the row a bar is reading was filed, and how long ago.

    Staleness is the first question anyone asks of point-in-time data -- a company that stopped
    reporting in 2015 should not sit in a 2024 book -- and computing it from the panel by hand is
    fiddly enough to get slightly wrong, so the engine derives it.
    """

    async def panel(self, extra: pl.DataFrame | None = None):
        frame = self.statements_frame() if extra is None else pl.concat(
            [self.statements_frame(), extra], how="diagonal")
        source = HuggingFaceDataSource.from_frame(
            frame=frame, name="fundamentals", knowledge_column="knowledge_date",
            entity_column="ticker", event_column=None, asset_service=self.asset_service,
            start_date=BUNDLE_START, end_date=END, session_timezone=str(self.calendar.tz),
            fields=FIELDS, resolution=Resolution.COALESCE)
        await source.materialize()
        rows = await source.get_data_by_window(
            fields=None, since=datetime.timedelta(days=3650),
            end_date=self.index[-1].to_pydatetime(), frequency=datetime.timedelta(days=1),
            assets=frozenset(self.listings.values()), include_end_date=True)
        return as_of_panel(rows, names={self.listings[t].sid: t for t in TICKERS},
                           index=self.index, coalesce=True)

    async def test_age_counts_from_the_filing_the_bar_is_reading(self):
        panel = await self.panel()
        filed = datetime.datetime.combine(datetime.date(2024, 3, 20), datetime.time(8, 0),
                                          tzinfo=self.calendar.tz)
        after = [ix for ix, stamp in enumerate(self.index) if stamp.date() > datetime.date(2024, 3, 20)]
        position = after[10]
        expected = (self.index[position].to_pydatetime() - filed).total_seconds() / 86_400.0
        self.assertAlmostEqual(float(panel.age_days["JNJ"].iloc[position]), expected, places=6)

    async def test_age_grows_between_filings(self):
        panel = await self.panel()
        ages = panel.age_days["JNJ"].dropna()
        self.assertGreater(len(ages), 50)
        self.assertTrue((ages >= 0).all(), "a filing cannot be read before it is filed")

    async def test_a_company_that_never_filed_is_all_missing_rather_than_an_error(self):
        """The case that bit the first version: an all-null column comes back tz-naive, and
        subtracting it from a tz-aware index raised somewhere unrelated."""
        # A name with no filings at all, alongside one that has them.
        empty_names = {self.listings["JNJ"].sid: "JNJ", 999_999: "NOBODY"}
        source_rows = pl.DataFrame({
            "date": [datetime.datetime.combine(datetime.date(2024, 3, 20), datetime.time(8, 0),
                                               tzinfo=self.calendar.tz)],
            "sid": [self.listings["JNJ"].sid], "revenue": [1.0],
            "gross_profit": [2.0], "total_assets": [3.0]})
        built = as_of_panel(source_rows, names=empty_names, index=self.index, coalesce=True)
        self.assertTrue(built.age_days["NOBODY"].isna().all())
        self.assertTrue(built.revenue["NOBODY"].isna().all())
        self.assertFalse(built.age_days["JNJ"].dropna().empty)


class LookAheadTests(FundamentalsTestCase):
    """A filing is invisible until it is filed, and on the bar it is filed."""

    async def test_a_statement_is_not_visible_before_its_knowledge_date(self):
        source = await self.source()
        await source.materialize()
        names = {self.listings[t].sid: t for t in TICKERS}
        rows = await source.get_data_by_window(
            fields=None, since=datetime.timedelta(days=3650),
            end_date=self.index[-1].to_pydatetime(), frequency=datetime.timedelta(days=1),
            assets=frozenset(self.listings.values()), include_end_date=True)
        panel = as_of_panel(rows, names=names, index=self.index, coalesce=True)

        filed = datetime.date(2024, 3, 20)
        before = [ix for ix, stamp in enumerate(self.index) if stamp.date() < filed][-1]
        after = [ix for ix, stamp in enumerate(self.index) if stamp.date() > filed][0]
        self.assertEqual(float(panel.revenue["JNJ"].iloc[before]), 1000.0,
                         "the March filing was visible before it was filed")
        self.assertEqual(float(panel.revenue["JNJ"].iloc[after]), 1100.0,
                         "the March filing was still invisible after it was filed")

    async def test_a_filing_stamped_exactly_on_a_bar_is_not_visible_in_it(self):
        """The strict half of the as-of join, which every other test here happens to miss.

        The statements above are stamped at 08:00 and the bars close at 16:00, so `<` and `<=`
        agree on all of them and an inclusive join passes unnoticed. A filing accepted at the very
        instant of a bar is the only case that separates the two -- and it is the case that
        matters, because that is the bar a strategy would first act on.
        """
        stamp = self.index[40]
        frame = pl.DataFrame({
            "ticker": ["JNJ", "JNJ"],
            "knowledge_date": [self.index[10].to_pydatetime(), stamp.to_pydatetime()],
            "revenue": [10.0, 999.0],
            "gross_profit": [1.0, 2.0],
            "total_assets": [100.0, 200.0],
        })
        source = HuggingFaceDataSource.from_frame(
            frame=frame, name="exact", knowledge_column="knowledge_date",
            entity_column="ticker", event_column=None, asset_service=self.asset_service,
            start_date=BUNDLE_START, end_date=END,
            session_timezone=str(self.calendar.tz), fields=FIELDS,
            resolution=Resolution.COALESCE)
        await source.materialize()
        rows = await source.get_data_by_window(
            fields=None, since=datetime.timedelta(days=3650),
            end_date=self.index[-1].to_pydatetime(), frequency=datetime.timedelta(days=1),
            assets=frozenset([self.listings["JNJ"]]), include_end_date=True)
        panel = as_of_panel(rows, names={self.listings["JNJ"].sid: "JNJ"}, index=self.index,
                            coalesce=True)

        self.assertEqual(float(panel.revenue["JNJ"].iloc[40]), 10.0,
                         "a filing stamped at this bar was already visible inside it")
        self.assertEqual(float(panel.revenue["JNJ"].iloc[41]), 999.0,
                         "and it should be visible on the next one")

        # The engine's own read agrees, which is what makes this the right rule rather than a
        # preference: `data.current` at that bar cannot see it either.
        current = await source.get_spot_value(
            assets=frozenset([self.listings["JNJ"]]), fields=frozenset(["revenue"]),
            dt=stamp.to_pydatetime())
        self.assertEqual(float(current["revenue"][0]), 10.0)

    async def test_a_signal_built_from_the_future_is_refused(self):
        """The causality check covers dataset panels too -- they are part of what a signal reads."""
        from ziplime.vectorized.signals import LookAheadError

        with self.assertRaises(LookAheadError):
            await self.run_strategy("fundamentals_look_ahead.py")


class EndToEndTests(FundamentalsTestCase):
    """A strategy that ranks on a fundamental, written vectorised and bar by bar."""

    async def test_both_versions_place_the_same_orders(self):
        vectorised = await self.run_strategy("fundamentals_vectorised.py")
        bar_by_bar = await self.run_strategy("fundamentals_bar_by_bar.py")

        def signatures(result):
            return [(t.asset.symbol, t.amount, round(float(t.price), 8))
                    for row in result.perf["transactions"] for t in row]

        placed = signatures(vectorised)
        self.assertGreater(len(placed), 1, "a strategy that never traded proves nothing")
        self.assertEqual(placed, signatures(bar_by_bar))

    async def test_both_versions_agree_bar_for_bar(self):
        vectorised = await self.run_strategy("fundamentals_vectorised.py")
        bar_by_bar = await self.run_strategy("fundamentals_bar_by_bar.py")
        for column in ("portfolio_value", "ending_cash", "returns"):
            np.testing.assert_allclose(
                vectorised.perf[column].to_numpy(dtype=float),
                bar_by_bar.perf[column].to_numpy(dtype=float), rtol=0, atol=1e-9,
                err_msg=f"{column} differs between the two versions")

    async def test_an_undeclared_dataset_says_how_to_declare_it(self):
        with self.assertRaises(KeyError) as raised:
            await self.run_strategy("fundamentals_undeclared.py")
        self.assertIn("context.datasets", str(raised.exception))


def _differs(from_panel, from_current) -> bool:
    """NaN in the panel is how it spells "nothing known yet", which `data.current` spells None."""
    panel_missing = from_panel is None or (isinstance(from_panel, float) and np.isnan(from_panel))
    if from_current is None:
        return not panel_missing
    if panel_missing:
        return True
    return abs(float(from_panel) - float(from_current)) > 1e-9


if __name__ == "__main__":
    unittest.main(verbosity=2)
