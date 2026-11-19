"""Metastrategies: algorithms that run other algorithms (``context.strategy``) and trade their units.

Two names on synthetic bars: JNJ drifts up, KO drifts down, both with noise so that a Sharpe ratio
exists. What is pinned here:

* a strategy run by a metastrategy has exactly the record it has when run alone;
* on every bar, the metastrategy sees its strategies as of that bar -- not one behind, not ahead;
* the metastrategy's book is units x NAV, and its returns follow the strategy it holds;
* the syntax from the design (``context.order(context.strategy1.portfolio, ...)``) works;
* a parameter grid becomes a cluster of strategies that can be ranked and rotated between;
* a grid key the strategy's config does not have is refused rather than silently ignored;
* a metastrategy can run metastrategies.
"""
import datetime
import json
import shutil
import tempfile
import unittest
from pathlib import Path

import numpy as np

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.meta import STRATEGY_VENUE
from ziplime.utils.calendar_utils import get_calendar

from tests.core.test_delisting import CALENDAR, END, PROJECT_ROOT, START, make_bundle

FIXTURES = PROJECT_ROOT / "tests" / "fixtures" / "meta"


def walk(asset, closes, start: float, drift: float, seed: int) -> list[dict]:
    rng = np.random.default_rng(seed)
    prices = start * np.cumprod(1.0 + drift + rng.normal(0.0, 0.01, len(closes)))
    return [{"date": close, "sid": asset.sid, "symbol": asset.symbol, "mic": asset.mic,
             "open": p, "high": p, "low": p, "close": p, "price": p, "volume": 1_000_000_000.0}
            for close, p in zip(closes, prices)]


class MetastrategyTests(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-meta-")
        self.temp = Path(self._temp.name)
        db_path = self.temp / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db_path)
        self.asset_service = get_asset_service(db_path=str(db_path))
        jnj, ko = [await self.asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=symbol, mic="XNYS"), asset_type=AssetType.EQUITY)
            for symbol in ("JNJ", "KO")]
        self.calendar = get_calendar(CALENDAR)
        self.start = datetime.datetime.combine(START, datetime.time.min, tzinfo=self.calendar.tz)
        self.end = datetime.datetime.combine(END, datetime.time.max, tzinfo=self.calendar.tz)
        sessions = self.calendar.sessions_in_range(START, END)
        self.sessions = len(sessions)
        closes = list(self.calendar.schedule.loc[sessions, "close"].dt.tz_convert(self.calendar.tz))
        self.rows = walk(jnj, closes, 100.0, 0.002, seed=1) + walk(ko, closes, 50.0, -0.002, seed=2)

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    async def simulate(self, algorithm_file: Path, config_file: Path | None = None,
                       same_bar_execution: bool = False, stop_on_error: bool = True):
        return await run_simulation(
            start_date=self.start, end_date=self.end, trading_calendar=CALENDAR,
            emission_rate=datetime.timedelta(days=1), total_cash=100_000.0,
            market_data_source=make_bundle(self.rows, self.calendar), custom_data_sources=[],
            algorithm_file=str(algorithm_file), config_file=str(config_file) if config_file else None,
            stop_on_error=stop_on_error, asset_service=self.asset_service,
            equity_commission=NoCommission(), max_leverage=1.0,
            same_bar_execution=same_bar_execution, price_used_in_order_execution="close",
            print_algo=False)

    async def test_a_strategy_runs_exactly_as_it_would_alone(self):
        config = self.temp / "hold.json"
        config.write_text(json.dumps({"symbol": "JNJ", "weight": 1.0}))
        alone = await self.simulate(FIXTURES / "hold.py", config_file=config)
        meta = await self.simulate(FIXTURES / "meta_lockstep.py")

        inside = meta.strategies["hold"]
        self.assertEqual(inside.errors, [])
        np.testing.assert_allclose(inside.perf["portfolio_value"].to_numpy(),
                                   alone.perf["portfolio_value"].to_numpy(), rtol=1e-12)
        self.assertAlmostEqual(inside.perf["sharpe"].iloc[-1], alone.perf["sharpe"].iloc[-1],
                               places=10)
        # And what the metastrategy read on the last bar is what the report says.
        self.assertAlmostEqual(meta.trading_algorithm.held.performance.sharpe,
                               alone.perf["sharpe"].iloc[-1], places=10)

    async def test_the_metastrategy_sees_its_strategies_as_of_the_same_bar(self):
        for same_bar in (False, True):
            with self.subTest(same_bar_execution=same_bar):
                result = await self.simulate(FIXTURES / "meta_lockstep.py", same_bar_execution=same_bar)
                context = result.trading_algorithm
                self.assertEqual(len(context.lag), self.sessions)
                self.assertEqual(set(context.lag), {0})

    async def test_the_book_is_units_times_nav_and_follows_the_strategy(self):
        result = await self.simulate(FIXTURES / "meta_lockstep.py")
        context = result.trading_algorithm
        held = context.held

        self.assertGreater(held.units, 0)
        position = context.portfolio.positions[held.asset]
        self.assertEqual(position.amount, held.units)
        self.assertEqual(position.asset.mic, STRATEGY_VENUE)
        self.assertAlmostEqual(context.portfolio.portfolio_value,
                               context.portfolio.cash + held.units * held.nav, places=6)
        # Fully invested from the second session on: the metastrategy earns what the strategy earns.
        meta_returns = result.perf["returns"].to_numpy()[3:]
        strategy_returns = result.strategies["hold"].perf["returns"].to_numpy()[3:]
        np.testing.assert_allclose(meta_returns, strategy_returns, atol=2e-4)
        self.assertGreater(held.weight, 0.98)

    async def test_the_syntax_of_the_design_runs(self):
        result = await self.simulate(FIXTURES / "meta_sharpe_switch.py")
        context = result.trading_algorithm
        self.assertEqual(len(context.seen), self.sessions)
        cash1, sharpe1, sharpe2 = context.seen[0]
        self.assertTrue(np.isnan(sharpe1) and np.isnan(sharpe2),
                        "one session of returns has no Sharpe ratio yet")
        self.assertEqual(context.strategy1.units, 50_000)
        self.assertEqual(context.strategy2.units, 0)
        self.assertGreater(context.strategy1.performance.sharpe, context.strategy2.performance.sharpe)
        self.assertEqual(set(result.strategies), {"s1", "s2"})

    async def test_a_grid_is_a_cluster_that_can_be_ranked_and_rotated(self):
        result = await self.simulate(FIXTURES / "meta_cluster.py")
        context = result.trading_algorithm
        cluster = context.cluster

        self.assertEqual(len(cluster), 4)
        self.assertEqual(set(result.strategies), {
            "hold[symbol=JNJ, weight=0.5]", "hold[symbol=JNJ, weight=1.0]",
            "hold[symbol=KO, weight=0.5]", "hold[symbol=KO, weight=1.0]"})
        leader = cluster.find(symbol="JNJ", weight=1.0)
        self.assertIs(cluster.best("returns"), leader)
        self.assertIs(cluster.ranked("returns")[-1], cluster.find(symbol="KO", weight=1.0))
        self.assertEqual(context.picks[-1], leader.label)
        # Rotated into the leader and out of everything else.
        held = [member.label for member in cluster if member.units]
        self.assertEqual(held, [leader.label])
        frame = cluster.to_frame()
        self.assertEqual(list(frame.index), [member.label for member in cluster])
        self.assertIn("sharpe", frame.columns)
        self.assertEqual(result.errors, [])

    async def test_strategies_and_listings_rebalance_together(self):
        result = await self.simulate(FIXTURES / "meta_mixed.py", stop_on_error=False)
        context = result.trading_algorithm
        self.assertEqual(result.errors, [], "a broken strategy does not break the metastrategy")
        broken = result.strategies["broken"]
        self.assertEqual(len(broken.errors), self.sessions - 2)
        self.assertIn("broken strategy", broken.errors[0].message)

        self.assertAlmostEqual(context.s1.weight, 0.5, delta=0.02)
        ko = context.portfolio.positions[context.ko]
        self.assertAlmostEqual(ko.amount * ko.last_sale_price / context.portfolio.portfolio_value,
                               0.4, delta=0.02)
        # 1% of every unit bought is charged: the first fill's cost basis is its NAV plus 1%.
        fills = [t for t in context._ledger.transaction_history if t.asset == context.s1.asset]
        first = fills[0]
        commissions = sum(o.commission for o in context.blotter.orders[STRATEGY_VENUE].values())
        self.assertGreater(commissions, 0.01 * first.amount * first.price * 0.99)

    async def test_a_metastrategy_can_run_metastrategies(self):
        result = await self.simulate(FIXTURES / "meta_nested.py")
        self.assertEqual(result.errors, [])
        inner = result.strategies["meta_lockstep"]
        self.assertEqual(list(inner.strategies), ["hold"])
        self.assertEqual(set(inner.trading_algorithm.lag), {0})
        self.assertEqual(len(inner.strategies["hold"].perf), self.sessions)

    async def test_a_grid_key_the_config_does_not_have_is_refused(self):
        meta = self.temp / "meta_typo.py"
        meta.write_text(
            "async def initialize(context):\n"
            f"    context.c = await context.strategy({str(FIXTURES / 'hold.py')!r},\n"
            "                                       grid={'wieght': [0.5, 1.0]})\n")
        with self.assertRaisesRegex(ValueError, "no field wieght"):
            await self.simulate(meta)


if __name__ == "__main__":
    unittest.main()
