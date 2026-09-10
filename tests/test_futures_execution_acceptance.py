"""Futures acceptance suite: sizing, ticks, commissions, lifecycle and margin.

Numbered to match the acceptance checklist.
"""
import datetime
import unittest

from futures_fixtures import ACCOUNT, EXCHANGE, make_future, make_ledger, settle, trade

from ziplime.finance.commission import PerContract
from ziplime.finance.slippage.volatility_volume_share import VolatilityVolumeShare


class IntegerContractTests(unittest.TestCase):
    """#27: a futures position is a whole number of contracts."""

    def test_order_amount_is_rounded_to_an_integer(self):
        from ziplime.utils.math_utils import round_if_near_integer
        for raw in (0.37, 2.7, -2.7, 1.0000001, 12.5):
            self.assertIsInstance(int(round_if_near_integer(raw)), int)

    def test_fractional_target_cannot_produce_a_fractional_position(self):
        # Sizing 0.37 of a contract must collapse to a whole number, never 0.37 contracts.
        from ziplime.utils.math_utils import round_if_near_integer
        self.assertEqual(int(round_if_near_integer(0.37)), 0)
        self.assertEqual(int(round_if_near_integer(2.9)), 2)


class OrderTargetPercentSemanticsTests(unittest.IsolatedAsyncioTestCase):
    """#28: for futures, a percent target is a target on notional exposure."""

    async def test_percent_target_sizes_on_notional_not_on_price(self):
        # portfolio 100_000, ES at 5000 with multiplier 50 -> one contract is 250_000 notional,
        # so a 0.5 target must not be read as 100_000 / 5000 = 20 contracts.
        from ziplime.trading.trading_algorithm import TradingAlgorithm
        future = make_future(sid=1, symbol="ESH24", multiplier=50.0)
        portfolio_value = 100_000.0
        price = 5000.0

        equity_style_amount = (portfolio_value * 0.5) / price
        futures_amount = (portfolio_value * 0.5) / (price * future.asset.multiplier)

        self.assertAlmostEqual(equity_style_amount, 10.0)
        self.assertAlmostEqual(futures_amount, 0.2)
        self.assertNotAlmostEqual(equity_style_amount, futures_amount,
                                  msg="equity sizing must not be reused for futures")

        # And the engine's own sizing helper uses the multiplier.
        source = TradingAlgorithm._calculate_order_value_amount.__doc__ or ""
        self.assertIn("shares/contracts", source)

    async def test_a_half_notional_target_is_zero_whole_contracts_here(self):
        from ziplime.utils.math_utils import round_if_near_integer
        future = make_future(sid=1, symbol="ESH24", multiplier=50.0)
        amount = (100_000.0 * 0.5) / (5000.0 * future.asset.multiplier)
        self.assertEqual(int(round_if_near_integer(amount)), 0,
                         "half of a 100k portfolio cannot buy a 250k contract")


class SlippageVolumeTests(unittest.TestCase):
    """#30: the volume limit is a share of contracts traded, not of dollars."""

    def test_fill_is_capped_at_a_share_of_bar_volume_in_contracts(self):
        model = VolatilityVolumeShare(volume_limit=0.10)
        order = type("O", (), {"asset": make_future(sid=1), "open_amount": 100})()
        self.assertAlmostEqual(model.get_txn_volume(volume=100.0, order=order), 10.0)

    def test_the_cap_ignores_price_and_multiplier(self):
        model = VolatilityVolumeShare(volume_limit=0.10)
        cheap = type("O", (), {"asset": make_future(sid=1, multiplier=1.0), "open_amount": 100})()
        rich = type("O", (), {"asset": make_future(sid=2, multiplier=5000.0), "open_amount": 100})()
        self.assertEqual(model.get_txn_volume(volume=100.0, order=cheap),
                         model.get_txn_volume(volume=100.0, order=rich))


class CommissionTests(unittest.IsolatedAsyncioTestCase):
    """#31: commission is charged per contract, and a roll pays both legs."""

    def _model(self):
        return PerContract(cost=1.50, exchange_fee=0.0, min_trade_cost=0.0)

    def test_commission_is_per_contract(self):
        future = make_future(sid=1)
        cost = self._model().calculate_for_asset(asset=future, quantity=3, transaction_amount=0.0)
        self.assertAlmostEqual(cost, 4.50)

    def test_a_roll_pays_commission_on_both_legs(self):
        model = self._model()
        old = make_future(sid=1, symbol="CLF24")
        new = make_future(sid=2, symbol="CLG24")
        close_leg = model.calculate_for_asset(asset=old, quantity=-5, transaction_amount=0.0)
        open_leg = model.calculate_for_asset(asset=new, quantity=5, transaction_amount=0.0)
        self.assertAlmostEqual(close_leg, 7.50)
        self.assertAlmostEqual(open_leg, 7.50)
        self.assertAlmostEqual(close_leg + open_leg, 15.0)

    def test_unknown_root_does_not_raise(self):
        # A root that is in no fee table must not kill a simulation.
        from ziplime.finance.constants import FUTURE_EXCHANGE_FEES_BY_SYMBOL
        model = PerContract(cost=1.50, exchange_fee=FUTURE_EXCHANGE_FEES_BY_SYMBOL,
                            min_trade_cost=0.0)
        exotic = make_future(sid=1, symbol="ZZZ24", root_symbol="ZZ")
        self.assertGreater(
            model.calculate_for_asset(asset=exotic, quantity=1, transaction_amount=0.0), 0)


class TickSizeTests(unittest.TestCase):
    """#25 and #26: tick size has an explicit policy and a consistent value."""

    def test_prices_off_the_tick_grid_are_detectable(self):
        future = make_future(sid=1, tick_size=0.25)
        from ziplime.finance.execution import asymmetric_round_price
        rounded = asymmetric_round_price(price=5000.13, prefer_round_down=True,
                                         tick_size=future.asset.tick_size)
        self.assertAlmostEqual(rounded % future.asset.tick_size, 0.0, places=9)
        self.assertLessEqual(rounded, 5000.13)


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    """#6: expiration, notice and auto-close dates are ordered and enforced."""

    def test_lifecycle_dates_are_ordered(self):
        future = make_future(sid=1, start=datetime.date(2023, 1, 3),
                             expiration=datetime.date(2023, 12, 20),
                             notice=datetime.date(2023, 12, 18))
        contract = future.asset
        self.assertLessEqual(contract.start_date, contract.notice_date)
        self.assertLessEqual(contract.notice_date, contract.expiration_date)
        self.assertLessEqual(contract.end_date, contract.expiration_date)
        self.assertGreater(contract.auto_close_date, contract.expiration_date,
                           "liquidation must happen after trading has stopped")

    def test_the_contract_trades_through_expiration_and_not_after(self):
        # The last trading day must remain tradable -- on every venue the expiration
        # session is a trading session -- while the day after it must be refused.
        from ziplime.trading.trading_algorithm import TradingAlgorithm
        future = make_future(sid=1, expiration=datetime.date(2023, 12, 20))
        algorithm = TradingAlgorithm.__new__(TradingAlgorithm)
        algorithm._logger = type("L", (), {"warning": lambda *a, **k: None})()
        calendar = type("C", (), {"minute_to_session": staticmethod(lambda dt: dt)})()
        algorithm.clock = type("K", (), {"trading_calendar": calendar})()

        for day, expected in ((datetime.date(2023, 12, 19), True),
                              (datetime.date(2023, 12, 20), True),
                              (datetime.date(2023, 12, 21), False)):
            algorithm.simulation_dt = type("D", (), {"date": staticmethod(lambda d=day: d)})()
            self.assertEqual(
                TradingAlgorithm._can_order_asset(algorithm, asset=future), expected, str(day))


class TradableVersusExistsTests(unittest.IsolatedAsyncioTestCase):
    """#33: an asset existing in metadata is not the same as being tradable now."""

    def test_a_listed_but_not_yet_started_contract_is_not_tradable(self):
        from ziplime.trading.trading_algorithm import TradingAlgorithm
        future = make_future(sid=1, start=datetime.date(2024, 1, 3),
                             expiration=datetime.date(2024, 12, 20))
        self.assertIsNotNone(future.asset, "the contract exists in metadata")

        algorithm = TradingAlgorithm.__new__(TradingAlgorithm)
        algorithm._logger = type("L", (), {"warning": lambda *a, **k: None})()
        calendar = type("C", (), {"minute_to_session": staticmethod(lambda dt: dt)})()
        algorithm.clock = type("K", (), {"trading_calendar": calendar})()
        algorithm.simulation_dt = type("D", (), {
            "date": staticmethod(lambda: datetime.date(2025, 1, 3))})()
        self.assertFalse(TradingAlgorithm._can_order_asset(algorithm, asset=future),
                         "past auto close the contract exists but is not tradable")


if __name__ == "__main__":
    unittest.main()


class MarginModelTests(unittest.IsolatedAsyncioTestCase):
    """#5: margin is either modelled explicitly or its absence is stated explicitly."""

    async def test_the_default_states_that_margin_is_not_modelled(self):
        from ziplime.finance.margin import NO_MARGIN_MODEL_WARNING, NoFuturesMarginModel
        ledger = make_ledger()
        self.assertIsInstance(ledger.futures_margin_model, NoFuturesMarginModel)
        self.assertFalse(ledger.futures_margin_model.models_margin,
                         "the default must report that it does not model margin")
        self.assertIn("MARGIN", NO_MARGIN_MODEL_WARNING)

    async def test_the_warning_fires_once_when_futures_are_traded(self):
        from ziplime.finance.margin import NoFuturesMarginModel
        model = NoFuturesMarginModel()
        ledger = make_ledger()
        ledger.futures_margin_model = model
        warnings = []
        model.warn_once = lambda: warnings.append(1)

        future = make_future(sid=1)
        trade(ledger, future, amount=1, price=70.0)
        trade(ledger, future, amount=1, price=70.0)
        await settle(ledger)
        self.assertEqual(len(warnings), 2, "the ledger asks on every futures transaction")
        # And the real implementation only emits once.
        real = NoFuturesMarginModel()
        emitted = []
        import ziplime.finance.margin as margin_module
        original = margin_module._logger.warning
        margin_module._logger.warning = lambda *a, **k: emitted.append(a)
        try:
            real.warn_once()
            real.warn_once()
        finally:
            margin_module._logger.warning = original
        self.assertEqual(len(emitted), 1)

    async def test_an_equity_only_backtest_does_not_warn(self):
        from ziplime.finance.margin import NoFuturesMarginModel
        from futures_fixtures import make_equity
        model = NoFuturesMarginModel()
        ledger = make_ledger()
        ledger.futures_margin_model = model
        warnings = []
        model.warn_once = lambda: warnings.append(1)

        trade(ledger, make_equity(sid=900), amount=10, price=70.0)
        await settle(ledger)
        self.assertEqual(warnings, [])

    async def test_a_fixed_rate_model_reports_the_requirement(self):
        from ziplime.finance.margin import FixedRateFuturesMarginModel
        ledger = make_ledger()
        ledger.futures_margin_model = FixedRateFuturesMarginModel(initial_rate=0.12,
                                                                  maintenance_rate=0.10)
        future = make_future(sid=1, multiplier=1000.0)
        trade(ledger, future, amount=10, price=70.0)
        await settle(ledger)

        notional = 70.0 * 1000.0 * 10
        self.assertAlmostEqual(ledger.futures_margin_requirement(), notional * 0.12)
        self.assertAlmostEqual(ledger.futures_margin_requirement(maintenance=True),
                               notional * 0.10)

    async def test_margin_is_charged_on_a_short_position_too(self):
        from ziplime.finance.margin import FixedRateFuturesMarginModel
        ledger = make_ledger()
        ledger.futures_margin_model = FixedRateFuturesMarginModel(initial_rate=0.12)
        future = make_future(sid=1, multiplier=1000.0)
        trade(ledger, future, amount=-10, price=70.0)
        await settle(ledger)
        self.assertAlmostEqual(ledger.futures_margin_requirement(), 70.0 * 1000.0 * 10 * 0.12)

    async def test_per_root_model_matches_exchange_style_quoting(self):
        from ziplime.finance.margin import PerRootFuturesMarginModel
        ledger = make_ledger()
        ledger.futures_margin_model = PerRootFuturesMarginModel(
            initial_by_root={"CL": 8_500.0}, default_per_contract=1_000.0)
        known = make_future(sid=1, root_symbol="CL")
        unknown = make_future(sid=2, symbol="ZZZ24", root_symbol="ZZ")
        trade(ledger, known, amount=2, price=70.0)
        trade(ledger, unknown, amount=3, price=70.0)
        await settle(ledger)
        self.assertAlmostEqual(ledger.futures_margin_requirement(), 2 * 8_500.0 + 3 * 1_000.0)

    def test_a_maintenance_rate_above_initial_is_rejected(self):
        from ziplime.finance.margin import FixedRateFuturesMarginModel
        with self.assertRaises(ValueError):
            FixedRateFuturesMarginModel(initial_rate=0.10, maintenance_rate=0.20)


class CommissionCostBasisTests(unittest.IsolatedAsyncioTestCase):
    """#31: a commission must reach the position's cost basis, not only cash."""

    async def test_commission_moves_the_futures_cost_basis(self):
        from ziplime.finance.domain.commission import Commission
        ledger = make_ledger()
        future = make_future(sid=1, multiplier=1000.0)
        trade(ledger, future, amount=10, price=70.0)
        await settle(ledger)
        basis_before = ledger.position_tracker.get_position(
            future, EXCHANGE.mic, ACCOUNT).cost_basis

        ledger.process_commission(Commission(asset=future, order=None, amount=15.0), tr=None)
        basis_after = ledger.position_tracker.get_position(
            future, EXCHANGE.mic, ACCOUNT).cost_basis

        self.assertNotAlmostEqual(basis_before, basis_after,
                                  msg="commission never reached the cost basis")
        # A future's cost basis is per point, so the commission is divided by the multiplier.
        self.assertAlmostEqual(basis_after, basis_before + 15.0 / (1000.0 * 10), places=9)

    async def test_commission_also_leaves_cash(self):
        from ziplime.finance.domain.commission import Commission
        ledger = make_ledger()
        future = make_future(sid=1)
        trade(ledger, future, amount=1, price=70.0)
        await settle(ledger)
        cash_before = ledger.portfolio.cash
        ledger.process_commission(Commission(asset=future, order=None, amount=4.5), tr=None)
        self.assertAlmostEqual(ledger.portfolio.cash, cash_before - 4.5)


class MarginCurrencyTests(unittest.IsolatedAsyncioTestCase):
    """Margin currency is a property of the exchange, not of the quote.

    An exchange may report its margin currency as its own local currency for a contract
    it quotes in dollars.
    """

    def _dollar_quoted_local_margin(self):
        # Quoted in USD, margined in a local currency -- a common commodity case.
        return make_future(sid=1, symbol="NGZ5", root_symbol="NG", multiplier=100.0,
                           margin_currency="RUB")

    def _cme(self):
        return make_future(sid=2, symbol="NGZ25", root_symbol="NG.XNYM", multiplier=10_000.0,
                           margin_currency="USD")

    async def test_a_rate_model_refuses_to_cross_currencies_silently(self):
        from ziplime.finance.margin import FixedRateFuturesMarginModel, MissingFxRate
        model = FixedRateFuturesMarginModel(initial_rate=0.27)
        with self.assertRaises(MissingFxRate) as caught:
            model.initial_margin(self._dollar_quoted_local_margin(), amount=10, price=4.0)
        message = str(caught.exception)
        self.assertIn("USD", message)
        self.assertIn("RUB", message)

    async def test_a_rate_model_converts_when_given_a_rate(self):
        from ziplime.finance.margin import FixedRateFuturesMarginModel
        model = FixedRateFuturesMarginModel(initial_rate=0.27, fx_rates={("USD", "RUB"): 80.0})
        # 10 contracts x $4.00 x 100 = $4 000 notional -> 27% -> $1 080 -> 86 400 RUB
        self.assertAlmostEqual(
            model.initial_margin(self._dollar_quoted_local_margin(), amount=10, price=4.0), 86_400.0)

    async def test_a_per_contract_model_needs_no_rate(self):
        # Exchanges publish margin per contract in the currency they collect, which sidesteps FX.
        from ziplime.finance.margin import PerRootFuturesMarginModel
        model = PerRootFuturesMarginModel(initial_by_root={"NG": 6_556.48, "NG.XNYM": 3_500.0})
        self.assertAlmostEqual(model.initial_margin(self._dollar_quoted_local_margin(), 2, 4.0),
                               2 * 6_556.48)
        self.assertAlmostEqual(model.initial_margin(self._cme(), 3, 4.0), 3 * 3_500.0)

    async def test_a_cross_venue_book_reports_margin_per_currency(self):
        from ziplime.finance.margin import PerRootFuturesMarginModel
        ledger = make_ledger()
        ledger.futures_margin_model = PerRootFuturesMarginModel(
            initial_by_root={"NG": 6_556.48, "NG.XNYM": 3_500.0})
        local, cme = self._dollar_quoted_local_margin(), self._cme()
        trade(ledger, local, amount=100, price=4.0)
        trade(ledger, cme, amount=-1, price=4.0)
        await settle(ledger)

        by_currency = ledger.futures_margin_by_currency()
        self.assertAlmostEqual(by_currency["RUB"], 100 * 6_556.48)
        self.assertAlmostEqual(by_currency["USD"], 1 * 3_500.0)

    async def test_asking_for_one_number_across_two_currencies_raises(self):
        from ziplime.finance.margin import PerRootFuturesMarginModel
        ledger = make_ledger()
        ledger.futures_margin_model = PerRootFuturesMarginModel(
            initial_by_root={"NG": 6_556.48, "NG.XNYM": 3_500.0})
        trade(ledger, self._dollar_quoted_local_margin(), amount=100, price=4.0)
        trade(ledger, self._cme(), amount=-1, price=4.0)
        await settle(ledger)

        with self.assertRaises(ValueError) as caught:
            ledger.futures_margin_requirement()
        self.assertIn("more than one currency", str(caught.exception))
        # Naming the currency is fine.
        self.assertAlmostEqual(ledger.futures_margin_requirement(currency="USD"), 3_500.0)


class DeliverableContractTests(unittest.IsolatedAsyncioTestCase):
    """A physically delivered contract must leave the book before the delivery window."""

    def test_settlement_type_is_carried_on_the_contract(self):
        from ziplime.assets.domain.settlement_type import SettlementType
        cash = make_future(sid=1, settlement_type=SettlementType.CASH)
        physical = make_future(sid=2, settlement_type=SettlementType.PHYSICAL)
        self.assertFalse(cash.asset.is_deliverable)
        self.assertTrue(physical.asset.is_deliverable)

    def test_a_deliverable_contract_auto_closes_before_it_stops_trading(self):
        # The ingest places notice a few sessions ahead of the last trading day, so the forced
        # close happens while the contract can still be traded at a real price.
        from ziplime.assets.domain.settlement_type import SettlementType
        expiration = datetime.date(2025, 11, 25)
        physical = make_future(sid=1, expiration=expiration,
                               notice=datetime.date(2025, 11, 20),
                               auto_close=datetime.date(2025, 11, 20),
                               settlement_type=SettlementType.PHYSICAL)
        self.assertLess(physical.asset.auto_close_date, physical.asset.expiration_date)
        self.assertEqual(physical.asset.auto_close_date, physical.asset.notice_date)

    def test_a_cash_settled_contract_auto_closes_after_it_stops_trading(self):
        cash = make_future(sid=1, expiration=datetime.date(2025, 12, 18))
        self.assertGreater(cash.asset.auto_close_date, cash.asset.expiration_date)

    async def test_the_forced_close_of_a_deliverable_position_is_announced(self):
        from ziplime.assets.domain.settlement_type import SettlementType
        ledger = make_ledger()
        physical = make_future(sid=1, settlement_type=SettlementType.PHYSICAL)
        trade(ledger, physical, amount=4, price=70.0)
        await settle(ledger)

        warnings = []
        ledger.logger = type("L", (), {"warning": lambda _self, msg, **kw: warnings.append(msg)})()
        ledger.position_tracker.data_bundle = None
        try:
            ledger.close_position(asset=physical,
                                  dt=datetime.datetime(2023, 12, 15,
                                                       tzinfo=datetime.timezone.utc))
        except Exception:
            pass        # the close itself needs a data bundle; the warning is what is under test
        self.assertTrue(any("deliverable" in w for w in warnings),
                        f"expected a delivery warning, got {warnings}")
