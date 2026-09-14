"""0DTE options: the instrument, its arithmetic, and the structures built on it.

A zero-day option concentrates every way an option backtest can be quietly wrong into one session.
The contract is listed this morning and gone tonight, so nothing can be resolved once and held.
Time to expiry is minutes rather than days, so a model that measures it in days misprices the
afternoon by a factor of eight. The premium goes through a multiplier of 100, so an error there is
a hundredfold in cash. And the contract settles at intrinsic value against a print that has
nothing to do with its last quote -- a wing that stopped being quoted carries a mark into a
settlement of zero, and a strike that finishes a dime in the money carries nothing into ten
dollars a contract.

Each class below pins one of those.
"""
import dataclasses
import datetime
import math
import unittest

import polars as pl

from options_fixtures import (
    EXPIRY, SESSION_CLOSE, SPOT, TZ, make_chain, make_option, underlying,
)

from ziplime.assets.domain.exercise_style import ExerciseStyle
from ziplime.assets.domain.option_type import OptionType
from ziplime.assets.entities.option_contract import format_occ_symbol, parse_occ_symbol
from ziplime.finance.options import strategies as option_strategies
from ziplime.finance.options.chain import OptionChain
from ziplime.finance.options.greeks import (
    aggregate_greeks, black_scholes_price, greeks, implied_volatility, position_greeks,
    time_to_expiry,
)


class OccSymbolTests(unittest.TestCase):
    """The identifier every real option feed speaks, and the one ziplime names listings by.

    It matters more here than a naming convention usually does: with 0DTE the symbol is the only
    thing that ties a contract generated today to the same contract arriving over a quote feed
    tomorrow, and it encodes the strike to a tenth of a cent so that 523.50 and 523.505 stay
    distinct.
    """

    def test_a_symbol_round_trips(self):
        symbol = format_occ_symbol("SPY", EXPIRY, OptionType.CALL, 523.0)
        self.assertEqual(symbol, "SPY240614C00523000")
        self.assertEqual(parse_occ_symbol(symbol),
                         ("SPY", EXPIRY, OptionType.CALL, 523.0))

    def test_a_fractional_strike_survives(self):
        symbol = format_occ_symbol("SPY", EXPIRY, OptionType.PUT, 523.5)
        self.assertEqual(parse_occ_symbol(symbol)[3], 523.5)

    def test_the_space_padded_form_the_occ_publishes_is_accepted(self):
        self.assertEqual(parse_occ_symbol("SPY   240614P00523500"),
                         ("SPY", EXPIRY, OptionType.PUT, 523.5))

    def test_something_that_is_not_an_option_symbol_is_refused(self):
        for value in ("SPY", "SPY240614X00523000", "240614C00523000", ""):
            with self.assertRaises(ValueError):
                parse_occ_symbol(value)


class ContractTests(unittest.TestCase):
    def test_a_zero_dte_contract_is_listed_and_expires_on_one_session(self):
        contract = make_option(1, 523.0, OptionType.CALL).asset
        self.assertTrue(contract.is_zero_dte_on(EXPIRY))
        self.assertEqual(contract.start_date, contract.expiration_date)
        self.assertEqual(contract.auto_close_date, contract.expiration_date)

    def test_intrinsic_value_is_the_payoff_at_expiry(self):
        call = make_option(1, 520.0, OptionType.CALL).asset
        put = make_option(2, 520.0, OptionType.PUT).asset
        self.assertAlmostEqual(call.intrinsic_value(SPOT), 3.41)
        self.assertAlmostEqual(put.intrinsic_value(SPOT), 0.0)
        self.assertAlmostEqual(put.intrinsic_value(515.0), 5.0)

    def test_settlement_applies_the_multiplier(self):
        """What actually reaches cash: one contract covers a hundred shares."""
        call = make_option(1, 520.0, OptionType.CALL).asset
        self.assertAlmostEqual(call.settlement_value(SPOT), 341.0)

    def test_moneyness_ranks_calls_and_puts_together(self):
        call = make_option(1, 520.0, OptionType.CALL).asset
        put = make_option(2, 526.0, OptionType.PUT).asset
        self.assertGreater(call.moneyness(SPOT), 0)
        self.assertGreater(put.moneyness(SPOT), 0)
        self.assertLess(make_option(3, 530.0, OptionType.CALL).asset.moneyness(SPOT), 0)

    def test_a_zero_multiplier_is_refused(self):
        """Measured on Limex's feed: MOEX options report ``multiplier = 0`` and carry the real
        size in ``contract_size``. An adapter that maps by name would produce a contract whose
        premium, value and settlement are all zero, and nothing would raise."""
        import dataclasses
        with self.assertRaises(ValueError) as raised:
            dataclasses.replace(make_option(1, 310.0, OptionType.CALL).asset, multiplier=0.0)
        self.assertIn("contract_size", str(raised.exception))

    def test_the_exercise_style_is_recorded_even_though_it_is_not_modelled(self):
        contract = make_option(1, 523.0, OptionType.CALL).asset
        self.assertIs(contract.exercise_style, ExerciseStyle.AMERICAN)


class TimeToExpiryTests(unittest.TestCase):
    """The number that separates a working 0DTE model from a useless one."""

    def at(self, hour: int, minute: int) -> float:
        return time_to_expiry(datetime.datetime(2024, 6, 14, hour, minute, tzinfo=TZ),
                              SESSION_CLOSE)

    def test_it_shrinks_through_the_session(self):
        morning, afternoon = self.at(9, 31), self.at(15, 55)
        self.assertGreater(morning, afternoon)
        # Six and a half hours against five minutes: nearly eighty to one.
        self.assertGreater(morning / afternoon, 70)

    def test_it_reaches_zero_at_the_close_and_stays_there(self):
        self.assertEqual(self.at(16, 0), 0.0)
        self.assertEqual(self.at(16, 30), 0.0)

    def test_a_naive_instant_is_refused(self):
        """Minutes matter here, and a naive datetime cannot say which minutes."""
        with self.assertRaises(ValueError):
            time_to_expiry(datetime.datetime(2024, 6, 14, 12, 0), SESSION_CLOSE)

    def test_measuring_in_whole_days_would_overprice_the_afternoon(self):
        """The error this exists to prevent, quantified."""
        honest = black_scholes_price(OptionType.CALL, SPOT, 523.0, self.at(15, 55), 0.0, 0.18)
        as_one_day = black_scholes_price(OptionType.CALL, SPOT, 523.0, 1 / 365, 0.0, 0.18)
        self.assertGreater(as_one_day, honest * 3)


class GreeksTests(unittest.TestCase):
    def test_greeks_at_expiry_are_the_limits_not_nan(self):
        """The last bar of every 0DTE run is exactly this case."""
        in_the_money = greeks(OptionType.CALL, SPOT, 520.0, 0.0, 0.0, 0.18)
        self.assertAlmostEqual(in_the_money.price, 3.41)
        self.assertEqual(in_the_money.delta, 1.0)
        self.assertEqual(in_the_money.gamma, 0.0)
        self.assertTrue(in_the_money.is_expired)

        out_of_the_money = greeks(OptionType.CALL, SPOT, 530.0, 0.0, 0.0, 0.18)
        self.assertEqual(out_of_the_money.price, 0.0)
        self.assertEqual(out_of_the_money.delta, 0.0)

    def test_an_expired_in_the_money_put_has_delta_minus_one(self):
        expired = greeks(OptionType.PUT, SPOT, 530.0, 0.0, 0.0, 0.18)
        self.assertEqual(expired.delta, -1.0)
        self.assertAlmostEqual(expired.price, 530.0 - SPOT)

    def test_gamma_grows_into_the_close(self):
        """Why a 0DTE book is flattened before the bell rather than held through it."""
        morning = greeks(OptionType.CALL, SPOT, 523.0,
                         time_to_expiry(datetime.datetime(2024, 6, 14, 9, 31, tzinfo=TZ),
                                        SESSION_CLOSE), 0.0, 0.18).gamma
        afternoon = greeks(OptionType.CALL, SPOT, 523.0,
                           time_to_expiry(datetime.datetime(2024, 6, 14, 15, 55, tzinfo=TZ),
                                          SESSION_CLOSE), 0.0, 0.18).gamma
        self.assertGreater(afternoon, morning * 3)

    def test_implied_volatility_round_trips(self):
        years = 0.0005
        price = black_scholes_price(OptionType.CALL, SPOT, 523.0, years, 0.04, 0.22)
        self.assertAlmostEqual(
            implied_volatility(price, OptionType.CALL, SPOT, 523.0, years, 0.04), 0.22, places=4)

    def test_an_unattainable_price_gives_none_rather_than_raising(self):
        """Routine on a 0DTE chain in the last hour, not an error."""
        years = 0.0005
        # Below intrinsic: every volatility down to zero reproduces it.
        self.assertIsNone(implied_volatility(0.10, OptionType.CALL, SPOT, 520.0, years, 0.0))
        # No time left: there is no volatility to solve for.
        self.assertIsNone(implied_volatility(1.0, OptionType.CALL, SPOT, 523.0, 0.0, 0.0))

    def test_position_greeks_scale_by_size_and_multiplier(self):
        per_unit = greeks(OptionType.CALL, SPOT, 523.0, 0.0005, 0.0, 0.18)
        book = position_greeks(per_unit, amount=-10, multiplier=100.0)
        self.assertAlmostEqual(book.delta, per_unit.delta * -1000)
        self.assertAlmostEqual(book.gamma, per_unit.gamma * -1000)

    def test_a_short_straddle_is_delta_neutral_and_short_gamma(self):
        """Neutral to within a rounding of one share, not exactly: at the money ``d1`` is half a
        volatility-unit above zero, so the two deltas do not quite cancel. That residual is the
        real one and is why a straddle is re-hedged rather than assumed flat."""
        years = 0.0005
        legs = [
            position_greeks(greeks(option_type, 523.0, 523.0, years, 0.0, 0.18), -1, 100.0)
            for option_type in (OptionType.CALL, OptionType.PUT)
        ]
        book = aggregate_greeks(legs)
        self.assertLess(abs(book.delta), 1.0, "under one share against 100 per leg")
        self.assertLess(book.gamma, 0)
        self.assertGreater(book.theta, 0, "a short option earns theta")


class ChainSelectionTests(unittest.TestCase):
    """A 0DTE strategy cannot name its contracts, so selection is the whole interface."""

    def setUp(self):
        self.chain = make_chain()

    def test_the_worked_example_chain(self):
        self.assertEqual(len(self.chain), 34)
        self.assertEqual(self.chain.strikes[0], 515.0)
        self.assertEqual(self.chain.strikes[-1], 531.0)
        self.assertEqual(len(self.chain.calls), 17)
        self.assertEqual(len(self.chain.puts), 17)

    def test_at_the_money_is_the_nearest_listed_strike(self):
        self.assertEqual(self.chain.atm(SPOT, OptionType.CALL).asset.strike, 523.0)

    def test_an_offset_counts_strikes_away_from_the_money_on_both_sides(self):
        """Positive is always further out of the money, whichever side it is."""
        self.assertEqual(self.chain.strike_offset(SPOT, OptionType.CALL, 2).asset.strike, 525.0)
        self.assertEqual(self.chain.strike_offset(SPOT, OptionType.PUT, 2).asset.strike, 521.0)

    def test_an_offset_past_the_edge_clamps_rather_than_raising(self):
        """A chain generated around the money genuinely runs out, and the wing wants the edge."""
        self.assertEqual(self.chain.strike_offset(SPOT, OptionType.CALL, 99).asset.strike, 531.0)
        self.assertEqual(self.chain.strike_offset(SPOT, OptionType.PUT, 99).asset.strike, 515.0)

    def test_a_strike_the_chain_does_not_carry_resolves_to_the_nearest(self):
        self.assertEqual(self.chain.nearest_strike(523.4, OptionType.CALL).asset.strike, 523.0)
        self.assertIsNone(self.chain.at_strike(523.4, OptionType.CALL))

    def test_a_tie_goes_to_the_lower_strike_every_time(self):
        """Determinism across runs, rather than depending on the order the chain came back in."""
        for _ in range(5):
            self.assertEqual(self.chain.nearest_strike(523.5, OptionType.CALL).asset.strike, 523.0)

    def test_selection_by_delta(self):
        put = self.chain.nearest_delta(0.25, OptionType.PUT, spot=SPOT, years_to_expiry=0.002,
                                       volatility=0.18)
        self.assertLess(put.asset.strike, SPOT, "a 25-delta put is out of the money")
        delta = greeks(OptionType.PUT, SPOT, put.asset.strike, 0.002, 0.0, 0.18).delta
        self.assertAlmostEqual(abs(delta), 0.25, delta=0.1)

    def test_the_underlying_price_can_be_recovered_from_put_call_parity(self):
        """For a feed that carries options but not the thing they are written on."""
        years, volatility = 0.002, 0.18
        prices = {}
        for listing in self.chain:
            contract = listing.asset
            prices[listing.sid] = black_scholes_price(contract.option_type, SPOT, contract.strike,
                                                      years, 0.0, volatility)
        self.assertAlmostEqual(self.chain.spot_from(prices), SPOT, places=1)

    def test_a_chain_spanning_two_expiries_is_refused(self):
        other = make_option(999, 523.0, OptionType.CALL,
                            expiration=EXPIRY + datetime.timedelta(days=1))
        with self.assertRaises(ValueError):
            OptionChain.from_listings(list(self.chain) + [other])

    def test_an_equity_listing_is_not_an_option(self):
        with self.assertRaises(TypeError):
            OptionChain.from_listings([underlying()])


class StrategyPayoffTests(unittest.TestCase):
    """Payoffs are solved on the kinks, not sampled, so the extremes are the real ones."""

    def setUp(self):
        self.chain = make_chain()

    def call(self, strike):
        return self.chain.at_strike(strike, OptionType.CALL)

    def put(self, strike):
        return self.chain.at_strike(strike, OptionType.PUT)

    def test_an_iron_condor_is_defined_risk_on_both_sides(self):
        condor = option_strategies.iron_condor(
            long_put=self.put(520.0), short_put=self.put(522.0),
            short_call=self.call(524.0), long_call=self.call(526.0))
        prices = {self.put(520.0).sid: 0.30, self.put(522.0).sid: 0.95,
                  self.call(524.0).sid: 0.90, self.call(526.0).sid: 0.28}
        net = condor.net_premium(prices)

        self.assertAlmostEqual(net, -127.0, places=6, msg="a credit is negative")
        self.assertAlmostEqual(condor.max_profit(net), 127.0)
        # Two dollars of width, times 100, less the credit kept.
        self.assertAlmostEqual(condor.max_loss(net), -73.0)
        self.assertEqual([round(b, 2) for b in condor.breakevens(net)], [520.73, 525.27])

    def test_the_condor_keeps_its_whole_credit_between_the_short_strikes(self):
        condor = option_strategies.iron_condor(
            long_put=self.put(520.0), short_put=self.put(522.0),
            short_call=self.call(524.0), long_call=self.call(526.0))
        net = -127.0
        self.assertAlmostEqual(condor.profit_at(523.0, net), 127.0)
        self.assertAlmostEqual(condor.profit_at(500.0, net), -73.0)
        self.assertAlmostEqual(condor.profit_at(545.0, net), -73.0)

    def test_an_iron_butterfly_can_be_built_at_all(self):
        """It could not, until it stopped delegating to `iron_condor`.

        A condor requires four *strictly* ascending strikes; an iron butterfly requires its middle
        two to be equal. Building one on top of the other meant every call raised "An iron condor
        needs ascending strikes ... got [520, 523, 523, 526]" -- a complaint about a structure the
        caller had not asked for, naming as an error the one property that defines this one. The
        function could not succeed for any input, and nothing covered it.
        """
        butterfly = option_strategies.iron_butterfly(
            long_put=self.put(520.0), short_put=self.put(523.0),
            short_call=self.call(523.0), long_call=self.call(526.0))
        # `strikes` is the distinct kink points of the payoff, so the shared body appears once.
        self.assertEqual(butterfly.strikes, [520.0, 523.0, 526.0])
        self.assertEqual(butterfly.name, "iron butterfly 520/523/526")
        self.assertEqual([leg.contract.strike for leg in butterfly.legs],
                         [520.0, 523.0, 523.0, 526.0])
        self.assertEqual([leg.ratio for leg in butterfly.legs], [1.0, -1.0, -1.0, 1.0])

    def test_an_iron_butterfly_is_defined_risk_and_peaks_at_its_body(self):
        butterfly = option_strategies.iron_butterfly(
            long_put=self.put(520.0), short_put=self.put(523.0),
            short_call=self.call(523.0), long_call=self.call(526.0))
        prices = {self.put(520.0).sid: 0.30, self.put(523.0).sid: 1.60,
                  self.call(523.0).sid: 1.55, self.call(526.0).sid: 0.28}
        net = butterfly.net_premium(prices)

        self.assertLess(net, 0.0, "selling the body for more than the wings cost is a credit")
        # The whole credit is kept only if it finishes exactly on the body -- the narrow profit
        # zone is what distinguishes this from a condor.
        self.assertAlmostEqual(butterfly.profit_at(523.0, net), -net)
        self.assertAlmostEqual(butterfly.max_profit(net), -net)
        self.assertGreater(butterfly.max_loss(net), -math.inf, "the wings cap it")
        # Three dollars of wing, times 100, less the credit kept.
        self.assertAlmostEqual(butterfly.max_loss(net), -300.0 - net)

    def test_short_legs_at_different_strikes_are_a_condor_and_say_so(self):
        with self.assertRaises(ValueError) as caught:
            option_strategies.iron_butterfly(
                long_put=self.put(520.0), short_put=self.put(522.0),
                short_call=self.call(524.0), long_call=self.call(526.0))
        self.assertIn("iron condor", str(caught.exception))

    def test_a_wing_inside_the_body_is_refused(self):
        """A wing at or inside the body caps nothing, which makes the structure something else."""
        with self.assertRaises(ValueError) as caught:
            option_strategies.iron_butterfly(
                long_put=self.put(523.0), short_put=self.put(523.0),
                short_call=self.call(523.0), long_call=self.call(526.0))
        self.assertIn("outside its body", str(caught.exception))

    def test_a_naked_short_wing_reports_unbounded_loss(self):
        """Not a caveat on a 0DTE contract: the whole move happens inside the session."""
        spread = option_strategies.ratio_spread(self.call(523.0), self.call(526.0),
                                                short_ratio=2)
        net = spread.net_premium({self.call(523.0).sid: 1.50, self.call(526.0).sid: 0.40})
        self.assertEqual(spread.max_loss(net), -math.inf)
        self.assertAlmostEqual(spread.max_profit(net), 230.0)

    def test_a_long_straddle_risks_its_premium_and_no_more(self):
        straddle = option_strategies.straddle(self.call(523.0), self.put(523.0))
        net = straddle.net_premium({self.call(523.0).sid: 1.20, self.put(523.0).sid: 0.95})
        self.assertAlmostEqual(net, 215.0)
        self.assertEqual(straddle.max_profit(net), math.inf)
        self.assertAlmostEqual(straddle.max_loss(net), -215.0)
        self.assertEqual([round(b, 2) for b in straddle.breakevens(net)], [520.85, 525.15])

    def test_a_butterfly_peaks_at_its_body(self):
        fly = option_strategies.butterfly(self.call(521.0), self.call(523.0), self.call(525.0))
        net = fly.net_premium({self.call(521.0).sid: 3.0, self.call(523.0).sid: 1.7,
                               self.call(525.0).sid: 0.8})
        self.assertAlmostEqual(net, 40.0)
        self.assertAlmostEqual(fly.max_profit(net), 160.0)
        self.assertAlmostEqual(fly.profit_at(523.0, net), 160.0)
        self.assertAlmostEqual(fly.max_loss(net), -40.0)

    def test_orders_are_whole_contracts_in_the_structure_s_ratios(self):
        condor = option_strategies.condor_around(self.chain, SPOT, body_steps=1, wing_steps=3)
        orders = dict(condor.orders(quantity=5))
        self.assertEqual(sorted(orders.values()), [-5, -5, 5, 5])

    def test_a_vertical_spread_names_its_own_direction(self):
        bull = option_strategies.vertical_spread(self.call(522.0), self.call(525.0))
        bear = option_strategies.vertical_spread(self.put(525.0), self.put(522.0))
        self.assertIn("bull call spread", bull.name)
        self.assertIn("bear put spread", bear.name)

    def test_structures_reject_legs_that_do_not_make_them(self):
        with self.assertRaises(ValueError):  # two sides is not a vertical
            option_strategies.vertical_spread(self.call(522.0), self.put(525.0))
        with self.assertRaises(ValueError):  # two strikes is not a straddle
            option_strategies.straddle(self.call(523.0), self.put(522.0))
        with self.assertRaises(ValueError):  # strikes out of order
            option_strategies.iron_condor(self.put(524.0), self.put(522.0),
                                          self.call(520.0), self.call(526.0))
        with self.assertRaises(ValueError):  # wings inside the body
            option_strategies.condor_around(self.chain, SPOT, body_steps=3, wing_steps=1)

    def test_a_leg_with_no_price_is_an_error_rather_than_a_skipped_leg(self):
        """A condor priced on three legs is not a condor, and its credit looks plausible."""
        condor = option_strategies.condor_around(self.chain, SPOT)
        with self.assertRaises(KeyError):
            condor.net_premium({condor.legs[0].listing.sid: 0.3})

    def test_structure_greeks_sum_across_legs(self):
        condor = option_strategies.condor_around(self.chain, SPOT, body_steps=2, wing_steps=5)
        book = condor.greeks(spot=SPOT, years_to_expiry=0.002, volatility=0.18, quantity=4)
        self.assertLess(book.gamma, 0, "a condor is short gamma")
        self.assertGreater(book.theta, 0, "and long theta")


class SyntheticChainTests(unittest.TestCase):
    """The generator, checked against properties rather than against its own output."""

    def setUp(self):
        from ziplime.data.data_sources.options.synthetic import (
            ChainSpec, SyntheticOptionChainSource,
        )
        self.ChainSpec = ChainSpec
        sessions = [EXPIRY - datetime.timedelta(days=1), EXPIRY]
        self.sessions = sessions
        self.closes = {
            session: datetime.datetime.combine(session, datetime.time(16, 0), tzinfo=TZ)
            for session in sessions
        }
        rows = []
        for session in sessions:
            stamp = datetime.datetime.combine(session, datetime.time(9, 35), tzinfo=TZ)
            # A deterministic ramp: the second session opens at 523.41 -- the worked example --
            # and drifts up two dollars, so the chain is centred where the example says.
            price = 520.0 if session == sessions[0] else SPOT
            while stamp <= self.closes[session]:
                rows.append({"date": stamp, "open": price, "high": price + 0.30,
                             "low": price - 0.30, "close": price})
                price += 0.02
                stamp += datetime.timedelta(minutes=5)
        self.bars = pl.DataFrame(rows)
        # The uniform grid from the brief -- 515 to 531 at a dollar -- rather than the
        # two-tier default, so the contracts these tests walk over stay countable.
        self.source = SyntheticOptionChainSource(
            underlying_symbol="SPY", mic="ARCX", underlying_bars=self.bars,
            session_closes=self.closes,
            chain=ChainSpec(near_step=1.0, near_reach=8.0, far_step=1.0, far_reach=8.0))

    def contracts(self):
        import asyncio
        return asyncio.run(self.source.contracts("SPY", "ARCX", self.sessions))

    def bars_for(self, specs):
        import asyncio
        return asyncio.run(self.source.bars(specs, self.bars["date"]))

    def test_the_worked_example_chain_is_what_comes_out(self):
        """SPY at 523.41 lists 515 to 531, calls and puts: 34 contracts."""
        specs = [s for s in self.contracts() if s.expiration_date == EXPIRY]
        self.assertEqual(len(specs), 34)
        self.assertEqual(sorted({s.strike for s in specs})[0], 515.0)
        self.assertEqual(sorted({s.strike for s in specs})[-1], 531.0)

    def test_every_contract_is_listed_and_expires_on_the_same_session(self):
        for spec in self.contracts():
            self.assertTrue(spec.is_zero_dte, spec.occ_symbol)

    def test_a_chain_is_centred_on_the_session_open_not_its_close(self):
        """Centring on the close would guarantee the at-the-money strike is where the underlying
        finished, which is the one thing a 0DTE strategy is trying to guess."""
        self.assertAlmostEqual(self.source.session_reference_price(EXPIRY), SPOT)
        last_close = float(self.bars.filter(
            pl.col("date").dt.date() == EXPIRY)["close"][-1])
        self.assertNotAlmostEqual(last_close, SPOT, places=1)

    def test_prices_reach_exactly_intrinsic_value_at_the_closing_bar(self):
        """The bar that settles. Anything else there and the settlement disagrees with the mark."""
        specs = self.contracts()
        frame = self.bars_for(specs)
        final = frame.filter(pl.col("date") == self.closes[EXPIRY])
        self.assertGreater(final.height, 0)
        for row in final.iter_rows(named=True):
            _, _, option_type, strike = parse_occ_symbol(row["symbol"])
            expected = option_type.intrinsic_value(row["underlying_price"], strike)
            self.assertAlmostEqual(row["close"], round(expected, 2), places=2, msg=row["symbol"])

    def test_a_put_takes_its_high_from_the_underlying_s_low(self):
        """Wrong in every hand-rolled option fixture, and it silently inverts stop logic."""
        specs = [s for s in self.contracts()
                 if s.expiration_date == EXPIRY and s.strike == 523.0]
        frame = self.bars_for(specs)
        # A bar in the middle of the session, where there is still time value to differ over.
        stamp = self.closes[EXPIRY] - datetime.timedelta(hours=3)
        underlying_bar = self.bars.filter(pl.col("date") == stamp).to_dicts()[0]
        for row in frame.filter(pl.col("date") == stamp).iter_rows(named=True):
            _, _, option_type, strike = parse_occ_symbol(row["symbol"])
            high_at = (underlying_bar["low"] if option_type is OptionType.PUT
                       else underlying_bar["high"])
            self.assertAlmostEqual(
                row["high"],
                round(black_scholes_price(
                    option_type, high_at, strike,
                    time_to_expiry(stamp, self.closes[EXPIRY]), self.source.rate,
                    self.source.surface.implied_volatility(
                        self.source._atm_by_session[EXPIRY], high_at, strike,
                        years_to_expiry=time_to_expiry(stamp, self.closes[EXPIRY]))), 2),
                places=2, msg=row["symbol"])

    def test_the_smile_makes_puts_dearer_than_calls_the_same_distance_out(self):
        specs = self.contracts()
        frame = self.bars_for(specs)
        stamp = self.closes[EXPIRY] - datetime.timedelta(hours=3)
        at = {row["symbol"]: row for row in frame.filter(pl.col("date") == stamp).iter_rows(named=True)}
        put = at[format_occ_symbol("SPY", EXPIRY, OptionType.PUT, 520.0)]
        call = at[format_occ_symbol("SPY", EXPIRY, OptionType.CALL, 526.0)]
        self.assertGreater(put["implied_volatility"], call["implied_volatility"])

    def test_at_the_money_volume_exceeds_the_wings(self):
        specs = self.contracts()
        frame = self.bars_for(specs)
        stamp = self.closes[EXPIRY] - datetime.timedelta(hours=3)
        at = {row["symbol"]: row for row in frame.filter(pl.col("date") == stamp).iter_rows(named=True)}
        atm = at[format_occ_symbol("SPY", EXPIRY, OptionType.CALL, 523.0)]
        wing = at[format_occ_symbol("SPY", EXPIRY, OptionType.CALL, 531.0)]
        self.assertGreater(atm["volume"], wing["volume"])

    def test_open_interest_builds_through_the_session_from_zero(self):
        specs = [s for s in self.contracts()
                 if s.expiration_date == EXPIRY and s.strike == 523.0
                 and s.option_type is OptionType.CALL]
        frame = self.bars_for(specs).sort("date")
        self.assertEqual(frame["open_interest"][0], 0.0)
        self.assertGreater(frame["open_interest"][-1], frame["open_interest"][0])

    def test_the_first_session_is_priced_off_the_seed_not_off_its_own_bars(self):
        """Using the session's own returns would be the most flattering look-ahead available."""
        first = self.sessions[0]
        self.assertAlmostEqual(self.source._atm_by_session[first], self.source.atm_model.seed)

    def test_a_bid_is_never_negative_and_never_above_the_ask(self):
        frame = self.bars_for(self.contracts())
        self.assertTrue((frame["bid"] >= 0).all())
        self.assertTrue((frame["ask"] >= frame["bid"]).all())

    def test_the_source_says_it_is_not_a_market(self):
        from ziplime.data.data_sources.options.synthetic import (
            SyntheticDataPerformanceClaim, refuse_performance_claims,
        )
        self.assertFalse(self.source.is_real_market_data)
        self.assertTrue(self.source.name.startswith("synthetic-"))
        with self.assertRaises(SyntheticDataPerformanceClaim):
            refuse_performance_claims(self.source)


class StrikeGridTests(unittest.TestCase):
    """The grid, against the one OPRA actually lists.

    Measured from the live reference feed on a 764.29 spot: 155 strikes from 550 to 950, of which
    51 sit between 740 and 790 at one-dollar spacing and the rest at five.
    """

    def setUp(self):
        from ziplime.data.data_sources.options.synthetic import ChainSpec
        self.ChainSpec = ChainSpec

    def steps(self, strikes):
        import collections
        return collections.Counter(round(b - a, 2) for a, b in zip(strikes, strikes[1:]))

    def test_the_grid_is_fine_near_the_money_and_coarse_in_the_wings(self):
        strikes = self.ChainSpec().strikes_around(764.29)
        steps = self.steps(strikes)
        self.assertGreater(steps[1.0], 30, "a dollar apart around the money")
        self.assertGreater(steps[5.0], 10, "five apart in the wings")
        near = [k for k in strikes if abs(k - 764.0) <= 5]
        self.assertEqual(near, [759.0, 760.0, 761.0, 762.0, 763.0, 764.0,
                                765.0, 766.0, 767.0, 768.0, 769.0])

    def test_a_uniform_grid_is_still_expressible(self):
        """The worked example from the brief: SPY at 523.41 lists 515 to 531 at a dollar."""
        uniform = self.ChainSpec(near_step=1.0, near_reach=8.0, far_step=1.0, far_reach=8.0)
        self.assertEqual(uniform.strikes_around(523.41),
                         [float(k) for k in range(515, 532)])

    def test_the_grid_is_fixed_and_the_money_moves_through_it(self):
        """An exchange lists strikes on a grid; it does not recentre on every print."""
        spec = self.ChainSpec()
        self.assertEqual(spec.strikes_around(523.41), spec.strikes_around(523.49))

    def test_the_wings_align_to_their_own_grid(self):
        """A real chain's five-dollar strikes land on multiples of five, not on spot plus five."""
        strikes = self.ChainSpec().strikes_around(764.29)
        wings = [k for k in strikes if abs(k - 764.0) > 25]
        self.assertTrue(all(k % 5 == 0 for k in wings), wings)

    def test_an_inverted_reach_is_refused(self):
        with self.assertRaises(ValueError):
            self.ChainSpec(near_reach=60.0, far_reach=20.0)


class PremiumStyleAccountingTests(unittest.TestCase):
    """The same trade on OPRA and on MOEX, side by side.

    One instrument, one price, two venues, two completely different books. Buying a call on OPRA
    costs the premium and leaves you holding an asset. Buying the same call on MOEX costs nothing
    and leaves you holding a margin obligation that pays out daily. Measured against the live
    Limex feed: SPY options come back AMERICAN and physically settled, SBER options EUROPEAN and
    cash settled, and MOEX options are margined.

    Getting this backwards does not raise. A margined option run through premium accounting
    charges money that never left the account and then values the position twice -- once as an
    asset, once through the margin flow. A premium option run through margined accounting is free.
    """

    def setUp(self):
        import pandas as pd

        from ziplime.assets.domain.premium_style import PremiumStyle
        from ziplime.finance.domain.ledger import Ledger
        from ziplime.finance.domain.transaction import Transaction

        self.PremiumStyle = PremiumStyle
        self.Transaction = Transaction
        self.Ledger = Ledger
        self.pd = pd

    def book(self, premium_style, multiplier=100.0):
        """A ledger holding one option of the given style, and the listing itself."""
        import dataclasses

        listing = make_option(1, 520.0, OptionType.CALL)
        contract = dataclasses.replace(listing.asset, premium_style=premium_style,
                                       multiplier=multiplier)
        listing = dataclasses.replace(listing, asset=contract)
        ledger = self.Ledger(trading_sessions=self.pd.DatetimeIndex([EXPIRY]),
                             data_frequency=datetime.timedelta(days=1))
        for field in ("cash", "starting_cash", "portfolio_value"):
            setattr(ledger._portfolio, field, 100_000.0)
        return ledger, listing

    def trade(self, ledger, listing, amount, price):
        ledger.process_transaction(self.Transaction(
            id=f"{amount}@{price}", amount=amount, price=price,
            dt=datetime.datetime.combine(EXPIRY, datetime.time(16, 0), tzinfo=TZ),
            exchange_name="ARCX", trading_account_id="account-1", asset=listing))

    def mark(self, ledger, listing, price):
        ledger.position_tracker.update_position(
            asset=listing, exchange_name="ARCX", trading_account_id="account-1",
            last_sale_price=price)
        ledger._dirty_portfolio = True

    # -- the trade ---------------------------------------------------------------------

    def test_upfront_charges_the_premium_and_margined_charges_nothing(self):
        upfront, listing = self.book(self.PremiumStyle.UPFRONT)
        self.trade(upfront, listing, amount=1, price=1.20)
        self.assertAlmostEqual(upfront._portfolio.cash, 100_000.0 - 120.0)

        margined, listing = self.book(self.PremiumStyle.MARGINED)
        self.trade(margined, listing, amount=1, price=1.20)
        self.assertAlmostEqual(margined._portfolio.cash, 100_000.0,
                               msg="no premium changes hands on a margined option")

    # -- what the position is worth ----------------------------------------------------

    def test_a_premium_option_has_value_and_a_margined_one_does_not(self):
        upfront, listing = self.book(self.PremiumStyle.UPFRONT)
        self.trade(upfront, listing, amount=2, price=1.20)
        self.mark(upfront, listing, 1.50)
        self.assertAlmostEqual(upfront.position_tracker.stats.net_value, 300.0)

        margined, listing = self.book(self.PremiumStyle.MARGINED)
        self.trade(margined, listing, amount=2, price=1.20)
        self.mark(margined, listing, 1.50)
        stats = margined.position_tracker.stats
        self.assertEqual(stats.net_value, 0.0, "the P&L already reached cash as variation margin")
        self.assertAlmostEqual(stats.net_exposure, 300.0, msg="but the exposure is real")

    # -- how the P&L arrives -----------------------------------------------------------

    def test_a_margined_option_settles_variation_margin_on_the_move(self):
        """The futures mechanism, reached by an option. 2 contracts x 0.30 x 100 = 60."""
        margined, listing = self.book(self.PremiumStyle.MARGINED)
        self.trade(margined, listing, amount=2, price=1.20)
        self.mark(margined, listing, 1.50)
        self.assertAlmostEqual(margined._get_payout_total(margined.position_tracker.positions), 60.0)

    def test_a_premium_option_settles_no_variation_margin(self):
        upfront, listing = self.book(self.PremiumStyle.UPFRONT)
        self.trade(upfront, listing, amount=2, price=1.20)
        self.mark(upfront, listing, 1.50)
        self.assertEqual(upfront._get_payout_total(upfront.position_tracker.positions), 0,
                         "its P&L is in the position's value, not in cash")

    def test_the_round_trip_earns_the_same_either_way(self):
        """The two books disagree about *when* the money moves, never about how much.

        Buy at 1.20, sell at 1.50, two contracts: 60 either way. If this ever diverged, one of the
        two paths would be double-counting.
        """
        results = {}
        for style in (self.PremiumStyle.UPFRONT, self.PremiumStyle.MARGINED):
            ledger, listing = self.book(style)
            self.trade(ledger, listing, amount=2, price=1.20)
            self.mark(ledger, listing, 1.50)
            self.trade(ledger, listing, amount=-2, price=1.50)
            results[style] = ledger._portfolio.cash - 100_000.0
        self.assertAlmostEqual(results[self.PremiumStyle.UPFRONT], 60.0)
        self.assertAlmostEqual(results[self.PremiumStyle.MARGINED], 60.0)

    # -- margin ------------------------------------------------------------------------

    def test_a_margined_option_ties_up_margin_and_a_premium_one_does_not(self):
        from ziplime.finance.margin import FixedRateFuturesMarginModel

        model = FixedRateFuturesMarginModel(initial_rate=0.15, maintenance_rate=0.12)
        for style, expected in ((self.PremiumStyle.MARGINED, 45.0),
                                (self.PremiumStyle.UPFRONT, 0.0)):
            ledger, listing = self.book(style)
            ledger.futures_margin_model = model
            self.trade(ledger, listing, amount=2, price=1.50)
            self.mark(ledger, listing, 1.50)
            # 2 contracts x 1.50 x 100 = 300 of notional, at 15%.
            self.assertAlmostEqual(ledger.futures_margin_requirement(), expected, msg=str(style))

    def test_a_margined_contract_can_have_a_multiplier_of_one(self):
        """Some venues write an option on one futures contract rather than on a hundred shares."""
        margined, listing = self.book(self.PremiumStyle.MARGINED, multiplier=1.0)
        self.trade(margined, listing, amount=10, price=16.59)
        self.mark(margined, listing, 18.00)
        self.assertAlmostEqual(margined._get_payout_total(margined.position_tracker.positions), 14.1)



def native_named_venue(name: str = "XTST"):
    """A margined, European, cash-settled venue that names contracts its own way.

    ziplime ships ``OPRA`` and nothing else: every other market's conventions arrive through
    :func:`~ziplime.data.data_sources.options.venues.register_venue`, from the package that speaks
    that market. What the tests below cover is the *library's* behaviour -- margined accounting, a
    venue's contract size winning over a feed's, a native symbol that still carries an OCC
    description -- so the venue they need is a shape rather than any particular exchange.
    """
    from ziplime.assets.domain.premium_style import PremiumStyle
    from ziplime.assets.domain.settlement_type import SettlementType
    from ziplime.data.data_sources.options.venues import OptionVenue

    def native_name(root: str, strike: float, option_type: OptionType,
                    expiration_date: datetime.date) -> str:
        side = "C" if option_type is OptionType.CALL else "P"
        return f"{root}-{int(strike)}{side}-{expiration_date:%y%m}"

    return OptionVenue(
        name=name, mic="XTST", premium_style=PremiumStyle.MARGINED,
        exercise_style=ExerciseStyle.EUROPEAN, settlement_type=SettlementType.CASH,
        contract_size=100.0, tick_size=0.01, symbol_format="native",
        symbol_formatter=native_name)


class VenueTests(unittest.TestCase):
    """Five conventions, one place to state them, and one way to add more.

    The OPRA values were measured against the live Limex reference feed rather than looked up: SPY
    came back AMERICAN, physically settled, 100 per contract, named ``SPY   260914C00765000``. The
    second venue here is built by :func:`native_named_venue` rather than imported, because a venue
    that is not OPRA reaches ziplime through :func:`register_venue` and these tests are about that
    seam holding, not about any one exchange.
    """

    def setUp(self):
        from ziplime.data.data_sources.options import venues
        self.venues = venues
        self.us = datetime.date(2026, 9, 14)
        self.ru = datetime.date(2026, 9, 16)
        self.other = native_named_venue()

    def test_two_venues_can_disagree_on_everything_that_matters(self):
        """Which is the point of holding the conventions as data rather than as branches."""
        from ziplime.assets.domain.premium_style import PremiumStyle

        us = self.venues.OPRA.contract("SPY", self.us, OptionType.CALL, 765.0, self.us)
        other = self.other.contract("ABC", self.ru, OptionType.CALL, 310.0, self.ru, root="AB")

        self.assertIs(us.premium_style, PremiumStyle.UPFRONT)
        self.assertIs(other.premium_style, PremiumStyle.MARGINED)
        self.assertIs(us.exercise_style, ExerciseStyle.AMERICAN)
        self.assertIs(other.exercise_style, ExerciseStyle.EUROPEAN)
        self.assertEqual(us.multiplier, 100.0)
        # The venue's contract size, not the feed's `multiplier` field, which is a placeholder on
        # at least one real feed -- see the module docstring in `venues`.
        self.assertEqual(other.multiplier, 100.0)

    def test_each_venue_names_a_contract_its_own_way(self):
        us = self.venues.OPRA.contract("SPY", self.us, OptionType.CALL, 765.0, self.us)
        other = self.other.contract("ABC", self.ru, OptionType.CALL, 310.0, self.ru, root="AB")
        self.assertEqual(us.listing_symbol, "SPY260914C00765000")
        self.assertEqual(other.listing_symbol, "AB-310C-2609")

    def test_a_natively_named_contract_still_has_a_canonical_occ_description(self):
        """The listing is named natively; the OCC form still describes it, for anything that
        wants to compare contracts across venues."""
        other = self.other.contract("ABC", self.ru, OptionType.CALL, 310.0, self.ru, root="AB")
        self.assertEqual(other.occ_symbol, "ABC260916C00310000")

    def test_the_mic_is_the_caller_s_but_the_conventions_are_the_venue_s(self):
        """Which exchange row a listing hangs off decides its calendar, and that is a deployment
        question; the premium style is the instrument."""
        from ziplime.assets.domain.premium_style import PremiumStyle

        spec = self.other.contract("ABC", self.ru, OptionType.CALL, 310.0, self.ru,
                                   root="AB", mic="XOTH")
        self.assertEqual(spec.mic, "XOTH")
        self.assertIs(spec.premium_style, PremiumStyle.MARGINED)

    def test_an_unknown_venue_says_which_ones_there_are(self):
        with self.assertRaises(KeyError) as raised:
            self.venues.get_venue("CBOE")
        self.assertIn("OPRA", str(raised.exception))

    def test_a_venue_registered_from_outside_is_then_reachable_by_name(self):
        """How a market's own package contributes its conventions."""
        venue = native_named_venue(name="XREG1")
        self.addCleanup(self.venues.VENUES.pop, "XREG1", None)

        self.venues.register_venue(venue)

        self.assertIs(self.venues.get_venue("xreg1"), venue)

    def test_registering_the_same_venue_twice_is_harmless(self):
        venue = native_named_venue(name="XREG2")
        self.addCleanup(self.venues.VENUES.pop, "XREG2", None)
        self.venues.register_venue(venue)

        self.assertIs(self.venues.register_venue(venue), venue)

    def test_a_name_collision_with_different_conventions_is_refused(self):
        """Two packages claiming one name means one of them is about to be ignored, and a venue is
        five conventions at once -- the wrong one books wrong numbers without raising."""
        self.addCleanup(self.venues.VENUES.pop, "XREG3", None)
        self.venues.register_venue(native_named_venue(name="XREG3"))
        clashing = dataclasses.replace(native_named_venue(name="XREG3"), contract_size=1.0)

        with self.assertRaises(ValueError) as raised:
            self.venues.register_venue(clashing)

        self.assertIn("XREG3", str(raised.exception))
        self.assertIs(self.venues.get_venue("XREG3").contract_size, 100.0)


class NativeNamingChainGenerationTests(unittest.TestCase):
    """The synthetic generator, pointed at a venue that is not OPRA.

    Everything the venue fixes has to survive the trip into the chain: the premium style, the
    exercise style, the contract size, and the venue's own name for a contract. A generator that
    quietly reverted to OPRA's conventions would produce a chain that prices and settles wrongly
    while looking entirely normal.
    """

    def setUp(self):
        from ziplime.data.data_sources.options import venues
        from ziplime.data.data_sources.options.synthetic import (
            ChainSpec, SyntheticOptionChainSource,
        )
        expiry = datetime.date(2026, 9, 16)
        closes = {expiry: datetime.datetime.combine(expiry, datetime.time(18, 45), tzinfo=TZ)}
        rows, price = [], 283.0
        stamp = datetime.datetime.combine(expiry, datetime.time(10, 5), tzinfo=TZ)
        while stamp <= closes[expiry]:
            rows.append({"date": stamp, "open": price, "high": price + 0.5,
                         "low": price - 0.5, "close": price})
            price += 0.05
            stamp += datetime.timedelta(minutes=5)
        self.expiry = expiry
        self.source = SyntheticOptionChainSource(
            underlying_symbol="ABC", mic="XTST", underlying_bars=pl.DataFrame(rows),
            session_closes=closes, venue=native_named_venue(), root="AB",
            chain=ChainSpec(near_step=5.0, near_reach=20.0, far_step=5.0, far_reach=20.0))

    def test_it_generates_natively_named_margined_contracts(self):
        import asyncio

        from ziplime.assets.domain.premium_style import PremiumStyle

        specs = asyncio.run(self.source.contracts("ABC", "XTST", [self.expiry]))
        self.assertTrue(specs)
        for spec in specs:
            self.assertIs(spec.premium_style, PremiumStyle.MARGINED)
            self.assertIs(spec.exercise_style, ExerciseStyle.EUROPEAN)
            self.assertEqual(spec.multiplier, 100.0)
            self.assertRegex(spec.listing_symbol, r"^AB-\d+[CP]-\d{4}$")

    def test_its_bars_are_filed_under_the_native_symbol(self):
        import asyncio

        specs = asyncio.run(self.source.contracts("ABC", "XTST", [self.expiry]))
        frame = asyncio.run(self.source.bars(specs, self.source._bars["date"]))
        self.assertTrue(frame.height)
        self.assertTrue(all(s.startswith("AB-") for s in frame["symbol"].unique()))



class NoArbitrageTests(unittest.TestCase):
    """Prices from the surface must be a market, not an arbitrage.

    A call is worth less the higher its strike and a put is worth more; that is not a modelling
    preference, it is what stops a vertical spread from being free money. The generated chain has
    to obey it, and it did not: the smile's parameters were chosen for a 0DTE chain whose strikes
    sit within a percent of the money, and applied unchanged to a monthly SBER chain reaching 20%
    away they priced the 330 call **above** the 320 call. A bear call spread on that chain had a
    long leg dearer than its short one -- a guaranteed loss dressed as a credit -- and the strategy
    built on it sized itself into six thousand lots.

    The fix was to scale the smile's steepness by maturity; these pin the property it has to have.
    """

    def prices(self, strikes, option_type, spot, years, atm=0.25):
        from ziplime.data.data_sources.options.surface import VolatilitySurface

        surface = VolatilitySurface()
        return [black_scholes_price(option_type, spot, strike, years, 0.0,
                                    surface.implied_volatility(atm, spot, strike, years))
                for strike in strikes]

    def assert_monotone(self, strikes, prices, decreasing, label):
        for (k0, p0), (k1, p1) in zip(zip(strikes, prices), zip(strikes[1:], prices[1:])):
            if decreasing:
                self.assertLessEqual(p1, p0 + 1e-9,
                                     f"{label}: {k1} costs {p1:.4f}, more than {k0} at {p0:.4f}")
            else:
                self.assertGreaterEqual(p1, p0 - 1e-9,
                                        f"{label}: {k1} costs {p1:.4f}, less than {k0} at {p0:.4f}")

    def generated_chain(self, venue, spot, chain_spec, session_close, life, sessions):
        """Run the generator over a flat underlying and return ``{(type, strike): price}``."""
        import asyncio

        from ziplime.data.data_sources.options.synthetic import SyntheticOptionChainSource

        closes = {day: datetime.datetime.combine(day, session_close, tzinfo=TZ) for day in sessions}
        rows = [{"date": closes[day], "open": spot, "high": spot * 1.01,
                 "low": spot * 0.99, "close": spot} for day in sessions]
        source = SyntheticOptionChainSource(
            underlying_symbol="X", mic="M", underlying_bars=pl.DataFrame(rows),
            session_closes=closes, chain=chain_spec, venue=venue, root="SR",
            life_sessions=life)
        specs = asyncio.run(source.contracts("X", "M", [sessions[-1]]))
        frame = asyncio.run(source.bars(specs, pl.Series([closes[sessions[0]]])))
        by_symbol = {row["symbol"]: row["close"] for row in frame.to_dicts()}
        return {(spec.option_type, spec.strike): by_symbol[spec.listing_symbol]
                for spec in specs if spec.listing_symbol in by_symbol}

    def assert_chain_monotone(self, chain, label):
        for option_type, decreasing in ((OptionType.CALL, True), (OptionType.PUT, False)):
            side = sorted((strike, price) for (kind, strike), price in chain.items()
                          if kind is option_type)
            self.assert_monotone([k for k, _ in side], [p for _, p in side], decreasing, label)

    def test_a_generated_monthly_chain_on_a_wide_grid_is_arbitrage_free(self):
        """The case that was broken: spot at 316.71, five-unit strikes, a month to run."""
        from ziplime.data.data_sources.options.synthetic import ChainSpec

        sessions = [datetime.date(2026, 8, day) for day in
                    (3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 17, 18, 19)]
        chain = self.generated_chain(
            native_named_venue(), 316.71,
            ChainSpec(near_step=5.0, near_reach=40.0, far_step=10.0, far_reach=80.0),
            datetime.time(18, 45), life=12, sessions=sessions)
        self.assertGreater(len(chain), 20)
        self.assert_chain_monotone(chain, "wide monthly grid")

    def test_a_generated_zero_day_chain_is_arbitrage_free(self):
        """And the case that always worked must keep working."""
        from ziplime.data.data_sources.options.synthetic import ChainSpec
        from ziplime.data.data_sources.options.venues import OPRA

        chain = self.generated_chain(
            OPRA, 523.41, ChainSpec(near_step=1.0, near_reach=20.0, far_step=5.0, far_reach=60.0),
            datetime.time(16, 0), life=0, sessions=[EXPIRY])
        self.assert_chain_monotone(chain, "SPY 0DTE")

    def test_a_credit_spread_on_a_generated_chain_is_actually_a_credit(self):
        """The symptom that surfaced the bug: the short leg has to be worth more than the long."""
        from ziplime.data.data_sources.options.synthetic import ChainSpec

        sessions = [datetime.date(2026, 8, day) for day in
                    (3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 17, 18, 19)]
        chain = self.generated_chain(
            native_named_venue(), 316.71,
            ChainSpec(near_step=5.0, near_reach=40.0, far_step=10.0, far_reach=80.0),
            datetime.time(18, 45), life=12, sessions=sessions)
        self.assertGreater(chain[(OptionType.CALL, 320.0)], chain[(OptionType.CALL, 330.0)],
                           "or the 'credit' spread is a debit that can only lose")

    def test_the_smile_is_steeper_with_less_time_left(self):
        """Which is the whole reason one set of parameters can describe both maturities."""
        from ziplime.data.data_sources.options.surface import VolatilitySurface

        surface = VolatilitySurface()
        spread_at = lambda years: (
            surface.implied_volatility(0.25, 300.0, 285.0, years)
            - surface.implied_volatility(0.25, 300.0, 300.0, years))
        self.assertGreater(spread_at(1 / 365), spread_at(30 / 365))


if __name__ == "__main__":
    unittest.main(verbosity=2)
