#
# Copyright 2015 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime
from copy import copy

import pandas as pd
import structlog

from ziplime.assets.domain.asset import Asset
from ziplime.data.data_portal import DataPortal
from ziplime.domain.data_frequency import DataFrequency
from ziplime.finance.domain.order_status import OrderStatus
from ziplime.finance.domain.simulation_paremeters import SimulationParameters
from ziplime.finance.metrics import MetricsTracker
from ziplime.domain.bar_data import BarData
from zipline.utils.api_support import ZiplineAPI
from zipline.utils.compat import ExitStack

from ziplime.gens.sim_engine import (
    BAR,
    SESSION_START,
    SESSION_END,
    EMISSION_RATE_END,
    BEFORE_TRADING_START_BAR,
)


class AlgorithmSimulator:
    EMISSION_TO_PERF_KEY_MAP = {"minute": "minute_perf", "daily": "daily_perf"}

    def __init__(
            self,
            algo,
            sim_params: SimulationParameters,
            data_portal: DataPortal,
            clock,
            benchmark_source,
            restrictions,
    ):

        # ==============
        # Simulation
        # Param Setup
        # ==============
        self.sim_params = sim_params
        self.data_portal = data_portal
        self.restrictions = restrictions

        # ==============
        # Algo Setup
        # ==============
        self.algo = algo

        self._logger = structlog.get_logger(__name__)

        # ==============
        # Snapshot Setup
        # ==============

        # This object is the way that user algorithms interact with OHLCV data,
        # fetcher data, and some API methods like `data.can_trade`.
        self.current_data = BarData(
            data_portal=self.data_portal,
            simulation_dt_func=self.get_simulation_dt,
            data_frequency=self.sim_params.data_frequency,
            trading_calendar=self.algo.trading_calendar,
            restrictions=self.restrictions,
        )

        # We don't have a datetime for the current snapshot until we
        # receive a message.
        self.simulation_dt = None

        self.clock = clock

        self.benchmark_source = benchmark_source

        # =============
        # Logging Setup
        # =============

        # Processor function for injecting the algo_dt into
        # user prints/logs.

        # TODO CHECK: Disabled the old logbook mechanism,
        # didn't replace with an equivalent `logging` approach.

    def get_simulation_dt(self) -> pd.Timestamp:
        return self.simulation_dt

    # TODO: simplify
    # flake8: noqa: C901
    def transform(self):
        """
        Main generator work loop.
        """

        emission_rate = self.algo.metrics_tracker.emission_rate

        def every_bar(
                dt_to_use: pd.Timestamp,
                current_data: BarData,
                handle_data,
        ):
            # print(f"dt_to_use: in every_bar: {dt_to_use}")
            for capital_change in calculate_minute_capital_changes(dt_to_use):
                yield capital_change

            self.simulation_dt = dt_to_use
            # called every tick (minute or day).
            self.algo.on_dt_changed(dt=dt_to_use)

            # handle any transactions and commissions coming out new orders
            # placed in the last bar

            (
                new_transactions,
                new_commissions,
                closed_orders,
            ) = self.algo.blotter.get_transactions(bar_data=current_data)
            # print(f"getting transactions for {current_data.current_dt}, new transactions: {len(new_transactions)}, new commissions: {len(new_commissions)}, closed orders: {len(closed_orders)}" )
            self.algo.blotter.prune_orders(closed_orders=closed_orders)

            for transaction in new_transactions:
                self.algo.metrics_tracker.process_transaction(transaction=transaction)

                # since this order was modified, record it
                order = self.algo.blotter.orders[transaction.order_id]
                self.algo.metrics_tracker.process_order(order=order)

            for commission in new_commissions:
                self.algo.metrics_tracker.process_commission(commission=commission)

            handle_data(context=self.algo, data=current_data, dt=dt_to_use)

            # grab any new orders from the blotter, then clear the list.
            # this includes cancelled orders.
            new_orders = self.algo.blotter.new_orders
            self.algo.blotter.new_orders = []

            # if we have any new orders, record them so that we know
            # in what perf period they were placed.
            for new_order in new_orders:
                self.algo.metrics_tracker.process_order(new_order)

        def once_a_day(
                midnight_dt,
                current_data=self.current_data,
                data_portal=self.data_portal,
        ):
            # process any capital changes that came overnight
            for capital_change in self.algo.calculate_capital_changes(
                    midnight_dt, emission_rate=emission_rate, is_interday=True
            ):
                yield capital_change

            # set all the timestamps
            self.simulation_dt = midnight_dt
            self.algo.on_dt_changed(midnight_dt)

            self.algo.metrics_tracker.handle_market_open(
                session_label=midnight_dt,
                data_portal=self.data_portal,
            )

            # handle any splits that impact any positions or any open orders.
            assets_we_care_about = (
                    self.algo.metrics_tracker.positions.keys() | self.algo.blotter.open_orders.keys()
            )

            if assets_we_care_about:
                splits = data_portal.get_splits(assets_we_care_about, midnight_dt)
                if splits:
                    self.algo.blotter.process_splits(splits)
                    self.algo.metrics_tracker.handle_splits(splits)

        def on_exit():
            # Remove references to algo, data portal, et al to break cycles
            # and ensure deterministic cleanup of these objects when the
            # simulation finishes.
            self.algo = None
            self.benchmark_source = self.current_data = self.data_portal = None

        with ExitStack() as stack:
            stack.callback(on_exit)
            stack.enter_context(ZiplineAPI(algo_instance=self.algo))

            if self.algo.data_frequency < datetime.timedelta(days=1):

                def execute_order_cancellation_policy():
                    self.algo.blotter.execute_cancel_policy(SESSION_END)

                def calculate_minute_capital_changes(dt: pd.Timestamp):
                    # process any capital changes that came between the last
                    # and current minutes
                    return self.algo.calculate_capital_changes(
                        dt, emission_rate=emission_rate, is_interday=False
                    )

            elif self.algo.data_frequency == datetime.timedelta(days=1):

                def execute_order_cancellation_policy():
                    self.algo.blotter.execute_daily_cancel_policy(SESSION_END)

                def calculate_minute_capital_changes(dt: pd.Timestamp):
                    return []

            else:

                def execute_order_cancellation_policy():
                    pass

                def calculate_minute_capital_changes(dt: pd.Timestamp):
                    return []

            for dt, action in self.clock:
                if action == BAR:
                    for capital_change_packet in every_bar(dt_to_use=dt, current_data=self.current_data,
                                                           handle_data=self.algo.event_manager.handle_data):
                        yield capital_change_packet
                elif action == SESSION_START:
                    for capital_change_packet in once_a_day(midnight_dt=dt):
                        yield capital_change_packet
                elif action == SESSION_END:
                    # End of the session.
                    positions = self.algo.metrics_tracker.positions
                    position_assets = self.algo.data_portal.asset_repository.retrieve_all(sids=[a.sid for a in positions])
                    self._cleanup_expired_assets(dt=dt, position_assets=position_assets)

                    execute_order_cancellation_policy()
                    self.algo.validate_account_controls()

                    yield self._get_daily_message(dt=dt)
                elif action == BEFORE_TRADING_START_BAR:
                    self.simulation_dt = dt
                    self.algo.on_dt_changed(dt=dt)
                    self.algo.before_trading_start(data=self.current_data)
                elif action == EMISSION_RATE_END:
                    minute_msg = self._get_minute_message(
                        dt=dt,
                    )

                    yield minute_msg

            risk_message = self.algo.metrics_tracker.handle_simulation_end()
            yield risk_message

    def _cleanup_expired_assets(self, dt: pd.Timestamp, position_assets):
        """
        Clear out any assets that have expired before starting a new sim day.

        Performs two functions:

        1. Finds all assets for which we have open orders and clears any
           orders whose assets are on or after their auto_close_date.

        2. Finds all assets for which we have positions and generates
           close_position events for any assets that have reached their
           auto_close_date.
        """
        algo = self.algo

        def past_auto_close_date(asset: Asset):
            acd = asset.auto_close_date
            if acd is not None:
                acd = acd
            return acd is not None and acd <= dt.date()

        # Remove positions in any sids that have reached their auto_close date.
        assets_to_clear = [
            asset
            for asset in position_assets
            if past_auto_close_date(asset)
        ]
        metrics_tracker = algo.metrics_tracker
        data_portal = self.data_portal
        for asset in assets_to_clear:
            metrics_tracker.process_close_position(asset=asset, dt=dt)

        # Remove open orders for any sids that have reached their auto close
        # date. These orders get processed immediately because otherwise they
        # would not be processed until the first bar of the next day.
        blotter = algo.blotter

        assets_to_cancel = [
            asset
            for asset in blotter.open_orders
            if past_auto_close_date(asset=asset)
        ]

        for asset in assets_to_cancel:
            blotter.cancel_all_orders_for_asset(asset=asset)

        # Make a copy here so that we are not modifying the list that is being
        # iterated over.
        for order in copy(blotter.new_orders):
            if order.status == OrderStatus.CANCELLED:
                metrics_tracker.process_order(order=order)
                blotter.new_orders.remove(order=order)

    def _get_daily_message(self, dt: pd.Timestamp):
        """
        Get a perf message for the given datetime.
        """
        perf_message = self.algo.metrics_tracker.handle_market_close(
            dt=dt,
            data_portal=self.data_portal,
        )
        perf_message["daily_perf"]["recorded_vars"] = self.algo.recorded_vars
        return perf_message

    def _get_minute_message(self, dt: pd.Timestamp):
        """
        Get a perf message for the given datetime.
        """
        rvars = self.algo.recorded_vars

        minute_message = self.algo.metrics_tracker.handle_minute_close(
            dt=dt,
        )

        minute_message["minute_perf"]["recorded_vars"] = rvars
        return minute_message
