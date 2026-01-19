from abc import abstractmethod

import numpy as np
import pandas as pd
from ziplime.lib.adjustment import (
    Float64Multiply,
)

from ziplime.pipeline.common import (
    SID_FIELD_NAME,
    TS_FIELD_NAME,
    NORMALIZED_QUARTERS, SHIFTED_NORMALIZED_QTRS,
)
from ziplime.pipeline.loaders.earnings_estimates_loader import EarningsEstimatesLoader
from ziplime.pipeline.loaders.utils import validate_split_adjusted_column_specs, add_new_adjustments


class SplitAdjustedEstimatesLoader(EarningsEstimatesLoader):
    """Estimates loader that loads data that needs to be split-adjusted.

    Parameters
    ----------
    split_adjustments_loader : SQLiteAdjustmentReader
        The loader to use for reading split adjustments.
    split_adjusted_column_names : iterable of str
        The column names that should be split-adjusted.
    split_adjusted_asof : pd.Timestamp
        The date that separates data into 2 halves: the first half is the set
        of dates up to and including the split_adjusted_asof date. All
        adjustments occurring during this first half are applied  to all
        dates in this first half. The second half is the set of dates after
        the split_adjusted_asof date. All adjustments occurring during this
        second half are applied sequentially as they appear in the timeline.
    """

    def __init__(
        self,
        estimates,
        name_map,
        split_adjustments_loader,
        split_adjusted_column_names,
        split_adjusted_asof,
    ):
        validate_split_adjusted_column_specs(name_map, split_adjusted_column_names)
        self._split_adjustments = split_adjustments_loader
        self._split_adjusted_column_names = split_adjusted_column_names
        self._split_adjusted_asof = split_adjusted_asof
        self._split_adjustment_dict = {}
        super(SplitAdjustedEstimatesLoader, self).__init__(estimates, name_map)

    @abstractmethod
    def collect_split_adjustments(
        self,
        adjustments_for_sid,
        requested_qtr_data,
        dates,
        sid,
        sid_idx,
        sid_estimates,
        split_adjusted_asof_idx,
        pre_adjustments,
        post_adjustments,
        requested_split_adjusted_columns,
    ):
        raise NotImplementedError("collect_split_adjustments")

    def get_adjustments_for_sid(
        self,
        group,
        dates,
        requested_qtr_data,
        last_per_qtr,
        sid_to_idx,
        columns,
        col_to_all_adjustments,
        split_adjusted_asof_idx=None,
        split_adjusted_cols_for_group=None,
    ):
        """Collects both overwrites and adjustments for a particular sid.

        Parameters
        ----------
        split_adjusted_asof_idx : int
            The integer index of the date on which the data was split-adjusted.
        split_adjusted_cols_for_group : list of str
            The names of requested columns that should also be split-adjusted.
        """
        all_adjustments_for_sid = {}
        sid = int(group.name)
        self.collect_overwrites_for_sid(
            group,
            dates,
            requested_qtr_data,
            last_per_qtr,
            sid_to_idx[sid],
            columns,
            all_adjustments_for_sid,
            sid,
        )
        (
            pre_adjustments,
            post_adjustments,
        ) = self.retrieve_split_adjustment_data_for_sid(
            dates, sid, split_adjusted_asof_idx
        )
        sid_estimates = self.estimates[self.estimates[SID_FIELD_NAME] == sid]
        # We might not have any overwrites but still have
        # adjustments, and we will need to manually add columns if
        # that is the case.
        for col_name in split_adjusted_cols_for_group:
            if col_name not in all_adjustments_for_sid:
                all_adjustments_for_sid[col_name] = {}

        self.collect_split_adjustments(
            all_adjustments_for_sid,
            requested_qtr_data,
            dates,
            sid,
            sid_to_idx[sid],
            sid_estimates,
            split_adjusted_asof_idx,
            pre_adjustments,
            post_adjustments,
            split_adjusted_cols_for_group,
        )
        self.merge_into_adjustments_for_all_sids(
            all_adjustments_for_sid, col_to_all_adjustments
        )

    def get_adjustments(
        self,
        zero_qtr_data,
        requested_qtr_data,
        last_per_qtr,
        dates,
        assets,
        columns,
        **kwargs,
    ):
        """Calculates both split adjustments and overwrites for all sids."""
        split_adjusted_cols_for_group = [
            self.name_map[col.name]
            for col in columns
            if self.name_map[col.name] in self._split_adjusted_column_names
        ]
        # Add all splits to the adjustment dict for this sid.
        split_adjusted_asof_idx = self.get_split_adjusted_asof_idx(dates)
        return super(SplitAdjustedEstimatesLoader, self).get_adjustments(
            zero_qtr_data,
            requested_qtr_data,
            last_per_qtr,
            dates,
            assets,
            columns,
            split_adjusted_cols_for_group=split_adjusted_cols_for_group,
            split_adjusted_asof_idx=split_adjusted_asof_idx,
        )

    def determine_end_idx_for_adjustment(
        self, adjustment_ts, dates, upper_bound, requested_quarter, sid_estimates
    ):
        """Determines the date until which the adjustment at the given date
        index should be applied for the given quarter.

        Parameters
        ----------
        adjustment_ts : pd.Timestamp
            The timestamp at which the adjustment occurs.
        dates : pd.DatetimeIndex
            The calendar dates over which the Pipeline is being computed.
        upper_bound : int
            The index of the upper bound in the calendar dates. This is the
            index until which the adjusment will be applied unless there is
            information for the requested quarter that comes in on or before
            that date.
        requested_quarter : float
            The quarter for which we are determining how the adjustment
            should be applied.
        sid_estimates : pd.DataFrame
            The DataFrame of estimates data for the sid for which we're
            applying the given adjustment.

        Returns
        -------
        end_idx : int
            The last index to which the adjustment should be applied for the
            given quarter/sid.
        """
        end_idx = upper_bound
        # Find the next newest kd that happens on or after
        # the date of this adjustment
        newest_kd_for_qtr = sid_estimates[
            (sid_estimates[NORMALIZED_QUARTERS] == requested_quarter)
            & (sid_estimates[TS_FIELD_NAME] >= adjustment_ts)
        ][TS_FIELD_NAME].min()
        if pd.notnull(newest_kd_for_qtr):
            newest_kd_idx = dates.searchsorted(newest_kd_for_qtr)
            # make_utc_aware(pd.DatetimeIndex(newest_kd_for_qtr))
            # We have fresh information that comes in
            # before the end of the overwrite and
            # presumably is already split-adjusted to the
            # current split. We should stop applying the
            # adjustment the day before this new
            # information comes in.
            if newest_kd_idx <= upper_bound:
                end_idx = newest_kd_idx - 1
        return end_idx

    def collect_pre_split_asof_date_adjustments(
        self,
        split_adjusted_asof_date_idx,
        sid_idx,
        pre_adjustments,
        requested_split_adjusted_columns,
    ):
        """Collect split adjustments that occur before the
        split-adjusted-asof-date. All those adjustments must first be
        UN-applied at the first date index and then re-applied on the
        appropriate dates in order to match point in time share pricing data.

        Parameters
        ----------
        split_adjusted_asof_date_idx : int
            The index in the calendar dates as-of which all data was
            split-adjusted.
        sid_idx : int
            The index of the sid for which adjustments should be collected in
            the adjusted array.
        pre_adjustments : tuple(list(float), list(int))
            The adjustment values, indexes in `dates`, and timestamps for
            adjustments that happened after the split-asof-date.
        requested_split_adjusted_columns : list of str
            The requested split adjusted columns.

        Returns
        -------
        col_to_split_adjustments : dict[str -> dict[int -> list of Adjustment]]
            The adjustments for this sid that occurred on or before the
            split-asof-date.
        """
        col_to_split_adjustments = {}
        if len(pre_adjustments[0]):
            adjustment_values, date_indexes = pre_adjustments
            for column_name in requested_split_adjusted_columns:
                col_to_split_adjustments[column_name] = {}
                # We need to undo all adjustments that happen before the
                # split_asof_date here by reversing the split ratio.
                col_to_split_adjustments[column_name][0] = [
                    Float64Multiply(
                        0,
                        split_adjusted_asof_date_idx,
                        sid_idx,
                        sid_idx,
                        1 / future_adjustment,
                    )
                    for future_adjustment in adjustment_values
                ]

                for adjustment, date_index in zip(adjustment_values, date_indexes):
                    adj = Float64Multiply(
                        0, split_adjusted_asof_date_idx, sid_idx, sid_idx, adjustment
                    )
                    add_new_adjustments(
                        col_to_split_adjustments, [adj], column_name, date_index
                    )

        return col_to_split_adjustments

    def collect_post_asof_split_adjustments(
        self,
        post_adjustments,
        requested_qtr_data,
        sid,
        sid_idx,
        sid_estimates,
        requested_split_adjusted_columns,
    ):
        """Collect split adjustments that occur after the
        split-adjusted-asof-date. Each adjustment needs to be applied to all
        dates on which knowledge for the requested quarter was older than the
        date of the adjustment.

        Parameters
        ----------
        post_adjustments : tuple(list(float), list(int), pd.DatetimeIndex)
            The adjustment values, indexes in `dates`, and timestamps for
            adjustments that happened after the split-asof-date.
        requested_qtr_data : pd.DataFrame
            The requested quarter data for each calendar date per sid.
        sid : int
            The sid for which adjustments need to be collected.
        sid_idx : int
            The index of `sid` in the adjusted array.
        sid_estimates : pd.DataFrame
            The raw estimates data for this sid.
        requested_split_adjusted_columns : list of str
            The requested split adjusted columns.
        Returns
        -------
        col_to_split_adjustments : dict[str -> dict[int -> list of Adjustment]]
            The adjustments for this sid that occurred after the
            split-asof-date.
        """
        col_to_split_adjustments = {}
        if post_adjustments:
            # Get an integer index
            requested_qtr_timeline = requested_qtr_data[SHIFTED_NORMALIZED_QTRS][
                sid
            ].reset_index()
            requested_qtr_timeline = requested_qtr_timeline[
                requested_qtr_timeline[sid].notnull()
            ]

            # Split the data into range by quarter and determine which quarter
            # was being requested in each range.
            # Split integer indexes up by quarter range
            qtr_ranges_idxs = np.split(
                requested_qtr_timeline.index,
                np.where(np.diff(requested_qtr_timeline[sid]) != 0)[0] + 1,
            )
            requested_quarters_per_range = [
                requested_qtr_timeline[sid][r[0]] for r in qtr_ranges_idxs
            ]
            # Try to apply each adjustment to each quarter range.
            for i, qtr_range in enumerate(qtr_ranges_idxs):
                for adjustment, date_index, timestamp in zip(*post_adjustments):
                    # In the default case, apply through the end of the quarter
                    upper_bound = qtr_range[-1]
                    # Find the smallest KD in estimates that is on or after the
                    # date of the given adjustment. Apply the given adjustment
                    # until that KD.
                    end_idx = self.determine_end_idx_for_adjustment(
                        timestamp,
                        requested_qtr_data.index,
                        upper_bound,
                        requested_quarters_per_range[i],
                        sid_estimates,
                    )
                    # In the default case, apply adjustment on the first day of
                    #  the quarter.
                    start_idx = qtr_range[0]
                    # If the adjustment happens during this quarter, apply the
                    # adjustment on the day it happens.
                    if date_index > start_idx:
                        start_idx = date_index
                    # We only want to apply the adjustment if we have any stale
                    # data to apply it to.
                    if qtr_range[0] <= end_idx:
                        for column_name in requested_split_adjusted_columns:
                            if column_name not in col_to_split_adjustments:
                                col_to_split_adjustments[column_name] = {}
                            adj = Float64Multiply(
                                # Always apply from first day of qtr
                                qtr_range[0],
                                end_idx,
                                sid_idx,
                                sid_idx,
                                adjustment,
                            )
                            add_new_adjustments(
                                col_to_split_adjustments, [adj], column_name, start_idx
                            )

        return col_to_split_adjustments

    def retrieve_split_adjustment_data_for_sid(
        self, dates, sid, split_adjusted_asof_idx
    ):
        """

        dates : pd.DatetimeIndex
            The calendar dates.
        sid : int
            The sid for which we want to retrieve adjustments.
        split_adjusted_asof_idx : int
            The index in `dates` as-of which the data is split adjusted.

        Returns
        -------
        pre_adjustments : tuple(list(float), list(int), pd.DatetimeIndex)
            The adjustment values and indexes in `dates` for
            adjustments that happened before the split-asof-date.
        post_adjustments : tuple(list(float), list(int), pd.DatetimeIndex)
            The adjustment values, indexes in `dates`, and timestamps for
            adjustments that happened after the split-asof-date.
        """
        adjustments = self._split_adjustments.get_adjustments_for_sid("splits", sid)
        sorted(adjustments, key=lambda adj: adj[0])
        # Get rid of any adjustments that happen outside of our date index.
        adjustments = list(filter(lambda x: dates[0] <= x[0] <= dates[-1], adjustments))
        adjustment_values = np.array([adj[1] for adj in adjustments])
        timestamps = pd.DatetimeIndex([adj[0] for adj in adjustments])
        # We need the first date on which we would have known about each
        # adjustment.
        date_indexes = dates.searchsorted(timestamps)
        pre_adjustment_idxs = np.where(date_indexes <= split_adjusted_asof_idx)[0]
        last_adjustment_split_asof_idx = -1
        if len(pre_adjustment_idxs):
            last_adjustment_split_asof_idx = pre_adjustment_idxs.max()
        pre_adjustments = (
            adjustment_values[: last_adjustment_split_asof_idx + 1],
            date_indexes[: last_adjustment_split_asof_idx + 1],
        )
        post_adjustments = (
            adjustment_values[last_adjustment_split_asof_idx + 1 :],
            date_indexes[last_adjustment_split_asof_idx + 1 :],
            timestamps[last_adjustment_split_asof_idx + 1 :],
        )
        return pre_adjustments, post_adjustments

    def _collect_adjustments(
        self,
        requested_qtr_data,
        sid,
        sid_idx,
        sid_estimates,
        split_adjusted_asof_idx,
        pre_adjustments,
        post_adjustments,
        requested_split_adjusted_columns,
    ):
        pre_adjustments_dict = self.collect_pre_split_asof_date_adjustments(
            split_adjusted_asof_idx,
            sid_idx,
            pre_adjustments,
            requested_split_adjusted_columns,
        )

        post_adjustments_dict = self.collect_post_asof_split_adjustments(
            post_adjustments,
            requested_qtr_data,
            sid,
            sid_idx,
            sid_estimates,
            requested_split_adjusted_columns,
        )
        return pre_adjustments_dict, post_adjustments_dict

    def merge_split_adjustments_with_overwrites(
        self, pre, post, overwrites, requested_split_adjusted_columns
    ):
        """Merge split adjustments with the dict containing overwrites.

        Parameters
        ----------
        pre : dict[str -> dict[int -> list]]
            The adjustments that occur before the split-adjusted-asof-date.
        post : dict[str -> dict[int -> list]]
            The adjustments that occur after the split-adjusted-asof-date.
        overwrites : dict[str -> dict[int -> list]]
            The overwrites across all time. Adjustments will be merged into
            this dictionary.
        requested_split_adjusted_columns : list of str
            List of names of split adjusted columns that are being requested.
        """
        for column_name in requested_split_adjusted_columns:
            # We can do a merge here because the timestamps in 'pre' and
            # 'post' are guaranteed to not overlap.
            if pre:
                # Either empty or contains all columns.
                for ts in pre[column_name]:
                    add_new_adjustments(
                        overwrites, pre[column_name][ts], column_name, ts
                    )
            if post:
                # Either empty or contains all columns.
                for ts in post[column_name]:
                    add_new_adjustments(
                        overwrites, post[column_name][ts], column_name, ts
                    )

