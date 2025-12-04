from ziplime.lib.adjustment import Float64Multiply
from ziplime.pipeline.common import SHIFTED_NORMALIZED_QTRS
from ziplime.pipeline.loaders.next_earnings_estimates_loader import NextEarningsEstimatesLoader
from ziplime.pipeline.loaders.split_adjusted_estimates_loader import SplitAdjustedEstimatesLoader


class NextSplitAdjustedEarningsEstimatesLoader(
    SplitAdjustedEstimatesLoader, NextEarningsEstimatesLoader
):
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
        """Collect split adjustments for future quarters. Re-apply adjustments
        that would be overwritten by overwrites. Merge split adjustments with
        overwrites into the given dictionary of splits for the given sid.

        Parameters
        ----------
        adjustments_for_sid : dict[str -> dict[int -> list]]
            The dictionary of adjustments to which splits need to be added.
            Initially it contains only overwrites.
        requested_qtr_data : pd.DataFrame
            The requested quarter data for each calendar date per sid.
        dates : pd.DatetimeIndex
            The calendar dates for which estimates data is requested.
        sid : int
            The sid for which adjustments need to be collected.
        sid_idx : int
            The index of `sid` in the adjusted array.
        sid_estimates : pd.DataFrame
            The raw estimates data for the given sid.
        split_adjusted_asof_idx : int
            The index in `dates` as-of which the data is split adjusted.
        pre_adjustments : tuple(list(float), list(int), pd.DatetimeIndex)
            The adjustment values and indexes in `dates` for
            adjustments that happened before the split-asof-date.
        post_adjustments : tuple(list(float), list(int), pd.DatetimeIndex)
            The adjustment values, indexes in `dates`, and timestamps for
            adjustments that happened after the split-asof-date.
        requested_split_adjusted_columns : list of str
            List of requested split adjusted column names.
        """
        (pre_adjustments_dict, post_adjustments_dict) = self._collect_adjustments(
            requested_qtr_data,
            sid,
            sid_idx,
            sid_estimates,
            split_adjusted_asof_idx,
            pre_adjustments,
            post_adjustments,
            requested_split_adjusted_columns,
        )
        for column_name in requested_split_adjusted_columns:
            for overwrite_ts in adjustments_for_sid[column_name]:
                # We need to cumulatively re-apply all adjustments up to the
                # split-adjusted-asof-date. We might not have any
                # pre-adjustments, so we should check for that.
                if overwrite_ts <= split_adjusted_asof_idx and pre_adjustments_dict:
                    for split_ts in pre_adjustments_dict[column_name]:
                        # The split has to have occurred during the span of
                        # the overwrite.
                        if split_ts < overwrite_ts:
                            # Create new adjustments here so that we can
                            # re-apply all applicable adjustments to ONLY
                            # the dates being overwritten.
                            adjustments_for_sid[column_name][overwrite_ts].extend(
                                [
                                    Float64Multiply(
                                        0,
                                        overwrite_ts - 1,
                                        sid_idx,
                                        sid_idx,
                                        adjustment.value,
                                    )
                                    for adjustment in pre_adjustments_dict[column_name][
                                        split_ts
                                    ]
                                ]
                            )
                # After the split-adjusted-asof-date, we need to re-apply all
                # adjustments that occur after that date and within the
                # bounds of the overwrite. They need to be applied starting
                # from the first date and until an end date. The end date is
                # the date of the newest information we get about
                # `requested_quarter` that is >= `split_ts`, or if there is no
                # new knowledge before `overwrite_ts`, then it is the date
                # before `overwrite_ts`.
                else:
                    # Overwrites happen at the first index of a new quarter,
                    # so determine here which quarter that is.
                    requested_quarter = requested_qtr_data[
                        SHIFTED_NORMALIZED_QTRS, sid
                    ].iloc[overwrite_ts]

                    for adjustment_value, date_index, timestamp in zip(
                        *post_adjustments
                    ):
                        if split_adjusted_asof_idx < date_index < overwrite_ts:
                            # Assume the entire overwrite contains stale data
                            upper_bound = overwrite_ts - 1
                            end_idx = self.determine_end_idx_for_adjustment(
                                timestamp,
                                dates,
                                upper_bound,
                                requested_quarter,
                                sid_estimates,
                            )
                            adjustments_for_sid[column_name][overwrite_ts].append(
                                Float64Multiply(
                                    0, end_idx, sid_idx, sid_idx, adjustment_value
                                )
                            )

        self.merge_split_adjustments_with_overwrites(
            pre_adjustments_dict,
            post_adjustments_dict,
            adjustments_for_sid,
            requested_split_adjusted_columns,
        )
