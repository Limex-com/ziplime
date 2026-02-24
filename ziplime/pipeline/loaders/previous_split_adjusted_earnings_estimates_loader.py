from ziplime.pipeline.loaders.previous_earnings_estimates_loader import PreviousEarningsEstimatesLoader
from ziplime.pipeline.loaders.split_adjusted_estimates_loader import SplitAdjustedEstimatesLoader


class PreviousSplitAdjustedEarningsEstimatesLoader(
    SplitAdjustedEstimatesLoader, PreviousEarningsEstimatesLoader
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
        """Collect split adjustments for previous quarters and apply them to the
        given dictionary of splits for the given sid. Since overwrites just
        replace all estimates before the new quarter with NaN, we don't need to
        worry about re-applying split adjustments.

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
        self.merge_split_adjustments_with_overwrites(
            pre_adjustments_dict,
            post_adjustments_dict,
            adjustments_for_sid,
            requested_split_adjusted_columns,
        )
