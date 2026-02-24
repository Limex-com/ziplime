from ziplime.pipeline.common import SID_FIELD_NAME, EVENT_DATE_FIELD_NAME, SIMULATION_DATES
from ziplime.pipeline.loaders.earnings_estimates_loader import EarningsEstimatesLoader


class PreviousEarningsEstimatesLoader(EarningsEstimatesLoader):
    searchsorted_side = "left"

    def create_overwrite_for_estimate(
        self,
        column,
        column_name,
        dates,
        next_qtr_start_idx,
        requested_quarter,
        sid,
        sid_idx,
        col_to_split_adjustments=None,
        split_adjusted_asof_idx=None,
        split_dict=None,
    ):
        return [
            self.overwrite_with_null(
                column,
                next_qtr_start_idx,
                sid_idx,
            )
        ]

    def get_shifted_qtrs(self, zero_qtrs, num_announcements):
        return zero_qtrs - (num_announcements - 1)

    def get_zeroth_quarter_idx(self, stacked_last_per_qtr):
        """Filters for releases that are on or after each simulation date and
        determines the previous quarter by picking out the most recent
        release relative to each date in the index.

        Parameters
        ----------
        stacked_last_per_qtr : pd.DataFrame
            A DataFrame with index of calendar dates, sid, and normalized
            quarters with each row being the latest estimate for the row's
            index values, sorted by event date.

        Returns
        -------
        previous_releases_per_date_index : pd.MultiIndex
            An index of calendar dates, sid, and normalized quarters, for only
            the rows that have a previous event.
        """
        previous_releases_per_date = (
            stacked_last_per_qtr.loc[
                stacked_last_per_qtr[EVENT_DATE_FIELD_NAME]
                <= stacked_last_per_qtr.index.get_level_values(SIMULATION_DATES)
            ]
            .groupby(
                level=[SIMULATION_DATES, SID_FIELD_NAME],
                as_index=False,
                # Here we take advantage of the fact that `stacked_last_per_qtr` is
                # sorted by event date.
            )
            .nth(-1)
        )
        return previous_releases_per_date.index
