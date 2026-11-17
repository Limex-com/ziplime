import pandas as pd

from ziplime.pipeline.common import (
    EVENT_DATE_FIELD_NAME,
    SID_FIELD_NAME, SIMULATION_DATES,
)
from ziplime.pipeline.loaders.earnings_estimates_loader import EarningsEstimatesLoader
from ziplime.pipeline.terms.bound_column import BoundColumn


class NextEarningsEstimatesLoader(EarningsEstimatesLoader):
    searchsorted_side = "right"

    def create_overwrite_for_estimate(
        self,
        column: BoundColumn,
        column_name: str,
        last_per_qtr: pd.DataFrame,
        next_qtr_start_idx: int,
        requested_quarter: float,
        sid: int,
        sid_idx: int,
        col_to_split_adjustments=None,
        split_adjusted_asof_idx: int | None = None,
    ):
        return [
            self.array_overwrites_dict[column.dtype](
                0,
                next_qtr_start_idx - 1,
                sid_idx,
                sid_idx,
                last_per_qtr[
                    column_name,
                    requested_quarter,
                    sid,
                ].values[:next_qtr_start_idx],
            )
        ]

    def get_shifted_qtrs(self, zero_qtrs: pd.Index, num_announcements: int):
        return zero_qtrs + (num_announcements - 1)

    def get_zeroth_quarter_idx(self, stacked_last_per_qtr: pd.DataFrame):
        """Filters for releases that are on or after each simulation date and
        determines the next quarter by picking out the upcoming release for
        each date in the index.

        Parameters
        ----------
        stacked_last_per_qtr : pd.DataFrame
            A DataFrame with index of calendar dates, sid, and normalized
            quarters with each row being the latest estimate for the row's
            index values, sorted by event date.

        Returns
        -------
        next_releases_per_date_index : pd.MultiIndex
            An index of calendar dates, sid, and normalized quarters, for only
            the rows that have a next event.
        """
        next_releases_per_date = (
            stacked_last_per_qtr.loc[
                stacked_last_per_qtr[EVENT_DATE_FIELD_NAME]
                >= stacked_last_per_qtr.index.get_level_values(SIMULATION_DATES)
            ]
            .groupby(
                level=[SIMULATION_DATES, SID_FIELD_NAME],
                as_index=False,
                # Here we take advantage of the fact that `stacked_last_per_qtr` is
                # sorted by event date.
            )
            .nth(0)
        )
        return next_releases_per_date.index
