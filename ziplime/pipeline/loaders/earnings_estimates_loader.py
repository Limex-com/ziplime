from abc import abstractmethod

import pandas as pd
from toolz import groupby

from ziplime.lib.adjusted_array import AdjustedArray
from ziplime.lib.adjustment import (
    Datetime641DArrayOverwrite,
    Datetime64Overwrite,
    Float641DArrayOverwrite,
    Float64Overwrite,
)

from ziplime.pipeline.common import (
    EVENT_DATE_FIELD_NAME,
    FISCAL_QUARTER_FIELD_NAME,
    FISCAL_YEAR_FIELD_NAME,
    SID_FIELD_NAME, NORMALIZED_QUARTERS, SHIFTED_NORMALIZED_QTRS, INVALID_NUM_QTRS_MESSAGE, SIMULATION_DATES,
)
from ziplime.pipeline.loaders.pipeline_loader import PipelineLoader
from ziplime.utils.numpy_utils import datetime64ns_dtype, float64_dtype
from ziplime.pipeline.loaders.utils import (
    ffill_across_cols,
    last_in_date_group, normalize_quarters, validate_column_specs, split_normalized_quarters, add_new_adjustments,
)



class EarningsEstimatesLoader(PipelineLoader):
    """An abstract pipeline loader for estimates data that can load data a
    variable number of quarters forwards/backwards from calendar dates
    depending on the `num_announcements` attribute of the columns' dataset.
    If split adjustments are to be applied, a loader, split-adjusted columns,
    and the split-adjusted asof-date must be supplied.

    Parameters
    ----------
    estimates : pd.DataFrame
        The raw estimates data; must contain at least 5 columns:
            sid : int64
                The asset id associated with each estimate.

            event_date : datetime64[ns]
                The date on which the event that the estimate is for will/has
                occurred.

            timestamp : datetime64[ns]
                The datetime where we learned about the estimate.

            fiscal_quarter : int64
                The quarter during which the event has/will occur.

            fiscal_year : int64
                The year during which the event has/will occur.

    name_map : dict[str -> str]
        A map of names of BoundColumns that this loader will load to the
        names of the corresponding columns in `events`.
    """

    def __init__(self, estimates, name_map):
        validate_column_specs(estimates, name_map)

        self.estimates = estimates[
            estimates[EVENT_DATE_FIELD_NAME].notnull()
            & estimates[FISCAL_QUARTER_FIELD_NAME].notnull()
            & estimates[FISCAL_YEAR_FIELD_NAME].notnull()
        ]
        self.estimates[NORMALIZED_QUARTERS] = normalize_quarters(
            self.estimates[FISCAL_YEAR_FIELD_NAME],
            self.estimates[FISCAL_QUARTER_FIELD_NAME],
        )

        self.array_overwrites_dict = {
            datetime64ns_dtype: Datetime641DArrayOverwrite,
            float64_dtype: Float641DArrayOverwrite,
        }
        self.scalar_overwrites_dict = {
            datetime64ns_dtype: Datetime64Overwrite,
            float64_dtype: Float64Overwrite,
        }

        self.name_map = name_map

    @abstractmethod
    def get_zeroth_quarter_idx(self, stacked_last_per_qtr):
        raise NotImplementedError("get_zeroth_quarter_idx")

    @abstractmethod
    def get_shifted_qtrs(self, zero_qtrs, num_announcements):
        raise NotImplementedError("get_shifted_qtrs")

    @abstractmethod
    def create_overwrite_for_estimate(
        self,
        column,
        column_name,
        last_per_qtr,
        next_qtr_start_idx,
        requested_quarter,
        sid,
        sid_idx,
        col_to_split_adjustments,
        split_adjusted_asof_idx,
    ):
        raise NotImplementedError("create_overwrite_for_estimate")

    @property
    @abstractmethod
    def searchsorted_side(self):
        return NotImplementedError("searchsorted_side")

    def get_requested_quarter_data(
        self,
        zero_qtr_data,
        zeroth_quarter_idx,
        stacked_last_per_qtr,
        num_announcements,
        dates,
    ):
        """Selects the requested data for each date.

        Parameters
        ----------
        zero_qtr_data : pd.DataFrame
            The 'time zero' data for each calendar date per sid.
        zeroth_quarter_idx : pd.Index
            An index of calendar dates, sid, and normalized quarters, for only
            the rows that have a next or previous earnings estimate.
        stacked_last_per_qtr : pd.DataFrame
            The latest estimate known with the dates, normalized quarter, and
            sid as the index.
        num_announcements : int
            The number of annoucements out the user requested relative to
            each date in the calendar dates.
        dates : pd.DatetimeIndex
            The calendar dates for which estimates data is requested.

        Returns
        --------
        requested_qtr_data : pd.DataFrame
            The DataFrame with the latest values for the requested quarter
            for all columns; `dates` are the index and columns are a MultiIndex
            with sids at the top level and the dataset columns on the bottom.
        """
        zero_qtr_data_idx = zero_qtr_data.index
        requested_qtr_idx = pd.MultiIndex.from_arrays(
            [
                zero_qtr_data_idx.get_level_values(0),
                zero_qtr_data_idx.get_level_values(1),
                self.get_shifted_qtrs(
                    zeroth_quarter_idx.get_level_values(
                        NORMALIZED_QUARTERS,
                    ),
                    num_announcements,
                ),
            ],
            names=[
                zero_qtr_data_idx.names[0],
                zero_qtr_data_idx.names[1],
                SHIFTED_NORMALIZED_QTRS,
            ],
        )

        requested_qtr_data = stacked_last_per_qtr.reindex(index=requested_qtr_idx)
        requested_qtr_data = requested_qtr_data.reset_index(
            SHIFTED_NORMALIZED_QTRS,
        )
        # Calculate the actual year/quarter being requested and add those in
        # as columns.
        (
            requested_qtr_data[FISCAL_YEAR_FIELD_NAME],
            requested_qtr_data[FISCAL_QUARTER_FIELD_NAME],
        ) = split_normalized_quarters(requested_qtr_data[SHIFTED_NORMALIZED_QTRS])
        # Once we're left with just dates as the index, we can reindex by all
        # dates so that we have a value for each calendar date.
        return requested_qtr_data.unstack(SID_FIELD_NAME).reindex(dates)

    def get_split_adjusted_asof_idx(self, dates):
        """Compute the index in `dates` where the split-adjusted-asof-date
        falls. This is the date up to which, and including which, we will
        need to unapply all adjustments for and then re-apply them as they
        come in. After this date, adjustments are applied as normal.

        Parameters
        ----------
        dates : pd.DatetimeIndex
            The calendar dates over which the Pipeline is being computed.

        Returns
        -------
        split_adjusted_asof_idx : int
            The index in `dates` at which the data should be split.
        """
        split_adjusted_asof_idx = dates.searchsorted(self._split_adjusted_asof)
        # make_utc_aware(pd.DatetimeIndex(self._split_adjusted_asof))
        # The split-asof date is after the date index.
        if split_adjusted_asof_idx == len(dates):
            split_adjusted_asof_idx = len(dates) - 1
        if self._split_adjusted_asof.tzinfo is not None:
            if self._split_adjusted_asof < dates[0]:
                split_adjusted_asof_idx = -1
        else:
            if self._split_adjusted_asof < dates[0]:
                split_adjusted_asof_idx = -1
        return split_adjusted_asof_idx

    def collect_overwrites_for_sid(
        self,
        group,
        dates,
        requested_qtr_data,
        last_per_qtr,
        sid_idx,
        columns,
        all_adjustments_for_sid,
        sid,
    ):
        """Given a sid, collect all overwrites that should be applied for this
        sid at each quarter boundary.

        Parameters
        ----------
        group : pd.DataFrame
            The data for `sid`.
        dates : pd.DatetimeIndex
            The calendar dates for which estimates data is requested.
        requested_qtr_data : pd.DataFrame
            The DataFrame with the latest values for the requested quarter
            for all columns.
        last_per_qtr : pd.DataFrame
            A DataFrame with a column MultiIndex of [self.estimates.columns,
            normalized_quarters, sid] that allows easily getting the timeline
            of estimates for a particular sid for a particular quarter.
        sid_idx : int
            The sid's index in the asset index.
        columns : list of BoundColumn
            The columns for which the overwrites should be computed.
        all_adjustments_for_sid : dict[int -> AdjustedArray]
            A dictionary of the integer index of each timestamp into the date
            index, mapped to adjustments that should be applied at that
            index for the given sid (`sid`). This dictionary is modified as
            adjustments are collected.
        sid : int
            The sid for which overwrites should be computed.
        """
        # If data was requested for only 1 date, there can never be any
        # overwrites, so skip the extra work.
        if len(dates) == 1:
            return

        next_qtr_start_indices = dates.searchsorted(
            pd.DatetimeIndex(group[EVENT_DATE_FIELD_NAME]),
            side=self.searchsorted_side,
        )

        qtrs_with_estimates = group.index.get_level_values(NORMALIZED_QUARTERS).values
        for idx in next_qtr_start_indices:
            if 0 < idx < len(dates):
                # Find the quarter being requested in the quarter we're
                # crossing into.
                requested_quarter = requested_qtr_data[
                    SHIFTED_NORMALIZED_QTRS,
                    sid,
                ].iloc[idx]
                # Only add adjustments if the next quarter starts somewhere
                # in our date index for this sid. Our 'next' quarter can
                # never start at index 0; a starting index of 0 means that
                # the next quarter's event date was NaT.
                self.create_overwrites_for_quarter(
                    all_adjustments_for_sid,
                    idx,
                    last_per_qtr,
                    qtrs_with_estimates,
                    requested_quarter,
                    sid,
                    sid_idx,
                    columns,
                )

    def get_adjustments_for_sid(
        self,
        group,
        dates,
        requested_qtr_data,
        last_per_qtr,
        sid_to_idx,
        columns,
        col_to_all_adjustments,
        **kwargs,
    ):
        """

        Parameters
        ----------
        group : pd.DataFrame
            The data for the given sid.
        dates : pd.DatetimeIndex
            The calendar dates for which estimates data is requested.
        requested_qtr_data : pd.DataFrame
            The DataFrame with the latest values for the requested quarter
            for all columns.
        last_per_qtr : pd.DataFrame
            A DataFrame with a column MultiIndex of [self.estimates.columns,
            normalized_quarters, sid] that allows easily getting the timeline
            of estimates for a particular sid for a particular quarter.
        sid_to_idx : dict[int -> int]
            A dictionary mapping sid to he sid's index in the asset index.
        columns : list of BoundColumn
            The columns for which the overwrites should be computed.
        col_to_all_adjustments : dict[int -> AdjustedArray]
            A dictionary of the integer index of each timestamp into the date
            index, mapped to adjustments that should be applied at that
            index. This dictionary is for adjustments for ALL sids. It is
            modified as adjustments are collected.
        kwargs :
            Additional arguments used in collecting adjustments; unused here.
        """
        # Collect all adjustments for a given sid.
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
        self.merge_into_adjustments_for_all_sids(
            all_adjustments_for_sid, col_to_all_adjustments
        )

    def merge_into_adjustments_for_all_sids(
        self, all_adjustments_for_sid, col_to_all_adjustments
    ):
        """Merge adjustments for a particular sid into a dictionary containing
        adjustments for all sids.

        Parameters
        ----------
        all_adjustments_for_sid : dict[int -> AdjustedArray]
            All adjustments for a particular sid.
        col_to_all_adjustments : dict[int -> AdjustedArray]
            All adjustments for all sids.
        """

        for col_name in all_adjustments_for_sid:
            if col_name not in col_to_all_adjustments:
                col_to_all_adjustments[col_name] = {}
            for ts in all_adjustments_for_sid[col_name]:
                adjs = all_adjustments_for_sid[col_name][ts]
                add_new_adjustments(col_to_all_adjustments, adjs, col_name, ts)

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
        """Creates an AdjustedArray from the given estimates data for the given
        dates.

        Parameters
        ----------
        zero_qtr_data : pd.DataFrame
            The 'time zero' data for each calendar date per sid.
        requested_qtr_data : pd.DataFrame
            The requested quarter data for each calendar date per sid.
        last_per_qtr : pd.DataFrame
            A DataFrame with a column MultiIndex of [self.estimates.columns,
            normalized_quarters, sid] that allows easily getting the timeline
            of estimates for a particular sid for a particular quarter.
        dates : pd.DatetimeIndex
            The calendar dates for which estimates data is requested.
        assets : pd.Int64Index
            An index of all the assets from the raw data.
        columns : list of BoundColumn
            The columns for which adjustments need to be calculated.
        kwargs :
            Additional keyword arguments that should be forwarded to
            `get_adjustments_for_sid` and to be used in computing adjustments
            for each sid.

        Returns
        -------
        col_to_all_adjustments : dict[int -> AdjustedArray]
            A dictionary of all adjustments that should be applied.
        """

        zero_qtr_data.sort_index(inplace=True)
        # Here we want to get the LAST record from each group of records
        # corresponding to a single quarter. This is to ensure that we select
        # the most up-to-date event date in case the event date changes.
        quarter_shifts = zero_qtr_data.groupby(
            level=[SID_FIELD_NAME, NORMALIZED_QUARTERS]
        ).nth(-1)

        col_to_all_adjustments = {}
        sid_to_idx = dict(zip(assets, range(len(assets))))
        quarter_shifts.groupby(level=SID_FIELD_NAME).apply(
            self.get_adjustments_for_sid,
            dates,
            requested_qtr_data,
            last_per_qtr,
            sid_to_idx,
            columns,
            col_to_all_adjustments,
            **kwargs,
        )
        return col_to_all_adjustments

    def create_overwrites_for_quarter(
        self,
        col_to_overwrites,
        next_qtr_start_idx,
        last_per_qtr,
        quarters_with_estimates_for_sid,
        requested_quarter,
        sid,
        sid_idx,
        columns,
    ):
        """Add entries to the dictionary of columns to adjustments for the given
        sid and the given quarter.

        Parameters
        ----------
        col_to_overwrites : dict [column_name -> list of ArrayAdjustment]
            A dictionary mapping column names to all overwrites for those
            columns.
        next_qtr_start_idx : int
            The index of the first day of the next quarter in the calendar
            dates.
        last_per_qtr : pd.DataFrame
            A DataFrame with a column MultiIndex of [self.estimates.columns,
            normalized_quarters, sid] that allows easily getting the timeline
            of estimates for a particular sid for a particular quarter; this
            is particularly useful for getting adjustments for 'next'
            estimates.
        quarters_with_estimates_for_sid : np.array
            An array of all quarters for which there are estimates for the
            given sid.
        requested_quarter : float
            The quarter for which the overwrite should be created.
        sid : int
            The sid for which to create overwrites.
        sid_idx : int
            The index of the sid in `assets`.
        columns : list of BoundColumn
            The columns for which to create overwrites.
        """
        for col in columns:
            column_name = self.name_map[col.name]
            if column_name not in col_to_overwrites:
                col_to_overwrites[column_name] = {}
            # If there are estimates for the requested quarter,
            # overwrite all values going up to the starting index of
            # that quarter with estimates for that quarter.
            if requested_quarter in quarters_with_estimates_for_sid:
                adjs = self.create_overwrite_for_estimate(
                    col,
                    column_name,
                    last_per_qtr,
                    next_qtr_start_idx,
                    requested_quarter,
                    sid,
                    sid_idx,
                )
                add_new_adjustments(
                    col_to_overwrites, adjs, column_name, next_qtr_start_idx
                )
            # There are no estimates for the quarter. Overwrite all
            # values going up to the starting index of that quarter
            # with the missing value for this column.
            else:
                adjs = [self.overwrite_with_null(col, next_qtr_start_idx, sid_idx)]
                add_new_adjustments(
                    col_to_overwrites, adjs, column_name, next_qtr_start_idx
                )

    def overwrite_with_null(self, column, next_qtr_start_idx, sid_idx):
        return self.scalar_overwrites_dict[column.dtype](
            0, next_qtr_start_idx - 1, sid_idx, sid_idx, column.missing_value
        )

    async def load_adjusted_array(self, domain, columns, dates, sids, mask):
        # Separate out getting the columns' datasets and the datasets'
        # num_announcements attributes to ensure that we're catching the right
        # AttributeError.
        col_to_datasets = {col: col.dataset for col in columns}
        try:
            groups = groupby(
                lambda col: col_to_datasets[col].num_announcements, col_to_datasets
            )
        except AttributeError as exc:
            raise AttributeError(
                "Datasets loaded via the "
                "EarningsEstimatesLoader must define a "
                "`num_announcements` attribute that defines "
                "how many quarters out the loader should load"
                " the data relative to `dates`."
            ) from exc
        if any(num_qtr < 0 for num_qtr in groups):
            raise ValueError(
                INVALID_NUM_QTRS_MESSAGE
                % ",".join(str(qtr) for qtr in groups if qtr < 0)
            )
        out = {}
        # To optimize performance, only work below on assets that are
        # actually in the raw data.
        data_query_cutoff_times = domain.data_query_cutoff_for_sessions(dates)
        assets_with_data = set(sids) & set(self.estimates[SID_FIELD_NAME])
        last_per_qtr, stacked_last_per_qtr = self.get_last_data_per_qtr(
            assets_with_data,
            columns,
            dates,
            data_query_cutoff_times,
        )
        # Determine which quarter is immediately next/previous for each
        # date.
        zeroth_quarter_idx = self.get_zeroth_quarter_idx(stacked_last_per_qtr)
        zero_qtr_data = stacked_last_per_qtr.loc[zeroth_quarter_idx]

        for num_announcements, columns in groups.items():
            requested_qtr_data = self.get_requested_quarter_data(
                zero_qtr_data,
                zeroth_quarter_idx,
                stacked_last_per_qtr,
                num_announcements,
                dates,
            )

            # Calculate all adjustments for the given quarter and accumulate
            # them for each column.
            col_to_adjustments = self.get_adjustments(
                zero_qtr_data, requested_qtr_data, last_per_qtr, dates, sids, columns
            )

            # Lookup the asset indexer once, this is so we can reindex
            # the assets returned into the assets requested for each column.
            # This depends on the fact that our column pd.MultiIndex has the same
            # sids for each field. This allows us to do the lookup once on
            # level 1 instead of doing the lookup each time per value in
            # level 0.
            # asset_indexer = sids.get_indexer_for(
            #     requested_qtr_data.columns.levels[1],
            # )
            for col in columns:
                column_name = self.name_map[col.name]
                # allocate the empty output with the correct missing value
                # shape = len(dates), len(sids)
                # output_array = np.full(shape=shape,
                #                        fill_value=col.missing_value,
                #                        dtype=col.dtype)
                # overwrite the missing value with values from the computed data
                try:
                    output_array = (
                        requested_qtr_data[column_name]
                        .reindex(sids, axis=1)
                        .to_numpy()
                        .astype(col.dtype)
                    )
                except Exception:
                    output_array = (
                        requested_qtr_data[column_name]
                        .reindex(sids, axis=1)
                        .to_numpy(na_value=col.missing_value)
                        .astype(col.dtype)
                    )

                # except ValueError:
                #     np.copyto(output_array[:, asset_indexer],
                #               requested_qtr_data[column_name].to_numpy(na_value=output_array.dtype),
                #               casting='unsafe')
                out[col] = AdjustedArray(
                    output_array,
                    # There may not be any adjustments at all (e.g. if
                    # len(date) == 1), so provide a default.
                    dict(col_to_adjustments.get(column_name, {})),
                    col.missing_value,
                )
        return out

    def get_last_data_per_qtr(
        self, assets_with_data, columns, dates, data_query_cutoff_times
    ):
        """Determine the last piece of information we know for each column on each
        date in the index for each sid and quarter.

        Parameters
        ----------
        assets_with_data : pd.Index
            Index of all assets that appear in the raw data given to the
            loader.
        columns : iterable of BoundColumn
            The columns that need to be loaded from the raw data.
        data_query_cutoff_times : pd.DatetimeIndex
            The calendar of dates for which data should be loaded.

        Returns
        -------
        stacked_last_per_qtr : pd.DataFrame
            A DataFrame indexed by [dates, sid, normalized_quarters] that has
            the latest information for each row of the index, sorted by event
            date.
        last_per_qtr : pd.DataFrame
            A DataFrame with columns that are a MultiIndex of [
            self.estimates.columns, normalized_quarters, sid].
        """
        # Get a DataFrame indexed by date with a MultiIndex of columns of
        # [self.estimates.columns, normalized_quarters, sid], where each cell
        # contains the latest data for that day.
        last_per_qtr = last_in_date_group(
            self.estimates,
            data_query_cutoff_times,
            assets_with_data,
            reindex=True,
            extra_groupers=[NORMALIZED_QUARTERS],
        )
        last_per_qtr.index = dates
        # Forward fill values for each quarter/sid/dataset column.
        ffill_across_cols(last_per_qtr, columns, self.name_map)
        # Stack quarter and sid into the index.
        stacked_last_per_qtr = last_per_qtr.stack(
            [SID_FIELD_NAME, NORMALIZED_QUARTERS],
        )
        # Set date index name for ease of reference
        stacked_last_per_qtr.index.set_names(
            SIMULATION_DATES,
            level=0,
            inplace=True,
        )
        stacked_last_per_qtr[EVENT_DATE_FIELD_NAME] = pd.to_datetime(
            stacked_last_per_qtr[EVENT_DATE_FIELD_NAME]
        )
        stacked_last_per_qtr = stacked_last_per_qtr.sort_values(EVENT_DATE_FIELD_NAME)
        return last_per_qtr, stacked_last_per_qtr




