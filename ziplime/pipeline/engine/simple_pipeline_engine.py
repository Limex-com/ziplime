"""Simple pipeline engine for engine."""

import datetime
from functools import partial
from typing import Callable
import pandas as pd
from numpy import array
from pandas import DatetimeIndex
from toolz import groupby
import numpy
import numpy as np
from ziplime.errors import NoFurtherDataError
from ziplime.lib.adjusted_array import ensure_adjusted_array, ensure_ndarray, AdjustedArray
from ziplime.utils.date_utils import compute_date_range_chunks
from ziplime.utils.numpy_utils import as_column
from ziplime.utils.pandas_utils import categorical_df_concat, explode
from ziplime.utils.string_formatting import bulleted_list
from ziplime.pipeline.pipeline import Pipeline
from ziplime.pipeline.terms.term import Term
from ziplime.pipeline.terms.loadable_term import LoadableTerm
from ziplime.pipeline.domain import Domain, GENERIC
from ziplime.pipeline.graph import maybe_specialize, ExecutionPlan
from ziplime.pipeline.hooks import DelegatingHooks, PipelineHooks
from ziplime.pipeline.terms.asset_exists import AssetExists
from ziplime.pipeline.terms.input_dates import InputDates
from ziplime.assets.services.asset_service import AssetService
from ziplime.pipeline.loaders.pipeline_loader import PipelineLoader
from ziplime.pipeline.engine_utils import default_populate_initial_workspace
from ziplime.pipeline.engine_utils import _pipeline_output_index
from ziplime.pipeline.engine.pipeline_engine import PipelineEngine


class SimplePipelineEngine(PipelineEngine):
    """PipelineEngine class that computes each term independently.

    Parameters
    ----------
    get_loader : callable
        A function that is given a loadable term and returns a PipelineLoader
        to use to retrieve raw data for that term.
    asset_service : ziplime.assets.AssetFinder
        An AssetFinder instance.  We depend on the AssetFinder to determine
        which assets are in the top-level universe at any point in time.
    populate_initial_workspace : callable, optional
        A function which will be used to populate the initial workspace when
        computing a pipeline. See
        :func:`ziplime.pipeline.engine.default_populate_initial_workspace`
        for more info.
    default_hooks : list, optional
        List of hooks that should be used to instrument all pipelines executed
        by this engine.

    See Also
    --------
    :func:`ziplime.pipeline.engine.default_populate_initial_workspace`
    """

    __slots__ = (
        "_get_loader",
        "_root_mask_term",
        "_root_mask_dates_term",
        "_populate_initial_workspace",
    )

    def __init__(
            self,
            get_loader: Callable,
            asset_service: AssetService,
            default_domain: Domain = GENERIC,
            populate_initial_workspace: Callable = None,
            default_hooks: list[PipelineHooks] | None = None,
    ) -> None:
        self._get_loader = get_loader
        self._asset_service = asset_service

        self._root_mask_term = AssetExists()
        self._root_mask_dates_term = InputDates()

        self._populate_initial_workspace = (
                populate_initial_workspace or default_populate_initial_workspace
        )
        self._default_domain = default_domain

        if default_hooks is None:
            self._default_hooks = []
        else:
            self._default_hooks = list(default_hooks)

    async def run_chunked_pipeline(
            self, pipeline: Pipeline, start_date: datetime.datetime, end_date: datetime.datetime, chunksize: int,
            hooks: list[PipelineHooks] = None
    ):
        """Compute values for ``pipeline`` from ``start_date`` to ``end_date``, in
        date chunks of size ``chunksize``.

        Chunked execution reduces memory consumption, and may reduce
        computation time depending on the contents of your pipeline.

        Parameters
        ----------
        pipeline : Pipeline
            The pipeline to run.
        start_date : datetime.datetime
            The start date to run the pipeline for.
        end_date : datetime.datetime
            The end date to run the pipeline for.
        chunksize : int
            The number of days to execute at a time.
        hooks : list[implements(PipelineHooks)], optional
            Hooks for instrumenting Pipeline execution.

        Returns
        -------
        result : pd.DataFrame
            A frame of computed results.

            The ``result`` columns correspond to the entries of
            `pipeline.columns`, which should be a dictionary mapping strings to
            instances of :class:`ziplime.pipeline.Term`.

            For each date between ``start_date`` and ``end_date``, ``result``
            will contain a row for each asset that passed `pipeline.screen`.
            A screen of ``None`` indicates that a row should be returned for
            each asset that existed each day.

        See Also
        --------
        :meth:`ziplime.pipeline.engine.PipelineEngine.run_pipeline`
        """
        domain = self.resolve_domain(pipeline)
        ranges = compute_date_range_chunks(
            sessions=domain.sessions(),
            start_date=start_date,
            end_date=end_date,
            chunksize=chunksize,
        )
        hooks = self._resolve_hooks(hooks=hooks)

        run_pipeline = partial(self._run_pipeline_impl, pipeline, hooks=hooks)
        with hooks.running_pipeline(pipeline, start_date, end_date):
            chunks = [await run_pipeline(s, e) for s, e in ranges]

        if len(chunks) == 1:
            # OPTIMIZATION: Don't make an extra copy in `categorical_df_concat`
            # if we don't have to.
            return chunks[0]

        # Filter out empty chunks. Empty dataframes lose dtype information,
        # which makes concatenation fail.
        nonempty_chunks = [c for c in chunks if len(c)]
        return categorical_df_concat(df_list=nonempty_chunks, inplace=True)

    async def run_pipeline(self, pipeline: Pipeline, start_date: datetime.datetime, end_date: datetime.datetime,
                           hooks: list[PipelineHooks] | None = None):
        """Compute values for ``pipeline`` from ``start_date`` to ``end_date``.

        Parameters
        ----------
        pipeline : ziplime.pipeline.Pipeline
            The pipeline to run.
        start_date : datetime.datetime
            Start date of the computed matrix.
        end_date : datetime.datetime
            End date of the computed matrix.
        hooks : list[implements(PipelineHooks)], optional
            Hooks for instrumenting Pipeline execution.

        Returns
        -------
        result : pd.DataFrame
            A frame of computed results.

            The ``result`` columns correspond to the entries of
            `pipeline.columns`, which should be a dictionary mapping strings to
            instances of :class:`ziplime.pipeline.Term`.

            For each date between ``start_date`` and ``end_date``, ``result``
            will contain a row for each asset that passed `pipeline.screen`.
            A screen of ``None`` indicates that a row should be returned for
            each asset that existed each day.
        """
        hooks = self._resolve_hooks(hooks=hooks)
        with hooks.running_pipeline(pipeline=pipeline, start_date=start_date, end_date=end_date):
            return await self._run_pipeline_impl(
                pipeline=pipeline,
                start_date=start_date,
                end_date=end_date,
                hooks=hooks,
            )

    async def _run_pipeline_impl(
            self, pipeline: Pipeline, start_date: datetime.date, end_date: datetime.date,
            hooks: list[PipelineHooks]
    ):
        """Shared core for ``run_pipeline`` and ``run_chunked_pipeline``."""
        # See notes at the top of this module for a description of the
        # algorithm implemented here.
        if end_date < start_date:
            raise ValueError(
                "start_date must be before or equal to end_date \n"
                f"start_date={start_date}, end_date={end_date}"
            )

        domain = self.resolve_domain(pipeline=pipeline)

        plan = pipeline.to_execution_plan(
            domain=domain,
            default_screen=self._root_mask_term,
            start_date=start_date,
            end_date=end_date,
        )
        extra_rows = plan.extra_rows[self._root_mask_term]
        root_mask = await self._compute_root_mask(
            domain=domain,
            start_date=start_date,
            end_date=end_date,
            extra_rows=extra_rows,
        )
        dates, sids, root_mask_values = explode(df=root_mask)

        workspace = self._populate_initial_workspace(
            initial_workspace={
                self._root_mask_term: root_mask_values,
                self._root_mask_dates_term: as_column(dates.values),
            },
            root_mask_term=self._root_mask_term,
            execution_plan=plan,
            dates=dates,
            assets=sids,
        )

        refcounts = plan.initial_refcounts(initial_terms=workspace)
        execution_order = plan.execution_order(workspace=workspace, refcounts=refcounts)

        with hooks.computing_chunk(terms=execution_order, start_date=start_date, end_date=end_date):
            results = await self.compute_chunk(
                graph=plan,
                dates=dates,
                sids=sids,
                workspace=workspace,
                refcounts=refcounts,
                execution_order=execution_order,
                hooks=hooks,
            )

        return await self._to_narrow(
            terms=plan.outputs,
            data=results,
            mask=results.pop(plan.screen_name),
            dates=dates[extra_rows:],
            assets=sids,
        )

    async def _compute_root_mask(
        self, domain: Domain, start_date: datetime.date, end_date: datetime.date, extra_rows: int
    ):
        """Compute a lifetimes matrix from our AssetFinder, then drop columns that
        didn't exist at all during the query dates.

        Parameters
        ----------
        domain : ziplime.pipeline.domain.Domain
            Domain for which we're computing a pipeline.
        start_date : pd.Timestamp
            Base start date for the matrix.
        end_date : pd.Timestamp
            End date for the matrix.
        extra_rows : int
            Number of extra rows to compute before `start_date`.
            Extra rows are needed by terms like moving averages that require a
            trailing window of data.

        Returns
        -------
        lifetimes : pd.DataFrame
            Frame of dtype `bool` containing dates from `extra_rows` days
            before `start_date`, continuing through to `end_date`.  The
            returned frame contains as columns all assets in our AssetFinder
            that existed for at least one day between `start_date` and
            `end_date`.
        """
        sessions = domain.sessions()

        if pd.to_datetime(start_date) not in sessions:
            raise ValueError(
                f"Pipeline start date ({start_date}) is not a trading session for "
                f"domain {domain}."
            )

        elif pd.to_datetime(end_date) not in sessions:
            raise ValueError(
                f"Pipeline end date {end_date} is not a trading session for "
                f"domain {domain}."
            )

        start_idx, end_idx = sessions.slice_locs(start=start_date, end=end_date)
        if start_idx < extra_rows:
            raise NoFurtherDataError.from_lookback_window(
                initial_message="Insufficient data to compute Pipeline:",
                first_date=sessions[0],
                lookback_start=start_date,
                lookback_length=extra_rows,
            )

        # NOTE: This logic should probably be delegated to the domain once we
        #       start adding more complex domains.
        #
        # Build lifetimes matrix reaching back to `extra_rows` days before
        # `start_date.`
        if domain.assets is not None:
            lifetimes = await self._asset_service.asset_lifetimes(
                dates=sessions[start_idx - extra_rows: end_idx],
                include_start_date=False,
                assets=domain.assets,
            )
        else:
            lifetimes = await self._asset_service.lifetimes(
                dates=sessions[start_idx - extra_rows: end_idx],
                include_start_date=False,
                country_codes=[domain.country_code, ],
            )

        if not lifetimes.columns.unique:
            columns = lifetimes.columns
            duplicated = columns[columns.duplicated()].unique()
            raise AssertionError("Duplicated sids: %d" % duplicated)

        # Filter out columns that didn't exist from the farthest look back
        # window through the end of the requested dates.
        existed = lifetimes.any()
        ret = lifetimes.loc[:, existed]
        num_assets = ret.shape[1]

        if num_assets == 0:
            raise ValueError(
                "Failed to find any assets with country_code {!r} that traded "
                "between {} and {}.\n"
                "This probably means that your asset db is old or that it has "
                "incorrect country/exchange metadata.".format(
                    domain.country_code,
                    start_date,
                    end_date,
                )
            )

        return ret

    @staticmethod
    def _inputs_for_term(term: Term, workspace: dict, graph: ExecutionPlan, domain: Domain, refcounts: dict[str, int]):
        """
        Compute inputs for the given term.

        This is mostly complicated by the fact that for each input we store as
        many rows as will be necessary to serve **any** computation requiring
        that input.
        """
        offsets = graph.offset
        out = []

        # We need to specialize here because we don't change ComputableTerm
        # after resolving domains, so they can still contain generic terms as
        # inputs.
        specialized = [maybe_specialize(term=t, domain=domain) for t in term.inputs]

        if term.windowed:
            # If term is windowed, then all input data should be instances of
            # AdjustedArray.
            for input_ in specialized:
                adjusted_array = ensure_adjusted_array(
                    workspace[input_],
                    input_.missing_value,
                )
                out.append(
                    adjusted_array.traverse(
                        window_length=term.window_length,
                        offset=offsets[term, input_],
                        # If the refcount for the input is > 1, we will need
                        # to traverse this array again so we must copy.
                        # If the refcount for the input == 0, this is the last
                        # traversal that will happen so we can invalidate
                        # the AdjustedArray and mutate the data in place.
                        copy=refcounts[input_] > 1,
                    )
                )
        else:
            # If term is not windowed, input_data may be an AdjustedArray or
            # np.ndarray. Coerce the former to the latter.
            for input_ in specialized:
                input_data = ensure_ndarray(workspace[input_])
                offset = offsets[term, input_]
                input_data = input_data[offset:]
                if refcounts[input_] > 1:
                    input_data = input_data.copy()
                out.append(input_data)
        return out

    async def compute_chunk(
            self, graph: ExecutionPlan, dates: DatetimeIndex, sids: pd.Index, workspace: dict[Term, AdjustedArray],
            refcounts: dict[Term, int], execution_order: list[Term],
            hooks: PipelineHooks
    ):
        """Compute the Pipeline terms in the graph for the requested start and end
        dates.

        This is where we do the actual work of running a pipeline.

        Parameters
        ----------
        graph : ziplime.pipeline.graph.ExecutionPlan
            Dependency graph of the terms to be executed.
        dates : pd.DatetimeIndex
            Row labels for our root mask.
        sids : pd.Int64Index
            Column labels for our root mask.
        workspace : dict
            Map from term -> output.
            Must contain at least entry for `self._root_mask_term` whose shape
            is `(len(dates), len(assets))`, but may contain additional
            pre-computed terms for testing or optimization purposes.
        refcounts : dict[Term, int]
            Dictionary mapping terms to number of dependent terms. When a
            term's refcount hits 0, it can be safely discarded from
            ``workspace``. See TermGraph.decref_dependencies for more info.
        execution_order : list[Term]
            Order in which to execute terms.
        hooks : implements(PipelineHooks)
            Hooks to instrument pipeline execution.

        Returns
        -------
        results : dict
            Dictionary mapping requested results to outputs.
        """
        self._validate_compute_chunk_params(graph=graph, dates=dates, sids=sids, initial_workspace=workspace)

        get_loader = self._get_loader

        # Copy the supplied initial workspace so we don't mutate it in place.
        workspace = workspace.copy()
        domain = graph.domain

        # Many loaders can fetch data more efficiently if we ask them to
        # retrieve all their inputs at once. For example, a loader backed by a
        # SQL database can fetch multiple columns from the database in a single
        # query.
        #
        # To enable these loaders to fetch their data efficiently, we group
        # together requests for LoadableTerms if they are provided by the same
        # loader and they require the same number of extra rows.
        #
        # The extra rows condition is a simplification: we don't currently have
        # a mechanism for asking a loader to fetch different windows of data
        # for different terms, so we only batch requests together when they're
        # going to produce data for the same set of dates.
        def loader_group_key(term: Term):
            loader = get_loader(term)
            extra_rows = graph.extra_rows[term]
            return loader, extra_rows

        # Only produce loader groups for the terms we expect to load.  This
        # ensures that we can run pipelines for graphs where we don't have a
        # loader registered for an atomic term if all the dependencies of that
        # term were supplied in the initial workspace.
        will_be_loaded = graph.loadable_terms - workspace.keys()
        loader_groups = groupby(
            loader_group_key,
            (t for t in execution_order if t in will_be_loaded),
        )

        for term in execution_order:
            # `term` may have been supplied in `initial_workspace`, or we may
            # have loaded `term` as part of a batch with another term coming
            # from the same loader (see note on loader_group_key above). In
            # either case, we already have the term computed, so don't
            # recompute.
            if term in workspace:
                continue

            # Asset labels are always the same, but date labels vary by how
            # many extra rows are needed.
            mask, mask_dates = graph.mask_and_dates_for_term(
                term=term,
                root_mask_term=self._root_mask_term,
                workspace=workspace,
                all_dates=dates,
            )

            if isinstance(term, LoadableTerm):
                loader = get_loader(term)
                to_load = sorted(
                    loader_groups[loader_group_key(term)], key=lambda t: t.dataset
                )
                self._ensure_can_load(loader=loader, terms=to_load)
                with hooks.loading_terms(to_load):
                    loaded = await loader.load_adjusted_array(
                        domain=domain,
                        columns=to_load,
                        dates=mask_dates,
                        sids=sids,
                        mask=mask,
                    )
                assert set(loaded) == set(to_load), (
                        "loader did not return an AdjustedArray for each column\n"
                        "expected: %r\n"
                        "got:      %r"
                        % (
                            sorted(to_load, key=repr),
                            sorted(loaded, key=repr),
                        )
                )
                workspace.update(loaded)
            else:
                with hooks.computing_term(term=term):
                    workspace[term] = term._compute(
                        self._inputs_for_term(
                            term=term,
                            workspace=workspace,
                            graph=graph,
                            domain=domain,
                            refcounts=refcounts,
                        ),
                        dates=mask_dates,
                        assets=sids,
                        mask=mask,
                    )
                if term.ndim == 2:
                    assert workspace[term].shape == mask.shape
                else:
                    assert workspace[term].shape == (mask.shape[0], 1)

                # Decref dependencies of ``term``, and clear any terms
                # whose refcounts hit 0.
                for garbage in graph.decref_dependencies(term=term, refcounts=refcounts):
                    del workspace[garbage]

        # At this point, all the output terms are in the workspace.
        out = {}
        graph_extra_rows = graph.extra_rows
        for name, term in graph.outputs.items():
            # Truncate off extra rows from outputs.
            out[name] = workspace[term][graph_extra_rows[term]:]
        return out

    async def _to_narrow(
        self,
        terms: dict[str, Term],
        data: dict[str, AdjustedArray],
        mask: np.ndarray,
        dates: numpy.ndarray,
        assets: numpy.ndarray,
    ):
        """
        Convert raw computed pipeline results into a DataFrame for public APIs.

        Parameters
        ----------
        terms : dict[str -> Term]
            Dict mapping column names to terms.
        data : dict[str -> ndarray[ndim=2]]
            Dict mapping column names to computed results for those names.
        mask : ndarray[bool, ndim=2]
            Mask array of values to keep.
        dates : ndarray[datetime64, ndim=1]
            Row index for arrays `data` and `mask`
        assets : ndarray[int64, ndim=2]
            Column index for arrays `data` and `mask`

        Returns
        -------
        results : pd.DataFrame
            The indices of `results` are as follows:

            index : two-tiered MultiIndex of (date, asset).
                Contains an entry for each (date, asset) pair corresponding to
                a `True` value in `mask`.
            columns : Index of str
                One column per entry in `data`.

        If mask[date, asset] is True, then result.loc[(date, asset), colname]
        will contain the value of data[colname][date, asset].
        """
        if not mask.any():
            # Manually handle the empty DataFrame case. This is a workaround
            # to pandas failing to tz_localize an empty dataframe with a
            # MultiIndex. It also saves us the work of applying a known-empty
            # mask to each array.
            #
            # Slicing `dates` here to preserve pandas metadata.
            empty_dates = dates[:0]
            empty_assets = array([], dtype=object)
            return pd.DataFrame(
                data={name: array([], dtype=arr.dtype) for name, arr in data.items()},
                index=pd.MultiIndex.from_arrays([empty_dates, empty_assets]),
            )
        # if "open_instance" in data.keys():
        #     data["open_instance"].tofile("../../open_instance.dat")
        final_columns = {}
        for name in data:
            # Each term that computed an output has its postprocess method
            # called on the filtered result.
            #
            # Using this to convert np.records to tuples
            final_columns[name] = terms[name].postprocess(data[name][mask])

        resolved_assets = array(await self._asset_service.get_assets_by_ids(ids=list(assets)))
        index = _pipeline_output_index(dates, resolved_assets, mask)
        return pd.DataFrame(
            data=final_columns, index=index, columns=final_columns.keys()
        )

    def _validate_compute_chunk_params(
        self,
        graph: ExecutionPlan,
        dates: pd.DatetimeIndex,
        sids: pd.Index,
        initial_workspace: dict[Term, AdjustedArray],
    ) -> None:
        """
        Verify that the values passed to compute_chunk are well-formed.
        """
        root = self._root_mask_term
        clsname = type(self).__name__

        # Writing this out explicitly so this errors in testing if we change
        # the name without updating this line.
        compute_chunk_name = self.compute_chunk.__name__
        if root not in initial_workspace:
            raise AssertionError(
                "root_mask values not supplied to {cls}.{method}".format(
                    cls=clsname,
                    method=compute_chunk_name,
                )
            )

        shape = initial_workspace[root].shape
        implied_shape = len(dates), len(sids)
        if shape != implied_shape:
            raise AssertionError(
                "root_mask shape is {shape}, but received dates/assets "
                "imply that shape should be {implied}".format(
                    shape=shape,
                    implied=implied_shape,
                )
            )

        for term in initial_workspace:
            if self._is_special_root_term(term):
                continue

            if term.domain is GENERIC:
                # XXX: We really shouldn't allow **any** generic terms to be
                # populated in the initial workspace. A generic term, by
                # definition, can't correspond to concrete data until it's
                # paired with a domain, and populate_initial_workspace isn't
                # given the domain of execution, so it can't possibly know what
                # data to use when populating a generic term.
                #
                # In our current implementation, however, we don't have a good
                # way to represent specializations of ComputableTerms that take
                # only generic inputs, so there's no good way for the initial
                # workspace to provide data for such terms except by populating
                # the generic ComputableTerm.
                #
                # The right fix for the above is to implement "full
                # specialization", i.e., implementing ``specialize`` uniformly
                # across all terms, not just LoadableTerms. Having full
                # specialization will also remove the need for all of the
                # remaining ``maybe_specialize`` calls floating around in this
                # file.
                #
                # In the meantime, disallowing ComputableTerms in the initial
                # workspace would break almost every test in
                # `test_filter`/`test_factor`/`test_classifier`, and fixing
                # them would require updating all those tests to compute with
                # more specialized terms. Once we have full specialization, we
                # can fix all the tests without a large volume of edits by
                # simply specializing their workspaces, so for now I'm leaving
                # this in place as a somewhat sharp edge.
                if isinstance(term, LoadableTerm):
                    raise ValueError(
                        "Loadable workspace terms must be specialized to a "
                        "domain, but got generic term {}".format(term)
                    )

            elif term.domain != graph.domain:
                raise ValueError(
                    "Initial workspace term {} has domain {}. "
                    "Does not match pipeline domain {}".format(
                        term,
                        term.domain,
                        graph.domain,
                    )
                )

    def resolve_domain(self, pipeline: Pipeline):
        """Resolve a concrete domain for ``pipeline``."""
        domain = pipeline.domain(default=self._default_domain)
        if domain is GENERIC:
            raise ValueError(
                "Unable to determine domain for Pipeline.\n"
                "Pass domain=<desired domain> to your Pipeline to set a "
                "domain."
            )
        return domain

    def _is_special_root_term(self, term: Term):
        return term is self._root_mask_term or term is self._root_mask_dates_term

    def _resolve_hooks(self, hooks: list[PipelineHooks]):
        if hooks is None:
            hooks = []
        return DelegatingHooks(self._default_hooks + hooks)

    def _ensure_can_load(self, loader: PipelineLoader, terms: list[LoadableTerm]) -> None:
        """Ensure that ``loader`` can load ``terms``."""
        if not loader.currency_aware:
            bad = [t for t in terms if t.currency_conversion is not None]
            if bad:
                raise ValueError(
                    "Requested currency conversion is not supported for the "
                    "following terms:\n{}".format(bulleted_list(bad))
                )
