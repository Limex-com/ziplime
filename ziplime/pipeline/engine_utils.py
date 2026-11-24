"""Workspace and output index helpers for pipeline engines."""

import pandas as pd
from numpy import arange
import numpy as np
from ziplime.lib.adjusted_array import AdjustedArray
from ziplime.utils.numpy_utils import repeat_first_axis, repeat_last_axis
from ziplime.pipeline.terms.term import Term
from ziplime.pipeline.graph import ExecutionPlan


def default_populate_initial_workspace(
        initial_workspace: dict[Term, AdjustedArray],
        root_mask_term: Term,
        execution_plan: ExecutionPlan,
        dates: pd.DatetimeIndex,
        assets: pd.Index
) -> dict[Term, AdjustedArray]:
    """The default implementation for ``populate_initial_workspace``. This
    function returns the ``initial_workspace`` argument without making any
    modifications.

    Parameters
    ----------
    initial_workspace : dict[array-like]
        The initial workspace before we have populated it with any cached
        terms.
    root_mask_term : Term
        The root mask term, normally ``AssetExists()``. This is needed to
        compute the dates for individual terms.
    execution_plan : ExecutionPlan
        The execution plan for the pipeline being run.
    dates : pd.DatetimeIndex
        All of the dates being requested in this pipeline run including
        the extra dates for look back windows.
    assets : pd.Int64Index
        All of the assets that exist for the window being computed.

    Returns
    -------
    populated_initial_workspace : dict[term, array-like]
        The workspace to begin computations with.
    """
    return initial_workspace


def _pipeline_output_index(dates: pd.DatetimeIndex, assets: np.ndarray, mask: np.ndarray):
    """
    Create a MultiIndex for a pipeline output.

    Parameters
    ----------
    dates : pd.DatetimeIndex
        Row labels for ``mask``.
    assets : pd.Index
        Column labels for ``mask``.
    mask : np.ndarray[bool]
        Mask array indicating date/asset pairs that should be included in
        output index.

    Returns
    -------
    index : pd.MultiIndex
        MultiIndex  containing (date,  asset) pairs  corresponding to  ``True``
        values in ``mask``.
    """
    date_labels = repeat_last_axis(arange(len(dates)), len(assets))[mask]
    asset_labels = repeat_first_axis(arange(len(assets)), len(dates))[mask]
    return pd.MultiIndex(
        [dates, assets],
        [date_labels, asset_labels],
        # TODO: We should probably add names for these.
        names=[None, None],
        verify_integrity=False,
    )
