"""Validation of event loader column specifications."""

import pandas as pd
from ziplime.pipeline.common import EVENT_DATE_FIELD_NAME, SID_FIELD_NAME, TS_FIELD_NAME
from ziplime.pipeline.terms.bound_column import BoundColumn


def required_event_fields(
    next_value_columns: dict[BoundColumn, str],
    previous_value_columns: dict[BoundColumn, str],
):
    """
    Compute the set of resource columns required to serve
    ``next_value_columns`` and ``previous_value_columns``.
    """
    # These metadata columns are used to align event indexers.
    return {
        TS_FIELD_NAME,
        SID_FIELD_NAME,
        EVENT_DATE_FIELD_NAME,
    }.union(
        # We also expect any of the field names that our loadable columns
        # are mapped to.
        next_value_columns.values(),
        previous_value_columns.values(),
    )


def validate_column_specs(
    events: pd.DataFrame,
    next_value_columns: dict[BoundColumn, str],
    previous_value_columns: dict[BoundColumn, str],
) -> None:
    """
    Verify that the columns of ``events`` can be used by an EventsLoader to
    serve the BoundColumns described by ``next_value_columns`` and
    ``previous_value_columns``.
    """
    required = required_event_fields(next_value_columns, previous_value_columns)
    received = set(events.columns)
    missing = required - received
    if missing:
        raise ValueError(
            "EventsLoader missing required columns {missing}.\n"
            "Got Columns: {received}\n"
            "Expected Columns: {required}".format(
                missing=sorted(missing),
                received=sorted(received),
                required=sorted(required),
            )
        )
