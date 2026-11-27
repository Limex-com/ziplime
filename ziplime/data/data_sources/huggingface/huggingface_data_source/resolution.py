"""Resolution for huggingface_data_source."""

import enum


class Resolution(enum.Enum):
    """How a window of rows collapses into "the value now".

    ``data.current`` asks a source for the current state, and what that means depends on the shape
    of the data rather than on the caller. A source that emits one row per instrument per day is
    current at its newest row. A source that republishes the same report as it is revised is not:
    the newest revision of a period carries only what that filing chose to repeat, so reading it as
    a row loses every column the filing left out.

    Making this a property of the source is the point. A strategy calls ``data.current`` the same
    way against prices, disclosures and financial statements, and the source decides what its own
    freshest view is -- rather than each strategy importing a different helper and having to know.
    """

    #: The newest row per instrument. Right for bars and for daily aggregates.
    LATEST_ROW = "latest_row"

    #: The newest non-null value per column, across every revision visible at the time. Right for
    #: anything republished under revision: a later filing that restates a period reports fewer
    #: line items than the original, so on SEC fundamentals this keeps 78% of ``total_assets`` that
    #: taking the newest row throws away.
    COALESCE = "coalesce"
