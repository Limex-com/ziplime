"""
Common constants for Pipeline.
"""

AD_FIELD_NAME = "asof_date"
ANNOUNCEMENT_FIELD_NAME = "announcement_date"
CASH_FIELD_NAME = "cash"
DAYS_SINCE_PREV = "days_since_prev"
DAYS_TO_NEXT = "days_to_next"
FISCAL_QUARTER_FIELD_NAME = "fiscal_quarter"
FISCAL_YEAR_FIELD_NAME = "fiscal_year"
NEXT_ANNOUNCEMENT = "next_announcement"
PREVIOUS_AMOUNT = "previous_amount"
PREVIOUS_ANNOUNCEMENT = "previous_announcement"

EVENT_DATE_FIELD_NAME = "event_date"
SID_FIELD_NAME = "sid"

TS_FIELD_NAME = "timestamp"
INVALID_NUM_QTRS_MESSAGE = (
    "Passed invalid number of quarters %s; " "must pass a number of quarters >= 0"
)
NEXT_FISCAL_QUARTER = "next_fiscal_quarter"
NEXT_FISCAL_YEAR = "next_fiscal_year"
NORMALIZED_QUARTERS = "normalized_quarters"
PREVIOUS_FISCAL_QUARTER = "previous_fiscal_quarter"
PREVIOUS_FISCAL_YEAR = "previous_fiscal_year"
SHIFTED_NORMALIZED_QTRS = "shifted_normalized_quarters"
SIMULATION_DATES = "dates"
# These metadata columns are used to align event indexers.
metadata_columns = frozenset(
    {
        TS_FIELD_NAME,
        SID_FIELD_NAME,
        EVENT_DATE_FIELD_NAME,
        FISCAL_QUARTER_FIELD_NAME,
        FISCAL_YEAR_FIELD_NAME,
    }
)
