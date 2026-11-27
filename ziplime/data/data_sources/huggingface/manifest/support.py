"""Support for manifest."""

import structlog


_logger = structlog.get_logger(__name__)


KNOWLEDGE_DATE_COLUMNS = (
    "feature_available_at",
    "knowledge_date",
    "knowledge_day",
    "available_at",
    "disclosure_date",
    "notification_date",
    "date",
)


EVENT_DATE_COLUMNS = (
    "event_date",
    "transaction_date",
    "period_of_report",
    "as_of_date",
    "held_as_of_date",
    "filing_date",
    "expiration_date",
)


FLOOR_EVENT_COLUMNS = ("event_date", "transaction_date", "period_of_report", "as_of_date")


ENTITY_COLUMNS = ("ticker", "asset_ticker", "symbol", "entity_id")


KNOWN_ENTITY_DOMAINS = frozenset({"us_equities"})
