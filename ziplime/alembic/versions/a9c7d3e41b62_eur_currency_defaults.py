"""Use EUR for new instruments and bond payments when currency is omitted.

Existing currency values are retained. Historical revisions keep their original
defaults so upgrades and downgrades remain reproducible.

Revision ID: a9c7d3e41b62
Revises: f4c9a8e12b60
"""
from alembic import op
import sqlalchemy as sa

revision = "a9c7d3e41b62"
down_revision = "f4c9a8e12b60"
branch_labels = None
depends_on = None

_COLUMNS = (
    ("futures_contracts", "margin_currency"),
    ("futures_root_symbols", "margin_currency"),
    ("bonds", "quote_currency"),
    ("bond_events", "currency"),
)


def _set_defaults(currency: str) -> None:
    for table, column in _COLUMNS:
        with op.batch_alter_table(table) as batch:
            batch.alter_column(column, existing_type=sa.String(), existing_nullable=False,
                               server_default=currency)


def upgrade() -> None:
    _set_defaults("EUR")


def downgrade() -> None:
    _set_defaults("RUB")
