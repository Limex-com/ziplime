"""settlement type and margin currency

A futures contract that settles by physical delivery has to be closed before the delivery window,
and the currency an exchange collects margin in is not always the currency the contract is quoted
in -- an exchange may quote a contract in dollars and still collect margin in its local
currency. Both facts have to be stored per contract, not assumed.

SQLite cannot add a NOT NULL column without a default, so both go through batch_alter_table with a
server default. The defaults describe the common case (cash settlement, rouble margin); the ingest
writes the real values.

Revision ID: c1a4f7d2e9b3
Revises: b60ffbf8f072
Create Date: 2026-08-26

"""
from alembic import op
import sqlalchemy as sa

revision = 'c1a4f7d2e9b3'
down_revision = 'b60ffbf8f072'
branch_labels = None
depends_on = None

_COLUMNS = (
    ("settlement_type", "CASH"),
    ("margin_currency", "RUB"),
)


def upgrade():
    for table in ("futures_contracts", "futures_root_symbols"):
        with op.batch_alter_table(table) as batch_op:
            for name, default in _COLUMNS:
                batch_op.add_column(sa.Column(name, sa.String(), nullable=False,
                                              server_default=default))


def downgrade():
    for table in ("futures_root_symbols", "futures_contracts"):
        with op.batch_alter_table(table) as batch_op:
            for name, _ in reversed(_COLUMNS):
                batch_op.drop_column(name)
