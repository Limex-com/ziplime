"""Persist the explicit currency of each cash dividend payout.

Revision ID: f4c9a8e12b60
Revises: d7b2e9c4a180
"""
from alembic import op
import sqlalchemy as sa

revision = "f4c9a8e12b60"
down_revision = "d7b2e9c4a180"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("dividend_payouts") as batch:
        batch.add_column(sa.Column("currency_id", sa.Integer(), nullable=False))
        batch.create_foreign_key(
            op.f("fk_dividend_payouts_currency_id_asset_router"), "asset_router", ["currency_id"], ["id"])
        batch.create_index(op.f("ix_dividend_payouts_currency_id"), ["currency_id"], unique=False)


def downgrade():
    with op.batch_alter_table("dividend_payouts") as batch:
        batch.drop_index(op.f("ix_dividend_payouts_currency_id"))
        batch.drop_constraint(op.f("fk_dividend_payouts_currency_id_asset_router"), type_="foreignkey")
        batch.drop_column("currency_id")
