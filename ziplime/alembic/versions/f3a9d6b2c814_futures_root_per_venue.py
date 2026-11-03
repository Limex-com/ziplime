"""futures chains keyed by root and exchange

A futures root was the primary key of its chain, so one database could hold only one ``NG``: the
Moscow Exchange's (100 MMBtu, quoted in dollars) or NYMEX's (10,000 MMBtu). Importing the second
skipped its root without a word and filed its contracts under the first one's specification.
Fourteen roots collide between the Moscow Exchange and CME/ICE -- NG, CL, ES, BZ, RB, HO, VX among
them -- so a database holding both markets needs the exchange in the key.

``futures_contracts.root_symbol`` loses its foreign key for the same reason: it pointed at the root
alone, and a contract learns its exchange from its listing.

Revision ID: f3a9d6b2c814
Revises: d7b2e9c4a180
Create Date: 2026-11-02

"""
from alembic import op

revision = 'f3a9d6b2c814'
down_revision = 'd7b2e9c4a180'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("futures_contracts", recreate="always") as batch_op:
        batch_op.drop_constraint("fk_futures_contracts_root_symbol_futures_root_symbols",
                                 type_="foreignkey")
    with op.batch_alter_table("futures_root_symbols", recreate="always") as batch_op:
        batch_op.drop_constraint("pk_futures_root_symbols", type_="primary")
        batch_op.create_primary_key("pk_futures_root_symbols", ["root_symbol", "mic"])


def downgrade():
    with op.batch_alter_table("futures_root_symbols", recreate="always") as batch_op:
        batch_op.drop_constraint("pk_futures_root_symbols", type_="primary")
        batch_op.create_primary_key("pk_futures_root_symbols", ["root_symbol"])
    with op.batch_alter_table("futures_contracts", recreate="always") as batch_op:
        batch_op.create_foreign_key("fk_futures_contracts_root_symbol_futures_root_symbols",
                                    "futures_root_symbols", ["root_symbol"], ["root_symbol"])
