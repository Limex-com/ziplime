"""option premium style

Whether an option's premium is paid at the trade or margined daily. It is what separates an OPRA
book from a MOEX one and it changes the accounting completely -- see
:class:`ziplime.assets.domain.premium_style.PremiumStyle`.

Defaults to UPFRONT, which is what every contract stored before this migration was: the only
source that existed wrote US-style contracts.

SQLite cannot add a NOT NULL column without a default, so this goes through batch_alter_table with
a server default, the same way the futures settlement columns did.

Revision ID: d7b2e9c4a180
Revises: a3f6c2d1e845
Create Date: 2026-09-12

"""
from alembic import op
import sqlalchemy as sa

revision = 'd7b2e9c4a180'
down_revision = 'a3f6c2d1e845'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('options_contracts') as batch_op:
        batch_op.add_column(sa.Column('premium_style', sa.String(), nullable=False,
                                      server_default='UPFRONT'))


def downgrade():
    with op.batch_alter_table('options_contracts') as batch_op:
        batch_op.drop_column('premium_style')
