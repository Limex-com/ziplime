"""futures root symbols

Adds the futures chain metadata table and links every contract to its root.

Autogenerate emitted a bare ``add_column``/``create_foreign_key`` pair, which SQLite cannot run:
it has no ``ALTER TABLE ... ADD CONSTRAINT`` and refuses ``ADD COLUMN ... NOT NULL`` without a
default. Both operations therefore go through ``batch_alter_table``, which recreates the table,
and the foreign key is named explicitly so that the downgrade can drop it.

Revision ID: b60ffbf8f072
Revises: b8e8c570ad43
Create Date: 2026-08-25 22:08:52.874981

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b60ffbf8f072'
down_revision = 'b8e8c570ad43'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'futures_root_symbols',
        sa.Column('root_symbol', sa.String(), nullable=False),
        sa.Column('description', sa.String(), nullable=False),
        sa.Column('mic', sa.String(), nullable=False),
        sa.Column('root_asset_id', sa.Integer(), nullable=False),
        sa.Column('multiplier', sa.Float(), nullable=False),
        sa.Column('tick_size', sa.Float(), nullable=False),
        sa.Column('quote_currency', sa.String(), nullable=False),
        sa.ForeignKeyConstraint(['mic'], ['exchanges.mic'],
                                name=op.f('fk_futures_root_symbols_mic_exchanges')),
        sa.ForeignKeyConstraint(['root_asset_id'], ['asset_router.id'],
                                name=op.f('fk_futures_root_symbols_root_asset_id_asset_router')),
        sa.PrimaryKeyConstraint('root_symbol', name=op.f('pk_futures_root_symbols')),
    )
    op.create_index(op.f('ix_futures_root_symbols_mic'), 'futures_root_symbols', ['mic'],
                    unique=False)
    op.create_index(op.f('ix_futures_root_symbols_root_asset_id'), 'futures_root_symbols',
                    ['root_asset_id'], unique=False)

    with op.batch_alter_table('futures_contracts') as batch_op:
        batch_op.add_column(sa.Column('root_symbol', sa.String(), nullable=False,
                                      server_default=''))
        batch_op.create_index(op.f('ix_futures_contracts_root_symbol'), ['root_symbol'],
                              unique=False)
        batch_op.create_foreign_key('fk_futures_contracts_root_symbol_futures_root_symbols',
                                    'futures_root_symbols', ['root_symbol'], ['root_symbol'])


def downgrade():
    with op.batch_alter_table('futures_contracts') as batch_op:
        batch_op.drop_constraint('fk_futures_contracts_root_symbol_futures_root_symbols',
                                 type_='foreignkey')
        batch_op.drop_index(op.f('ix_futures_contracts_root_symbol'))
        batch_op.drop_column('root_symbol')

    op.drop_index(op.f('ix_futures_root_symbols_root_asset_id'),
                  table_name='futures_root_symbols')
    op.drop_index(op.f('ix_futures_root_symbols_mic'), table_name='futures_root_symbols')
    op.drop_table('futures_root_symbols')
