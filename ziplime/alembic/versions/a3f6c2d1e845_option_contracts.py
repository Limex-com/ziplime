"""option contracts

Adds the table behind :class:`~ziplime.assets.entities.option_contract.OptionContract`.

Indexed on ``(underlying_symbol, expiration_date)`` because that is the question every option run
asks, once per session: *give me today's chain on this underlying*. With 0DTE the table grows by a
full chain every session -- 34 rows a day for a 17-strike chain of calls and puts -- so the scan
that answers it is the one that has to stay cheap.

Revision ID: a3f6c2d1e845
Revises: e5c8b13a7f42
Create Date: 2026-09-12

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'a3f6c2d1e845'
down_revision = 'e5c8b13a7f42'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'options_contracts',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('isin', sa.String(), nullable=True),
        sa.Column('asset_name', sa.String(), nullable=False),
        sa.Column('start_date', sa.Date(), nullable=False),
        sa.Column('end_date', sa.Date(), nullable=False),
        sa.Column('first_traded', sa.Date(), nullable=False),
        sa.Column('auto_close_date', sa.Date(), nullable=False),
        sa.Column('underlying_asset_id', sa.Integer(), nullable=True),
        sa.Column('underlying_exchange_asset_sid', sa.Integer(), nullable=True),
        sa.Column('underlying_symbol', sa.String(), nullable=False),
        sa.Column('option_type', sa.String(), nullable=False),
        sa.Column('strike', sa.Float(), nullable=False),
        sa.Column('expiration_date', sa.Date(), nullable=False),
        sa.Column('multiplier', sa.Float(), nullable=False),
        sa.Column('tick_size', sa.Float(), nullable=False),
        sa.Column('exercise_style', sa.String(), nullable=False),
        sa.Column('settlement_type', sa.String(), nullable=False),
        sa.ForeignKeyConstraint(['id'], ['asset_router.id'],
                                name=op.f('fk_options_contracts_id_asset_router')),
        sa.ForeignKeyConstraint(['underlying_asset_id'], ['asset_router.id'],
                                name=op.f('fk_options_contracts_underlying_asset_id_asset_router')),
        sa.ForeignKeyConstraint(['underlying_exchange_asset_sid'], ['exchange_assets.sid'],
                                name=op.f('fk_options_contracts_underlying_sid_exchange_assets')),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_options_contracts')),
    )
    op.create_index(op.f('ix_options_contracts_underlying_asset_id'), 'options_contracts',
                    ['underlying_asset_id'], unique=False)
    op.create_index(op.f('ix_options_contracts_underlying_exchange_asset_sid'), 'options_contracts',
                    ['underlying_exchange_asset_sid'], unique=False)
    # The chain lookup: one underlying, one expiry.
    op.create_index('ix_options_contracts_chain', 'options_contracts',
                    ['underlying_symbol', 'expiration_date'], unique=False)


def downgrade():
    op.drop_index('ix_options_contracts_chain', table_name='options_contracts')
    op.drop_index(op.f('ix_options_contracts_underlying_exchange_asset_sid'),
                  table_name='options_contracts')
    op.drop_index(op.f('ix_options_contracts_underlying_asset_id'), table_name='options_contracts')
    op.drop_table('options_contracts')
