"""bonds and bond events

Adds the two tables bond trading needs: ``bonds`` for the instruments and ``bond_events`` for
their schedules -- coupons, amortization instalments, maturity and offer windows.

``bonds`` mirrors the other asset tables: the primary key is the ``asset_router`` id, so a bond is
addressable by the same sid machinery as an equity or a futures contract. ``bond_events`` is one
flat table with a ``event_type`` discriminator rather than a table per event kind, because the
simulation asks one question per session -- "what is payable today" -- and one indexed table
answers it in a single scan.

Revision ID: e5c8b13a7f42
Revises: c1a4f7d2e9b3
Create Date: 2026-08-27

"""
from alembic import op
import sqlalchemy as sa

revision = 'e5c8b13a7f42'
down_revision = 'c1a4f7d2e9b3'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'bonds',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('isin', sa.String(), nullable=True),
        sa.Column('asset_name', sa.String(), nullable=False),
        sa.Column('start_date', sa.Date(), nullable=False),
        sa.Column('end_date', sa.Date(), nullable=False),
        sa.Column('first_traded', sa.Date(), nullable=False),
        sa.Column('auto_close_date', sa.Date(), nullable=False),
        sa.Column('face_value', sa.Float(), nullable=False),
        sa.Column('maturity_date', sa.Date(), nullable=False),
        sa.Column('coupon_rate', sa.Float(), nullable=False, server_default='0'),
        sa.Column('coupon_frequency', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('quote_currency', sa.String(), nullable=False, server_default='RUB'),
        sa.Column('day_count', sa.String(), nullable=False, server_default='ACT_365'),
        sa.Column('price_quotation', sa.String(), nullable=False,
                  server_default='PERCENT_OF_FACE'),
        sa.Column('is_amortized', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.ForeignKeyConstraint(['id'], ['asset_router.id'],
                                name=op.f('fk_bonds_id_asset_router')),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_bonds')),
    )
    op.create_index(op.f('ix_bonds_maturity_date'), 'bonds', ['maturity_date'], unique=False)

    op.create_table(
        'bond_events',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('asset_id', sa.Integer(), nullable=False),
        sa.Column('event_type', sa.String(), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('value', sa.Float(), nullable=False, server_default='0'),
        sa.Column('currency', sa.String(), nullable=False, server_default='RUB'),
        sa.Column('record_date', sa.Date(), nullable=True),
        sa.Column('period_start_date', sa.Date(), nullable=True),
        sa.Column('face_value', sa.Float(), nullable=True),
        sa.Column('value_percent', sa.Float(), nullable=True),
        sa.Column('new_face_value', sa.Float(), nullable=True),
        sa.Column('initial_face_value', sa.Float(), nullable=True),
        sa.Column('amortization_percent', sa.Float(), nullable=True),
        sa.Column('offer_type', sa.String(), nullable=True),
        sa.Column('offer_price', sa.Float(), nullable=True),
        sa.Column('offer_start_date', sa.Date(), nullable=True),
        sa.Column('offer_end_date', sa.Date(), nullable=True),
        sa.Column('offer_agent', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(['asset_id'], ['asset_router.id'],
                                name=op.f('fk_bond_events_asset_id_asset_router')),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_bond_events')),
    )
    op.create_index(op.f('ix_bond_events_asset_id'), 'bond_events', ['asset_id'], unique=False)
    op.create_index(op.f('ix_bond_events_date'), 'bond_events', ['date'], unique=False)
    op.create_index(op.f('ix_bond_events_record_date'), 'bond_events', ['record_date'],
                    unique=False)


def downgrade():
    op.drop_index(op.f('ix_bond_events_record_date'), table_name='bond_events')
    op.drop_index(op.f('ix_bond_events_date'), table_name='bond_events')
    op.drop_index(op.f('ix_bond_events_asset_id'), table_name='bond_events')
    op.drop_table('bond_events')
    op.drop_index(op.f('ix_bonds_maturity_date'), table_name='bonds')
    op.drop_table('bonds')
