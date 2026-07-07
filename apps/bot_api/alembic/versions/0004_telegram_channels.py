"""add telegram_channels table

Revision ID: 0004_telegram_channels
Revises: 0003_payment_providers_rucis
Create Date: 2026-07-07
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0004_telegram_channels"
down_revision = "0003_payment_providers_rucis"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "telegram_channels",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("username", sa.String(128), nullable=False, unique=True),
        sa.Column("display_name", sa.String(256), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("added_by_admin_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index("ix_telegram_channels_username", "telegram_channels", ["username"])
    op.create_index("ix_telegram_channels_active", "telegram_channels", ["is_active"])


def downgrade() -> None:
    op.drop_index("ix_telegram_channels_active", table_name="telegram_channels")
    op.drop_index("ix_telegram_channels_username", table_name="telegram_channels")
    op.drop_table("telegram_channels")
