"""payment hardening fields

Revision ID: 0002_payment_hardening
Revises: 0001_initial_bot_tables
Create Date: 2026-05-01
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0002_payment_hardening"
down_revision = "0001_initial_bot_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "payments",
        sa.Column("idempotency_key", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "payments",
        sa.Column("payment_url", sa.Text(), nullable=True),
    )
    op.add_column(
        "payments",
        sa.Column("invoice_url", sa.Text(), nullable=True),
    )
    op.add_column(
        "payments",
        sa.Column("return_url", sa.Text(), nullable=True),
    )
    op.add_column(
        "payments",
        sa.Column("success_url", sa.Text(), nullable=True),
    )
    op.add_column(
        "payments",
        sa.Column("fail_url", sa.Text(), nullable=True),
    )

    op.create_index(
        "ix_payments_idempotency_key",
        "payments",
        ["idempotency_key"],
    )
    op.create_index(
        "ix_payments_status_created_at",
        "payments",
        ["status", "created_at"],
    )
    op.create_unique_constraint(
        "uq_payments_user_provider_idempotency",
        "payments",
        ["user_id", "provider", "idempotency_key"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_payments_user_provider_idempotency",
        "payments",
        type_="unique",
    )
    op.drop_index("ix_payments_status_created_at", table_name="payments")
    op.drop_index("ix_payments_idempotency_key", table_name="payments")

    op.drop_column("payments", "fail_url")
    op.drop_column("payments", "success_url")
    op.drop_column("payments", "return_url")
    op.drop_column("payments", "invoice_url")
    op.drop_column("payments", "payment_url")
    op.drop_column("payments", "idempotency_key")