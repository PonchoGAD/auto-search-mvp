"""add yookassa and stars payment providers

Revision ID: 0003_payment_providers_rucis
Revises: 0002_payment_hardening
Create Date: 2026-06-06
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0003_payment_providers_rucis"
down_revision = "0002_payment_hardening"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # PostgreSQL does not support ALTER CONSTRAINT — must drop and recreate
    op.drop_constraint("ck_payments_provider_valid", "payments", type_="check")
    op.create_check_constraint(
        "ck_payments_provider_valid",
        "payments",
        "provider IN ('stub', 'yookassa', 'stars', 'telegram', 'stripe')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_payments_provider_valid", "payments", type_="check")
    op.create_check_constraint(
        "ck_payments_provider_valid",
        "payments",
        "provider IN ('stub', 'click', 'stripe', 'telegram')",
    )
