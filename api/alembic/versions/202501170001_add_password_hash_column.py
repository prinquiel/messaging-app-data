"""add password hash column to users

Revision ID: 202501170001
Revises: 
Create Date: 2025-01-17 00:00:00
"""

from alembic import op


revision = "202501170001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash VARCHAR(128)")


def downgrade() -> None:
    op.execute("ALTER TABLE users DROP COLUMN IF EXISTS password_hash")


