"""Add safe, idempotent operations for blocked failure cleanup."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0010_failure_operations"
down_revision = "0009_failure_cleanups"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "agent_failure_cleanups",
        sa.Column(
            "version",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
    )
    op.add_column(
        "agent_failure_cleanups",
        sa.Column(
            "last_operation_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
    )
    op.add_column(
        "agent_failure_cleanups",
        sa.Column("last_operation_action", sa.String(32), nullable=True),
    )
    op.add_column(
        "agent_failure_cleanups",
        sa.Column("last_operation_hash", sa.String(64), nullable=True),
    )
    op.add_column(
        "agent_failure_cleanups",
        sa.Column("last_operation_reason", sa.Text(), nullable=True),
    )
    op.add_column(
        "agent_failure_cleanups",
        sa.Column(
            "last_operation_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "agent_failure_cleanups",
        sa.Column("last_operation_by", sa.String(255), nullable=True),
    )
    op.create_index(
        "ix_agent_failure_blocked_page",
        "agent_failure_cleanups",
        ["updated_at", "task_id"],
        postgresql_where=sa.text("state = 'BLOCKED'"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_agent_failure_blocked_page",
        table_name="agent_failure_cleanups",
    )
    for name in (
        "last_operation_by",
        "last_operation_at",
        "last_operation_reason",
        "last_operation_hash",
        "last_operation_action",
        "last_operation_id",
        "version",
    ):
        op.drop_column("agent_failure_cleanups", name)
