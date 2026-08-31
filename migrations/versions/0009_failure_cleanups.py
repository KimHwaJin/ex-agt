"""Agent-owned durable failure compensation and blocked request scanning."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0009_failure_cleanups"
down_revision = "0008_api_requests"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_failure_cleanups",
        sa.Column(
            "task_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("agent_tasks.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("session_id", sa.String(255), nullable=False),
        sa.Column("turn", postgresql.JSONB(), nullable=False),
        sa.Column("source", postgresql.JSONB(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column(
            "state", sa.String(32), nullable=False, server_default="PENDING"
        ),
        sa.Column(
            "attempts", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column(
            "next_attempt_at", sa.DateTime(timezone=True), nullable=False
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "execution_id", postgresql.UUID(as_uuid=True), nullable=True
        ),
        sa.Column("executor_status", sa.String(32), nullable=True),
        sa.Column("preserve_terminal", sa.Boolean(), nullable=False),
        sa.Column("final_status", sa.String(64), nullable=False),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("created_by", sa.String(255), nullable=False),
        sa.Column("updated_by", sa.String(255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_agent_failure_due",
        "agent_failure_cleanups",
        ["next_attempt_at", "task_id"],
        postgresql_where=sa.text("state = 'PENDING'"),
    )
    op.create_index(
        "ix_agent_api_blocked",
        "agent_api_requests",
        ["request_id"],
        postgresql_where=sa.text("state = 'BLOCKED'"),
    )


def downgrade() -> None:
    op.drop_index("ix_agent_api_blocked", table_name="agent_api_requests")
    op.drop_table("agent_failure_cleanups")
