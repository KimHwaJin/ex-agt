"""Add durable API invocation admission; no legacy commands are enqueued."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0008_api_requests"
down_revision = "0007_executor_effects"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_api_requests",
        sa.Column(
            "request_id", postgresql.UUID(as_uuid=True), primary_key=True
        ),
        sa.Column(
            "task_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("agent_tasks.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("session_id", sa.String(255), nullable=False),
        sa.Column("command", postgresql.JSONB(), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("target_node", sa.String(64), nullable=False),
        sa.Column("base_checkpoint_id", sa.String(255), nullable=True),
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
        sa.Column("created_by", sa.String(255), nullable=False),
        sa.Column("updated_by", sa.String(255), nullable=False),
    )
    op.create_index(
        "ix_agent_api_requests_task_id", "agent_api_requests", ["task_id"]
    )
    op.create_index(
        "uq_agent_api_active_session",
        "agent_api_requests",
        ["session_id"],
        unique=True,
        postgresql_where=sa.text("state IN ('PENDING','RUNNING','BLOCKED')"),
    )
    op.create_index(
        "ix_agent_api_due",
        "agent_api_requests",
        ["next_attempt_at", "request_id"],
        postgresql_where=sa.text("state IN ('PENDING','RUNNING')"),
    )


def downgrade() -> None:
    op.drop_table("agent_api_requests")
