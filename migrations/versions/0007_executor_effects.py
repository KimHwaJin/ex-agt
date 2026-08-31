"""Persist immutable Agent-to-Executor requests and received responses."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0007_executor_effects"
down_revision = "0006_api_audit_actors"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_executor_effects",
        sa.Column("key", sa.String(255), primary_key=True),
        sa.Column(
            "task_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("agent_tasks.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("input_sha256", sa.String(64), nullable=False),
        sa.Column("request_sha256", sa.String(64), nullable=False),
        sa.Column("request", postgresql.JSONB(), nullable=False),
        sa.Column("response", postgresql.JSONB(), nullable=True),
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
        sa.Column(
            "created_by",
            sa.String(255),
            nullable=False,
            server_default="AGENT",
        ),
        sa.Column(
            "updated_by",
            sa.String(255),
            nullable=False,
            server_default="AGENT",
        ),
    )
    op.create_index(
        "ix_agent_executor_effects_task_id",
        "agent_executor_effects",
        ["task_id"],
    )


def downgrade() -> None:
    op.drop_table("agent_executor_effects")
