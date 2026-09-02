"""Add durable Redis Stream maintenance jobs."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0011_stream_maintenance"
down_revision = "0010_failure_operations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_stream_maintenance_jobs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
        ),
        sa.Column("stream_alias", sa.String(64), nullable=False),
        sa.Column("stream_key", sa.String(255), nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column(
            "state",
            sa.String(16),
            nullable=False,
            server_default="PENDING",
        ),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("retention_seconds", sa.Integer(), nullable=False),
        sa.Column(
            "minimum_retained_entries",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "attempts",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result", postgresql.JSONB(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
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
        sa.UniqueConstraint(
            "created_by",
            "idempotency_key",
            name="uq_agent_stream_maintenance_idempotency",
        ),
        sa.CheckConstraint(
            "action IN ('PLAN','TRIM')",
            name="ck_agent_stream_maintenance_action",
        ),
        sa.CheckConstraint(
            "state IN ('PENDING','RUNNING','SUCCEEDED','FAILED')",
            name="ck_agent_stream_maintenance_state",
        ),
        sa.CheckConstraint(
            "retention_seconds > 0",
            name="ck_agent_stream_maintenance_retention",
        ),
        sa.CheckConstraint(
            "minimum_retained_entries >= 0",
            name="ck_agent_stream_maintenance_minimum_entries",
        ),
    )
    op.create_index(
        "ix_agent_stream_maintenance_page",
        "agent_stream_maintenance_jobs",
        ["created_at", "id"],
    )
    op.create_index(
        "uq_agent_stream_maintenance_active_trim",
        "agent_stream_maintenance_jobs",
        ["stream_key"],
        unique=True,
        postgresql_where=sa.text(
            "action = 'TRIM' AND state IN ('PENDING','RUNNING')"
        ),
    )
    op.create_index(
        "ix_agent_stream_maintenance_due",
        "agent_stream_maintenance_jobs",
        ["next_attempt_at", "id"],
        postgresql_where=sa.text("state IN ('PENDING','RUNNING')"),
    )


def downgrade() -> None:
    op.drop_table("agent_stream_maintenance_jobs")
