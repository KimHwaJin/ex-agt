"""Create durable Executor event worker tables.

Revision ID: ew_0001
Revises: None

Frozen initial schema: never import runtime models or read schema.sql here.
For an existing host Alembic chain, copy the operations into a host revision.
"""

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects import postgresql

revision = "ew_0001"
down_revision = None
branch_labels = None
depends_on = None


def _audit() -> list[sa.Column]:
    return [
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column("updated_by", sa.Text(), nullable=False),
    ]


def upgrade() -> None:
    op.create_table(
        "ew_bindings",
        sa.Column("namespace", sa.Text(), nullable=False),
        sa.Column("execution_id", postgresql.UUID(), nullable=False),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("task_id", sa.Text(), nullable=False),
        sa.Column(
            "last_sequence",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "next_scan_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("last_error", sa.Text()),
        *_audit(),
        sa.Column(
            "catch_up_version",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "caught_up_version",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.PrimaryKeyConstraint(
            "namespace", "execution_id", name="ew_bindings_pkey"
        ),
    )
    op.create_table(
        "ew_inbox",
        sa.Column("namespace", sa.Text(), nullable=False),
        sa.Column("event_id", postgresql.UUID(), nullable=False),
        sa.Column("execution_id", postgresql.UUID(), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("event", postgresql.JSONB(), nullable=False),
        sa.Column(
            "state",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'RECEIVED'"),
        ),
        *_audit(),
        sa.PrimaryKeyConstraint("namespace", "event_id", name="ew_inbox_pkey"),
        sa.UniqueConstraint(
            "namespace",
            "execution_id",
            "sequence",
            name="ew_inbox_namespace_execution_id_sequence_key",
        ),
        sa.CheckConstraint("sequence > 0", name="ew_inbox_sequence_check"),
        sa.CheckConstraint(
            "state IN ('RECEIVED', 'ROUTED', 'IGNORED')",
            name="ew_inbox_state_check",
        ),
    )
    op.create_table(
        "ew_commands",
        sa.Column("namespace", sa.Text(), nullable=False),
        sa.Column("command_id", postgresql.UUID(), nullable=False),
        sa.Column("event_id", postgresql.UUID(), nullable=False),
        sa.Column("execution_id", postgresql.UUID(), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column(
            "state",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'READY'"),
        ),
        sa.Column(
            "failure_attempts",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "generation",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("last_error", sa.Text()),
        *_audit(),
        sa.PrimaryKeyConstraint(
            "namespace", "command_id", name="ew_commands_pkey"
        ),
        sa.UniqueConstraint(
            "namespace", "event_id", name="ew_commands_namespace_event_id_key"
        ),
        sa.ForeignKeyConstraint(
            ["namespace", "event_id"],
            ["ew_inbox.namespace", "ew_inbox.event_id"],
            name="ew_commands_namespace_event_id_fkey",
        ),
        sa.CheckConstraint(
            "state IN ('READY', 'RUNNING', 'DONE', 'FAILED', 'IGNORED')",
            name="ew_commands_state_check",
        ),
    )
    op.create_table(
        "ew_outbox",
        sa.Column("namespace", sa.Text(), nullable=False),
        sa.Column("command_id", postgresql.UUID(), nullable=False),
        sa.Column(
            "generation",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "state",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'PENDING'"),
        ),
        sa.Column("claim_token", postgresql.UUID()),
        sa.Column("claim_until", sa.DateTime(timezone=True)),
        *_audit(),
        sa.PrimaryKeyConstraint(
            "namespace", "command_id", name="ew_outbox_pkey"
        ),
        sa.ForeignKeyConstraint(
            ["namespace", "command_id"],
            ["ew_commands.namespace", "ew_commands.command_id"],
            name="ew_outbox_namespace_command_id_fkey",
        ),
        sa.CheckConstraint(
            "state IN ('PENDING', 'CLAIMED', 'SENT')",
            name="ew_outbox_state_check",
        ),
    )
    op.create_table(
        "ew_audit",
        sa.Column(
            "id", sa.BigInteger(), sa.Identity(always=True), primary_key=True
        ),
        sa.Column("namespace", sa.Text(), nullable=False),
        sa.Column("command_id", postgresql.UUID(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        *_audit(),
    )
    op.create_index(
        "ew_commands_order",
        "ew_commands",
        ["namespace", "execution_id", "sequence"],
        postgresql_where=sa.text("state NOT IN ('DONE', 'IGNORED')"),
    )
    op.create_index(
        "ew_bindings_scan", "ew_bindings", ["namespace", "next_scan_at"]
    )
    op.create_index(
        "ew_inbox_unrouted",
        "ew_inbox",
        ["namespace", "execution_id", "sequence"],
        postgresql_where=sa.text("state = 'RECEIVED'"),
    )
    op.create_index(
        "ew_outbox_pending", "ew_outbox", ["namespace", "state", "claim_until"]
    )


def downgrade() -> None:
    if (
        context.get_x_argument(as_dictionary=True).get(
            "allow_worker_table_drop"
        )
        != "true"
    ):
        raise RuntimeError(
            "Initial downgrade deletes ALL worker namespaces and their data; "
            "backup first and pass -x allow_worker_table_drop=true explicitly"
        )
    for table in (
        "ew_outbox",
        "ew_commands",
        "ew_inbox",
        "ew_audit",
        "ew_bindings",
    ):
        op.drop_table(table)
