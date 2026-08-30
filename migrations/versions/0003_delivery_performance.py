"""Add durable Redis delivery and query indexes."""

from alembic import op

revision = "0003_delivery_performance"
down_revision = "0002_embedding_dimension"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE agent_workflow_commands "
        "ADD COLUMN IF NOT EXISTS publish_claimed_at timestamptz"
    )
    op.execute(
        "ALTER TABLE agent_task_events ADD COLUMN IF NOT EXISTS "
        "delivery_state varchar(32) NOT NULL DEFAULT 'PENDING'"
    )
    op.execute(
        "ALTER TABLE agent_task_events ADD COLUMN IF NOT EXISTS "
        "delivery_attempt_count integer NOT NULL DEFAULT 0"
    )
    op.execute(
        "ALTER TABLE agent_task_events ADD COLUMN IF NOT EXISTS "
        "delivery_last_error text"
    )
    op.execute(
        "ALTER TABLE agent_task_events ADD COLUMN IF NOT EXISTS "
        "delivery_claimed_at timestamptz"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_agent_task_events_task_id_id "
        "ON agent_task_events (task_id, id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "ix_agent_task_events_delivery_state_id "
        "ON agent_task_events (delivery_state, id)"
    )
    op.execute("DROP INDEX IF EXISTS ix_agent_task_events_task_id")


def downgrade() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_agent_task_events_task_id "
        "ON agent_task_events (task_id)"
    )
    op.execute("DROP INDEX IF EXISTS ix_agent_task_events_delivery_state_id")
    op.execute("DROP INDEX IF EXISTS ix_agent_task_events_task_id_id")
    op.execute(
        "ALTER TABLE agent_task_events DROP COLUMN IF EXISTS "
        "delivery_claimed_at"
    )
    op.execute(
        "ALTER TABLE agent_task_events DROP COLUMN IF EXISTS "
        "delivery_last_error"
    )
    op.execute(
        "ALTER TABLE agent_task_events DROP COLUMN IF EXISTS "
        "delivery_attempt_count"
    )
    op.execute(
        "ALTER TABLE agent_task_events DROP COLUMN IF EXISTS delivery_state"
    )
    op.execute(
        "ALTER TABLE agent_workflow_commands DROP COLUMN IF EXISTS "
        "publish_claimed_at"
    )
