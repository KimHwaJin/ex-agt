"""Add reviewed Workflow versions and lifecycle audit actions."""

from alembic import op

revision = "0005_workflow_lifecycle"
down_revision = "0004_workflow_promotion"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE agent_workflow_versions ADD COLUMN IF NOT EXISTS "
        "review_status varchar(32) NOT NULL DEFAULT 'APPROVED'"
    )
    op.execute(
        "ALTER TABLE agent_workflow_versions ADD COLUMN IF NOT EXISTS "
        "reviewed_by varchar(255)"
    )
    op.execute(
        "ALTER TABLE agent_workflow_versions ADD COLUMN IF NOT EXISTS "
        "reviewed_at timestamptz"
    )
    op.execute(
        "ALTER TABLE agent_workflow_versions ADD COLUMN IF NOT EXISTS "
        "review_reason text"
    )
    op.execute(
        "UPDATE agent_workflow_versions SET reviewed_by = promoted_by, "
        "reviewed_at = created_at WHERE review_status = 'APPROVED' "
        "AND reviewed_at IS NULL"
    )
    op.execute(
        "WITH ranked AS ("
        "SELECT id, row_number() OVER (PARTITION BY workflow_id "
        "ORDER BY version DESC) AS position "
        "FROM agent_workflow_versions WHERE active) "
        "UPDATE agent_workflow_versions AS versions SET active = false "
        "FROM ranked WHERE versions.id = ranked.id "
        "AND ranked.position > 1"
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS "
        "uq_agent_workflow_versions_active "
        "ON agent_workflow_versions (workflow_id) WHERE active"
    )
    op.execute(
        "CREATE TABLE IF NOT EXISTS agent_workflow_lifecycle_actions ("
        "id uuid PRIMARY KEY, "
        "workflow_id uuid NOT NULL REFERENCES agent_workflows(id) "
        "ON DELETE RESTRICT, "
        "workflow_version_id uuid REFERENCES agent_workflow_versions(id) "
        "ON DELETE RESTRICT, "
        "actor_user_id varchar(255) NOT NULL, "
        "action varchar(64) NOT NULL, "
        "idempotency_key varchar(255) NOT NULL, "
        "request_hash varchar(64) NOT NULL, "
        "reason text, "
        "policy_version varchar(128) NOT NULL, "
        "result_payload jsonb NOT NULL, "
        "created_at timestamptz NOT NULL DEFAULT now(), "
        "updated_at timestamptz NOT NULL DEFAULT now(), "
        "CONSTRAINT uq_agent_workflow_lifecycle_action_idem UNIQUE "
        "(idempotency_key))"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "ix_agent_workflow_lifecycle_actions_workflow_id "
        "ON agent_workflow_lifecycle_actions (workflow_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "ix_agent_workflow_lifecycle_actions_workflow_version_id "
        "ON agent_workflow_lifecycle_actions (workflow_version_id)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS agent_workflow_lifecycle_actions")
    op.execute("DROP INDEX IF EXISTS uq_agent_workflow_versions_active")
    for name in (
        "review_reason",
        "reviewed_at",
        "reviewed_by",
        "review_status",
    ):
        op.execute(
            f"ALTER TABLE agent_workflow_versions DROP COLUMN IF EXISTS {name}"
        )
