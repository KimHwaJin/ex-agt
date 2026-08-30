"""Add immutable Workflow promotion lineage and input templates."""

from alembic import op

revision = "0004_workflow_promotion"
down_revision = "0003_delivery_performance"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE agent_workflows ADD COLUMN IF NOT EXISTS "
        "owner_user_id varchar(255)"
    )
    op.execute(
        "ALTER TABLE agent_workflows ADD COLUMN IF NOT EXISTS "
        "owner_project_id varchar(255)"
    )
    op.execute(
        "ALTER TABLE agent_workflows ADD COLUMN IF NOT EXISTS "
        "status varchar(32) NOT NULL DEFAULT 'ACTIVE'"
    )
    op.execute(
        "ALTER TABLE agent_workflows ADD COLUMN IF NOT EXISTS "
        "latest_version integer NOT NULL DEFAULT 1"
    )
    op.execute(
        "ALTER TABLE agent_workflows ADD COLUMN IF NOT EXISTS "
        "access_policy jsonb NOT NULL DEFAULT '{}'::jsonb"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_agent_workflows_owner_user_id "
        "ON agent_workflows (owner_user_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_agent_workflows_owner_project_id "
        "ON agent_workflows (owner_project_id)"
    )
    columns = [
        "source_task_id uuid",
        "source_plan_id uuid",
        "source_plan_revision_id uuid",
        "source_execution_id uuid",
        "objective text",
        "strategy_summary text",
        "runtime_profile varchar(128)",
        "input_contract jsonb NOT NULL DEFAULT '{}'::jsonb",
        "output_contract jsonb NOT NULL DEFAULT '{}'::jsonb",
        "tool_registry_snapshot_hash varchar(64)",
        "searchable_text text",
        "searchable_text_hash varchar(64)",
        "embedding_model varchar(255)",
        "embedding_dimension integer",
        "request_examples jsonb NOT NULL DEFAULT '[]'::jsonb",
        "tags jsonb NOT NULL DEFAULT '[]'::jsonb",
        "promotion_policy_version varchar(128)",
    ]
    for definition in columns:
        op.execute(
            "ALTER TABLE agent_workflow_versions ADD COLUMN IF NOT EXISTS "
            f"{definition}"
        )
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "ix_agent_workflow_versions_source_task_id "
        "ON agent_workflow_versions (source_task_id)"
    )
    op.execute(
        "CREATE TABLE IF NOT EXISTS agent_workflow_steps ("
        "id uuid PRIMARY KEY, "
        "workflow_version_id uuid NOT NULL REFERENCES "
        "agent_workflow_versions(id) ON DELETE CASCADE, "
        "source_plan_revision_id uuid, "
        "sequence integer NOT NULL, "
        "skill_ref jsonb NOT NULL, "
        "tool_ref jsonb NOT NULL, "
        "purpose text NOT NULL, "
        "selection_rationale text NOT NULL, "
        "parameter_template jsonb NOT NULL, "
        "expected_outputs jsonb NOT NULL DEFAULT '[]'::jsonb, "
        "validation_criteria jsonb NOT NULL DEFAULT '[]'::jsonb, "
        "timeout_seconds integer NOT NULL, "
        "CONSTRAINT uq_agent_workflow_step_sequence UNIQUE "
        "(workflow_version_id, sequence))"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "ix_agent_workflow_steps_workflow_version_id "
        "ON agent_workflow_steps (workflow_version_id)"
    )
    op.execute(
        "CREATE TABLE IF NOT EXISTS agent_successful_execution_steps ("
        "id uuid PRIMARY KEY, "
        "task_id uuid NOT NULL REFERENCES agent_tasks(id) ON DELETE CASCADE, "
        "execution_sequence integer NOT NULL, "
        "operation_id uuid NOT NULL, "
        "source_plan_id uuid NOT NULL, "
        "source_plan_revision_id uuid NOT NULL, "
        "registry_snapshot_hash varchar(64) NOT NULL, "
        "step_payload jsonb NOT NULL, "
        "step_payload_hash varchar(64) NOT NULL, "
        "created_at timestamptz NOT NULL DEFAULT now(), "
        "updated_at timestamptz NOT NULL DEFAULT now(), "
        "CONSTRAINT uq_agent_successful_execution_step UNIQUE "
        "(task_id, execution_sequence))"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "ix_agent_successful_execution_steps_task_id "
        "ON agent_successful_execution_steps (task_id)"
    )
    op.execute(
        "CREATE TABLE IF NOT EXISTS agent_workflow_promotions ("
        "id uuid PRIMARY KEY, "
        "task_id uuid NOT NULL REFERENCES agent_tasks(id) ON DELETE CASCADE, "
        "actor_user_id varchar(255) NOT NULL, "
        "idempotency_key varchar(255) NOT NULL, "
        "request_hash varchar(64) NOT NULL, "
        "workflow_id uuid NOT NULL REFERENCES agent_workflows(id) "
        "ON DELETE RESTRICT, "
        "workflow_version_id uuid NOT NULL REFERENCES "
        "agent_workflow_versions(id) ON DELETE RESTRICT, "
        "policy_version varchar(128) NOT NULL, "
        "created_at timestamptz NOT NULL DEFAULT now(), "
        "updated_at timestamptz NOT NULL DEFAULT now(), "
        "CONSTRAINT uq_agent_workflow_promotion_idem UNIQUE "
        "(idempotency_key))"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_agent_workflow_promotions_task_id "
        "ON agent_workflow_promotions (task_id)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS agent_workflow_promotions")
    op.execute("DROP TABLE IF EXISTS agent_successful_execution_steps")
    op.execute("DROP TABLE IF EXISTS agent_workflow_steps")
    op.execute(
        "DROP INDEX IF EXISTS ix_agent_workflow_versions_source_task_id"
    )
    for name in (
        "promotion_policy_version",
        "tags",
        "request_examples",
        "embedding_dimension",
        "embedding_model",
        "searchable_text_hash",
        "searchable_text",
        "tool_registry_snapshot_hash",
        "output_contract",
        "input_contract",
        "runtime_profile",
        "strategy_summary",
        "objective",
        "source_execution_id",
        "source_plan_revision_id",
        "source_plan_id",
        "source_task_id",
    ):
        op.execute(
            f"ALTER TABLE agent_workflow_versions DROP COLUMN IF EXISTS {name}"
        )
    op.execute("DROP INDEX IF EXISTS ix_agent_workflows_owner_project_id")
    op.execute("DROP INDEX IF EXISTS ix_agent_workflows_owner_user_id")
    for name in (
        "access_policy",
        "latest_version",
        "status",
        "owner_project_id",
        "owner_user_id",
    ):
        op.execute(f"ALTER TABLE agent_workflows DROP COLUMN IF EXISTS {name}")
