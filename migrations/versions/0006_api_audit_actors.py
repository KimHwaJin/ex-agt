"""Add API resource creator and updater actor fields."""

from alembic import op

revision = "0006_api_audit_actors"
down_revision = "0005_workflow_lifecycle"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table in (
        "agent_tasks",
        "agent_workflows",
        "agent_workflow_versions",
    ):
        op.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
            "created_by varchar(255)"
        )
        op.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
            "updated_by varchar(255)"
        )

    op.execute(
        "UPDATE agent_tasks SET "
        "created_by = COALESCE(created_by, user_id), "
        "updated_by = COALESCE(updated_by, user_id)"
    )
    op.execute(
        "UPDATE agent_workflows AS workflows SET "
        "created_by = COALESCE(workflows.created_by, "
        "workflows.owner_user_id, (SELECT versions.promoted_by "
        "FROM agent_workflow_versions AS versions "
        "WHERE versions.workflow_id = workflows.id "
        "ORDER BY versions.version LIMIT 1), 'SYSTEM'), "
        "updated_by = COALESCE(workflows.updated_by, "
        "workflows.owner_user_id, (SELECT versions.promoted_by "
        "FROM agent_workflow_versions AS versions "
        "WHERE versions.workflow_id = workflows.id AND versions.active "
        "ORDER BY versions.version DESC LIMIT 1), (SELECT versions.promoted_by "
        "FROM agent_workflow_versions AS versions "
        "WHERE versions.workflow_id = workflows.id "
        "ORDER BY versions.version LIMIT 1), 'SYSTEM')"
    )
    op.execute(
        "UPDATE agent_workflows SET "
        "created_by = COALESCE(created_by, owner_user_id, 'SYSTEM'), "
        "updated_by = COALESCE(updated_by, owner_user_id, 'SYSTEM')"
    )
    op.execute(
        "UPDATE agent_workflow_versions SET "
        "created_by = COALESCE(created_by, promoted_by, 'SYSTEM'), "
        "updated_by = COALESCE(updated_by, reviewed_by, promoted_by, "
        "'SYSTEM')"
    )

    for table in (
        "agent_tasks",
        "agent_workflows",
        "agent_workflow_versions",
    ):
        for column in ("created_by", "updated_by"):
            op.execute(
                f"ALTER TABLE {table} ALTER COLUMN {column} "
                "SET DEFAULT 'SYSTEM'"
            )
            op.execute(
                f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"
            )


def downgrade() -> None:
    for table in (
        "agent_workflow_versions",
        "agent_workflows",
        "agent_tasks",
    ):
        op.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS updated_by")
        op.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS created_by")
