"""Immutable, versioned plan and exact cell-source snapshots."""

from alembic import op

revision = "management_0005"
down_revision = "management_0004"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE management.execution_plans (
            run_id uuid NOT NULL REFERENCES management.runs(run_id),
            plan_version integer NOT NULL CHECK (plan_version > 0),
            plan_id uuid NOT NULL,
            plan_sha256 text NOT NULL CHECK (length(plan_sha256) = 64),
            snapshot jsonb NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            created_by uuid NOT NULL REFERENCES management.users(user_uuid),
            updated_at timestamptz NOT NULL DEFAULT now(),
            updated_by uuid NOT NULL REFERENCES management.users(user_uuid),
            PRIMARY KEY (run_id, plan_version)
        )
    """)


def downgrade():
    op.execute("DROP TABLE management.execution_plans")
