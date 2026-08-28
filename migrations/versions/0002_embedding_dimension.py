"""Use the internal Qwen embedding vector dimension."""

from alembic import op

revision = "0002_embedding_dimension"
down_revision = "0001_agent_baseline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE agent_workflow_versions "
        "ALTER COLUMN embedding TYPE vector(1024)"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE agent_workflow_versions "
        "ALTER COLUMN embedding TYPE vector(1536)"
    )
