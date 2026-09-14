"""Bind public approval requests to exact LangGraph interrupts."""

from alembic import op

revision = "management_0004"
down_revision = "management_0003"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        "ALTER TABLE management.run_interrupts "
        "ADD COLUMN graph_interrupt_id text"
    )
    op.execute(
        "CREATE UNIQUE INDEX interrupts_graph_identity "
        "ON management.run_interrupts(run_id, graph_interrupt_id)"
    )


def downgrade():
    op.execute("DROP INDEX management.interrupts_graph_identity")
    op.execute(
        "ALTER TABLE management.run_interrupts DROP COLUMN graph_interrupt_id"
    )
