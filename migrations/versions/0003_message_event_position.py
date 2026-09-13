"""Persist projection positions for snapshot plus stream reconciliation."""

from alembic import op

revision = "management_0003"
down_revision = "management_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE management.messages
        ADD COLUMN event_sequence bigint NOT NULL DEFAULT 0
    """)
    op.execute("""
        UPDATE management.messages m SET event_sequence = r.event_seq
        FROM management.runs r WHERE r.run_id = m.run_id
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE management.messages DROP COLUMN event_sequence")
