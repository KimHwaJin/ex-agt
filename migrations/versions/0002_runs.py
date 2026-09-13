"""Durable conversation runs, outputs, interrupts and replay events."""

from alembic import op

revision = "management_0002"
down_revision = "management_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE management.runs (
            run_id uuid PRIMARY KEY,
            session_id uuid NOT NULL REFERENCES management.sessions,
            project_id uuid NOT NULL REFERENCES management.projects,
            owner_user_uuid uuid NOT NULL REFERENCES management.users,
            status text NOT NULL CHECK (status IN (
                'queued', 'running', 'awaiting_input', 'waiting_execution',
                'cancelling', 'completed', 'rejected', 'failed', 'cancelled'
            )),
            backend text NOT NULL,
            input jsonb NOT NULL,
            checkpoint jsonb NOT NULL DEFAULT '{}',
            event_seq bigint NOT NULL DEFAULT 0,
            first_event_seq bigint NOT NULL DEFAULT 1,
            error jsonb,
            due_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            created_by uuid NOT NULL REFERENCES management.users,
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            updated_by uuid NOT NULL REFERENCES management.users,
            UNIQUE (run_id, session_id)
        )
    """)
    op.execute("""
        CREATE UNIQUE INDEX runs_one_active_session
        ON management.runs(session_id)
        WHERE status NOT IN ('completed', 'rejected', 'failed', 'cancelled')
    """)
    op.execute("""
        CREATE INDEX runs_session_page ON management.runs
            (session_id, created_at DESC, run_id DESC)
    """)
    op.execute("""
        CREATE INDEX runs_work ON management.runs(due_at, run_id)
        WHERE status IN (
            'queued', 'running', 'waiting_execution', 'cancelling'
        )
    """)
    op.execute("""
        ALTER TABLE management.sessions
        ADD COLUMN active_run_id uuid REFERENCES management.runs,
        ADD COLUMN is_locked boolean NOT NULL DEFAULT false,
        ADD COLUMN lock_reason text,
        ADD CONSTRAINT session_lock_owner CHECK (
            (NOT is_locked AND lock_reason IS NULL)
            OR (is_locked AND active_run_id IS NOT NULL
                AND lock_reason IS NOT NULL)
        )
    """)
    op.execute("""
        CREATE TABLE management.messages (
            message_id uuid PRIMARY KEY,
            session_id uuid NOT NULL,
            run_id uuid NOT NULL,
            role text NOT NULL CHECK (role IN ('user', 'assistant')),
            content jsonb NOT NULL,
            status text NOT NULL CHECK (
                status IN ('streaming', 'completed', 'interrupted', 'failed')
            ),
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            created_by uuid NOT NULL REFERENCES management.users,
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            updated_by uuid NOT NULL REFERENCES management.users,
            FOREIGN KEY (run_id, session_id)
                REFERENCES management.runs(run_id, session_id)
        )
    """)
    op.execute("""
        CREATE INDEX messages_session_page ON management.messages
            (session_id, created_at DESC, message_id DESC)
    """)
    op.execute("""
        CREATE TABLE management.run_interrupts (
            interrupt_id uuid PRIMARY KEY,
            run_id uuid NOT NULL REFERENCES management.runs,
            response_type text NOT NULL,
            schema_version integer NOT NULL,
            payload jsonb NOT NULL,
            status text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'answered', 'cancelled')),
            response jsonb,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            created_by uuid NOT NULL REFERENCES management.users,
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            updated_by uuid NOT NULL REFERENCES management.users
        )
    """)
    op.execute("""
        CREATE UNIQUE INDEX interrupts_one_pending
        ON management.run_interrupts(run_id) WHERE status = 'pending'
    """)
    op.execute("""
        CREATE TABLE management.run_executions (
            binding_id uuid PRIMARY KEY,
            run_id uuid NOT NULL REFERENCES management.runs,
            execution_id uuid UNIQUE,
            idempotency_key text UNIQUE NOT NULL,
            status text NOT NULL,
            simulated boolean NOT NULL DEFAULT false,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            created_by uuid NOT NULL REFERENCES management.users,
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            updated_by uuid NOT NULL REFERENCES management.users
        )
    """)
    op.execute("""
        CREATE INDEX executions_run ON management.run_executions(run_id)
    """)
    op.execute("""
        CREATE TABLE management.run_events (
            run_id uuid NOT NULL REFERENCES management.runs,
            sequence bigint NOT NULL,
            type text NOT NULL,
            data jsonb NOT NULL,
            source_key text,
            occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (run_id, sequence),
            UNIQUE (run_id, source_key)
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE management.run_events")
    op.execute("DROP TABLE management.run_executions")
    op.execute("DROP TABLE management.run_interrupts")
    op.execute("DROP TABLE management.messages")
    op.execute("""
        ALTER TABLE management.sessions
        DROP CONSTRAINT session_lock_owner,
        DROP COLUMN lock_reason,
        DROP COLUMN is_locked,
        DROP COLUMN active_run_id
    """)
    op.execute("DROP TABLE management.runs")
