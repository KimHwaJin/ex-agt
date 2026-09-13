"""Fresh management schema, independent of the former Agent database."""

from alembic import op

revision = "management_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA management")
    op.execute("""
        CREATE TABLE management.users (
            user_uuid uuid PRIMARY KEY,
            user_id varchar(200) UNIQUE NOT NULL
                CHECK (length(btrim(user_id)) > 0),
            status varchar(16) NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'disabled')),
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            created_by uuid NOT NULL REFERENCES management.users(user_uuid),
            updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            updated_by uuid NOT NULL REFERENCES management.users(user_uuid)
        )
    """)
    op.execute("""
        CREATE TABLE management.projects (
            project_id uuid PRIMARY KEY,
            owner_user_uuid uuid NOT NULL
                REFERENCES management.users(user_uuid),
            name varchar(100) NOT NULL CHECK (length(btrim(name)) > 0),
            description varchar(2000),
            is_default boolean NOT NULL DEFAULT false,
            version integer NOT NULL DEFAULT 1 CHECK (version > 0),
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            created_by uuid NOT NULL REFERENCES management.users(user_uuid),
            updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            updated_by uuid NOT NULL REFERENCES management.users(user_uuid),
            deleted_at timestamptz,
            deleted_by uuid REFERENCES management.users(user_uuid),
            CHECK (NOT is_default OR deleted_at IS NULL),
            CHECK ((deleted_at IS NULL) = (deleted_by IS NULL))
        )
    """)
    op.execute("""
        CREATE UNIQUE INDEX projects_default_owner
        ON management.projects (owner_user_uuid)
        WHERE is_default AND deleted_at IS NULL
    """)
    op.execute("""
        CREATE INDEX projects_owner_page
        ON management.projects
            (owner_user_uuid, created_at DESC, project_id DESC)
        WHERE deleted_at IS NULL
    """)
    op.execute("""
        CREATE TABLE management.sessions (
            session_id uuid PRIMARY KEY,
            project_id uuid NOT NULL
                REFERENCES management.projects(project_id),
            title varchar(200) NOT NULL CHECK (length(btrim(title)) > 0),
            version integer NOT NULL DEFAULT 1 CHECK (version > 0),
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            created_by uuid NOT NULL REFERENCES management.users(user_uuid),
            updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            updated_by uuid NOT NULL REFERENCES management.users(user_uuid),
            deleted_at timestamptz,
            deleted_by uuid REFERENCES management.users(user_uuid),
            CHECK ((deleted_at IS NULL) = (deleted_by IS NULL))
        )
    """)
    op.execute("""
        CREATE INDEX sessions_project_page ON management.sessions
            (project_id, created_at DESC, session_id DESC)
        WHERE deleted_at IS NULL
    """)
    op.execute("""
        CREATE TABLE management.requests (
            user_uuid uuid NOT NULL REFERENCES management.users(user_uuid),
            scope varchar(100) NOT NULL,
            request_key varchar(200) NOT NULL,
            fingerprint char(64) NOT NULL,
            response jsonb,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            PRIMARY KEY (user_uuid, scope, request_key)
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE management.requests")
    op.execute("DROP TABLE management.sessions")
    op.execute("DROP TABLE management.projects")
    op.execute("DROP TABLE management.users")
    op.execute("DROP SCHEMA management")
