"""Opt-in empty-schema initialization; never upgrade existing schemas."""

import asyncio
import logging
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg import AsyncConnection, sql
from psycopg.rows import DictRow, dict_row
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from d_test.agent_service.infrastructure.database.setup_locks import (
    acquire_checkpoint_lock,
    acquire_management_lock,
)
from d_test.agent_service.settings import Settings

logger = logging.getLogger("agent_service.database")
MANAGEMENT_COLUMNS = {
    "users": {"user_uuid", "user_id"},
    "projects": {"project_id", "owner_user_uuid"},
    "sessions": {"session_id", "active_run_id", "is_locked", "lock_reason"},
    "requests": {"user_uuid", "scope", "request_key"},
    "runs": {"run_id", "checkpoint", "event_seq"},
    "messages": {"message_id", "event_sequence"},
    "run_interrupts": {"interrupt_id", "graph_interrupt_id"},
    "run_executions": {"execution_id", "run_id"},
    "run_events": {"run_id", "sequence"},
    "execution_plans": {"run_id", "plan_version", "snapshot"},
}
CHECKPOINT_COLUMNS = {
    "checkpoints": {
        "thread_id",
        "checkpoint_ns",
        "checkpoint_id",
        "parent_checkpoint_id",
        "type",
        "checkpoint",
        "metadata",
    },
    "checkpoint_blobs": {
        "thread_id",
        "checkpoint_ns",
        "channel",
        "version",
        "type",
        "blob",
    },
    "checkpoint_writes": {
        "thread_id",
        "checkpoint_ns",
        "checkpoint_id",
        "task_id",
        "idx",
        "channel",
        "type",
        "blob",
        "task_path",
    },
}


class BootstrapError(RuntimeError):
    """Safe startup error: never include connection strings or SQL values."""

    def __init__(self, code: str, component: str):
        self.code = code
        self.component = component
        super().__init__(
            f"{code}: {component}; DB 상태·권한과 migration을 확인하세요."
        )


def migration_config(settings: Settings) -> Config:
    path = Path(settings.database_migration_config).resolve()
    if not path.is_file():
        raise BootstrapError("MIGRATION_FILES_MISSING", "management")
    config = Config(str(path))
    config.attributes["agent_settings"] = settings
    if not ScriptDirectory.from_config(config).get_heads():
        raise BootstrapError("MIGRATION_FILES_MISSING", "management")
    return config


async def management_empty(connection, config: Config) -> bool:
    schema = await connection.scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace "
            "WHERE nspname='management')"
        )
    )
    version = await connection.scalar(
        text("SELECT to_regclass('public.management_alembic_version')")
    )
    if not schema and version is None:
        return True
    if not schema or version is None:
        raise BootstrapError("UNMANAGED_OR_PARTIAL_SCHEMA", "management")
    revisions = set(
        (
            await connection.execute(
                text(
                    "SELECT version_num FROM public.management_alembic_version"
                )
            )
        ).scalars()
    )
    if revisions != set(ScriptDirectory.from_config(config).get_heads()):
        raise BootstrapError("MIGRATION_REQUIRED", "management")
    rows = await connection.execute(
        text(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema='management'"
        )
    )
    check_columns(rows, MANAGEMENT_COLUMNS, "management")
    return False


def check_columns(rows, required, component):
    actual: dict[str, set[str]] = {}
    for table, column in rows:
        actual.setdefault(table, set()).add(column)
    if any(
        not columns <= actual.get(table, set())
        for table, columns in required.items()
    ):
        raise BootstrapError("UNMANAGED_OR_PARTIAL_SCHEMA", component)


async def checkpoint_empty(connection, schema: str) -> bool:
    row = await (
        await connection.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace "
            "WHERE nspname=%s) AS ok",
            (schema,),
        )
    ).fetchone()
    if not row["ok"]:
        return True
    row = await (
        await connection.execute(
            "SELECT to_regclass(%s) AS version",
            (f"{schema}.checkpoint_migrations",),
        )
    ).fetchone()
    if row["version"] is None:
        raise BootstrapError("UNMANAGED_OR_PARTIAL_SCHEMA", "checkpoints")
    rows = await (
        await connection.execute(
            sql.SQL(
                "SELECT v FROM {}.checkpoint_migrations ORDER BY v"
            ).format(sql.Identifier(schema))
        )
    ).fetchall()
    if [row["v"] for row in rows] != list(
        range(len(AsyncPostgresSaver.MIGRATIONS))
    ):
        raise BootstrapError("MIGRATION_REQUIRED", "checkpoints")
    rows = await (
        await connection.execute(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema=%s",
            (schema,),
        )
    ).fetchall()
    check_columns(
        ((row["table_name"], row["column_name"]) for row in rows),
        CHECKPOINT_COLUMNS,
        "checkpoints",
    )
    return False


def apply_migrations(connection, config):
    config.attributes["connection"] = connection
    try:
        command.upgrade(config, "head")
    finally:
        config.attributes.pop("connection", None)


async def bootstrap(settings: Settings) -> None:
    config = await asyncio.to_thread(migration_config, settings)
    url = (
        settings.database_url.get_secret_value()
        .replace("postgresql://", "postgresql+psycopg://", 1)
        .replace("postgres://", "postgresql+psycopg://", 1)
    )
    engine = create_async_engine(
        url,
        poolclass=NullPool,
        hide_parameters=True,
        connect_args={"connect_timeout": 5},
    )
    try:
        async with engine.connect() as management:
            # Session lock stays held across migration commit. NullPool closes
            # this exact connection on every exit, releasing the lock.
            await acquire_management_lock(management)
            checkpoint_url = (
                settings.checkpoint_database_url or settings.database_url
            ).get_secret_value()
            async with await AsyncConnection[DictRow].connect(
                checkpoint_url,
                autocommit=True,
                row_factory=dict_row,
                connect_timeout=5,
            ) as checkpoints:
                schema = settings.checkpoint_schema
                await acquire_checkpoint_lock(checkpoints, schema)
                # Preflight BOTH databases before performing any DDL.
                create_management = await management_empty(management, config)
                create_checkpoints = await checkpoint_empty(
                    checkpoints, schema
                )
                await management.commit()
                if create_management:
                    async with management.begin():
                        await management.run_sync(apply_migrations, config)
                    logger.info("database_initialized component=management")
                else:
                    logger.info("database_current component=management")
                if create_checkpoints:
                    await checkpoints.execute(
                        sql.SQL("CREATE SCHEMA {}").format(
                            sql.Identifier(schema)
                        )
                    )
                    await checkpoints.execute(
                        sql.SQL("SET search_path TO {}").format(
                            sql.Identifier(schema)
                        )
                    )
                    # Library migrations include CREATE INDEX CONCURRENTLY:
                    # use autocommit, not a surrounding SQL transaction.
                    await AsyncPostgresSaver(checkpoints).setup()
                    logger.info("database_initialized component=checkpoints")
                else:
                    logger.info("database_current component=checkpoints")
                await management_empty(management, config)
                await checkpoint_empty(checkpoints, schema)
    finally:
        await engine.dispose()


async def initialize_database(settings: Settings) -> None:
    if settings.database_bootstrap == "off":
        return
    logger.info("database_bootstrap_started")
    try:
        async with asyncio.timeout(
            settings.database_bootstrap_timeout_seconds
        ):
            await bootstrap(settings)
    except BootstrapError as error:
        logger.error(
            "database_bootstrap_failed code=%s component=%s",
            error.code,
            error.component,
        )
        raise
    except TimeoutError:
        logger.error("database_bootstrap_failed code=BOOTSTRAP_TIMEOUT")
        raise BootstrapError("BOOTSTRAP_TIMEOUT", "database") from None
    except Exception as error:
        # Detailed driver exceptions can carry SQL parameters or credentials.
        logger.error(
            "database_bootstrap_failed code=BOOTSTRAP_FAILED error_type=%s",
            type(error).__name__,
        )
        raise BootstrapError("BOOTSTRAP_FAILED", "database") from None
