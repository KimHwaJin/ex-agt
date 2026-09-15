"""Explicit upgrade or an injected, locked empty-database bootstrap."""

import os
from pathlib import Path

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from d_test.agent_service.bootstrap.configuration import load_settings
from d_test.agent_service.bootstrap.event_loop import run_async
from d_test.agent_service.infrastructure.database.setup_locks import (
    acquire_management_lock,
)


def database_url() -> str:
    settings = context.config.attributes.get("agent_settings")
    value = (
        settings.database_url.get_secret_value()
        if settings is not None
        else os.environ.get("MANAGEMENT_DATABASE_URL")
    )
    if value is None:
        root = Path(__file__).resolve().parents[1]
        value = load_settings(root).database_url.get_secret_value()
    return value.replace("postgresql://", "postgresql+psycopg://", 1).replace(
        "postgres://", "postgresql+psycopg://", 1
    )


def configure(connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=None,
        version_table="management_alembic_version",
        version_table_schema="public",
    )
    with context.begin_transaction():
        context.run_migrations()


async def online() -> None:
    engine = create_async_engine(database_url(), poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            await acquire_management_lock(connection)
            await connection.run_sync(configure)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    context.configure(
        url=database_url(),
        literal_binds=True,
        version_table="management_alembic_version",
        version_table_schema="public",
    )
    with context.begin_transaction():
        context.run_migrations()
elif context.config.attributes.get("connection") is not None:
    configure(context.config.attributes["connection"])
else:
    run_async(online())
