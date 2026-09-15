"""Opt-in disposable database tests; never clear an existing schema."""

import asyncio
import os
import sys
from time import monotonic
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import httpx
import pytest
from psycopg import AsyncConnection, sql
from pydantic import ValidationError

from d_test.agent_service.infrastructure.database.bootstrap import (
    BootstrapError,
    initialize_database,
)
from d_test.agent_service.infrastructure.database.setup_locks import (
    MANAGEMENT_LOCK,
)
from d_test.api_service import create_app


@pytest.fixture
async def fresh_database():
    admin_url = os.environ.get("MANAGEMENT_BOOTSTRAP_TEST_DATABASE_URL")
    if not admin_url:
        raise pytest.skip.Exception(
            "Set dedicated MANAGEMENT_BOOTSTRAP_TEST_DATABASE_URL"
        )
    parsed = urlsplit(admin_url)
    if parsed.scheme != "postgresql" or parsed.path != "/chatapp":
        raise RuntimeError("Bootstrap tests need an isolated chatapp server")
    created = []
    async with await AsyncConnection.connect(
        admin_url, autocommit=True
    ) as admin:

        async def create():
            name = "bootstrap_" + uuid4().hex
            await admin.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name))
            )
            created.append(name)
            return urlunsplit(parsed._replace(path="/" + name))

        try:
            yield create
        finally:
            for name in created:
                # Exact UUID database created by this fixture, never a schema
                # or database supplied by the caller.
                await admin.execute(
                    sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                        sql.Identifier(name)
                    )
                )


@pytest.fixture
async def bootstrap_settings(settings, fresh_database):
    return settings.model_copy(
        update={
            "database_url": settings.database_url.__class__(
                await fresh_database()
            ),
            "database_bootstrap": "initialize_if_empty",
        }
    )


async def query(settings, statement, params=()):
    async with await AsyncConnection.connect(
        settings.database_url.get_secret_value(), autocommit=True
    ) as connection:
        result = await connection.execute(statement, params)
        if result.description:
            return await result.fetchall()


async def test_off_does_not_read_files_or_connect(settings, monkeypatch):
    async def forbidden(_):
        raise AssertionError("Must not bootstrap")

    monkeypatch.setattr(
        "d_test.agent_service.infrastructure.database.bootstrap.bootstrap",
        forbidden,
    )
    await initialize_database(settings)


@pytest.mark.parametrize("schema", ["public", "management", "pg_catalog"])
def test_bootstrap_requires_dedicated_checkpoint_schema(settings, schema):
    with pytest.raises(ValidationError):
        settings.__class__.model_validate(
            {
                **settings.model_dump(),
                "database_bootstrap": "initialize_if_empty",
                "checkpoint_schema": schema,
            }
        )


async def test_missing_migration_files_fail_before_connect(settings):
    with pytest.raises(BootstrapError) as caught:
        await initialize_database(
            settings.model_copy(
                update={
                    "database_bootstrap": "initialize_if_empty",
                    "database_migration_config": "missing-alembic-file.ini",
                }
            )
        )
    assert caught.value.code == "MIGRATION_FILES_MISSING"


async def test_driver_error_does_not_expose_credentials(
    settings, monkeypatch, caplog
):
    async def broken(_):
        raise RuntimeError("secret-password-sql-parameters")

    monkeypatch.setattr(
        "d_test.agent_service.infrastructure.database.bootstrap.bootstrap",
        broken,
    )
    with pytest.raises(BootstrapError) as caught:
        await initialize_database(
            settings.model_copy(
                update={
                    "database_bootstrap": "initialize_if_empty",
                }
            )
        )
    assert caught.value.code == "BOOTSTRAP_FAILED"
    assert "secret-password" not in str(caught.value)
    assert "secret-password" not in caplog.text
    assert "database_bootstrap_failed" in caplog.text


@pytest.mark.postgres
async def test_concurrent_initialization_then_readonly_skip(
    bootstrap_settings, monkeypatch
):
    settings = bootstrap_settings
    await asyncio.gather(*(initialize_database(settings) for _ in range(4)))
    user = uuid4()
    await query(
        settings,
        "INSERT INTO management.users "
        "(user_uuid,user_id,created_by,updated_by) VALUES (%s,%s,%s,%s)",
        (user, "bootstrap-owner", user, user),
    )
    snapshot = await query(
        settings,
        "SELECT version_num,xmin::text,ctid::text "
        "FROM public.management_alembic_version",
    )
    versions = await query(
        settings,
        "SELECT v,xmin::text,ctid::text "
        "FROM agent_checkpoints.checkpoint_migrations ORDER BY v",
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("Current schema must not run DDL")

    monkeypatch.setattr(
        "d_test.agent_service.infrastructure.database.bootstrap.apply_migrations",
        forbidden,
    )
    monkeypatch.setattr(
        "d_test.agent_service.infrastructure.database.bootstrap."
        "AsyncPostgresSaver.setup",
        forbidden,
    )
    await initialize_database(settings)
    assert (
        await query(
            settings,
            "SELECT version_num,xmin::text,ctid::text "
            "FROM public.management_alembic_version",
        )
        == snapshot
    )
    assert (
        await query(
            settings,
            "SELECT v,xmin::text,ctid::text "
            "FROM agent_checkpoints.checkpoint_migrations ORDER BY v",
        )
        == versions
    )
    assert await query(
        settings,
        "SELECT user_id FROM management.users WHERE user_uuid=%s",
        (user,),
    ) == [("bootstrap-owner",)]


@pytest.mark.postgres
@pytest.mark.parametrize("revision", ["management_0004", "future_version"])
async def test_existing_revision_is_never_upgraded(
    bootstrap_settings, revision
):
    settings = bootstrap_settings
    await initialize_database(settings)
    await query(
        settings,
        "UPDATE public.management_alembic_version SET version_num=%s",
        (revision,),
    )
    with pytest.raises(BootstrapError) as caught:
        await initialize_database(settings)
    assert caught.value.code == "MIGRATION_REQUIRED"
    assert await query(
        settings, "SELECT version_num FROM public.management_alembic_version"
    ) == [(revision,)]


@pytest.mark.postgres
async def test_unmanaged_schema_does_not_initialize_other_schema(
    bootstrap_settings,
):
    settings = bootstrap_settings
    await query(settings, "CREATE SCHEMA management")
    with pytest.raises(BootstrapError) as caught:
        await initialize_database(settings)
    assert caught.value.code == "UNMANAGED_OR_PARTIAL_SCHEMA"
    assert await query(
        settings, "SELECT to_regnamespace('agent_checkpoints')"
    ) == [(None,)]


@pytest.mark.postgres
@pytest.mark.parametrize(
    "table",
    [
        "management.execution_plans",
        "agent_checkpoints.checkpoint_writes",
    ],
)
async def test_partial_schema_is_not_repaired(bootstrap_settings, table):
    settings = bootstrap_settings
    await initialize_database(settings)
    schema, name = table.split(".")
    await query(
        settings,
        sql.SQL("DROP TABLE {}.{}").format(
            sql.Identifier(schema), sql.Identifier(name)
        ),
    )
    with pytest.raises(BootstrapError) as caught:
        await initialize_database(settings)
    assert caught.value.code == "UNMANAGED_OR_PARTIAL_SCHEMA"
    assert await query(settings, "SELECT to_regclass(%s)", (table,)) == [
        (None,)
    ]


@pytest.mark.postgres
async def test_checkpoint_preflight_prevents_management_writes(
    bootstrap_settings, fresh_database
):
    settings = bootstrap_settings
    # Separate checkpoint DB, already present but on an old version.
    checkpoint = settings.model_copy(
        update={
            "database_url": settings.database_url.__class__(
                await fresh_database()
            )
        }
    )
    await initialize_database(checkpoint)
    await query(
        checkpoint,
        "DELETE FROM agent_checkpoints.checkpoint_migrations "
        "WHERE v=(SELECT max(v) FROM agent_checkpoints.checkpoint_migrations)",
    )
    configured = settings.model_copy(
        update={"checkpoint_database_url": checkpoint.database_url}
    )
    with pytest.raises(BootstrapError) as caught:
        await initialize_database(configured)
    assert caught.value.code == "MIGRATION_REQUIRED"
    assert caught.value.component == "checkpoints"
    assert await query(settings, "SELECT to_regnamespace('management')") == [
        (None,)
    ]


@pytest.mark.postgres
async def test_lock_timeout_releases_resources_and_can_retry(
    bootstrap_settings,
):
    settings = bootstrap_settings.model_copy(
        update={
            "database_bootstrap_timeout_seconds": 1,
        }
    )
    async with await AsyncConnection.connect(
        settings.database_url.get_secret_value(), autocommit=True
    ) as blocker:
        await blocker.execute(
            "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
            (MANAGEMENT_LOCK,),
        )
        started = monotonic()
        with pytest.raises(BootstrapError) as caught:
            await initialize_database(settings)
        assert caught.value.code == "BOOTSTRAP_TIMEOUT"
        assert monotonic() - started < 10
    await initialize_database(bootstrap_settings)


@pytest.mark.postgres
async def test_lifespan_initializes_before_pool_checks(bootstrap_settings):
    app = create_app(bootstrap_settings)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.get("/health/ready")).status_code == 200
            response = await client.post(
                "/api/v1/me", headers={"X-User-Id": "bootstrap-api"}
            )
            assert response.status_code == 200
    assert not hasattr(app.state, "management_runtime")


@pytest.mark.postgres
async def test_failed_management_migration_rolls_back(
    bootstrap_settings, monkeypatch
):
    from sqlalchemy import text

    def broken(connection, _config):
        connection.execute(text("CREATE SCHEMA management"))
        raise RuntimeError("simulated DDL failure")

    with monkeypatch.context() as patch:
        patch.setattr(
            "d_test.agent_service.infrastructure.database.bootstrap."
            "apply_migrations",
            broken,
        )
        with pytest.raises(BootstrapError):
            await initialize_database(bootstrap_settings)
    assert await query(
        bootstrap_settings, "SELECT to_regnamespace('management')"
    ) == [(None,)]
    assert await query(
        bootstrap_settings, "SELECT to_regnamespace('agent_checkpoints')"
    ) == [(None,)]
    await initialize_database(bootstrap_settings)


@pytest.mark.postgres
async def test_partial_checkpoint_failure_blocks_next_boot(
    bootstrap_settings, monkeypatch
):
    async def broken(_saver):
        raise RuntimeError("simulated checkpoint setup failure")

    with monkeypatch.context() as patch:
        patch.setattr(
            "d_test.agent_service.infrastructure.database.bootstrap."
            "AsyncPostgresSaver.setup",
            broken,
        )
        with pytest.raises(BootstrapError) as caught:
            await initialize_database(bootstrap_settings)
        assert caught.value.code == "BOOTSTRAP_FAILED"
    # Management committed, checkpoint schema exists but is not initialized.
    with pytest.raises(BootstrapError) as caught:
        await initialize_database(bootstrap_settings)
    assert caught.value.code == "UNMANAGED_OR_PARTIAL_SCHEMA"
    assert caught.value.component == "checkpoints"


@pytest.mark.postgres
async def test_separate_processes_initialize_same_database(bootstrap_settings):
    script = """
import asyncio
import os
from d_test.agent_service.settings import Settings
from d_test.agent_service.infrastructure.database.bootstrap import (
    initialize_database,
)
settings = Settings(
    database_url=os.environ['BOOTSTRAP_CHILD_DATABASE_URL'],
    cursor_secret='test-process-cursor-secret-0123456789',
    database_bootstrap='initialize_if_empty',
)
asyncio.run(initialize_database(settings))
"""
    children = []
    try:
        for _ in range(3):
            children.append(
                await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-c",
                    script,
                    env={
                        **os.environ,
                        "BOOTSTRAP_CHILD_DATABASE_URL": (
                            bootstrap_settings.database_url.get_secret_value()
                        ),
                    },
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
            )
        async with asyncio.timeout(30):
            outputs = await asyncio.gather(
                *(child.communicate() for child in children)
            )
        for child, (_, stderr) in zip(children, outputs, strict=True):
            assert child.returncode == 0, stderr.decode()
        await initialize_database(bootstrap_settings)
    finally:
        for child in children:
            if child.returncode is None:
                child.kill()
            await child.wait()
