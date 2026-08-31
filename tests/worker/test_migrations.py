from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from sqlalchemy import make_url

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "worker_migrations" / "alembic.ini"
TABLES = ["ew_bindings", "ew_inbox", "ew_commands", "ew_outbox", "ew_audit"]


async def alembic(*args: str, url: str | None) -> tuple[int, str]:
    environment = dict(os.environ)
    for name in ("EW_DATABASE_URL", "EW_REDIS_URL"):
        environment.pop(name, None)
    if url is not None:
        environment["EW_DATABASE_URL"] = url
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "alembic",
        "-c",
        str(CONFIG),
        *args,
        # Verify paths are relative to ini, not the process working directory.
        cwd=ROOT.parent,
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 30)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    assert process.returncode is not None
    return process.returncode, (stdout + stderr).decode()


async def test_offline_upgrade_needs_no_redis_or_langgraph():
    code, output = await alembic(
        "upgrade",
        "head",
        "--sql",
        url="postgresql://worker:p%25ssword@not-a-host/worker",
    )
    assert code == 0, output
    for table in TABLES:
        assert f"CREATE TABLE {table}" in output
    assert "CREATE TABLE ew_alembic_version" in output
    assert "ew_schema_migrations" not in output
    assert "CREATE TABLE checkpoints" not in output
    assert "p%25ssword" not in output


async def test_database_url_required_without_loading_worker_settings():
    code, output = await alembic("upgrade", "head", "--sql", url=None)
    assert code != 0
    assert "EW_DATABASE_URL is required" in output
    assert "redis_url" not in output


async def test_only_postgresql_driver_is_accepted():
    code, output = await alembic("upgrade", "head", "--sql", url="sqlite://")
    assert code != 0
    assert "require PostgreSQL" in output


async def test_revision_chain_is_independent():
    code, output = await alembic("heads", url=None)
    assert code == 0, output
    assert "ew_0001 (head)" in output


@pytest.fixture
async def migration_db():
    raw_url = os.environ.get("TEST_DATABASE_URL")
    if not raw_url:
        pytest.skip("Requires isolated PostgreSQL")  # ty: ignore
    assert raw_url is not None
    url = make_url(raw_url).set(drivername="postgresql")
    base = url.render_as_string(hide_password=False)
    # Owned, per-test schema: no existing database or user schema is dropped.
    schema = f"ew_migration_test_{uuid4().hex}"
    async with await psycopg.AsyncConnection.connect(base) as conn:
        await conn.execute(
            sql.SQL("CREATE SCHEMA {}").format(
                sql.Identifier(schema),
            )
        )
    scoped = url.set(query={**url.query, "options": f"-csearch_path={schema}"})
    try:
        yield scoped.render_as_string(hide_password=False), schema
    finally:
        async with await psycopg.AsyncConnection.connect(base) as conn:
            await conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(
                    sql.Identifier(schema),
                )
            )


async def catalog(url: str, schema: str) -> dict[str, list]:
    async with await psycopg.AsyncConnection.connect(url) as conn:
        columns = await conn.execute(
            """SELECT table_name,column_name,data_type,udt_name,is_nullable,
            column_default,is_identity,identity_generation
            FROM information_schema.columns WHERE table_schema=%s
            AND table_name=ANY(%s) ORDER BY table_name,column_name""",
            (schema, TABLES),
        )
        result = {"columns": await columns.fetchall()}
        constraints = await conn.execute(
            """SELECT t.relname,c.conname,c.contype,
            pg_get_constraintdef(c.oid) FROM pg_constraint c
            JOIN pg_class t ON t.oid=c.conrelid
            JOIN pg_namespace n ON n.oid=t.relnamespace
            WHERE n.nspname=%s AND t.relname=ANY(%s)
            ORDER BY t.relname,c.conname""",
            (schema, TABLES),
        )
        result["constraints"] = await constraints.fetchall()
        indexes = await conn.execute(
            """SELECT tablename,indexname,indexdef FROM pg_indexes
            WHERE schemaname=%s AND tablename=ANY(%s)
            ORDER BY tablename,indexname""",
            (schema, TABLES),
        )
        result["indexes"] = [
            (table, name, definition.replace(f"{schema}.", ""))
            for table, name, definition in await indexes.fetchall()
        ]
        return result


@pytest.mark.postgres
async def test_fresh_upgrade_and_repeat_preserve_data_and_host_version(
    migration_db,
):
    url, schema = migration_db
    async with await psycopg.AsyncConnection.connect(url) as conn:
        await conn.execute("CREATE TABLE alembic_version (version_num text)")
        await conn.execute("INSERT INTO alembic_version VALUES ('host-head')")
        await conn.execute("CREATE TABLE checkpoints (value text)")
    # Two deployment Jobs may start together; the DB lock serializes DDL.
    results = await asyncio.gather(
        alembic("upgrade", "head", url=url),
        alembic("upgrade", "head", url=url),
    )
    assert all(code == 0 for code, _ in results), results
    async with await psycopg.AsyncConnection.connect(url) as conn:
        await conn.execute(
            """INSERT INTO ew_bindings
            (namespace,execution_id,session_id,task_id,created_by,updated_by)
            VALUES ('keep',%s,'session','task','test','test')""",
            (uuid4(),),
        )
    before = await catalog(url, schema)
    code, output = await alembic("upgrade", "head", url=url)
    assert code == 0, output
    assert await catalog(url, schema) == before
    async with await psycopg.AsyncConnection.connect(url) as conn:
        cur = await conn.execute("SELECT version_num FROM ew_alembic_version")
        assert await cur.fetchall() == [("ew_0001",)]
        cur = await conn.execute("SELECT version_num FROM alembic_version")
        assert await cur.fetchall() == [("host-head",)]
        cur = await conn.execute("SELECT session_id FROM ew_bindings")
        assert await cur.fetchall() == [("session",)]
        await conn.execute("SELECT * FROM checkpoints")
        cur = await conn.execute("SELECT to_regclass('ew_schema_migrations')")
        assert await cur.fetchone() == (None,)


@pytest.mark.postgres
async def test_existing_schema_requires_explicit_verified_stamp(
    migration_db,
):
    url, schema = migration_db
    code, output = await alembic("upgrade", "head", url=url)
    assert code == 0, output
    expected = await catalog(url, schema)
    # Simulate an existing, verified schema without Alembic version metadata.
    # This is not a claim of parity with the deleted legacy schema.sql.
    async with await psycopg.AsyncConnection.connect(url) as conn:
        await conn.execute("DROP TABLE ew_alembic_version")
    assert await catalog(url, schema) == expected
    code, output = await alembic("upgrade", "head", url=url)
    assert code != 0
    assert "already exists" in output
    # Only after the full schema comparison above is stamping appropriate.
    code, output = await alembic("stamp", "ew_0001", url=url)
    assert code == 0, output
    code, output = await alembic("upgrade", "head", url=url)
    assert code == 0, output


@pytest.mark.postgres
async def test_initial_downgrade_requires_explicit_data_loss_opt_in(
    migration_db,
):
    url, schema = migration_db
    code, output = await alembic("upgrade", "head", url=url)
    assert code == 0, output
    before = await catalog(url, schema)
    code, output = await alembic("downgrade", "base", url=url)
    assert code != 0
    assert "allow_worker_table_drop=true" in output
    assert await catalog(url, schema) == before


@pytest.mark.postgres
async def test_partial_existing_schema_rolls_back_new_tables(migration_db):
    url, _ = migration_db
    async with await psycopg.AsyncConnection.connect(url) as conn:
        # The conflict occurs after ew_bindings would have been created.
        await conn.execute("CREATE TABLE ew_inbox (sentinel text)")
        await conn.execute("INSERT INTO ew_inbox VALUES ('keep')")
    code, output = await alembic("upgrade", "head", url=url)
    assert code != 0
    assert "already exists" in output
    async with await psycopg.AsyncConnection.connect(url) as conn:
        cur = await conn.execute("SELECT to_regclass('ew_bindings')")
        assert await cur.fetchone() == (None,)
        cur = await conn.execute("SELECT sentinel FROM ew_inbox")
        assert await cur.fetchone() == ("keep",)
