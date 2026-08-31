import asyncio
import os
import sys
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from sqlalchemy import make_url

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        "TEST_DATABASE_URL" not in os.environ,
        reason="Requires isolated PostgreSQL",
    ),
]


async def migrate(url, target):
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "alembic",
        "upgrade",
        target,
        env={**os.environ, "AGENT_DATABASE_URL": url},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 30)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    assert process.returncode == 0, (stdout + stderr).decode()


async def test_upgrade_from_previous_head_preserves_task_data():
    url = make_url(os.environ["TEST_DATABASE_URL"])
    admin = url.set(drivername="postgresql").render_as_string(
        hide_password=False
    )
    # Only a newly created fixture-owned database is removed in finally.
    database = f"agent_effects_test_{uuid4().hex}"
    async with await psycopg.AsyncConnection.connect(
        admin,
        autocommit=True,
    ) as conn:
        await conn.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database))
        )
    scoped = url.set(database=database)
    db_url = scoped.render_as_string(hide_password=False)
    driver_url = scoped.set(drivername="postgresql").render_as_string(
        hide_password=False
    )
    try:
        await migrate(db_url, "0006_api_audit_actors")
        task_id = uuid4()
        async with await psycopg.AsyncConnection.connect(driver_url) as conn:
            cursor = await conn.execute(
                "SELECT to_regclass('agent_executor_effects')"
            )
            assert await cursor.fetchone() == (None,)
            await conn.execute(
                """INSERT INTO agent_tasks
                (id, user_id, project_id, session_id, input_message_id,
                 user_message, status, version, created_by, updated_by)
                VALUES (%s,'user','project','session',%s,'keep','ACCEPTED',
                        1,'user','user')""",
                (task_id, uuid4()),
            )
        await migrate(db_url, "0007_executor_effects")
        async with await psycopg.AsyncConnection.connect(driver_url) as conn:
            cursor = await conn.execute(
                "SELECT to_regclass('agent_api_requests')"
            )
            assert await cursor.fetchone() == (None,)
        await migrate(db_url, "head")
        await migrate(db_url, "head")
        async with await psycopg.AsyncConnection.connect(driver_url) as conn:
            cursor = await conn.execute(
                "SELECT user_message FROM agent_tasks WHERE id=%s", (task_id,)
            )
            assert await cursor.fetchone() == ("keep",)
            cursor = await conn.execute(
                "SELECT count(*) FROM agent_executor_effects"
            )
            assert await cursor.fetchone() == (0,)
            cursor = await conn.execute(
                "SELECT version_num FROM alembic_version"
            )
            assert await cursor.fetchone() == ("0008_api_requests",)
            cursor = await conn.execute(
                "SELECT count(*) FROM agent_api_requests"
            )
            assert await cursor.fetchone() == (0,)
    finally:
        async with await psycopg.AsyncConnection.connect(
            admin,
            autocommit=True,
        ) as conn:
            await conn.execute(
                sql.SQL("DROP DATABASE {}").format(sql.Identifier(database))
            )
