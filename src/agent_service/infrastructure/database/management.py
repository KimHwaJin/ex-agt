"""Parameterized SQL and transaction boundaries; isolated management schema."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, LiteralString
from uuid import UUID, uuid4

from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from agent_service.domain.management import DomainError, not_found
from agent_service.settings import Settings

Pool = AsyncConnectionPool[AsyncConnection[DictRow]]

PROJECT_SELECT: LiteralString = """
SELECT p.*, p.owner_user_uuid AS user_uuid, u.user_id
FROM management.projects p
JOIN management.users u ON u.user_uuid = p.owner_user_uuid
"""
SESSION_SELECT: LiteralString = """
SELECT s.* FROM management.sessions s
JOIN management.projects p ON p.project_id = s.project_id
"""


def create_pool(settings: Settings) -> Pool:
    return Pool(
        settings.database_url.get_secret_value(),
        connection_class=AsyncConnection[DictRow],
        open=False,
        min_size=settings.pool_min_size,
        max_size=settings.pool_max_size,
        timeout=settings.pool_timeout,
        kwargs={
            "autocommit": True,
            "row_factory": dict_row,
            "connect_timeout": 5,
        },
        check=Pool.check_connection,
    )


class Store:
    def __init__(self, pool: Pool):
        self.pool = pool

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator["Repository"]:
        async with self.pool.connection() as connection:
            async with connection.transaction():
                yield Repository(connection)


class Repository:
    def __init__(self, connection: AsyncConnection[DictRow]):
        self.connection = connection

    async def one(
        self, statement: LiteralString, parameters: tuple = ()
    ) -> dict[str, Any] | None:
        cursor = await self.connection.execute(statement, parameters)
        return await cursor.fetchone()

    async def all(
        self, statement: LiteralString, parameters: tuple
    ) -> list[dict[str, Any]]:
        cursor = await self.connection.execute(statement, parameters)
        return await cursor.fetchall()

    async def provision_user(self, user_id: str) -> dict[str, Any]:
        candidate = uuid4()
        await self.connection.execute(
            """
            INSERT INTO management.users
                (user_uuid, user_id, created_by, updated_by)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING
            """,
            (candidate, user_id, candidate, candidate),
        )
        row = await self.one(
            """
            SELECT * FROM management.users WHERE user_id = %s FOR UPDATE
            """,
            (user_id,),
        )
        assert row is not None
        return row

    async def user(self, user_uuid: UUID) -> dict[str, Any]:
        row = await self.one(
            """
            SELECT * FROM management.users
            WHERE user_uuid = %s FOR SHARE
            """,
            (user_uuid,),
        )
        if row is None:
            raise DomainError(
                "USER_NOT_INITIALIZED", "먼저 POST /me를 호출해 주세요.", 403
            )
        return row

    async def default_project(self, owner: UUID) -> dict[str, Any]:
        await self.connection.execute(
            """
            INSERT INTO management.projects
                (project_id, owner_user_uuid, name, is_default,
                 created_by, updated_by)
            VALUES (%s, %s, '기본 프로젝트', true, %s, %s)
            ON CONFLICT (owner_user_uuid)
                WHERE is_default AND deleted_at IS NULL
            DO NOTHING
            """,
            (uuid4(), owner, owner, owner),
        )
        return await self.get_default_project(owner)

    async def get_default_project(self, owner: UUID) -> dict[str, Any]:
        """Read the default project without provisioning or repairing it."""
        row = await self.one(
            PROJECT_SELECT
            + """
            WHERE p.owner_user_uuid = %s AND p.is_default
                AND p.deleted_at IS NULL
            """,
            (owner,),
        )
        if row is None:
            raise DomainError(
                "DEFAULT_PROJECT_NOT_FOUND",
                "기본 프로젝트가 없습니다. POST /me를 호출해 주세요.",
                404,
            )
        return row

    async def project(
        self,
        owner: UUID,
        project_id: UUID,
        *,
        deleted: bool = False,
        lock: bool = False,
    ) -> dict[str, Any]:
        predicate = "" if deleted else " AND p.deleted_at IS NULL"
        locking = " FOR UPDATE OF p" if lock else " FOR SHARE OF p"
        row = await self.one(
            PROJECT_SELECT
            + """
            WHERE p.owner_user_uuid = %s AND p.project_id = %s
            """
            + predicate
            + locking,
            (owner, project_id),
        )
        if row is None:
            raise not_found()
        return row

    async def projects(
        self,
        owner: UUID,
        limit: int,
        boundary: tuple[datetime, UUID] | None,
    ) -> list[dict[str, Any]]:
        predicate = ""
        parameters: tuple = (owner,)
        if boundary:
            predicate = " AND (p.created_at, p.project_id) < (%s, %s)"
            parameters += boundary
        return await self.all(
            PROJECT_SELECT
            + """
            WHERE p.owner_user_uuid = %s AND p.deleted_at IS NULL
            """
            + predicate
            + """
            ORDER BY p.created_at DESC, p.project_id DESC LIMIT %s
            """,
            (*parameters, limit),
        )

    async def insert_project(
        self, owner: UUID, name: str, description: str | None
    ) -> UUID:
        project_id = uuid4()
        await self.connection.execute(
            """
            INSERT INTO management.projects
                (project_id, owner_user_uuid, name, description,
                 created_by, updated_by)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (project_id, owner, name, description, owner, owner),
        )
        return project_id

    async def update_project(
        self,
        owner: UUID,
        project_id: UUID,
        version: int,
        name: str,
        description: str | None,
    ) -> None:
        row = await self.one(
            """
            UPDATE management.projects
            SET name = %s, description = %s, version = version + 1,
                updated_at = clock_timestamp(), updated_by = %s
            WHERE project_id = %s AND owner_user_uuid = %s
                AND version = %s AND deleted_at IS NULL
            RETURNING project_id
            """,
            (name, description, owner, project_id, owner, version),
        )
        if row is None:
            raise DomainError("VERSION_CONFLICT", "최신 정보를 조회해 주세요.")

    async def delete_project(self, owner: UUID, project_id: UUID) -> None:
        await self.connection.execute(
            """
            UPDATE management.projects
            SET deleted_at = clock_timestamp(), deleted_by = %s,
                updated_at = clock_timestamp(), updated_by = %s,
                version = version + 1
            WHERE project_id = %s AND owner_user_uuid = %s
                AND NOT is_default AND deleted_at IS NULL
            """,
            (owner, owner, project_id, owner),
        )

    async def session(
        self,
        owner: UUID,
        session_id: UUID,
        *,
        deleted: bool = False,
        lock: bool = False,
    ) -> dict[str, Any]:
        # Lock parent before child, matching project/session mutation order.
        parent = await self.one(
            """
            SELECT project_id FROM management.sessions WHERE session_id = %s
            """,
            (session_id,),
        )
        if parent is None:
            raise not_found()
        await self.project(owner, parent["project_id"])
        predicate = "" if deleted else " AND s.deleted_at IS NULL"
        locking = " FOR UPDATE OF s" if lock else ""
        row = await self.one(
            SESSION_SELECT
            + """
            WHERE s.session_id = %s AND p.owner_user_uuid = %s
                AND p.deleted_at IS NULL
            """
            + predicate
            + locking,
            (session_id, owner),
        )
        if row is None:
            raise not_found()
        return row

    async def sessions(
        self,
        owner: UUID,
        project_id: UUID,
        limit: int,
        boundary: tuple[datetime, UUID] | None,
    ) -> list[dict[str, Any]]:
        await self.project(owner, project_id)
        predicate = ""
        parameters: tuple = (owner, project_id)
        if boundary:
            predicate = " AND (s.created_at, s.session_id) < (%s, %s)"
            parameters += boundary
        return await self.all(
            SESSION_SELECT
            + """
            WHERE p.owner_user_uuid = %s AND p.project_id = %s
                AND p.deleted_at IS NULL AND s.deleted_at IS NULL
            """
            + predicate
            + """
            ORDER BY s.created_at DESC, s.session_id DESC LIMIT %s
            """,
            (*parameters, limit),
        )

    async def insert_session(
        self, owner: UUID, project_id: UUID, title: str
    ) -> UUID:
        # The caller holds a share lock on the active parent until commit.
        session_id = uuid4()
        await self.connection.execute(
            """
            INSERT INTO management.sessions
                (session_id, project_id, title, created_by, updated_by)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (session_id, project_id, title, owner, owner),
        )
        return session_id

    async def update_session(
        self, owner: UUID, session_id: UUID, version: int, title: str
    ) -> None:
        row = await self.one(
            """
            UPDATE management.sessions s
            SET title = %s, version = s.version + 1,
                updated_at = clock_timestamp(), updated_by = %s
            FROM management.projects p
            WHERE s.session_id = %s AND s.version = %s
                AND s.project_id = p.project_id AND p.owner_user_uuid = %s
                AND s.deleted_at IS NULL AND p.deleted_at IS NULL
            RETURNING s.session_id
            """,
            (title, owner, session_id, version, owner),
        )
        if row is None:
            raise DomainError("VERSION_CONFLICT", "최신 정보를 조회해 주세요.")

    async def delete_session(self, owner: UUID, session_id: UUID) -> None:
        await self.connection.execute(
            """
            UPDATE management.sessions s
            SET deleted_at = clock_timestamp(), deleted_by = %s,
                updated_at = clock_timestamp(), updated_by = %s,
                version = s.version + 1
            FROM management.projects p
            WHERE s.session_id = %s AND s.project_id = p.project_id
                AND p.owner_user_uuid = %s AND p.deleted_at IS NULL
                AND s.deleted_at IS NULL
            """,
            (owner, owner, session_id, owner),
        )

    async def reserve(
        self, owner: UUID, scope: str, key: str, fingerprint: str
    ) -> dict[str, Any] | None:
        await self.connection.execute(
            """
            INSERT INTO management.requests
                (user_uuid, scope, request_key, fingerprint)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_uuid, scope, request_key) DO NOTHING
            """,
            (owner, scope, key, fingerprint),
        )
        row = await self.one(
            """
            SELECT * FROM management.requests
            WHERE user_uuid = %s AND scope = %s AND request_key = %s
            FOR UPDATE
            """,
            (owner, scope, key),
        )
        assert row is not None
        if row["fingerprint"] != fingerprint:
            raise DomainError(
                "IDEMPOTENCY_CONFLICT", "같은 키에 다른 요청이 전달됐습니다."
            )
        return row["response"]

    async def finish(
        self, owner: UUID, scope: str, key: str, response: dict
    ) -> None:
        await self.connection.execute(
            """
            UPDATE management.requests SET response = %s
            WHERE user_uuid = %s AND scope = %s AND request_key = %s
            """,
            (Jsonb(response), owner, scope, key),
        )
