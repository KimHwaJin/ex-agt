"""Process-owned pool; an execution keeps its saver connection alive."""

from contextlib import asynccontextmanager

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg import sql

from d_test.agent_service.infrastructure.database.management import create_pool
from d_test.agent_service.settings import Settings


class Checkpoints:
    def __init__(self, settings: Settings):
        self.schema = settings.checkpoint_schema
        self.pool = create_pool(
            settings.model_copy(
                update={
                    "database_url": settings.checkpoint_database_url
                    or settings.database_url,
                    "pool_min_size": 1,
                    "pool_max_size": settings.checkpoint_pool_max_size,
                }
            )
        )

    @asynccontextmanager
    async def connection(self):
        async with self.pool.connection() as connection:
            await connection.execute(
                sql.SQL("SET search_path TO {}").format(
                    sql.Identifier(self.schema)
                )
            )
            yield connection

    async def open(self, *, validate=True):
        await self.pool.open(wait=True)
        if validate:
            async with self.connection() as connection:
                row = await (
                    await connection.execute(
                        sql.SQL(
                            "SELECT max(v) AS v FROM {}.checkpoint_migrations"
                        ).format(sql.Identifier(self.schema))
                    )
                ).fetchone()
                if (
                    not row
                    or row["v"] != len(AsyncPostgresSaver.MIGRATIONS) - 1
                ):
                    raise RuntimeError("Run checkpoint migrations first")
                for table in (
                    "checkpoints",
                    "checkpoint_blobs",
                    "checkpoint_writes",
                ):
                    await connection.execute(
                        sql.SQL("SELECT 1 FROM {}.{} LIMIT 0").format(
                            sql.Identifier(self.schema), sql.Identifier(table)
                        )
                    )

    async def close(self):
        await self.pool.close()

    async def setup(self):
        # Deployment command only, never called by API/worker startup.
        async with self.connection() as connection:
            await connection.execute(
                "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
                (f"checkpoint-setup:{self.schema}",),
            )
            try:
                await connection.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                        sql.Identifier(self.schema)
                    )
                )
                await AsyncPostgresSaver(connection).setup()
            finally:
                await connection.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                    (f"checkpoint-setup:{self.schema}",),
                )

    @asynccontextmanager
    async def session(self, session_id):
        # The saver AND session advisory lock use the same connection.
        # A lost connection cannot keep writing checkpoints through a new one.
        async with self.connection() as connection:
            key = f"graph:{self.schema}:{session_id}"
            row = await (
                await connection.execute(
                    "SELECT pg_try_advisory_lock("
                    "hashtextextended(%s, 0)) AS ok",
                    (key,),
                )
            ).fetchone()
            if not row or not row["ok"]:
                yield None
                return
            try:
                yield AsyncPostgresSaver(connection)
            finally:
                try:
                    if not connection.closed:
                        await connection.execute(
                            "SELECT pg_advisory_unlock("
                            "hashtextextended(%s, 0))",
                            (key,),
                        )
                except BaseException:
                    await connection.close()
                    raise
