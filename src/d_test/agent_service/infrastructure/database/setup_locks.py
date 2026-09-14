"""Short lock attempts: never block concurrent index builds with snapshots."""

import asyncio

from sqlalchemy import text

MANAGEMENT_LOCK = "chatapp:management-schema"


async def acquire_management_lock(connection) -> None:
    """SQLAlchemy connection; hold session lock, but no waiting transaction."""
    while True:
        acquired = await connection.scalar(
            text("SELECT pg_try_advisory_lock(hashtextextended(:key, 0))"),
            {"key": MANAGEMENT_LOCK},
        )
        await connection.commit()
        if acquired:
            return
        await asyncio.sleep(0.1)


async def acquire_checkpoint_lock(connection, schema: str) -> None:
    """Psycopg autocommit connection; caller closes or explicitly unlocks."""
    if not connection.autocommit:
        raise RuntimeError("Checkpoint setup lock requires autocommit")
    while True:
        row = await (
            await connection.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s, 0)) AS ok",
                (f"checkpoint-setup:{schema}",),
            )
        ).fetchone()
        if row["ok"]:
            return
        await asyncio.sleep(0.1)
