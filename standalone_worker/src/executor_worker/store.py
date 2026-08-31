from __future__ import annotations

import asyncio
from importlib.resources import files
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from executor_worker.contracts import EventContext, ExecutorEvent


class Store:
    """Own tables only; no dependency on the recipient's Agent schema."""

    def __init__(self, pool: AsyncConnectionPool, namespace: str) -> None:
        self.pool = pool
        self.namespace = namespace

    async def migrate(self) -> None:
        """Legacy initial-DDL helper; use Alembic for versioned deployments."""
        # Explicit deployment step, never implicit during Worker startup.
        source = await asyncio.to_thread(
            files("executor_worker").joinpath("schema.sql").read_bytes,
        )
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(178521093)")
            await conn.execute(source, prepare=False)

    async def register(
        self,
        *,
        execution_id: UUID,
        session_id: str,
        task_id: str,
        actor: str = "api",
    ) -> None:
        if not session_id or not task_id or not actor:
            raise ValueError("session_id, task_id and actor are required")
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute(
                """INSERT INTO ew_bindings
                (namespace, execution_id, session_id, task_id,
                 created_by, updated_by) VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING""",
                (
                    self.namespace,
                    execution_id,
                    session_id,
                    task_id,
                    actor,
                    actor,
                ),
            )
            cur = await conn.execute(
                """SELECT session_id, task_id FROM ew_bindings
                WHERE namespace=%s AND execution_id=%s""",
                (self.namespace, execution_id),
            )
            if await cur.fetchone() != (session_id, task_id):
                raise ValueError("Execution binding is immutable")

    async def ingest(
        self,
        event: ExecutorEvent,
        *,
        catch_up: bool = False,
    ) -> None:
        data = event.model_dump(mode="json")
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute(
                """INSERT INTO ew_inbox
                (namespace,event_id,execution_id,sequence,event,
                 created_by,updated_by) VALUES (%s,%s,%s,%s,%s,'worker',
                 'worker') ON CONFLICT DO NOTHING""",
                (
                    self.namespace,
                    event.event_id,
                    event.execution_id,
                    event.event_sequence,
                    Jsonb(data),
                ),
            )
            cur = await conn.execute(
                """SELECT event FROM ew_inbox
                WHERE namespace=%s AND event_id=%s""",
                (self.namespace, event.event_id),
            )
            row = await cur.fetchone()
            if row is None or row[0] != data:
                raise ValueError("Conflicting event identity or sequence")
            if catch_up:
                await conn.execute(
                    """UPDATE ew_bindings SET
                    catch_up_version=catch_up_version+1,updated_at=now(),
                    updated_by='worker' WHERE namespace=%s
                    AND execution_id=%s""",
                    (self.namespace, event.execution_id),
                )

    async def finish_catch_up(self, execution_id: UUID, version: int) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                """UPDATE ew_bindings SET
                caught_up_version=greatest(caught_up_version,%s),
                updated_at=now(),updated_by='worker'
                WHERE namespace=%s AND execution_id=%s""",
                (version, self.namespace, execution_id),
            )

    async def scan_candidates(self, limit: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """SELECT b.* FROM ew_bindings b
                    WHERE b.namespace=%s AND b.next_scan_at<=now()
                    AND (b.catch_up_version>b.caught_up_version
                    OR EXISTS (SELECT 1 FROM ew_inbox i
                        WHERE i.namespace=b.namespace
                        AND i.execution_id=b.execution_id
                        AND i.sequence>b.last_sequence))
                    ORDER BY b.next_scan_at, b.execution_id LIMIT %s""",
                    (self.namespace, limit),
                )
                return await cur.fetchall()

    async def scan_error(self, execution_id: UUID, error: str) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                """UPDATE ew_bindings SET last_error=%s,
                next_scan_at=now()+interval '5 seconds',
                updated_at=now(),updated_by='worker'
                WHERE namespace=%s AND execution_id=%s""",
                (error[:2000], self.namespace, execution_id),
            )

    async def advance(
        self,
        execution_id: UUID,
        event_types: set[str],
        limit: int,
    ) -> tuple[int, int | None]:
        """Atomically route a contiguous Inbox prefix into the Outbox.

        Returns (number advanced, gap-after-sequence or None).
        """
        async with self.pool.connection() as conn, conn.transaction():
            cur = await conn.execute(
                """SELECT last_sequence FROM ew_bindings
                WHERE namespace=%s AND execution_id=%s FOR UPDATE""",
                (self.namespace, execution_id),
            )
            row = await cur.fetchone()
            if row is None:
                return 0, None
            sequence = row[0]
            cur = await conn.execute(
                """SELECT event FROM ew_inbox WHERE namespace=%s
                AND execution_id=%s AND sequence>%s
                ORDER BY sequence LIMIT %s""",
                (self.namespace, execution_id, sequence, limit),
            )
            events = await cur.fetchall()
            count = 0
            gap = None
            for (data,) in events:
                event = ExecutorEvent.model_validate(data)
                if event.event_sequence != sequence + 1:
                    gap = sequence
                    break
                routed = event.event_type in event_types
                if routed:
                    command_id = uuid5(
                        NAMESPACE_URL,
                        f"{self.namespace}/event/{event.event_id}",
                    )
                    await conn.execute(
                        """INSERT INTO ew_commands
                        (namespace,command_id,event_id,execution_id,sequence,
                         created_by,updated_by)
                        VALUES (%s,%s,%s,%s,%s,'worker','worker')""",
                        (
                            self.namespace,
                            command_id,
                            event.event_id,
                            execution_id,
                            event.event_sequence,
                        ),
                    )
                    await conn.execute(
                        """INSERT INTO ew_outbox
                        (namespace,command_id,created_by,updated_by)
                        VALUES (%s,%s,'worker','worker')""",
                        (self.namespace, command_id),
                    )
                await conn.execute(
                    """UPDATE ew_inbox SET state=%s,updated_at=now(),
                    updated_by='worker' WHERE namespace=%s AND event_id=%s""",
                    (
                        "ROUTED" if routed else "IGNORED",
                        self.namespace,
                        event.event_id,
                    ),
                )
                sequence = event.event_sequence
                count += 1
            await conn.execute(
                """UPDATE ew_bindings SET last_sequence=%s,
                next_scan_at=now(),last_error=NULL,updated_at=now(),
                updated_by='worker' WHERE namespace=%s AND execution_id=%s""",
                (sequence, self.namespace, execution_id),
            )
            return count, gap

    async def claim_outbox(
        self,
        limit: int,
        lease_seconds: int,
    ) -> tuple[UUID, list[dict[str, Any]]]:
        token = uuid4()
        async with self.pool.connection() as conn, conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """WITH picked AS (
                    SELECT o.command_id FROM ew_outbox o
                    JOIN ew_commands c USING (namespace, command_id)
                    WHERE o.namespace=%s AND c.state='READY'
                    AND (o.state='PENDING' OR
                         (o.state='CLAIMED' AND o.claim_until<now()))
                    AND NOT EXISTS (SELECT 1 FROM ew_commands earlier
                        WHERE earlier.namespace=c.namespace
                        AND earlier.execution_id=c.execution_id
                        AND earlier.sequence<c.sequence
                        AND earlier.state NOT IN ('DONE','IGNORED'))
                    ORDER BY o.created_at,o.command_id LIMIT %s
                    FOR UPDATE OF o SKIP LOCKED)
                    UPDATE ew_outbox o SET state='CLAIMED',claim_token=%s,
                    claim_until=now()+make_interval(secs => %s),
                    updated_at=now(),updated_by='worker'
                    FROM picked WHERE o.namespace=%s
                    AND o.command_id=picked.command_id RETURNING o.*""",
                    (
                        self.namespace,
                        limit,
                        token,
                        lease_seconds,
                        self.namespace,
                    ),
                )
                return token, await cur.fetchall()

    async def finish_publications(
        self,
        token: UUID,
        command_ids: list[UUID],
        *,
        sent: bool,
    ) -> None:
        if not command_ids:
            return
        async with self.pool.connection() as conn:
            await conn.execute(
                """UPDATE ew_outbox SET state=%s,claim_token=NULL,
                claim_until=NULL,updated_at=now(),updated_by='worker'
                WHERE namespace=%s AND claim_token=%s
                AND command_id=ANY(%s)""",
                (
                    "SENT" if sent else "PENDING",
                    self.namespace,
                    token,
                    command_ids,
                ),
            )

    async def command(self, command_id: UUID) -> dict[str, Any] | None:
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """SELECT c.*,i.event,b.session_id,b.task_id
                    FROM ew_commands c JOIN ew_inbox i
                    ON i.namespace=c.namespace AND i.event_id=c.event_id
                    JOIN ew_bindings b ON b.namespace=c.namespace
                    AND b.execution_id=c.execution_id
                    WHERE c.namespace=%s AND c.command_id=%s""",
                    (self.namespace, command_id),
                )
                return await cur.fetchone()

    def context(self, row: dict[str, Any]) -> EventContext:
        return EventContext(
            namespace=self.namespace,
            session_id=row["session_id"],
            task_id=row["task_id"],
            execution_id=row["execution_id"],
            command_id=row["command_id"],
            event=ExecutorEvent.model_validate(row["event"]),
        )

    async def set_state(
        self,
        command_id: UUID,
        state: str,
        *,
        error: str | None = None,
        failed_attempt: bool = False,
    ) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                """UPDATE ew_commands SET state=%s,last_error=%s,
                failure_attempts=failure_attempts+%s,
                updated_at=now(),updated_by='worker'
                WHERE namespace=%s AND command_id=%s""",
                (
                    state,
                    error[:2000] if error else None,
                    int(failed_attempt),
                    self.namespace,
                    command_id,
                ),
            )
            if state in {"DONE", "IGNORED", "FAILED"}:
                await conn.execute(
                    """UPDATE ew_outbox SET state='SENT',claim_token=NULL,
                    claim_until=NULL,updated_at=now(),updated_by='worker'
                    WHERE namespace=%s AND command_id=%s""",
                    (self.namespace, command_id),
                )

    async def resolve_failed(
        self,
        command_id: UUID,
        *,
        retry: bool,
        actor: str,
        reason: str,
    ) -> None:
        """Caller must hold the same session guard as the Dispatcher."""
        if not actor.strip() or not reason.strip():
            raise ValueError("actor and reason are required")
        async with self.pool.connection() as conn, conn.transaction():
            cur = await conn.execute(
                """UPDATE ew_commands SET state=%s,
                generation=generation+1,failure_attempts=0,
                last_error=NULL,updated_at=now(),updated_by=%s
                WHERE namespace=%s AND command_id=%s AND state='FAILED'
                RETURNING generation""",
                (
                    "READY" if retry else "IGNORED",
                    actor,
                    self.namespace,
                    command_id,
                ),
            )
            row = await cur.fetchone()
            if row is None:
                raise ValueError("Command is not FAILED")
            if retry:
                await conn.execute(
                    """UPDATE ew_outbox SET generation=%s,state='PENDING',
                    claim_token=NULL,claim_until=NULL,
                    updated_at=now(),updated_by=%s
                    WHERE namespace=%s AND command_id=%s""",
                    (row[0], actor, self.namespace, command_id),
                )
            await conn.execute(
                """INSERT INTO ew_audit
                (namespace,command_id,action,reason,created_by,updated_by)
                VALUES (%s,%s,%s,%s,%s,%s)""",
                (
                    self.namespace,
                    command_id,
                    "RETRY" if retry else "SKIP",
                    reason,
                    actor,
                    actor,
                ),
            )

    async def counts(self) -> dict[str, int]:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """SELECT 'command:'||state,count(*) FROM ew_commands
                WHERE namespace=%s GROUP BY state UNION ALL
                SELECT 'inbox:'||state,count(*) FROM ew_inbox
                WHERE namespace=%s GROUP BY state UNION ALL
                SELECT 'outbox:'||state,count(*) FROM ew_outbox
                WHERE namespace=%s GROUP BY state""",
                (self.namespace,) * 3,
            )
            return dict(await cur.fetchall())
