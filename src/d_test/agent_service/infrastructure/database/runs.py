"""Run persistence. Mutations share a session-then-run transaction lock."""

from datetime import datetime
from typing import LiteralString
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from d_test.agent_service.domain.management import DomainError, not_found
from d_test.agent_service.domain.runs import RunEvent

from .management import Repository


class RunRepository(Repository):
    async def run(self, owner: UUID, run_id: UUID, *, lock=False) -> dict:
        parent = await self.one(
            "SELECT session_id FROM management.runs "
            "WHERE run_id = %s AND owner_user_uuid = %s",
            (run_id, owner),
        )
        if parent is None:
            raise not_found()
        await self.session(owner, parent["session_id"], lock=lock)
        suffix = " FOR UPDATE" if lock else ""
        row = await self.one(
            "SELECT * FROM management.runs WHERE run_id = %s" + suffix,
            (run_id,),
        )
        assert row is not None
        return row

    async def create_run(
        self, owner: UUID, session: dict, content: dict, backend: str
    ) -> dict:
        run_id = uuid4()
        row = await self.one(
            """
            INSERT INTO management.runs
                (run_id, session_id, project_id, owner_user_uuid, status,
                 backend, input, created_by, updated_by)
            VALUES (%s, %s, %s, %s, 'queued', %s, %s, %s, %s)
            RETURNING *
            """,
            (
                run_id,
                session["session_id"],
                session["project_id"],
                owner,
                backend,
                Jsonb(content),
                owner,
                owner,
            ),
        )
        assert row is not None
        await self.connection.execute(
            """
            UPDATE management.sessions SET active_run_id = %s,
                updated_at = clock_timestamp(), updated_by = %s
            WHERE session_id = %s
            """,
            (run_id, owner, session["session_id"]),
        )
        return row

    async def set_status(self, run: dict, status: str) -> None:
        await self.connection.execute(
            """
            UPDATE management.runs SET status = %s,
                updated_at = clock_timestamp(), updated_by = %s
            WHERE run_id = %s
            """,
            (status, run["owner_user_uuid"], run["run_id"]),
        )
        run["status"] = status
        await self.emit(run, "run.status_changed", {"status": status})

    async def checkpoint(self, run: dict, data: dict, delay: float) -> None:
        await self.connection.execute(
            """
            UPDATE management.runs SET checkpoint = %s,
                due_at = clock_timestamp() + %s * interval '1 second',
                updated_at = clock_timestamp(), updated_by = %s
            WHERE run_id = %s
            """,
            (Jsonb(data), delay, run["owner_user_uuid"], run["run_id"]),
        )
        run["checkpoint"] = data

    async def lock_session(self, run: dict) -> None:
        row = await self.one(
            """
            UPDATE management.sessions SET is_locked = true,
                lock_reason = 'execution', updated_at = clock_timestamp(),
                updated_by = %s
            WHERE session_id = %s AND active_run_id = %s
            RETURNING session_id
            """,
            (run["owner_user_uuid"], run["session_id"], run["run_id"]),
        )
        if row is None:
            raise DomainError("RUN_NOT_ACTIVE", "활성 실행이 아닙니다.")

    async def release_session(self, run: dict) -> None:
        await self.connection.execute(
            """
            UPDATE management.sessions SET active_run_id = NULL,
                is_locked = false, lock_reason = NULL,
                updated_at = clock_timestamp(), updated_by = %s
            WHERE session_id = %s AND active_run_id = %s
            """,
            (run["owner_user_uuid"], run["session_id"], run["run_id"]),
        )

    async def seen(self, run: dict, key: str) -> dict | None:
        return await self.one(
            "SELECT * FROM management.run_events "
            "WHERE run_id = %s AND source_key = %s",
            (run["run_id"], key),
        )

    async def emit(
        self, run: dict, kind: str, data: dict, key: str | None = None
    ) -> RunEvent:
        if key:
            prior = await self.seen(run, key)
            if prior:
                if prior["type"] != kind or prior["data"] != data:
                    raise DomainError(
                        "OUTPUT_CONFLICT", "중복 출력의 내용이 다릅니다."
                    )
                return RunEvent.model_validate(prior)
        sequence = await self.one(
            "UPDATE management.runs SET event_seq = event_seq + 1 "
            "WHERE run_id = %s RETURNING event_seq",
            (run["run_id"],),
        )
        assert sequence is not None
        row = await self.one(
            """
            INSERT INTO management.run_events
                (run_id, sequence, type, data, source_key)
            VALUES (%s, %s, %s, %s, %s) RETURNING *
            """,
            (run["run_id"], sequence["event_seq"], kind, Jsonb(data), key),
        )
        assert row is not None
        return RunEvent.model_validate(row)

    async def page(
        self,
        session_id: UUID,
        limit: int,
        boundary: tuple[datetime, UUID] | None,
        *,
        messages: bool,
    ) -> list[dict]:
        # SQL identifiers are fixed server constants, never request strings.
        table: LiteralString = "messages" if messages else "runs"
        key: LiteralString = "message_id" if messages else "run_id"
        predicate: LiteralString = ""
        params: tuple = (session_id,)
        if boundary:
            predicate = " AND (created_at, " + key + ") < (%s, %s)"
            params += boundary
        return await self.all(
            "SELECT * FROM management."
            + table
            + " WHERE session_id = %s"
            + predicate
            + " ORDER BY created_at DESC, "
            + key
            + " DESC LIMIT %s",
            (*params, limit),
        )

    async def pending(self, run_id: UUID) -> list[dict]:
        return await self.all(
            "SELECT * FROM management.run_interrupts "
            "WHERE run_id = %s AND status = 'pending'",
            (run_id,),
        )

    async def executions(self, run_id: UUID) -> list[dict]:
        return await self.all(
            "SELECT * FROM management.run_executions "
            "WHERE run_id = %s ORDER BY created_at, binding_id",
            (run_id,),
        )

    async def events(self, run_id: UUID, after: int) -> list[RunEvent]:
        rows = await self.all(
            "SELECT * FROM management.run_events "
            "WHERE run_id = %s AND sequence > %s "
            "ORDER BY sequence LIMIT 100",
            (run_id, after),
        )
        return [RunEvent.model_validate(row) for row in rows]
