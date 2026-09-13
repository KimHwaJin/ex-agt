"""Atomic message projection and event recording, shared by all drivers."""

from uuid import UUID

from psycopg.types.json import Jsonb

from agent_service.domain.management import DomainError
from agent_service.domain.runs import Message
from agent_service.infrastructure.database.runs import RunRepository


class OutputWriter:
    """Caller must hold the run lock inside the same database transaction."""

    def __init__(self, repository: RunRepository, run: dict):
        self.repo = repository
        self.run = run

    async def start(
        self, message_id: UUID, content: list[dict], *, role="assistant"
    ) -> None:
        status = "completed" if role == "user" else "streaming"
        inserted = await self.repo.one(
            """
            INSERT INTO management.messages
                (message_id, session_id, run_id, role, content, status,
                 created_by, updated_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (message_id) DO NOTHING RETURNING *
            """,
            (
                message_id,
                self.run["session_id"],
                self.run["run_id"],
                role,
                Jsonb(content),
                status,
                self.run["owner_user_uuid"],
                self.run["owner_user_uuid"],
            ),
        )
        if inserted is not None:
            message = Message.model_validate(inserted).model_dump(mode="json")
            kind = "message.completed" if role == "user" else "message.started"
            event = await self.repo.emit(self.run, kind, message)
            await self.stamp(message_id, event.sequence)

    async def stamp(self, message_id: UUID, sequence: int) -> None:
        await self.repo.connection.execute(
            "UPDATE management.messages SET event_sequence = %s "
            "WHERE message_id = %s AND run_id = %s",
            (sequence, message_id, self.run["run_id"]),
        )

    async def append(self, message_id: UUID, text: str, key: str) -> None:
        payload = {
            "message_id": str(message_id),
            "block_index": 0,
            "delta": text,
        }
        if await self.repo.seen(self.run, key):
            # emit checks that a producer did not reuse the key for new text.
            await self.repo.emit(self.run, "message.delta", payload, key)
            return
        row = await self.repo.one(
            "SELECT * FROM management.messages "
            "WHERE message_id = %s AND run_id = %s FOR UPDATE",
            (message_id, self.run["run_id"]),
        )
        if row is None or row["status"] != "streaming":
            raise DomainError(
                "MESSAGE_NOT_OPEN", "작성 중인 메시지가 아닙니다."
            )
        content = row["content"]
        if not content:
            content = [{"type": "text", "text": text}]
        else:
            content[0]["text"] += text
        await self.repo.connection.execute(
            """
            UPDATE management.messages SET content = %s,
                updated_at = clock_timestamp(), updated_by = %s
            WHERE message_id = %s
            """,
            (Jsonb(content), self.run["owner_user_uuid"], message_id),
        )
        event = await self.repo.emit(self.run, "message.delta", payload, key)
        await self.stamp(message_id, event.sequence)

    async def finish(self, message_id: UUID, status="completed") -> None:
        row = await self.repo.one(
            """
            UPDATE management.messages SET status = %s,
                updated_at = clock_timestamp(), updated_by = %s
            WHERE message_id = %s AND run_id = %s AND status = 'streaming'
            RETURNING *
            """,
            (
                status,
                self.run["owner_user_uuid"],
                message_id,
                self.run["run_id"],
            ),
        )
        if row:
            event = await self.repo.emit(
                self.run,
                "message.completed",
                Message.model_validate(row).model_dump(mode="json"),
            )
            await self.stamp(message_id, event.sequence)

    async def terminate(self, status: str, error: dict | None = None) -> None:
        rows = await self.repo.all(
            "SELECT message_id FROM management.messages "
            "WHERE run_id = %s AND status = 'streaming'",
            (self.run["run_id"],),
        )
        for row in rows:
            await self.finish(
                row["message_id"],
                "failed" if status == "failed" else "interrupted",
            )
        await self.repo.connection.execute(
            "UPDATE management.run_interrupts SET status = 'cancelled', "
            "updated_at = clock_timestamp(), updated_by = %s "
            "WHERE run_id = %s AND status = 'pending'",
            (self.run["owner_user_uuid"], self.run["run_id"]),
        )
        await self.repo.connection.execute(
            "UPDATE management.runs SET error = %s WHERE run_id = %s",
            (Jsonb(error) if error else None, self.run["run_id"]),
        )
        await self.repo.set_status(self.run, status)
        await self.repo.emit(
            self.run, f"run.{status}", {"status": status, "error": error}
        )
        await self.repo.release_session(self.run)
