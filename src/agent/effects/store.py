from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent.effects.models import ExecutorEffect
from ex_agent.persistence.database import transaction


def digest(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class EffectRecord:
    key: str
    task_id: UUID
    kind: str
    input_sha256: str
    request: dict[str, Any]
    response: dict[str, Any] | None


class EffectStore:
    """Short transactions only; never hold a DB connection during HTTP/LLM."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self.sessions = sessions

    async def get(self, key: str) -> EffectRecord | None:
        async with self.sessions() as session:
            row = await session.get(ExecutorEffect, key)
            return _record(row) if row is not None else None

    async def prepare(
        self,
        *,
        key: str,
        task_id: UUID,
        kind: str,
        input_sha256: str,
        request: dict[str, Any],
    ) -> EffectRecord:
        async with transaction(self.sessions) as session:
            await session.execute(
                insert(ExecutorEffect)
                .values(
                    key=key,
                    task_id=task_id,
                    kind=kind,
                    input_sha256=input_sha256,
                    request_sha256=digest(request),
                    request=request,
                    created_by="AGENT",
                    updated_by="AGENT",
                )
                .on_conflict_do_nothing(index_elements=[ExecutorEffect.key])
            )
            row = await session.get(ExecutorEffect, key)
            assert row is not None
            record = _record(row)
            validate_identity(record, task_id, kind, input_sha256)
            # First committed request wins, including any generated text.
            return record

    async def complete(
        self, key: str, response: dict[str, Any]
    ) -> EffectRecord:
        async with transaction(self.sessions) as session:
            row = await session.get(ExecutorEffect, key, with_for_update=True)
            if row is None:
                raise LookupError("Executor request was not prepared")
            if row.response is not None:
                # Executor replays may return a newer execution version.
                identity = {
                    k: v
                    for k, v in response.items()
                    if k != "execution_version"
                }
                previous = {
                    k: v
                    for k, v in row.response.items()
                    if k != "execution_version"
                }
                if identity != previous:
                    raise ValueError(
                        "Executor effect response identity changed"
                    )
            else:
                row.response = response
                row.updated_by = "AGENT"
            return _record(row)


def validate_identity(
    record: EffectRecord, task_id: UUID, kind: str, input_sha256: str
) -> None:
    if (record.task_id, record.kind, record.input_sha256) != (
        task_id,
        kind,
        input_sha256,
    ):
        raise ValueError("Executor effect key was reused with different input")


def _record(row: ExecutorEffect) -> EffectRecord:
    if digest(row.request) != row.request_sha256:
        raise ValueError("Persisted Executor request checksum mismatch")
    return EffectRecord(
        row.key,
        row.task_id,
        row.kind,
        row.input_sha256,
        row.request,
        row.response,
    )
