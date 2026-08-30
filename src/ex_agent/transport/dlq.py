from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, cast

from redis.asyncio import Redis

_REPLAY_SCRIPT = """
local prior = redis.call('get', KEYS[3])
if prior then
    return {0, prior}
end
local existing = redis.call('xrange', KEYS[1], ARGV[1], ARGV[1])
if #existing == 0 then
    return {-1, 'missing'}
end
local fields = cjson.decode(ARGV[2])
local arguments = {}
for key, value in pairs(fields) do
    table.insert(arguments, key)
    table.insert(arguments, value)
end
local target_id = redis.call('xadd', KEYS[2], '*', unpack(arguments))
redis.call(
    'xadd', KEYS[4], '*',
    'schema_version', '1',
    'action', 'REPLAYED',
    'dlq_stream', KEYS[1],
    'dlq_entry_id', ARGV[1],
    'failure_id', ARGV[3],
    'source_stream', KEYS[2],
    'source_message_id', ARGV[4],
    'target_message_id', target_id,
    'actor', ARGV[5],
    'reason', ARGV[6],
    'acted_at', ARGV[7]
)
redis.call('xdel', KEYS[1], ARGV[1])
local marker = cjson.encode({
    action = 'REPLAYED',
    target_message_id = target_id
})
redis.call('set', KEYS[3], marker, 'EX', ARGV[8])
return {1, marker}
"""

_DISCARD_SCRIPT = """
local prior = redis.call('get', KEYS[2])
if prior then
    return {0, prior}
end
local existing = redis.call('xrange', KEYS[1], ARGV[1], ARGV[1])
if #existing == 0 then
    return {-1, 'missing'}
end
redis.call(
    'xadd', KEYS[3], '*',
    'schema_version', '1',
    'action', 'DISCARDED',
    'dlq_stream', KEYS[1],
    'dlq_entry_id', ARGV[1],
    'failure_id', ARGV[2],
    'source_stream', ARGV[3],
    'source_message_id', ARGV[4],
    'target_message_id', '',
    'actor', ARGV[5],
    'reason', ARGV[6],
    'acted_at', ARGV[7]
)
redis.call('xdel', KEYS[1], ARGV[1])
local marker = cjson.encode({action = 'DISCARDED'})
redis.call('set', KEYS[2], marker, 'EX', ARGV[8])
return {1, marker}
"""


class DeadLetterFormatError(ValueError):
    pass


class DeadLetterAction(StrEnum):
    REPLAYED = "REPLAYED"
    DISCARDED = "DISCARDED"


@dataclass(frozen=True)
class DeadLetterEntry:
    entry_id: str
    schema_version: str
    failure_id: str
    source_stream: str
    source_group: str
    source_message_id: str
    reason: str
    retry_attempts: int
    fields: dict[str, str]
    dead_lettered_at: str | None = None
    consumer: str | None = None
    error_type: str | None = None
    reclaimed: bool = False

    @classmethod
    def from_redis(
        cls,
        entry_id: str,
        values: dict[str, str],
    ) -> DeadLetterEntry:
        try:
            fields_value = json.loads(values["fields"])
            if not isinstance(fields_value, dict):
                raise TypeError("fields is not an object")
            fields = {
                str(key): str(value) for key, value in fields_value.items()
            }
            source_stream = values["source_stream"]
            source_group = values["source_group"]
            source_message_id = values["source_message_id"]
            reason = values["reason"]
            retry_attempts = int(values.get("retry_attempts", "0"))
        except (
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as error:
            raise DeadLetterFormatError(
                f"Invalid DLQ entry {entry_id}: {error}"
            ) from error
        failure_id = values.get("failure_id") or _stable_identity(
            source_stream,
            source_group,
            source_message_id,
        )
        return cls(
            entry_id=entry_id,
            schema_version=values.get("schema_version", "0"),
            failure_id=failure_id,
            source_stream=source_stream,
            source_group=source_group,
            source_message_id=source_message_id,
            reason=reason,
            retry_attempts=retry_attempts,
            fields=fields,
            dead_lettered_at=values.get("dead_lettered_at"),
            consumer=values.get("consumer"),
            error_type=values.get("error_type"),
            reclaimed=values.get("reclaimed", "false").lower() == "true",
        )


@dataclass(frozen=True)
class DeadLetterPage:
    entries: list[DeadLetterEntry]
    next_cursor: str | None


@dataclass(frozen=True)
class DeadLetterActionResult:
    entry_id: str
    action: DeadLetterAction
    applied: bool
    target_message_id: str | None = None


class DeadLetterManager:
    """Inspect and atomically replay or discard Redis Stream DLQ entries."""

    def __init__(
        self,
        redis: Redis,
        dlq_stream: str,
        *,
        audit_stream: str | None = None,
        marker_prefix: str = "redis-stream-consumer:dlq-actions",
        marker_ttl_seconds: int = 7776000,
    ) -> None:
        if not dlq_stream:
            raise ValueError("dlq_stream cannot be empty")
        if not marker_prefix:
            raise ValueError("marker_prefix cannot be empty")
        if marker_ttl_seconds < 1:
            raise ValueError("marker_ttl_seconds must be positive")
        self._redis = redis
        self._dlq_stream = dlq_stream
        self._audit_stream = audit_stream or f"{dlq_stream}.audit"
        self._marker_prefix = marker_prefix
        self._marker_ttl_seconds = marker_ttl_seconds

    async def list_entries(
        self,
        *,
        limit: int = 50,
        after: str | None = None,
    ) -> DeadLetterPage:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        minimum = "-" if after is None else f"({after}"
        rows: Any = await self._redis.xrange(
            self._dlq_stream,
            min=minimum,
            max="+",
            count=limit + 1,
        )
        visible = rows[:limit]
        entries = [
            DeadLetterEntry.from_redis(str(entry_id), values)
            for entry_id, values in visible
        ]
        next_cursor = entries[-1].entry_id if len(rows) > limit else None
        return DeadLetterPage(entries, next_cursor)

    async def get(self, entry_id: str) -> DeadLetterEntry | None:
        rows: Any = await self._redis.xrange(
            self._dlq_stream,
            min=entry_id,
            max=entry_id,
            count=1,
        )
        if not rows:
            return None
        row_id, values = rows[0]
        return DeadLetterEntry.from_redis(str(row_id), values)

    async def replay(
        self,
        entry_id: str,
        *,
        actor: str,
        reason: str,
    ) -> DeadLetterActionResult:
        _validate_action(actor, reason)
        marker = await self._marker(entry_id)
        if marker is not None:
            return _marker_result(entry_id, marker)
        entry = await self.get(entry_id)
        if entry is None:
            raise LookupError(f"Unknown DLQ entry: {entry_id}")
        result = await _await_eval(
            self._redis.eval(
                _REPLAY_SCRIPT,
                4,
                self._dlq_stream,
                entry.source_stream,
                self._marker_key(entry_id),
                self._audit_stream,
                entry_id,
                json.dumps(entry.fields, sort_keys=True),
                entry.failure_id,
                entry.source_message_id,
                actor,
                reason,
                _now(),
                str(self._marker_ttl_seconds),
            )
        )
        return _script_result(entry_id, result)

    async def replay_many(
        self,
        entry_ids: Sequence[str],
        *,
        actor: str,
        reason: str,
    ) -> list[DeadLetterActionResult]:
        return [
            await self.replay(entry_id, actor=actor, reason=reason)
            for entry_id in entry_ids
        ]

    async def discard(
        self,
        entry_id: str,
        *,
        actor: str,
        reason: str,
    ) -> DeadLetterActionResult:
        _validate_action(actor, reason)
        marker = await self._marker(entry_id)
        if marker is not None:
            return _marker_result(entry_id, marker)
        entry = await self.get(entry_id)
        if entry is None:
            raise LookupError(f"Unknown DLQ entry: {entry_id}")
        result = await _await_eval(
            self._redis.eval(
                _DISCARD_SCRIPT,
                3,
                self._dlq_stream,
                self._marker_key(entry_id),
                self._audit_stream,
                entry_id,
                entry.failure_id,
                entry.source_stream,
                entry.source_message_id,
                actor,
                reason,
                _now(),
                str(self._marker_ttl_seconds),
            )
        )
        return _script_result(entry_id, result)

    async def discard_many(
        self,
        entry_ids: Sequence[str],
        *,
        actor: str,
        reason: str,
    ) -> list[DeadLetterActionResult]:
        return [
            await self.discard(entry_id, actor=actor, reason=reason)
            for entry_id in entry_ids
        ]

    async def _marker(self, entry_id: str) -> str | None:
        value = await self._redis.get(self._marker_key(entry_id))
        return str(value) if value is not None else None

    def _marker_key(self, entry_id: str) -> str:
        identity = f"{self._dlq_stream}\0{entry_id}".encode()
        digest = hashlib.sha256(identity).hexdigest()
        return f"{self._marker_prefix}:{digest}"


def _script_result(entry_id: str, result: Any) -> DeadLetterActionResult:
    if not isinstance(result, (list, tuple)) or len(result) != 2:
        raise RuntimeError("DLQ action returned an invalid response")
    status = int(result[0])
    if status == -1:
        raise LookupError(f"Unknown DLQ entry: {entry_id}")
    marker = str(result[1])
    parsed = _marker_result(entry_id, marker)
    return DeadLetterActionResult(
        entry_id=parsed.entry_id,
        action=parsed.action,
        applied=status == 1,
        target_message_id=parsed.target_message_id,
    )


def _marker_result(entry_id: str, marker: str) -> DeadLetterActionResult:
    try:
        payload = json.loads(marker)
        action = DeadLetterAction(payload["action"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise RuntimeError("DLQ action marker is invalid") from error
    target = payload.get("target_message_id")
    return DeadLetterActionResult(
        entry_id=entry_id,
        action=action,
        applied=False,
        target_message_id=str(target) if target else None,
    )


def _validate_action(actor: str, reason: str) -> None:
    if not actor.strip():
        raise ValueError("actor cannot be empty")
    if not reason.strip():
        raise ValueError("reason cannot be empty")


def _stable_identity(stream: str, group: str, message_id: str) -> str:
    identity = f"{stream}\0{group}\0{message_id}".encode()
    return hashlib.sha256(identity).hexdigest()


def _now() -> str:
    return datetime.now(UTC).isoformat()


async def _await_eval(value: Awaitable[Any] | Any) -> Any:
    return await cast(Awaitable[Any], value)


__all__ = [
    "DeadLetterAction",
    "DeadLetterActionResult",
    "DeadLetterEntry",
    "DeadLetterFormatError",
    "DeadLetterManager",
    "DeadLetterPage",
]
