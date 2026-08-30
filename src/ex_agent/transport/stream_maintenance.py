from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from redis.asyncio import Redis

_SAFE_TRIM_SCRIPT = """
local function decimal_less(left, right)
    left = string.gsub(left, '^0+', '')
    right = string.gsub(right, '^0+', '')
    if left == '' then left = '0' end
    if right == '' then right = '0' end
    if string.len(left) ~= string.len(right) then
        return string.len(left) < string.len(right)
    end
    return left < right
end

local function id_less(left, right)
    local left_ms, left_seq = string.match(left, '^(%d+)%-(%d+)$')
    local right_ms, right_seq = string.match(right, '^(%d+)%-(%d+)$')
    if left_ms == right_ms then
        return decimal_less(left_seq, right_seq)
    end
    return decimal_less(left_ms, right_ms)
end

local function minimum(left, right)
    if id_less(right, left) then
        return right
    end
    return left
end

if redis.call('exists', KEYS[1]) == 0 then
    return {0, '0-0', 0, 0}
end

local length = redis.call('xlen', KEYS[1])
local minimum_entries = tonumber(ARGV[2])
if length <= minimum_entries then
    return {0, '0-0', 0, length}
end

local boundary = ARGV[1]
if minimum_entries > 0 then
    local tail = redis.call(
        'xrevrange', KEYS[1], '+', '-', 'count', minimum_entries
    )
    if #tail < minimum_entries then
        return {0, '0-0', 0, length}
    end
    boundary = minimum(boundary, tail[#tail][1])
end

local groups = redis.call('xinfo', 'groups', KEYS[1])
for _, group in ipairs(groups) do
    local name = nil
    local last_delivered = '0-0'
    local pending_count = 0
    for index = 1, #group, 2 do
        if group[index] == 'name' then
            name = group[index + 1]
        elseif group[index] == 'last-delivered-id' then
            last_delivered = group[index + 1]
        elseif group[index] == 'pending' then
            pending_count = tonumber(group[index + 1])
        end
    end
    boundary = minimum(boundary, last_delivered)
    if pending_count > 0 and name then
        local pending = redis.call('xpending', KEYS[1], name)
        if pending[2] then
            boundary = minimum(boundary, pending[2])
        end
    end
end

local removed = redis.call('xtrim', KEYS[1], 'minid', '=', boundary)
return {removed, boundary, #groups, length}
"""


@dataclass(frozen=True)
class ConsumerGroupBoundary:
    name: str
    last_delivered_id: str
    pending_count: int
    oldest_pending_id: str | None


@dataclass(frozen=True)
class StreamTrimPlan:
    stream: str
    stream_exists: bool
    stream_length: int
    retention_boundary_id: str
    tail_boundary_id: str | None
    groups: list[ConsumerGroupBoundary]
    trim_before_id: str | None

    @property
    def can_trim(self) -> bool:
        return self.trim_before_id not in {None, "0-0"}


@dataclass(frozen=True)
class StreamTrimResult:
    stream: str
    removed_entries: int
    trim_before_id: str
    inspected_groups: int
    stream_length_before: int


class SafeStreamTrimmer:
    """Plan and atomically trim only entries safe for every group."""

    def __init__(
        self,
        redis: Redis,
        *,
        retention_seconds: int = 604800,
        minimum_retained_entries: int = 1000,
    ) -> None:
        if retention_seconds < 1:
            raise ValueError("retention_seconds must be positive")
        if minimum_retained_entries < 0:
            raise ValueError("minimum_retained_entries cannot be negative")
        self._redis = redis
        self._retention_seconds = retention_seconds
        self._minimum_retained_entries = minimum_retained_entries

    async def plan(
        self,
        stream: str,
        *,
        now: datetime | None = None,
    ) -> StreamTrimPlan:
        _validate_stream(stream)
        retention_boundary = self._retention_boundary(now)
        if not await self._redis.exists(stream):
            return StreamTrimPlan(
                stream=stream,
                stream_exists=False,
                stream_length=0,
                retention_boundary_id=retention_boundary,
                tail_boundary_id=None,
                groups=[],
                trim_before_id=None,
            )
        length = int(await self._redis.xlen(stream))
        if length <= self._minimum_retained_entries:
            return StreamTrimPlan(
                stream=stream,
                stream_exists=True,
                stream_length=length,
                retention_boundary_id=retention_boundary,
                tail_boundary_id=None,
                groups=await self._groups(stream),
                trim_before_id=None,
            )
        tail_boundary, groups = await asyncio.gather(
            self._tail_boundary(stream),
            self._groups(stream),
        )
        boundaries = [retention_boundary]
        if tail_boundary is not None:
            boundaries.append(tail_boundary)
        for group in groups:
            boundaries.append(group.last_delivered_id)
            if group.oldest_pending_id is not None:
                boundaries.append(group.oldest_pending_id)
        return StreamTrimPlan(
            stream=stream,
            stream_exists=True,
            stream_length=length,
            retention_boundary_id=retention_boundary,
            tail_boundary_id=tail_boundary,
            groups=groups,
            trim_before_id=min(boundaries, key=_stream_id_parts),
        )

    async def trim(
        self,
        stream: str,
        *,
        now: datetime | None = None,
    ) -> StreamTrimResult:
        _validate_stream(stream)
        result = await _await_eval(
            self._redis.eval(
                _SAFE_TRIM_SCRIPT,
                1,
                stream,
                self._retention_boundary(now),
                str(self._minimum_retained_entries),
            )
        )
        if not isinstance(result, (list, tuple)) or len(result) != 4:
            raise RuntimeError("safe trim returned an invalid response")
        return StreamTrimResult(
            stream=stream,
            removed_entries=int(result[0]),
            trim_before_id=_text(result[1]),
            inspected_groups=int(result[2]),
            stream_length_before=int(result[3]),
        )

    async def _groups(self, stream: str) -> list[ConsumerGroupBoundary]:
        raw_groups: Any = await self._redis.xinfo_groups(stream)
        pending_groups = [
            _text(values["name"])
            for values in raw_groups
            if int(values.get("pending", 0))
        ]
        pending_values = await asyncio.gather(
            *(self._redis.xpending(stream, group) for group in pending_groups)
        )
        pending_by_group = dict(
            zip(pending_groups, pending_values, strict=True)
        )
        groups: list[ConsumerGroupBoundary] = []
        for values in raw_groups:
            name = _text(values["name"])
            pending_count = int(values.get("pending", 0))
            oldest_pending_id: str | None = None
            if pending_count:
                pending: Any = pending_by_group[name]
                raw_oldest = pending.get("min")
                if raw_oldest is not None:
                    oldest_pending_id = _text(raw_oldest)
            groups.append(
                ConsumerGroupBoundary(
                    name=name,
                    last_delivered_id=_text(
                        values.get("last-delivered-id", "0-0")
                    ),
                    pending_count=pending_count,
                    oldest_pending_id=oldest_pending_id,
                )
            )
        return groups

    async def _tail_boundary(self, stream: str) -> str | None:
        if self._minimum_retained_entries == 0:
            return None
        rows: Any = await self._redis.xrevrange(
            stream,
            max="+",
            min="-",
            count=self._minimum_retained_entries,
        )
        if len(rows) < self._minimum_retained_entries:
            return None
        return _text(rows[-1][0])

    def _retention_boundary(self, now: datetime | None) -> str:
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        cutoff = max(
            0,
            int(current.timestamp() * 1000) - self._retention_seconds * 1000,
        )
        return f"{cutoff}-0"


def _stream_id_parts(value: str) -> tuple[int, int]:
    try:
        milliseconds, sequence = value.split("-", 1)
        return int(milliseconds), int(sequence)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Invalid Redis Stream ID: {value}") from error


def _validate_stream(stream: str) -> None:
    if not stream.strip():
        raise ValueError("stream cannot be empty")


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


async def _await_eval(value: Awaitable[Any] | Any) -> Any:
    return await cast(Awaitable[Any], value)


__all__ = [
    "ConsumerGroupBoundary",
    "SafeStreamTrimmer",
    "StreamTrimPlan",
    "StreamTrimResult",
]
