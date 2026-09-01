"""Read-only safety checks for the legacy-to-integrated Worker cutover."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from typing import Any

from psycopg import AsyncConnection
from redis.asyncio import Redis
from redis.exceptions import ResponseError


@dataclass(frozen=True)
class StreamGroupState:
    stream: str
    group: str
    exists: bool
    pending: int | None = None
    lag: int | None = None
    last_delivered_id: str | None = None


@dataclass(frozen=True)
class CutoverSnapshot:
    admissions_frozen: bool
    active_tasks: int
    unfinished_commands: int
    unpublished_product_events: int
    locked_sessions: int
    command_group: StreamGroupState
    executor_event_group: StreamGroupState

    def blockers(self) -> tuple[str, ...]:
        blockers: list[str] = []
        if not self.admissions_frozen:
            blockers.append("new task admission is not confirmed frozen")
        for label, count in (
            ("active legacy tasks", self.active_tasks),
            ("unfinished legacy commands", self.unfinished_commands),
            ("unpublished product events", self.unpublished_product_events),
            ("locked sessions", self.locked_sessions),
        ):
            if count:
                blockers.append(f"{label}: {count}")
        for state in (self.command_group, self.executor_event_group):
            name = f"{state.stream}/{state.group}"
            if not state.exists:
                blockers.append(f"consumer group is missing: {name}")
                continue
            if state.pending is None or state.lag is None:
                blockers.append(f"consumer group progress is unknown: {name}")
                continue
            if state.pending:
                blockers.append(
                    f"consumer group pending {name}: {state.pending}"
                )
            if state.lag:
                blockers.append(f"consumer group lag {name}: {state.lag}")
        return tuple(blockers)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CutoverReport:
    ready: bool
    stable: bool
    blockers: tuple[str, ...]
    first: CutoverSnapshot
    second: CutoverSnapshot | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class CutoverProbe:
    """Inspect old Worker drain evidence without changing DB or Redis."""

    def __init__(
        self,
        *,
        database_url: str,
        redis_url: str,
        command_stream: str,
        command_group: str,
        executor_event_stream: str,
        executor_event_group: str,
    ) -> None:
        self.database_url = _psycopg_url(database_url)
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.command_stream = command_stream
        self.command_group = command_group
        self.executor_event_stream = executor_event_stream
        self.executor_event_group = executor_event_group

    async def close(self) -> None:
        await self.redis.aclose()

    async def snapshot(self, *, admissions_frozen: bool) -> CutoverSnapshot:
        database, groups = await asyncio.gather(
            self._database_counts(),
            self._group_states(),
        )
        return CutoverSnapshot(
            admissions_frozen=admissions_frozen,
            active_tasks=database[0],
            unfinished_commands=database[1],
            unpublished_product_events=database[2],
            locked_sessions=database[3],
            command_group=groups[0],
            executor_event_group=groups[1],
        )

    async def stable_report(
        self,
        *,
        admissions_frozen: bool,
        stable_seconds: float,
    ) -> CutoverReport:
        if stable_seconds < 0:
            raise ValueError("stable_seconds cannot be negative")
        first = await self.snapshot(admissions_frozen=admissions_frozen)
        first_blockers = first.blockers()
        if first_blockers:
            return CutoverReport(
                ready=False,
                stable=False,
                blockers=first_blockers,
                first=first,
            )
        await asyncio.sleep(stable_seconds)
        second = await self.snapshot(admissions_frozen=admissions_frozen)
        second_blockers = second.blockers()
        if second_blockers:
            return CutoverReport(
                ready=False,
                stable=False,
                blockers=second_blockers,
                first=first,
                second=second,
            )
        if first != second:
            return CutoverReport(
                ready=False,
                stable=False,
                blockers=("drain evidence changed between stable samples",),
                first=first,
                second=second,
            )
        return CutoverReport(
            ready=True,
            stable=True,
            blockers=(),
            first=first,
            second=second,
        )

    async def _database_counts(self) -> tuple[int, int, int, int]:
        async with await AsyncConnection.connect(self.database_url) as conn:
            cursor = await conn.execute(
                """SELECT
                (SELECT count(*) FROM agent_tasks
                 WHERE status NOT IN
                 ('SUCCEEDED','REJECTED','FAILED','CANCELLED')),
                (SELECT count(*) FROM agent_workflow_commands
                 WHERE state NOT IN ('DONE','FAILED')),
                (SELECT count(*) FROM agent_task_events
                 WHERE delivery_state <> 'PUBLISHED'),
                (SELECT count(*) FROM agent_session_locks WHERE locked)"""
            )
            row = await cursor.fetchone()
        if row is None:
            raise RuntimeError("Cutover database query returned no row")
        return (
            int(row[0]),
            int(row[1]),
            int(row[2]),
            int(row[3]),
        )

    async def _group_states(
        self,
    ) -> tuple[StreamGroupState, StreamGroupState]:
        command, executor = await asyncio.gather(
            self._group_state(self.command_stream, self.command_group),
            self._group_state(
                self.executor_event_stream,
                self.executor_event_group,
            ),
        )
        return command, executor

    async def _group_state(
        self,
        stream: str,
        group: str,
    ) -> StreamGroupState:
        try:
            groups = await self.redis.xinfo_groups(stream)
        except ResponseError as error:
            if "no such key" in str(error).lower():
                return StreamGroupState(stream, group, exists=False)
            raise
        entry = next((item for item in groups if item["name"] == group), None)
        if entry is None:
            return StreamGroupState(stream, group, exists=False)
        lag = entry.get("lag")
        return StreamGroupState(
            stream=stream,
            group=group,
            exists=True,
            pending=int(entry["pending"]),
            lag=None if lag is None else int(lag),
            last_delivered_id=str(entry["last-delivered-id"]),
        )


def _psycopg_url(url: str) -> str:
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


__all__ = [
    "CutoverProbe",
    "CutoverReport",
    "CutoverSnapshot",
    "StreamGroupState",
]
