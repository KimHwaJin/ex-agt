"""Read-only safety checks for the legacy-to-integrated Worker cutover."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

import httpx
from psycopg import AsyncConnection
from redis.asyncio import Redis
from redis.exceptions import ResponseError

ADMISSION_SCOPE = "NEW_TASK_START"
ADMISSION_STATE = "FROZEN"


@dataclass(frozen=True)
class AdmissionFreezeState:
    """One observable BFF admission-freeze state."""

    source: str
    verified: bool
    schema_version: int | None = None
    state: str | None = None
    scope: str | None = None
    freeze_id: str | None = None
    revision: str | None = None
    frozen_at: str | None = None
    expires_at: str | None = None
    error: str | None = None

    def blockers(self) -> tuple[str, ...]:
        blockers: list[str] = []
        if not self.verified:
            detail = self.error or "unknown verification failure"
            blockers.append(f"admission freeze is not verified: {detail}")
        if self.state != ADMISSION_STATE:
            blockers.append(
                f"admission state must be {ADMISSION_STATE}: {self.state}"
            )
        if self.scope != ADMISSION_SCOPE:
            blockers.append(
                f"admission scope must be {ADMISSION_SCOPE}: {self.scope}"
            )
        return tuple(blockers)


class AdmissionFreezeProbe(Protocol):
    async def snapshot(self) -> AdmissionFreezeState: ...

    async def close(self) -> None: ...


class HttpAdmissionFreezeProbe:
    """Verify a correlated freeze receipt from the trusted BFF boundary."""

    def __init__(
        self,
        *,
        url: str,
        expected_freeze_id: str,
        bearer_token: str,
        timeout_seconds: float = 5,
        client: httpx.AsyncClient | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        parsed_url = httpx.URL(url)
        if parsed_url.query or parsed_url.fragment or parsed_url.userinfo:
            raise ValueError(
                "admission evidence URL cannot contain credentials, query, "
                "or fragment"
            )
        if parsed_url.scheme not in {"http", "https"}:
            raise ValueError("admission evidence URL must use HTTP or HTTPS")
        if not expected_freeze_id.strip():
            raise ValueError("expected_freeze_id cannot be empty")
        if not bearer_token.strip():
            raise ValueError("admission bearer token cannot be empty")
        self._url = str(parsed_url)
        self._expected_freeze_id = expected_freeze_id
        self._bearer_token = bearer_token
        self._now = now or (lambda: datetime.now(UTC))
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=timeout_seconds,
            follow_redirects=False,
        )

    async def snapshot(self) -> AdmissionFreezeState:
        try:
            response = await self._client.get(
                self._url,
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {self._bearer_token}",
                },
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, Mapping):
                raise ValueError("response body must be a JSON object")
            return self._validate(payload)
        except (httpx.HTTPError, ValueError, TypeError) as error:
            return AdmissionFreezeState(
                source=self._url,
                verified=False,
                error=f"{type(error).__name__}: {error}",
            )

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def _validate(self, payload: Mapping[str, Any]) -> AdmissionFreezeState:
        schema_version = payload.get("schema_version")
        state = _required_text(payload, "state")
        scope = _required_text(payload, "scope")
        freeze_id = _required_text(payload, "freeze_id")
        revision = _required_text(payload, "revision")
        frozen_at = _timestamp(payload, "frozen_at", required=True)
        expires_at = _timestamp(payload, "expires_at", required=False)
        assert frozen_at is not None
        if schema_version != 1:
            raise ValueError("schema_version must be 1")
        if state != ADMISSION_STATE:
            raise ValueError(f"state must be {ADMISSION_STATE}")
        if scope != ADMISSION_SCOPE:
            raise ValueError(f"scope must be {ADMISSION_SCOPE}")
        if freeze_id != self._expected_freeze_id:
            raise ValueError("freeze_id does not match this deployment")
        if expires_at is not None and expires_at <= self._now():
            raise ValueError("freeze receipt has expired")
        return AdmissionFreezeState(
            source=self._url,
            verified=True,
            schema_version=1,
            state=state,
            scope=scope,
            freeze_id=freeze_id,
            revision=revision,
            frozen_at=frozen_at.isoformat(),
            expires_at=(
                None if expires_at is None else expires_at.isoformat()
            ),
        )


class UnsafeStaticAdmissionFreezeProbe:
    """Local rehearsal escape hatch; never use as production evidence."""

    async def snapshot(self) -> AdmissionFreezeState:
        return AdmissionFreezeState(
            source="unsafe-operator-assertion",
            verified=True,
            schema_version=1,
            state=ADMISSION_STATE,
            scope=ADMISSION_SCOPE,
            freeze_id="unsafe-local-rehearsal",
            revision="unsafe-local-rehearsal",
            frozen_at="not-observed",
        )

    async def close(self) -> None:
        return None


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
    admission: AdmissionFreezeState
    active_tasks: int
    unfinished_commands: int
    unpublished_product_events: int
    locked_sessions: int
    command_group: StreamGroupState
    executor_event_group: StreamGroupState

    def blockers(self) -> tuple[str, ...]:
        blockers = list(self.admission.blockers())
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
        admission_probe: AdmissionFreezeProbe,
    ) -> None:
        self.database_url = _psycopg_url(database_url)
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.command_stream = command_stream
        self.command_group = command_group
        self.executor_event_stream = executor_event_stream
        self.executor_event_group = executor_event_group
        self.admission_probe = admission_probe

    async def close(self) -> None:
        await asyncio.gather(
            self.redis.aclose(),
            self.admission_probe.close(),
        )

    async def snapshot(self) -> CutoverSnapshot:
        database, groups, admission = await asyncio.gather(
            self._database_counts(),
            self._group_states(),
            self.admission_probe.snapshot(),
        )
        return CutoverSnapshot(
            admission=admission,
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
        stable_seconds: float,
    ) -> CutoverReport:
        if stable_seconds < 0:
            raise ValueError("stable_seconds cannot be negative")
        first = await self.snapshot()
        first_blockers = first.blockers()
        if first_blockers:
            return CutoverReport(
                ready=False,
                stable=False,
                blockers=first_blockers,
                first=first,
            )
        await asyncio.sleep(stable_seconds)
        second = await self.snapshot()
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


def _required_text(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _timestamp(
    payload: Mapping[str, Any],
    key: str,
    *,
    required: bool,
) -> datetime | None:
    value = payload.get(key)
    if value is None and not required:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{key} must be an RFC 3339 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{key} must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{key} must include a timezone")
    return parsed


__all__ = [
    "ADMISSION_SCOPE",
    "ADMISSION_STATE",
    "AdmissionFreezeProbe",
    "AdmissionFreezeState",
    "CutoverProbe",
    "CutoverReport",
    "CutoverSnapshot",
    "HttpAdmissionFreezeProbe",
    "StreamGroupState",
    "UnsafeStaticAdmissionFreezeProbe",
]
