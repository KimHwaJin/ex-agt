"""Durable API use cases, independent of routing and agent implementation."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from agent_service.application.cursors import CursorCodec
from agent_service.application.management import active_user, fingerprint
from agent_service.application.outputs import OutputWriter
from agent_service.application.run_backends import (
    initial_state,
    resumed_state,
    select_model,
)
from agent_service.domain.management import DomainError, Page
from agent_service.domain.runs import (
    TERMINAL,
    Message,
    MessageInput,
    RunDetail,
    RunEvent,
    RunReceipt,
    RunRequest,
    RunSummary,
)
from agent_service.infrastructure.database.management import Store
from agent_service.infrastructure.database.runs import RunRepository
from agent_service.settings import Settings


class RunService:
    def __init__(self, store: Store, cursors: CursorCodec, settings: Settings):
        self.store = store
        self.cursors = cursors
        self.settings = settings

    @asynccontextmanager
    async def repository(self) -> AsyncIterator[RunRepository]:
        async with self.store.transaction() as base:
            yield RunRepository(base.connection)

    async def submit(
        self, owner: UUID, key: str, request: RunRequest
    ) -> tuple[RunReceipt, int]:
        if owner != request.user_id:
            raise DomainError("IDENTITY_MISMATCH", "사용자가 다릅니다.", 403)
        async with self.repository() as repo:
            active_user(await repo.user(owner))
            request_data = request.model_dump(mode="json", exclude={"stream"})
            if request.main_model_name is None:
                # Preserve fingerprints admitted before model selection.
                request_data.pop("main_model_name")
            digest = fingerprint(request_data)
            previous = await repo.reserve(owner, "agent.runs", key, digest)
            session = await repo.session(owner, request.session_id, lock=True)
            if previous:
                # Auth is rechecked even when replaying a prior receipt.
                await repo.run(owner, UUID(previous["run_id"]))
                return RunReceipt.model_validate(previous), previous["after"]
            if self.settings.agent_backend == "disabled":
                raise DomainError(
                    "AGENT_NOT_CONFIGURED",
                    "실행기가 연결되지 않았습니다.",
                    503,
                )
            if isinstance(request.input, MessageInput):
                model_name = select_model(
                    self.settings, request.main_model_name
                )
                if session["active_run_id"]:
                    code = (
                        "SESSION_LOCKED"
                        if session["is_locked"]
                        else ("RUN_ACTIVE")
                    )
                    raise DomainError(code, "현재 실행을 먼저 완료해 주세요.")
                run = await repo.create_run(
                    owner,
                    session,
                    request.input.model_dump(mode="json"),
                    self.settings.agent_backend,
                )
                await repo.checkpoint(
                    run,
                    initial_state(self.settings, model_name),
                    0,
                )
                after = 0
                await repo.emit(
                    run,
                    "run.accepted",
                    {
                        "run_id": str(run["run_id"]),
                        "session_id": str(request.session_id),
                        "status": "queued",
                        "backend": run["backend"],
                    },
                )
                await OutputWriter(repo, run).start(
                    uuid4(),
                    request.input.model_dump(mode="json")["content"],
                    role="user",
                )
            else:
                target = await repo.run(owner, request.input.run_id)
                if target["session_id"] != request.session_id:
                    raise DomainError(
                        "RUN_NOT_WAITING", "세션에 속한 실행이 아닙니다."
                    )
                run = await repo.run(owner, request.input.run_id, lock=True)
                self.require_backend(run)
                if (
                    run["session_id"] != request.session_id
                    or session["active_run_id"] != run["run_id"]
                    or run["status"] != "awaiting_input"
                ):
                    raise DomainError(
                        "RUN_NOT_WAITING", "응답을 기다리는 실행이 아닙니다."
                    )
                pending = await repo.pending(run["run_id"])
                response = request.input.response.model_dump(mode="json")
                if (
                    not pending
                    or pending[0]["interrupt_id"] != request.input.interrupt_id
                    or pending[0]["response_type"] != response["type"]
                    or pending[0]["schema_version"]
                    != response["schema_version"]
                ):
                    raise DomainError(
                        "INTERRUPT_MISMATCH", "현재 입력 요청과 다릅니다."
                    )
                after = run["event_seq"]
                await repo.connection.execute(
                    "UPDATE management.run_interrupts "
                    "SET status = 'answered', response = %s, "
                    "updated_at = clock_timestamp(), updated_by = %s "
                    "WHERE interrupt_id = %s",
                    (Jsonb(response), owner, request.input.interrupt_id),
                )
                cp = resumed_state(run, response, pending[0])
                model_name = select_model(
                    self.settings,
                    request.main_model_name,
                    run["checkpoint"].get("model_name"),
                )
                if model_name is not None:
                    cp["model_name"] = model_name
                await repo.checkpoint(run, cp, 0)
                await repo.set_status(run, "queued")
                await repo.emit(
                    run,
                    "run.accepted",
                    {
                        "run_id": str(run["run_id"]),
                        "session_id": str(request.session_id),
                        "status": "queued",
                        "resumed": True,
                        "backend": run["backend"],
                    },
                )
            if model_name is not None:
                await repo.emit(
                    run,
                    "model.selected",
                    {
                        "model_name": model_name,
                        "model_provider": self.settings.model_provider,
                        "selected_by": str(owner),
                        "resumed": not isinstance(request.input, MessageInput),
                    },
                )
            receipt = RunReceipt.model_validate(run)
            await repo.finish(
                owner,
                "agent.runs",
                key,
                {
                    **receipt.model_dump(mode="json"),
                    "after": after,
                },
            )
            return receipt, after

    async def detail(self, owner: UUID, run_id: UUID) -> RunDetail:
        async with self.repository() as repo:
            active_user(await repo.user(owner))
            # A short lock makes the status, interrupt and event watermark
            # coherent. No DB connection is retained by a streaming response.
            run = await repo.run(owner, run_id, lock=True)
            return RunDetail.model_validate(
                {
                    **run,
                    "executions": await repo.executions(run_id),
                    "pending_interrupts": await repo.pending(run_id),
                    "last_event_id": f"{run_id}:{run['event_seq']}",
                }
            )

    async def listing(
        self,
        owner: UUID,
        session_id: UUID,
        limit: int,
        cursor: str | None,
        *,
        messages: bool,
    ) -> Page:
        kind = "messages" if messages else "runs"
        scope = f"{kind}:{owner}:{session_id}"
        boundary = self.cursors.decode(scope, cursor)
        async with self.repository() as repo:
            active_user(await repo.user(owner))
            await repo.session(owner, session_id)
            rows = await repo.page(
                session_id, limit + 1, boundary, messages=messages
            )
        model = Message if messages else RunSummary
        items = [model.model_validate(row) for row in rows[:limit]]
        more = len(rows) > limit
        next_cursor = None
        if more:
            last = rows[limit - 1]
            key = "message_id" if messages else "run_id"
            next_cursor = self.cursors.encode(
                scope, last["created_at"], last[key]
            )
        return Page(items=items, has_more=more, next_cursor=next_cursor)

    async def cancel(self, owner: UUID, run_id: UUID) -> RunReceipt:
        async with self.repository() as repo:
            active_user(await repo.user(owner))
            run = await repo.run(owner, run_id, lock=True)
            if run["status"] in {"cancelled", "cancelling"}:
                return RunReceipt.model_validate(run)
            if run["status"] in TERMINAL:
                raise DomainError(
                    "RUN_NOT_CANCELLABLE", "이미 종료된 실행입니다."
                )
            executions = await repo.executions(run_id)
            if (
                run["status"] in {"queued", "awaiting_input"}
                and not executions
            ):
                await OutputWriter(repo, run).terminate("cancelled")
            else:
                self.require_backend(run)
                await repo.set_status(run, "cancelling")
                await repo.checkpoint(run, run["checkpoint"], 0)
            return RunReceipt.model_validate(run)

    def require_backend(self, run: dict) -> None:
        if run["backend"] != self.settings.agent_backend:
            raise DomainError(
                "RUN_BACKEND_UNAVAILABLE",
                "이 실행은 이전 실행기가 필요합니다. "
                "해당 backend로 실행기를 구동한 뒤 처리해 주세요.",
                409,
            )

    @staticmethod
    def parse_event_id(run_id: UUID, token: str | None) -> int:
        if token is None:
            return 0
        try:
            prefix, sequence = token.split(":")
            if UUID(prefix) != run_id or not sequence.isascii():
                raise ValueError
            if not sequence.isdecimal() or len(sequence) > 19:
                raise ValueError
            return int(sequence)
        except (ValueError, TypeError):
            raise DomainError(
                "INVALID_EVENT_ID", "유효하지 않은 이벤트 ID입니다.", 422
            ) from None

    async def event_batch(
        self, owner: UUID, run_id: UUID, after: int
    ) -> tuple[list[RunEvent], str, int]:
        async with self.repository() as repo:
            active_user(await repo.user(owner))
            run = await repo.run(owner, run_id, lock=True)
            if after > run["event_seq"]:
                raise DomainError(
                    "INVALID_EVENT_ID", "존재하지 않는 이벤트 ID입니다.", 422
                )
            if after < run["first_event_seq"] - 1:
                raise DomainError(
                    "STREAM_CURSOR_EXPIRED",
                    "메시지와 상태를 복원해 주세요.",
                    410,
                )
            events = await repo.events(run_id, after)
            if events and events[0].sequence != after + 1:
                raise DomainError(
                    "STREAM_GAP", "이벤트 복원이 필요합니다.", 410
                )
            return events, run["status"], run["event_seq"]
