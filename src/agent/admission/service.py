"""Durable direct invocation, with explicit evidence before replaying input."""

import asyncio
from typing import Any
from uuid import UUID

from langgraph.types import Command

from agent.admission.contracts import ApiRequest, RequestRecord
from agent.admission.graph import INPUT_NODES
from agent.admission.store import RequestStore, validate_command
from agent.session import (
    InvocationGuard,
    SessionConflictError,
    validate_resume_snapshot,
    validate_start_snapshot,
)
from worker.contracts import DeferEvent


class UnsafeRecoveryError(RuntimeError):
    """Cannot prove which input owns the pending graph invocation."""


class AdmissionService:
    def __init__(
        self,
        graph: Any,
        guard: InvocationGuard,
        store: RequestStore,
        *,
        max_attempts: int = 5,
        retry_seconds: float = 5,
        invocation_timeout: float = 360,
        snapshot_projector: Any | None = None,
    ) -> None:
        if max_attempts < 1 or retry_seconds <= 0 or invocation_timeout <= 0:
            raise ValueError("Recovery budgets must be positive")
        self.graph = graph
        self.guard = guard
        self.store = store
        self.max_attempts = max_attempts
        self.retry_seconds = retry_seconds
        self.invocation_timeout = invocation_timeout
        self.snapshot_projector = snapshot_projector

    async def handle(self, command: ApiRequest) -> RequestRecord:
        """Normal router path: persist admission, then directly invoke."""
        record = await self.accept(command)
        if record.state in ("APPLIED", "REJECTED", "BLOCKED", "COMPENSATED"):
            return record
        return await self.execute(command.request_id)

    async def accept(self, command: ApiRequest) -> RequestRecord:
        """Commit Task + request without requiring the API process to live."""
        turn = command.turn
        async with self.guard.hold(turn.session_id):
            existing = await self.store.get(command.request_id)
            if existing is not None:
                validate_command(existing, command)
                return existing
            config = {"configurable": {"thread_id": turn.session_id}}
            before = await self.graph.aget_state(config)
            if command.kind == "START":
                if validate_start_snapshot(before, turn):
                    raise SessionConflictError(
                        "Task already started; reuse its request ID"
                    )
                target = "begin_task"
            else:
                assert command.interrupt_id is not None
                assert command.payload is not None
                _, target = validate_resume_snapshot(
                    before,
                    turn,
                    command.interrupt_id,
                    command.payload,
                )
                if target not in INPUT_NODES:
                    raise SessionConflictError("Unsupported input node")
            return await self.store.accept(
                command,
                target_node=target,
                base_checkpoint_id=checkpoint_id(before),
            )

    async def execute(self, request_id: UUID) -> RequestRecord:
        """Internal API/recovery call. Not an unauthenticated HTTP endpoint."""
        existing = await self.store.get(request_id)
        if existing is None:
            raise LookupError("Unknown API request")
        try:
            async with self.guard.hold(existing.command.turn.session_id):
                # Re-read/claim only after taking the API/Worker shared guard.
                existing = await self.store.get(request_id)
                assert existing is not None
                if existing.state not in ("PENDING", "RUNNING"):
                    return existing
                if existing.attempts >= self.max_attempts:
                    # The last allowed invocation may have finished before
                    # the API process died. Read-only proof costs no attempt.
                    before = await self.graph.aget_state(
                        {
                            "configurable": {
                                "thread_id": existing.command.turn.session_id,
                            }
                        }
                    )
                    try:
                        complete = (
                            accepted(before, existing)
                            and not (next_invocation(before, existing)[0])
                        )
                    except UnsafeRecoveryError:
                        complete = False
                    if complete:
                        await self._project(before)
                        return await self.store.finish(
                            existing, state="APPLIED"
                        )
                record = await self.store.claim(
                    request_id,
                    max_attempts=self.max_attempts,
                    recovery_delay=self.retry_seconds,
                )
                if record.state != "RUNNING":
                    return record
                return await self._execute(record)
        except DeferEvent:
            await self.store.defer_busy(request_id, self.retry_seconds)
            result = await self.store.get(request_id)
            assert result is not None
            return result

    async def _execute(self, record: RequestRecord) -> RequestRecord:
        config = {
            "configurable": {
                "thread_id": record.command.turn.session_id,
                "api_action": record.action,
            }
        }
        try:
            before = await self.graph.aget_state(config)
            try:
                invoke, value = next_invocation(before, record)
            except SessionConflictError as error:
                return await self.store.finish(
                    record,
                    state="REJECTED",
                    error=str(error),
                )
            if invoke:
                async with asyncio.timeout(self.invocation_timeout):
                    await self.graph.ainvoke(value, config, durability="sync")
            after = await self.graph.aget_state(config)
            if not accepted(after, record):
                raise UnsafeRecoveryError(
                    "Graph did not checkpoint API acceptance"
                )
            if next_invocation(after, record)[0]:
                raise UnsafeRecoveryError(
                    "Graph returned with unfinished API work"
                )
            await self._project(after)
            return await self.store.finish(record, state="APPLIED")
        except UnsafeRecoveryError as error:
            return await self.store.finish(
                record, state="BLOCKED", error=str(error)
            )
        except Exception as error:
            # Cancellation/lease-loss (BaseException) leaves RUNNING durable.
            # Recovery will acquire the same guard before resuming it.
            return await self.store.finish(
                record,
                state="BLOCKED"
                if record.attempts >= self.max_attempts
                else "PENDING",
                error=f"{type(error).__name__}: {error}"[:2000],
                delay=min(
                    300, self.retry_seconds * 2 ** min(record.attempts - 1, 6)
                ),
            )

    async def _project(self, snapshot: Any) -> None:
        if self.snapshot_projector is not None:
            await self.snapshot_projector(snapshot)


def checkpoint_id(snapshot: Any) -> str | None:
    return (snapshot.config or {}).get("configurable", {}).get("checkpoint_id")


def accepted(snapshot: Any, record: RequestRecord) -> bool:
    receipt = snapshot.values.get("api_receipts", {}).get(
        str(record.command.request_id)
    )
    if receipt is None:
        return False
    expected = {
        "task_id": record.command.turn.active_task_id,
        "fingerprint": record.fingerprint,
    }
    if receipt != expected:
        raise UnsafeRecoveryError("API acceptance receipt identity mismatch")
    return True


def owns_pending(snapshot: Any, record: RequestRecord) -> bool:
    interrupted = any(t.interrupts for t in snapshot.tasks)
    return bool(
        snapshot.next
        and not interrupted
        and (
            snapshot.values.get("invocation_owner")
            == {
                "source": "API",
                "id": str(record.command.request_id),
            }
        )
    )


def next_invocation(snapshot: Any, record: RequestRecord) -> tuple[bool, Any]:
    """Only the original interrupt may receive the original resume again."""
    command = record.command
    turn = command.turn
    if accepted(snapshot, record):
        # Another API input or Executor event may already own the graph.
        # Never recover that invocation using an old request's identity.
        if owns_pending(snapshot, record):
            return True, None
        if snapshot.next and not any(t.interrupts for t in snapshot.tasks):
            owner = snapshot.values.get("invocation_owner", {})
            if owner.get("source") != "EXECUTOR" or not owner.get("id"):
                raise UnsafeRecoveryError(
                    "Pending invocation owner is unknown"
                )
        return False, None
    state = snapshot.values
    if command.kind == "START":
        # LangGraph can commit input before the pure begin_task node succeeds.
        if snapshot.next == ("begin_task",) and state.get(
            "turn"
        ) == turn.model_dump(mode="json"):
            return True, None
        if state.get("task_requests", {}).get(turn.active_task_id) is not None:
            raise UnsafeRecoveryError("Task started without this API receipt")
        validate_start_snapshot(snapshot, turn)
        if checkpoint_id(snapshot) != record.base_checkpoint_id:
            raise SessionConflictError("Session changed after API admission")
        return True, {"turn": turn.model_dump(mode="json")}
    assert command.interrupt_id is not None and command.payload is not None
    if any(t.interrupts for t in snapshot.tasks):
        signal, name = validate_resume_snapshot(
            snapshot,
            turn,
            command.interrupt_id,
            command.payload,
        )
        if name != record.target_node:
            raise SessionConflictError("Input node changed after admission")
        return True, Command(
            resume={command.interrupt_id: signal.model_dump(mode="json")}
        )
    if (
        snapshot.next == (record.target_node,)
        and checkpoint_id(snapshot) == record.base_checkpoint_id
        and state.get("task_requests", {}).get(turn.active_task_id)
        == turn.fingerprint
    ):
        # Resume writes may have persisted before the pure wait node returned.
        return True, None
    raise SessionConflictError("Original interrupt is no longer available")
