"""One session-guarded compensation: confirm, checkpoint, then release."""

import asyncio
from typing import Any
from uuid import UUID

from agent.admission.service import accepted, next_invocation
from agent.admission.store import RequestStore
from agent.failure.executor import FailureExecutor, failure_message
from agent.failure.graph import (
    UnsafeCleanupError,
    settle_graph,
    validate_target,
)
from agent.failure.store import FailureStore
from agent.session import InvocationGuard
from worker import DeferEvent, EventContext, IgnoreEvent
from worker.store import Store


class FailureService:
    def __init__(
        self,
        graph: Any,
        guard: InvocationGuard,
        store: FailureStore,
        requests: RequestStore,
        executor: FailureExecutor,
        *,
        max_attempts: int = 20,
        retry_seconds: float = 5,
        timeout_seconds: float = 60,
    ) -> None:
        if max_attempts < 1 or retry_seconds <= 0 or timeout_seconds <= 0:
            raise ValueError("Cleanup budgets must be positive")
        self.graph, self.guard, self.store = graph, guard, store
        self.requests, self.executor = requests, executor
        self.max_attempts, self.retry_seconds = max_attempts, retry_seconds
        self.timeout_seconds = timeout_seconds

    async def capture_api(self, request_id: UUID) -> None:
        request = await self.requests.get(request_id)
        if request is None or request.state != "BLOCKED":
            return
        async with self.guard.hold(request.command.turn.session_id):
            request = await self.requests.get(request_id)
            if request is None or request.state != "BLOCKED":
                return
            task_id = UUID(request.command.turn.active_task_id)
            if await self.store.get(task_id) is None:
                snapshot = await self.graph.aget_state(
                    {
                        "configurable": {
                            "thread_id": request.command.turn.session_id,
                        }
                    }
                )
                try:
                    applied = (
                        accepted(snapshot, request)
                        and not next_invocation(snapshot, request)[0]
                    )
                except (RuntimeError, ValueError):
                    applied = False
                if applied:
                    await self.requests.resolve_applied(request)
                    return
            await self.store.ensure(
                task_id,
                request.command.turn.session_id,
                source={"kind": "API", "request_id": str(request_id)},
                reason=request.last_error
                or "API invocation attempts exhausted",
            )

    async def capture_worker(
        self, worker_store: Store, command_id: UUID
    ) -> None:
        row = await worker_store.command(command_id)
        if row is None or row["state"] != "FAILED":
            return
        async with self.guard.hold(row["session_id"]):
            row = await worker_store.command(command_id)
            if row is None or row["state"] != "FAILED":
                return
            ctx = worker_store.context(row)
            record = await self.store.get(UUID(ctx.task_id))
            if record is not None:
                if record.session_id != ctx.session_id or (
                    record.execution_id
                    and record.execution_id != ctx.execution_id
                ):
                    raise UnsafeCleanupError("Failed command binding mismatch")
                if record.state == "DONE":
                    await worker_store.resolve_failed(
                        command_id,
                        retry=False,
                        actor="agent-cleanup",
                        reason="Task failure cleanup completed",
                    )
                return
            snapshot = await self.graph.aget_state(ctx.graph_config)
            receipt = snapshot.values.get("ew_receipts", {}).get(
                str(command_id)
            )
            owns_pending = (
                snapshot.next
                and not any(t.interrupts for t in snapshot.tasks)
                and (
                    snapshot.values.get("invocation_owner")
                    == {
                        "source": "EXECUTOR",
                        "id": str(command_id),
                    }
                )
            )
            if receipt == str(ctx.event.event_id) and not owns_pending:
                await worker_store.resolve_failed(
                    command_id,
                    retry=False,
                    actor="agent-cleanup",
                    reason="Checkpoint proves event applied",
                )
                return
            await self.store.ensure(
                UUID(ctx.task_id),
                ctx.session_id,
                execution_id=ctx.execution_id,
                source={
                    "kind": "WORKER",
                    "namespace": ctx.namespace,
                    "command_id": str(command_id),
                    "event_id": str(ctx.event.event_id),
                },
                reason=row["last_error"]
                or "Worker handler attempts exhausted",
            )

    async def execute(self, task_id: UUID) -> None:
        record = await self.store.get(task_id)
        if record is None or record.state != "PENDING":
            return
        try:
            async with self.guard.hold(record.session_id):
                record = await self.store.get(task_id)
                assert record is not None
                if record.state != "PENDING":
                    return
                # Final DB response may be lost after graph settlement on the
                # last attempt. Complete the DB without running any work.
                if record.executor_status and record.message:
                    snapshot = await self.graph.aget_state(
                        {
                            "configurable": {
                                "thread_id": record.session_id,
                            }
                        }
                    )
                    try:
                        settled = (
                            not snapshot.next
                            and snapshot.values.get(
                                "failure_receipts", {}
                            ).get(str(task_id))
                            == record.message
                        ) or not validate_target(snapshot, record)
                    except UnsafeCleanupError as error:
                        await self.store.defer(
                            record,
                            f"{type(error).__name__}: {error}",
                            delay=0,
                            blocked=True,
                        )
                        return
                    if settled:
                        await self.store.finish(record)
                        return
                record = await self.store.claim(
                    task_id,
                    max_attempts=self.max_attempts,
                    delay=self.retry_seconds,
                )
                if record.state != "PENDING":
                    return
                try:
                    async with asyncio.timeout(self.timeout_seconds):
                        snapshot = await self.graph.aget_state(
                            {
                                "configurable": {
                                    "thread_id": record.session_id,
                                }
                            }
                        )
                        validate_target(snapshot, record)
                        if record.executor_status is None:
                            execution_id = await self.executor.resolve(
                                record, snapshot
                            )
                            if execution_id:
                                record = await self.store.bind_execution(
                                    record, execution_id
                                )
                            status = (
                                await self.executor.confirm(
                                    record, execution_id
                                )
                                if execution_id
                                else "NOT_REQUIRED"
                            )
                            record = await self.store.confirm(
                                record,
                                execution_id=execution_id,
                                status=status,
                                message=failure_message(record.reason, status),
                            )
                        await settle_graph(self.graph, record)
                        await self.store.finish(record)
                except Exception as error:
                    await self.store.defer(
                        record,
                        f"{type(error).__name__}: {error}",
                        blocked=isinstance(error, UnsafeCleanupError)
                        or record.attempts >= self.max_attempts,
                        delay=min(
                            300,
                            self.retry_seconds
                            * 2 ** min(record.attempts - 1, 6),
                        ),
                    )
        except DeferEvent:
            await self.store.defer_busy(task_id, self.retry_seconds)

    def protect(self, handler):
        """Wrap the Agent handler; Dispatcher already holds session guard."""

        async def guarded(context: EventContext) -> None:
            record = await self.store.get(UUID(context.task_id))
            if record:
                if record.session_id != context.session_id:
                    raise UnsafeCleanupError(
                        "Cleanup handler identity mismatch"
                    )
                if record.state == "DONE":
                    raise IgnoreEvent("Task failure cleanup has completed")
                raise DeferEvent("Task failure cleanup owns this invocation")
            await handler(context)

        return guarded
