"""Serialized graph calls; durable API request admission is a separate layer.

Do not expose the raw graph or arbitrary State patches to HTTP callers.
This coordinator does not replace authentication, the business Task store,
or the durable request journal/recovery loop required before production.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol

from langgraph.types import Command

from agent.graph.state import TaskTurn
from ex_agent.domain.contracts import (
    CancelRequestedSignal,
    ExecutorBoundarySignal,
    PlanReviewDecision,
    ResumeSignal,
    WorkflowSelectionDecision,
)
from ex_agent.graph.node_groups.common import validate_resume_signal


class InvocationGuard(Protocol):
    def hold(self, session_id: str) -> AbstractAsyncContextManager[None]: ...


class SessionConflictError(ValueError):
    """Reject a stale or conflicting request before writing a checkpoint."""


def validate_user_decision(boundary: dict, signal: ResumeSignal) -> None:
    if isinstance(signal, PlanReviewDecision):
        values = signal.model_dump(mode="json")
        fields = (
            "plan_revision_id",
            "plan_revision_number",
            "public_payload_hash",
        )
        if any(values[key] != boundary.get(key) for key in fields):
            raise SessionConflictError("Plan decision is stale")
        if (
            signal.decision.value == "APPROVE"
            and boundary.get("risk", {}).get("level") == "HIGH"
            and not signal.risk_acknowledged
        ):
            raise SessionConflictError("Risk acknowledgement is required")
    elif isinstance(signal, WorkflowSelectionDecision):
        if signal.proposal_version != boundary.get("proposal_version"):
            raise SessionConflictError("Workflow proposal is stale")
        if signal.workflow_version_id is None:
            return
        candidate = next(
            (
                item
                for item in boundary.get("candidates", [])
                if item["workflow_version_id"]
                == str(signal.workflow_version_id)
            ),
            None,
        )
        if candidate is None or candidate.get("public_payload_hash") != (
            signal.public_payload_hash
        ):
            raise SessionConflictError("Workflow candidate is stale")
        if (candidate.get("risk") or {}).get("level") == "HIGH" and not (
            signal.risk_acknowledged
        ):
            raise SessionConflictError("Risk acknowledgement is required")


class SessionCoordinator:
    def __init__(self, graph: Any, guard: InvocationGuard) -> None:
        self.graph = graph
        self.guard = guard

    async def start(self, turn: TaskTurn) -> Any:
        config = {"configurable": {"thread_id": turn.session_id}}
        async with self.guard.hold(turn.session_id):
            before = await self.graph.aget_state(config)
            state = before.values
            for key in ("user_id", "project_id", "session_id"):
                if key in state and state[key] != getattr(turn, key):
                    raise SessionConflictError("Session ownership mismatch")
            fingerprint = state.get("task_requests", {}).get(
                turn.active_task_id
            )
            if fingerprint is not None:
                if (
                    fingerprint != turn.fingerprint
                    or state.get("active_task_id") != turn.active_task_id
                ):
                    raise SessionConflictError("Task ID reuse is forbidden")
                # A retry is a read, not a restart or recovery attempt.
                return before
            if before.next:
                raise SessionConflictError("Session has unfinished work")
            await self.graph.ainvoke(
                {"turn": turn.model_dump(mode="json")},
                config,
                durability="sync",
            )
            return await self.graph.aget_state(config)

    async def resume_user(
        self,
        *,
        turn: TaskTurn,
        interrupt_id: str,
        payload: dict[str, Any],
    ) -> Any:
        signal = validate_resume_signal(payload)
        if isinstance(signal, ExecutorBoundarySignal):
            raise SessionConflictError("Executor events belong to Worker")
        config = {"configurable": {"thread_id": turn.session_id}}
        async with self.guard.hold(turn.session_id):
            before = await self.graph.aget_state(config)
            state = before.values
            if (
                state.get("active_task_id") != turn.active_task_id
                or state.get("task_requests", {}).get(turn.active_task_id)
                != turn.fingerprint
            ):
                raise SessionConflictError("Task identity mismatch")
            boundaries = [i for t in before.tasks for i in t.interrupts]
            if len(boundaries) != 1 or boundaries[0].id != interrupt_id:
                raise SessionConflictError("Interrupt is stale")
            boundary = boundaries[0].value
            expected = (
                "EXECUTOR_EVENT"
                if isinstance(signal, CancelRequestedSignal)
                else signal.type.value
            )
            if not isinstance(boundary, dict) or (
                boundary.get("kind") != expected
                or boundary.get("task_id") != turn.active_task_id
            ):
                raise SessionConflictError("Interrupt kind mismatch")
            if (
                isinstance(signal, CancelRequestedSignal)
                and str(signal.task_id) != turn.active_task_id
            ):
                raise SessionConflictError("Cancel task identity mismatch")
            validate_user_decision(boundary, signal)
            await self.graph.ainvoke(
                Command(resume={interrupt_id: signal.model_dump(mode="json")}),
                config,
                durability="sync",
            )
            return await self.graph.aget_state(config)
