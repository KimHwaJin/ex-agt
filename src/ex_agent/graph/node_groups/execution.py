from __future__ import annotations

from typing import Any

from langgraph.types import interrupt

from ex_agent.domain.contracts import (
    CancelRequestedSignal,
    ExecutorBoundarySignal,
    ExecutorReconciliation,
    MultiDecision,
)
from ex_agent.domain.enums import TaskStatus
from ex_agent.graph.node_groups.common import (
    WorkflowNodeGroup,
    validate_resume_signal,
)
from ex_agent.graph.state import AgentGraphState


class ExecutionNodes(WorkflowNodeGroup):
    async def submit_execution(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        receipt = await self._services.submit_execution(state)
        return {
            "phase": TaskStatus.WAITING_FOR_EXECUTOR_EVENT,
            "execution_id": str(receipt.execution_id),
            "current_operation_id": str(receipt.operation_id),
        }

    def wait_external_signal(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        raw = interrupt(
            {
                "kind": "EXECUTOR_EVENT",
                "task_id": state["active_task_id"],
                "execution_id": state["execution_id"],
                "last_event_sequence": state.get(
                    "last_executor_event_sequence",
                    0,
                ),
                "cancellable": True,
            }
        )
        signal = validate_resume_signal(raw)
        if not isinstance(
            signal,
            ExecutorBoundarySignal | CancelRequestedSignal,
        ):
            raise TypeError("Executor boundary or cancel signal is required")
        return {"external_signal": signal.model_dump(mode="json")}

    async def reconcile_executor(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        signal = ExecutorBoundarySignal.model_validate(
            state["external_signal"]
        )
        result = await self._services.reconcile_executor(state, signal)
        return {
            "executor_reconciliation": result.model_dump(mode="json"),
            "last_executor_event_sequence": signal.event_sequence,
        }

    async def adapt_multi_plan(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        reconciliation = ExecutorReconciliation.model_validate(
            state["executor_reconciliation"]
        )
        decision = await self._services.adapt_multi_plan(
            state,
            reconciliation,
        )
        updates: dict[str, Any] = {
            "multi_action": decision.action.value,
            "multi_decision": decision.model_dump(mode="json"),
        }
        if reconciliation.outcome.value == "OPERATION_FAILED":
            updates["correction_count"] = state.get("correction_count", 0) + 1
        if decision.next_step is not None:
            plan = state["plan"].model_copy(
                update={"steps": [decision.next_step]}
            )
            updates["plan"] = plan
        return updates

    async def append_operation(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        decision = MultiDecision.model_validate(state["multi_decision"])
        receipt = await self._services.append_operation(state, decision)
        return {"current_operation_id": str(receipt.operation_id)}

    async def finalize_execution(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        await self._services.finalize_execution(state)
        return {"execution_finalized": True}

    async def cancel_execution(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        signal = CancelRequestedSignal.model_validate(state["external_signal"])
        await self._services.cancel_execution(state, signal.reason)
        return {"phase": TaskStatus.CANCEL_REQUESTED}


__all__ = ["ExecutionNodes"]
