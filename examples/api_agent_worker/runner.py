from __future__ import annotations

import hashlib
import json
from typing import Any
from uuid import UUID

from langgraph.types import Command

from ex_agent.transport.consumer import PermanentMessageError
from examples.api_agent_worker.contracts import ExecutorBoundarySignal
from examples.api_agent_worker.ports import BoundaryNotReadyError


def _action(kind: str, action_id: str, payload: dict[str, Any]) -> dict:
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fingerprint = hashlib.sha256(f"{kind}:{encoded}".encode()).hexdigest()
    return {
        "kind": kind,
        "action_id": action_id,
        "fingerprint": fingerprint,
        "payload": payload,
    }


def _interrupts(snapshot: Any) -> list[Any]:
    return [item for task in snapshot.tasks for item in task.interrupts]


class SharedGraphRunner:
    """Call only while the host's shared RunGuard is held.

    API and Worker construct separate graph instances with the same schema,
    persistent checkpoint database, and task-scoped thread IDs.
    """

    def __init__(self, graph: Any) -> None:
        self.graph = graph

    @staticmethod
    def config(task_id: UUID) -> dict[str, Any]:
        return {"configurable": {"thread_id": str(task_id)}}

    async def view(self, task_id: UUID) -> dict[str, Any]:
        snapshot = await self.graph.aget_state(self.config(task_id))
        return {
            "task_id": str(task_id),
            "phase": snapshot.values.get("phase", "NOT_STARTED"),
            "execution_id": snapshot.values.get("execution_id") or None,
            "interrupts": [item.value for item in _interrupts(snapshot)],
        }

    async def start(self, task_id: UUID, objective: str) -> None:
        config = self.config(task_id)
        snapshot = await self.graph.aget_state(config)
        if snapshot.values:
            if snapshot.values.get("objective") != objective:
                raise ValueError("Task identity was reused with new input")
            # Do not restart another user's boundary on HTTP redelivery.
            if (
                snapshot.next
                and not _interrupts(snapshot)
                and not snapshot.values.get("pending_action")
            ):
                await self.graph.ainvoke(None, config)
            return
        await self.graph.ainvoke(
            {
                "task_id": str(task_id),
                "objective": objective,
                "phase": "PLAN_REVIEW",
                "execution_id": "",
                "pending_action": {},
                "receipts": {},
                "last_event_sequence": 0,
                "applied_count": 0,
            },
            config,
        )

    async def review(
        self, task_id: UUID, request_id: UUID, approved: bool
    ) -> bool:
        return await self._apply(
            task_id,
            _action(
                "USER_REVIEW", f"user:{request_id}", {"approved": approved}
            ),
            expected_kind="PLAN_REVIEW",
        )

    async def executor_signal(
        self,
        task_id: UUID,
        command_id: UUID,
        signal: ExecutorBoundarySignal,
    ) -> bool:
        return await self._apply(
            task_id,
            _action(
                "EXECUTOR_SIGNAL",
                f"command:{command_id}",
                signal.model_dump(mode="json"),
            ),
            expected_kind="EXECUTOR_EVENT",
            signal=signal,
        )

    async def _apply(
        self,
        task_id: UUID,
        action: dict,
        *,
        expected_kind: str,
        signal: ExecutorBoundarySignal | None = None,
    ) -> bool:
        config = self.config(task_id)
        snapshot = await self.graph.aget_state(config)
        receipts = snapshot.values.get("receipts", {})
        prior = receipts.get(action["action_id"])
        if prior is not None and prior != action["fingerprint"]:
            raise PermanentMessageError("Action identity payload mismatch")
        pending = snapshot.values.get("pending_action", {})
        interrupted = _interrupts(snapshot)
        if prior is not None:
            if (
                pending.get("action_id") == action["action_id"]
                and snapshot.next
                and not interrupted
            ):
                await self.graph.ainvoke(None, config)
            return False
        # A preceding wait node may already have checkpointed this action.
        # Continue its pending node, never inject a second resume value.
        if snapshot.next and not interrupted:
            if pending != action:
                raise BoundaryNotReadyError(
                    "Another invocation needs recovery"
                )
            await self.graph.ainvoke(None, config)
            await self._require_receipt(config, action)
            return True
        if len(interrupted) != 1:
            if (
                signal is not None
                and not snapshot.next
                and snapshot.values.get("phase")
                in {"SUCCEEDED", "FAILED", "CANCELLED"}
            ):
                # A final operation and its execution can both emit a
                # boundary. Consume the later one without reopening END.
                self._validate_signal(snapshot.values, signal)
                await self.graph.aupdate_state(
                    config,
                    {
                        "receipts": {
                            **receipts,
                            action["action_id"]: action["fingerprint"],
                        },
                        "last_event_sequence": signal.event_sequence,
                    },
                )
                return True
            raise BoundaryNotReadyError("Expected one ready interrupt")
        boundary = interrupted[0]
        if boundary.value.get("kind") != expected_kind:
            raise BoundaryNotReadyError("Checkpoint belongs to another input")
        if signal is not None:
            self._validate_signal(snapshot.values, signal)
        await self.graph.ainvoke(Command(resume={boundary.id: action}), config)
        await self._require_receipt(config, action)
        return True

    @staticmethod
    def _validate_signal(values: dict, signal: ExecutorBoundarySignal) -> None:
        if values.get("execution_id") != str(signal.execution_id):
            raise PermanentMessageError("Execution binding mismatch")
        if signal.event_sequence <= values["last_event_sequence"]:
            raise PermanentMessageError("Unrecognized stale boundary")

    async def _require_receipt(self, config: dict, action: dict) -> None:
        snapshot = await self.graph.aget_state(config)
        if (
            snapshot.values.get("receipts", {}).get(action["action_id"])
            != (action["fingerprint"])
        ):
            raise BoundaryNotReadyError("Command did not reach its checkpoint")
