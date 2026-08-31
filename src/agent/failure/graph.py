"""Retire pending graph work only after durable Executor terminal proof."""

from typing import Any

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.graph import END

from agent.failure.models import FailureCleanup
from agent.graph.state import TaskTurn
from ex_agent.domain.enums import TaskStatus


class UnsafeCleanupError(RuntimeError):
    """Conflicting evidence requires operator attention, never a new submit."""


def failure_settled(state: Any) -> dict:
    """Checkpoint update anchor, not an externally invocable work node."""
    return {}


def validate_target(snapshot: Any, record: FailureCleanup) -> bool:
    turn = TaskTurn.model_validate(record.turn)
    values = snapshot.values
    for key in ("user_id", "project_id", "session_id"):
        if key in values and values[key] != getattr(turn, key):
            raise UnsafeCleanupError("Checkpoint ownership mismatch")
    active = values.get("active_task_id")
    if active and active != str(record.task_id):
        if record.preserve_terminal:
            return False  # Late failure for an already completed old Task.
        phase = values.get("workflow", {}).get("phase")
        if snapshot.next or not phase or not TaskStatus(phase).is_terminal:
            raise UnsafeCleanupError("Another Task owns the checkpoint")
        if str(record.task_id) in values.get("task_requests", {}):
            raise UnsafeCleanupError("Cannot rewind an old failed Task")
    fingerprint = values.get("task_requests", {}).get(str(record.task_id))
    if fingerprint is not None and fingerprint != turn.fingerprint:
        raise UnsafeCleanupError("Task checkpoint fingerprint mismatch")
    return True


async def settle_graph(graph: Any, record: FailureCleanup) -> None:
    if not record.executor_status or not record.message:
        raise UnsafeCleanupError("No durable terminal proof")
    config = {
        "configurable": {"thread_id": record.session_id, "api_action": None}
    }
    snapshot = await graph.aget_state(config)
    if not validate_target(snapshot, record):
        return
    if (
        snapshot.values.get("failure_receipts", {}).get(str(record.task_id))
        == record.message
        and not snapshot.next
    ):
        return
    # Clear tasks, including stale interrupts/pending resume writes. This is
    # deliberately NOT ainvoke(None): failed business nodes must not run.
    await graph.aupdate_state(config, None, as_node=END)
    snapshot = await graph.aget_state(config)
    turn = TaskTurn.model_validate(record.turn)
    values = snapshot.values
    same_task = values.get("active_task_id") == str(record.task_id)
    workflow = (
        dict(values.get("workflow", {})) if same_task else turn.model_dump()
    )
    workflow.update(phase=record.final_status, terminal_message=record.message)
    execution_id = str(record.execution_id) if record.execution_id else ""
    if execution_id:
        workflow["execution_id"] = execution_id
    await graph.aupdate_state(
        config,
        {
            "turn": record.turn,
            "user_id": turn.user_id,
            "project_id": turn.project_id,
            "session_id": turn.session_id,
            "active_task_id": str(record.task_id),
            "execution_id": execution_id,
            "workflow": workflow,
            "ew_pending": {},
            "invocation_owner": {
                "source": "FAILURE",
                "id": str(record.task_id),
            },
            "task_requests": {
                **values.get("task_requests", {}),
                str(record.task_id): turn.fingerprint,
            },
            "failure_receipts": {
                **values.get("failure_receipts", {}),
                str(record.task_id): record.message,
            },
            "ew_sequences": {
                **values.get("ew_sequences", {}),
                **(
                    {
                        execution_id: values.get("ew_sequences", {}).get(
                            execution_id, 0
                        )
                    }
                    if execution_id
                    else {}
                ),
            },
            "messages": [
                HumanMessage(
                    content=turn.user_message, id=turn.current_input_message_id
                ),
                AIMessage(
                    content=record.message, id=f"task:{record.task_id}:result"
                ),
            ],
        },
        as_node="failure_settled",
    )
    after = await graph.aget_state(config)
    if after.next:
        raise UnsafeCleanupError("Failure settlement left pending graph work")
