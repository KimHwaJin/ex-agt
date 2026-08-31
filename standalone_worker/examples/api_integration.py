"""Functions to call from the recipient's existing API service.

No routes, authorization, task admission or Executor submission are imposed.
Those stay with the recipient's API+Agent.
"""

from typing import Any
from uuid import UUID

from executor_worker import DeferEvent, ExecutorWorker


async def attach_execution(
    worker: ExecutorWorker,
    graph: Any,
    *,
    execution_id: UUID,
    session_id: str,
    task_id: str,
) -> None:
    # Use the same namespace/Redis and checkpoint DB as Worker.
    # For real submission, acquire this guard BEFORE graph invocation;
    # register the returned execution in the idempotent submit node.
    async with worker.guard.hold(session_id):
        config = {"configurable": {"thread_id": session_id}}
        snapshot = await graph.aget_state(config)
        if snapshot.next:
            if snapshot.values.get(
                "active_task_id"
            ) == task_id and snapshot.values.get("execution_id") == str(
                execution_id
            ):
                await worker.bindings.register(
                    execution_id=execution_id,
                    session_id=session_id,
                    task_id=task_id,
                )
                return
            raise DeferEvent("Previous task has not finished")
        if snapshot.values.get("active_task_id") == task_id:
            if snapshot.values.get("execution_id") != str(execution_id):
                raise ValueError("Task identity reused with another execution")
            return
        await worker.bindings.register(
            execution_id=execution_id,
            session_id=session_id,
            task_id=task_id,
        )
        await graph.ainvoke(
            {
                "active_task_id": task_id,
                "execution_id": str(execution_id),
                "ew_pending": {},
                "finished": False,
                # Preserve receipts/sequences when moving to the next Task.
                "ew_receipts": snapshot.values.get("ew_receipts", {}),
                "ew_sequences": snapshot.values.get("ew_sequences", {}),
                "results": [],
            },
            config,
            durability="sync",
        )
