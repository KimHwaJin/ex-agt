"""Map original Executor event types to host-owned async handlers."""

from __future__ import annotations

from worker.contracts import EventHandler


def build_handlers(resume_graph: EventHandler) -> dict[str, EventHandler]:
    """Edit this registry to match events the host graph waits for."""

    return {
        "execution.operation_completed": resume_graph,
        "execution.completed": resume_graph,
    }
