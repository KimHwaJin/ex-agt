"""The only place that imports and builds the receiving Agent's graph."""

from __future__ import annotations

from typing import Any

from agent.graph import build_graph
from agent_worker.graph_boundary import ExecutionBindings


def build_agent_graph(
    *,
    bindings: ExecutionBindings,
    checkpointer: Any,
) -> Any:
    """Compile the sample graph with Worker-owned dependencies.

    Replace the ``agent.graph`` import with the receiving Agent's graph
    builder. Keep this small wrapper so Worker infrastructure does not import
    the rest of that Agent directly.
    """

    return build_graph(
        bindings=bindings,
        checkpointer=checkpointer,
    )
