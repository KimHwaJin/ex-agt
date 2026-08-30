from __future__ import annotations

from ex_agent.application.ports import WorkflowServices
from ex_agent.graph.node_groups.common import (
    WorkflowNodeGroup,
)
from ex_agent.graph.node_groups.common import (
    persisted_plan_updates as _persisted_plan_updates,
)
from ex_agent.graph.node_groups.conversation import ConversationNodes
from ex_agent.graph.node_groups.execution import ExecutionNodes
from ex_agent.graph.node_groups.planning import PlanningNodes
from ex_agent.graph.node_groups.terminal import TerminalNodes


class WorkflowNodes(
    ConversationNodes,
    PlanningNodes,
    ExecutionNodes,
    TerminalNodes,
):
    """Compatibility façade exposing every node used by the graph builder."""

    def __init__(self, services: WorkflowServices) -> None:
        WorkflowNodeGroup.__init__(self, services)


__all__ = ["WorkflowNodes", "_persisted_plan_updates"]
