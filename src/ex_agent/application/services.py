from __future__ import annotations

from langchain_core.embeddings import Embeddings
from langchain_core.language_models import BaseChatModel

from ex_agent.application.capabilities.common import (
    bounded_report_markdown as _bounded_report_markdown,
)
from ex_agent.application.capabilities.common import (
    executor_source_path as _executor_source_path,
)
from ex_agent.application.capabilities.common import (
    state_execution_mode as _state_execution_mode,
)
from ex_agent.application.capabilities.conversation import (
    ConversationCapability,
)
from ex_agent.application.capabilities.execution import ExecutionCapability
from ex_agent.application.capabilities.planning import PlanningCapability
from ex_agent.application.capabilities.reporting import ReportingCapability
from ex_agent.config import Settings
from ex_agent.executor.client import ExecutorClient
from ex_agent.llm.factory import build_chat_model, build_embeddings
from ex_agent.persistence.repository import AgentRepository
from ex_agent.planners.agent import PlannerAgent
from ex_agent.tools.compiler import SourceCompiler
from ex_agent.tools.registry import ToolRegistry


class DefaultWorkflowServices(
    ConversationCapability,
    PlanningCapability,
    ExecutionCapability,
    ReportingCapability,
):
    """Composition root for the capabilities consumed by the graph."""

    def __init__(
        self,
        settings: Settings,
        repository: AgentRepository,
        executor: ExecutorClient,
        registry: ToolRegistry,
        *,
        model: BaseChatModel | None = None,
        embeddings: Embeddings | None = None,
    ) -> None:
        resolved_model = model or build_chat_model(settings)
        resolved_embeddings = embeddings or build_embeddings(settings)
        compiler = SourceCompiler(registry)
        planner = PlannerAgent(
            settings,
            registry,
            model=resolved_model,
            audit_sink=repository,
        )
        ConversationCapability.__init__(
            self,
            repository,
            resolved_model,
        )
        PlanningCapability.__init__(
            self,
            settings,
            repository,
            registry,
            resolved_model,
            resolved_embeddings,
            planner,
            compiler,
        )
        ExecutionCapability.__init__(
            self,
            settings,
            repository,
            executor,
            registry,
            resolved_model,
            compiler,
        )
        ReportingCapability.__init__(
            self,
            settings,
            repository,
            executor,
            resolved_model,
        )


__all__ = [
    "DefaultWorkflowServices",
    "_bounded_report_markdown",
    "_executor_source_path",
    "_state_execution_mode",
]
