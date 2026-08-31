import asyncio

from agent.effects.files import input_file, restore_files
from ex_agent.application.capabilities.common import (
    task_id,
    validate_plan_execution_mode,
)
from ex_agent.application.state import AgentGraphState
from ex_agent.config import Settings
from ex_agent.domain.contracts import CompiledStep, PersistedPlan, PlanDraft
from ex_agent.persistence.repository import AgentRepository
from ex_agent.tools.compiler import SourceCompiler
from ex_agent.tools.registry import ToolRegistry


class DurablePlans:
    def __init__(
        self,
        settings: Settings,
        repository: AgentRepository,
        registry: ToolRegistry,
        compiler: SourceCompiler,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.registry = registry
        self.compiler = compiler

    async def persist(
        self,
        state: AgentGraphState,
        plan: PlanDraft,
    ) -> PersistedPlan:
        validate_plan_execution_mode(state, plan)
        compiled = await asyncio.to_thread(self._compile, state, plan)
        return await self.repository.plans.persist(
            task_id(state),
            plan,
            compiled,
            self.registry.registry_snapshot_hash(),
            state.get("revision_feedback"),
            expected_revision_number=state.get("plan_revision_number", 0) + 1,
        )

    def _compile(
        self,
        state: AgentGraphState,
        plan: PlanDraft,
    ) -> list[tuple[CompiledStep, str]]:
        compiled = []
        files = []
        for step in plan.steps:
            item = self.compiler.compile(step)
            source = input_file(state["active_task_id"], item.source, "py")
            files.append(source)
            compiled.append((item, source["path"]))
        restore_files(self.settings.executor_shared_storage_root, files)
        return compiled
