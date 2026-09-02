"""Build the same durable Agent graph for API and Worker processes."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from langchain_core.embeddings import Embeddings
from langchain_core.language_models import BaseChatModel
from sqlalchemy import text

from agent.admission.recovery import RequestRecovery
from agent.admission.service import AdmissionService
from agent.admission.store import RequestStore
from agent.effects.runner import ExecutorEffectSender
from agent.effects.store import EffectStore
from agent.failure.executor import FailureExecutor
from agent.failure.operations import (
    FailureOperations,
    FailureOperatorPolicy,
)
from agent.failure.recovery import FailureRecovery
from agent.failure.service import FailureService
from agent.failure.store import FailureStore
from agent.graph import build_session_graph
from agent.integrations.langgraph_adapter import SessionGraphAdapter
from agent.projections import TaskStateProjector
from agent.runtime.delivery import ProductEventRecovery
from agent.runtime.lifecycle import AgentRuntime
from agent.services import SessionWorkflowServices
from ex_agent.config import Settings
from ex_agent.executor.client import ExecutorClient
from ex_agent.llm.factory import build_chat_model, build_embeddings
from ex_agent.persistence.database import (
    create_engine,
    create_session_factory,
)
from ex_agent.persistence.repository import AgentRepository
from ex_agent.tools.registry import ToolRegistry
from ex_agent.transport.streams import ProductEventPublisher


@dataclass(frozen=True)
class AgentRuntimeResources:
    """Resources shared by an API host and its Worker counterpart."""

    graph: Any
    admission: AdmissionService
    event_handler: Any
    lifecycle: AgentRuntime
    repository: AgentRepository
    executor: ExecutorClient
    engine: Any
    registry: ToolRegistry
    failure_operations: FailureOperations


@asynccontextmanager
async def open_agent_runtime(
    settings: Settings,
    worker: Any,
    checkpointer: Any,
    *,
    model: BaseChatModel | None = None,
    embeddings: Embeddings | None = None,
    executor: ExecutorClient | None = None,
    verify_schema: bool = True,
) -> AsyncIterator[AgentRuntimeResources]:
    """Assemble resources; migrations and checkpointer setup stay external."""

    engine = create_engine(settings.agent_database_url)
    sessions = create_session_factory(engine)
    repository = AgentRepository(sessions)
    registry = ToolRegistry(settings.agent_skill_root)
    await asyncio.to_thread(registry.load)
    executor_client = executor or ExecutorClient(
        settings.executor_base_url,
        timeout_seconds=settings.executor_request_timeout_seconds,
    )
    owns_executor = executor is None
    try:
        if verify_schema:
            await _verify_schema(sessions, worker, checkpointer)
        resolved_model = model or build_chat_model(settings)
        resolved_embeddings = embeddings or build_embeddings(settings)
        services = SessionWorkflowServices(
            settings,
            repository,
            executor_client,
            registry,
            sessions=sessions,
            model=resolved_model,
            embeddings=resolved_embeddings,
        )
        graph = build_session_graph(
            services,
            worker.bindings,
            checkpointer=checkpointer,
        )
        requests = RequestStore(sessions)
        projector = TaskStateProjector(sessions)
        admission = AdmissionService(
            graph,
            worker.guard,
            requests,
            snapshot_projector=projector,
        )
        effects = EffectStore(sessions)
        sender = ExecutorEffectSender(settings, executor_client)
        failure_store = FailureStore(sessions)
        failure = FailureService(
            graph,
            worker.guard,
            failure_store,
            requests,
            FailureExecutor(effects, sender, executor_client),
            max_attempts=settings.agent_failure_max_attempts,
            retry_seconds=settings.agent_failure_retry_seconds,
            timeout_seconds=(
                settings.executor_failure_cleanup_timeout_seconds
            ),
        )
        lifecycle = AgentRuntime(
            RequestRecovery(
                admission,
                concurrency=settings.agent_recovery_concurrency,
                batch_size=settings.agent_recovery_batch_size,
                poll_seconds=(settings.agent_request_recovery_poll_seconds),
            ),
            FailureRecovery(
                failure,
                worker.store,
                concurrency=settings.agent_recovery_concurrency,
                batch_size=settings.agent_recovery_batch_size,
                poll_seconds=(settings.agent_failure_recovery_poll_seconds),
            ),
            ProductEventRecovery(
                ProductEventPublisher(
                    settings,
                    repository,
                    worker.redis,
                ),
                poll_seconds=settings.outbox_poll_milliseconds / 1000,
                idle_seconds=settings.outbox_idle_max_milliseconds / 1000,
            ),
        )
        adapter = failure.protect(
            SessionGraphAdapter(graph, snapshot_projector=projector)
        )
        failure_operations = FailureOperations(
            failure,
            failure_store,
            FailureOperatorPolicy(settings.agent_failure_operator_user_ids),
        )
        resources = AgentRuntimeResources(
            graph=graph,
            admission=admission,
            event_handler=adapter,
            lifecycle=lifecycle,
            repository=repository,
            executor=executor_client,
            engine=engine,
            registry=registry,
            failure_operations=failure_operations,
        )
        yield resources
    finally:
        if "lifecycle" in locals():
            await lifecycle.stop(settings.worker_shutdown_grace_seconds)
        if owns_executor:
            await executor_client.close()
        await engine.dispose()


async def _verify_schema(sessions, worker, checkpointer) -> None:
    """Fail before consuming when deployment initialization is incomplete."""

    async with sessions() as session:
        for statement in (
            "SELECT 1 FROM agent_tasks LIMIT 0",
            "SELECT 1 FROM agent_api_requests LIMIT 0",
            "SELECT 1 FROM agent_executor_effects LIMIT 0",
            """SELECT version,last_operation_id,last_operation_hash
            FROM agent_failure_cleanups LIMIT 0""",
        ):
            await session.execute(text(statement))
    await worker.store.counts()
    if not callable(getattr(checkpointer, "aget_tuple", None)):
        raise ValueError("Agent runtime requires an async checkpointer")
    await checkpointer.aget_tuple(
        {"configurable": {"thread_id": "__agent_runtime_probe__"}}
    )
