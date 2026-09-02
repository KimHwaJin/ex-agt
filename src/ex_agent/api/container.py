from typing import Any

from fastapi import Depends, Request
from redis.asyncio import Redis

from ex_agent.api.identity import (
    ForwardedUserId,
    IdentityProvider,
    TrustedHeaderIdentityProvider,
)
from ex_agent.application.promotions import WorkflowPromotionService
from ex_agent.application.workflow_lifecycle import WorkflowLifecycleService
from ex_agent.config import Settings
from ex_agent.llm.factory import build_embeddings
from ex_agent.metrics import record_readiness
from ex_agent.persistence.database import (
    create_engine,
    create_session_factory,
)
from ex_agent.persistence.repository import AgentRepository
from ex_agent.readiness import ReadinessResult
from ex_agent.tools.registry import ToolRegistry


class ApiContainer:
    def __init__(
        self,
        settings: Settings,
        *,
        engine: Any | None = None,
        repository: AgentRepository | None = None,
        redis: Redis | None = None,
        registry: ToolRegistry | None = None,
        admission: Any | None = None,
        runtime_lifecycle: Any | None = None,
        failure_operations: Any | None = None,
        stream_maintenance_operations: Any | None = None,
    ) -> None:
        self.settings = settings
        self._owns_engine = engine is None
        self._owns_redis = redis is None
        self.engine = engine or create_engine(settings.agent_database_url)
        self.repository = repository or AgentRepository(
            create_session_factory(self.engine)
        )
        if registry is None:
            registry = ToolRegistry(settings.agent_skill_root)
            registry.load()
        self.promotions = WorkflowPromotionService(
            settings,
            self.repository,
            registry,
            build_embeddings(settings),
        )
        self.workflow_lifecycle = WorkflowLifecycleService(
            settings,
            self.repository,
            self.promotions,
        )
        self.redis = redis or Redis.from_url(
            settings.agent_redis_url, decode_responses=True
        )
        self.admission = admission
        self.runtime_lifecycle = runtime_lifecycle
        self.failure_operations = failure_operations
        self.stream_maintenance_operations = stream_maintenance_operations
        self.identity: IdentityProvider = TrustedHeaderIdentityProvider()
        record_readiness("api", ReadinessResult.starting())

    async def close(self) -> None:
        if self._owns_redis:
            await self.redis.aclose()
        if self._owns_engine:
            await self.engine.dispose()

    @classmethod
    def from_runtime(
        cls,
        settings: Settings,
        *,
        runtime: Any,
        redis: Redis,
    ) -> "ApiContainer":
        return cls(
            settings,
            engine=runtime.engine,
            repository=runtime.repository,
            redis=redis,
            registry=runtime.registry,
            admission=runtime.admission,
            runtime_lifecycle=runtime.lifecycle,
            failure_operations=runtime.failure_operations,
            stream_maintenance_operations=(
                runtime.stream_maintenance_operations
            ),
        )


def api_container(request: Request) -> ApiContainer:
    return request.app.state.container


async def current_user(
    forwarded_user_id: ForwardedUserId = None,
    container: ApiContainer = Depends(api_container),
) -> str:
    return await container.identity.user_id(forwarded_user_id)


__all__ = ["ApiContainer", "api_container", "current_user"]
