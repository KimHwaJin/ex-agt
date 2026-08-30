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
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.engine = create_engine(settings.agent_database_url)
        self.repository = AgentRepository(create_session_factory(self.engine))
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
        self.redis = Redis.from_url(
            settings.agent_redis_url,
            decode_responses=True,
        )
        self.identity: IdentityProvider = TrustedHeaderIdentityProvider()
        record_readiness("api", ReadinessResult.starting())

    async def close(self) -> None:
        await self.redis.aclose()
        await self.engine.dispose()


def api_container(request: Request) -> ApiContainer:
    return request.app.state.container


async def current_user(
    forwarded_user_id: ForwardedUserId = None,
    container: ApiContainer = Depends(api_container),
) -> str:
    return await container.identity.user_id(forwarded_user_id)


__all__ = ["ApiContainer", "api_container", "current_user"]
