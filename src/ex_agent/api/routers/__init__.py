"""FastAPI route groups."""

from ex_agent.api.routers.promotions import promotion_router
from ex_agent.api.routers.workflows import workflow_router

__all__ = ["promotion_router", "workflow_router"]
