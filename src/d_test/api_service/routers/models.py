"""Expose model names, never credentials or endpoint overrides."""

from fastapi import APIRouter, Request

from ..dependencies import CurrentUser
from ..dependencies.services import run_service as service

router = APIRouter(prefix="/api/v1", tags=["agent"])


@router.get("/agent/models")
async def available_models(request: Request, user: CurrentUser):
    runs = service(request)
    # CurrentUser already resolves an active persisted identity.
    settings = runs.settings
    names = (
        settings.selectable_models
        if settings.agent_backend == "langgraph"
        else ()
    )
    return {
        "items": [{"name": name} for name in names],
        "default_model_name": settings.model_name if names else None,
        "source": "configured",
    }
