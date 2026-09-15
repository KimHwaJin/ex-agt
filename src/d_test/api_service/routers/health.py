"""Standalone health routes; not installed over host health routes."""

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter()


@router.get("/health/live", include_in_schema=False)
async def live():
    return {"status": "ok"}


@router.get("/health/ready", include_in_schema=False)
async def ready(request: Request):
    runtime = request.app.state.management_runtime
    try:
        await runtime.resources.ready()
    except RuntimeError:
        return JSONResponse({"status": "not_ready"}, status_code=503)
    return {"status": "ready"}
