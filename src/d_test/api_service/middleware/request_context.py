"""Request IDs, no-store headers and safe request timing logs."""

import logging
from time import monotonic
from uuid import uuid4

from fastapi import FastAPI, Request

logger = logging.getLogger("api_service")


def install_request_context(app: FastAPI) -> None:
    @app.middleware("http")
    async def trace_request(request: Request, call_next):
        if not (
            request.url.path.startswith("/api/v1/")
            or request.url.path in {"/health/live", "/health/ready"}
        ):
            return await call_next(request)
        request_id = str(uuid4())
        request.state.management_request_id = request_id
        started = monotonic()
        response = await call_next(request)
        response.headers["X-Request-Id"] = request_id
        response.headers["Cache-Control"] = "no-store"
        # Never log headers, bodies, URLs with query strings, or tokens.
        logger.info(
            "management_request component=api request_id=%s status=%s "
            "elapsed_ms=%.2f",
            request_id,
            response.status_code,
            (monotonic() - started) * 1000,
        )
        return response
