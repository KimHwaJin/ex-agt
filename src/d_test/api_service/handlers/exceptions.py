"""HTTP error mapping without exposing request bodies or DB secrets."""

import logging
from typing import cast

from fastapi import FastAPI, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg import OperationalError
from psycopg_pool import PoolTimeout

from d_test.agent_service.domain.management import DomainError

logger = logging.getLogger("api_service")


def install_exception_handlers(app: FastAPI) -> None:
    async def domain_error(request: Request, exc: Exception):
        error = cast(DomainError, exc)
        return JSONResponse(
            status_code=error.status,
            content={
                "code": error.code,
                "message": error.message,
                "request_id": getattr(
                    request.state, "management_request_id", None
                ),
            },
        )

    async def validation_error(request: Request, exc: Exception):
        error = cast(RequestValidationError, exc)
        if not hasattr(request.state, "management_request_id"):
            return await request_validation_exception_handler(request, error)
        return JSONResponse(
            status_code=422,
            content={
                "code": "VALIDATION_ERROR",
                "message": "요청 필드를 확인해 주세요.",
                "request_id": request.state.management_request_id,
                "errors": [
                    {"location": list(item["loc"]), "type": item["type"]}
                    for item in error.errors()
                ],
            },
        )

    async def database_error(request: Request, exc: Exception):
        if not hasattr(request.state, "management_request_id"):
            raise exc
        logger.warning(
            "management_database_unavailable type=%s", type(exc).__name__
        )
        return JSONResponse(
            status_code=503,
            content={
                "code": "DATABASE_UNAVAILABLE",
                "message": "일시적으로 요청을 처리할 수 없습니다.",
                "request_id": request.state.management_request_id,
            },
        )

    app.add_exception_handler(DomainError, domain_error)
    # Preserve template-wide validation handlers when already customized.
    existing = app.exception_handlers.get(RequestValidationError)
    if existing is None or existing is request_validation_exception_handler:
        app.add_exception_handler(RequestValidationError, validation_error)
    if OperationalError not in app.exception_handlers:
        app.add_exception_handler(OperationalError, database_error)
    if PoolTimeout not in app.exception_handlers:
        app.add_exception_handler(PoolTimeout, database_error)
