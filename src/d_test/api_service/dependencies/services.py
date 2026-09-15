"""Resolve application services from the active API lifespan."""

from typing import Annotated, cast

from fastapi import Depends, Request

from d_test.agent_service.application.management import ManagementService
from d_test.agent_service.application.runs import RunService


def service(request: Request) -> ManagementService:
    return cast(
        ManagementService, request.app.state.management_runtime.service
    )


def run_service(request: Request) -> RunService:
    return request.app.state.management_runtime.resources.runs


Service = Annotated[ManagementService, Depends(service)]
