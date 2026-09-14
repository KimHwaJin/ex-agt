"""Resolve transport identity once and expose an internal current user."""

from typing import Annotated, cast
from uuid import UUID

from fastapi import Depends, Header, Request

from d_test.agent_service.application.management import ManagementService
from d_test.agent_service.domain.management import DomainError, User

from .security import IdentityProvider


def service(request: Request) -> ManagementService:
    return cast(
        ManagementService, request.app.state.management_runtime.service
    )


def identity(request: Request) -> IdentityProvider:
    return cast(
        IdentityProvider, request.app.state.management_runtime.identity
    )


async def current_user(
    request: Request,
    provider: Annotated[IdentityProvider, Depends(identity)],
    management: Annotated[ManagementService, Depends(service)],
    claimed_uuid: Annotated[str | None, Header(alias="X-User-UUID")] = None,
) -> User:
    user_uuid = await provider.user_uuid(request)
    if claimed_uuid is not None:
        try:
            matches = UUID(claimed_uuid) == user_uuid
        except ValueError:
            matches = False
        if not matches:
            raise DomainError(
                "IDENTITY_MISMATCH",
                "인증 사용자와 요청 사용자가 다릅니다.",
                403,
            )
    return await management.user(user_uuid)


async def employee_id(
    request: Request,
    provider: Annotated[IdentityProvider, Depends(identity)],
    claimed_id: Annotated[str | None, Header(alias="X-User-Id")] = None,
) -> str:
    resolved = await provider.employee_id(request)
    if not resolved or len(resolved) > 200:
        raise DomainError("INVALID_USER_ID", "유효하지 않은 사번입니다.", 422)
    if claimed_id is not None and claimed_id.strip() != resolved:
        raise DomainError(
            "IDENTITY_MISMATCH", "인증 사용자와 요청 사용자가 다릅니다.", 403
        )
    return resolved


CurrentUser = Annotated[User, Depends(current_user)]
Service = Annotated[ManagementService, Depends(service)]
EmployeeId = Annotated[str, Depends(employee_id)]
IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=200,
        pattern=r"^[A-Za-z0-9._:-]+$",
    ),
]
