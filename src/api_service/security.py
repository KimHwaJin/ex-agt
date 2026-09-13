"""Replaceable identity boundary, not a home-grown SSO implementation."""

from ipaddress import ip_address, ip_network
from typing import Protocol
from uuid import UUID

from fastapi import Request

from agent_service.domain.management import DomainError
from agent_service.settings import Settings


class IdentityProvider(Protocol):
    async def employee_id(self, request: Request) -> str:
        """Resolve a trusted external employee identity for provisioning."""
        ...

    async def user_uuid(self, request: Request) -> UUID:
        """Resolve the verified principal to an existing internal UUID."""
        ...


class HeaderIdentityProvider:
    """Explicit local development or trusted-gateway integration only."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.networks = [
            ip_network(value) for value in settings.trusted_proxy_cidrs
        ]

    def guard(self, request: Request) -> None:
        if request.headers.get("authorization"):
            # Never silently ignore a token and downgrade to UUID identity.
            raise DomainError(
                "AUTH_MODE_MISMATCH",
                "이 실행 모드는 토큰 인증을 지원하지 않습니다.",
                401,
            )
        if self.settings.auth_mode == "development_header":
            return
        if self.settings.auth_mode != "trusted_header":
            raise DomainError("AUTH_REQUIRED", "인증이 필요합니다.", 401)
        try:
            address = ip_address(request.client.host if request.client else "")
        except ValueError:
            raise DomainError(
                "UNTRUSTED_GATEWAY", "접근이 거절됐습니다.", 403
            ) from None
        if not any(address in network for network in self.networks):
            raise DomainError("UNTRUSTED_GATEWAY", "접근이 거절됐습니다.", 403)

    def single_header(self, request: Request, name: str) -> str:
        self.guard(request)
        values = request.headers.getlist(name)
        if len(values) != 1 or not values[0].strip():
            raise DomainError(
                "IDENTITY_HEADER_REQUIRED",
                f"{name} 헤더를 하나만 전달해 주세요.",
                401,
            )
        return values[0].strip()

    async def employee_id(self, request: Request) -> str:
        value = self.single_header(request, "X-User-Id")
        if len(value) > 200:
            raise DomainError("INVALID_USER_ID", "사번이 너무 깁니다.", 422)
        return value

    async def user_uuid(self, request: Request) -> UUID:
        value = self.single_header(request, "X-User-UUID")
        try:
            return UUID(value)
        except ValueError:
            raise DomainError(
                "INVALID_USER_UUID", "유효한 사용자 UUID가 필요합니다.", 422
            ) from None
