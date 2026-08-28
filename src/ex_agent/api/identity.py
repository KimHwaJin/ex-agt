from __future__ import annotations

from typing import Annotated, Protocol

from fastapi import Header, HTTPException


class IdentityProvider(Protocol):
    async def user_id(self, forwarded_user_id: str | None) -> str: ...


class TrustedHeaderIdentityProvider:
    """Replaceable V1 boundary for the BFF-authenticated user identity."""

    async def user_id(self, forwarded_user_id: str | None) -> str:
        if not forwarded_user_id:
            raise HTTPException(
                status_code=401,
                detail="X-User-ID header is required",
            )
        return forwarded_user_id


ForwardedUserId = Annotated[str | None, Header(alias="X-User-ID")]
