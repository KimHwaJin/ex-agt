from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, Protocol

from fastapi import Header, HTTPException, Request
from redis.asyncio import Redis

_KEY_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_NONCE = re.compile(r"^[A-Za-z0-9._:-]{16,128}$")
_SIGNATURE = re.compile(r"^[A-Za-z0-9_-]{43}$")


@dataclass(frozen=True)
class IdentityHeaders:
    user_id: str | None
    signature_version: str | None = None
    key_id: str | None = None
    timestamp: str | None = None
    nonce: str | None = None
    signature: str | None = None


class IdentityProvider(Protocol):
    async def user_id(
        self,
        request: Request,
        headers: IdentityHeaders,
    ) -> str: ...


class TrustedHeaderIdentityProvider:
    """Development-only boundary that trusts the forwarded user header."""

    async def user_id(
        self,
        request: Request,
        headers: IdentityHeaders,
    ) -> str:
        del request
        return _required_user(headers.user_id)


class SignedBffIdentityProvider:
    """Verify a body-bound BFF HMAC and reject nonce replay via Redis."""

    def __init__(
        self,
        redis: Redis,
        keys_json: str,
        *,
        max_clock_skew_seconds: int = 300,
        nonce_prefix: str = "agent:bff-auth:nonce",
        clock: Callable[[], float] = time.time,
    ) -> None:
        if max_clock_skew_seconds < 1:
            raise ValueError("BFF clock skew must be positive")
        if not nonce_prefix.strip():
            raise ValueError("BFF nonce prefix cannot be empty")
        self._redis = redis
        self._keys = _decode_keys(keys_json)
        self._max_clock_skew_seconds = max_clock_skew_seconds
        self._nonce_prefix = nonce_prefix.rstrip(":")
        self._clock = clock

    async def user_id(
        self,
        request: Request,
        headers: IdentityHeaders,
    ) -> str:
        user_id = _required_user(headers.user_id)
        if headers.signature_version != "v1":
            raise _unauthorized("Unsupported BFF signature version")
        if not headers.key_id or not _KEY_ID.fullmatch(headers.key_id):
            raise _unauthorized("Invalid BFF key ID")
        if not headers.nonce or not _NONCE.fullmatch(headers.nonce):
            raise _unauthorized("Invalid BFF nonce")
        if not headers.signature or not _SIGNATURE.fullmatch(
            headers.signature
        ):
            raise _unauthorized("Invalid BFF signature")
        if not headers.timestamp or not headers.timestamp.isdigit():
            raise _unauthorized("Invalid BFF timestamp")
        try:
            timestamp = int(headers.timestamp or "")
        except ValueError as error:
            raise _unauthorized("Invalid BFF timestamp") from error
        if abs(int(self._clock()) - timestamp) > self._max_clock_skew_seconds:
            raise _unauthorized("Expired BFF signature")
        secret = self._keys.get(headers.key_id)
        if secret is None:
            raise _unauthorized("Unknown BFF key ID")
        body = await request.body()
        expected = sign_bff_request(
            secret,
            method=request.method,
            raw_path=_raw_path(request),
            raw_query=_raw_query(request),
            user_id=user_id,
            timestamp=timestamp,
            nonce=headers.nonce,
            body=body,
        )
        if not hmac.compare_digest(expected, headers.signature):
            raise _unauthorized("Invalid BFF signature")
        replay_key = _replay_key(
            self._nonce_prefix,
            headers.key_id,
            headers.nonce,
        )
        try:
            accepted = await self._redis.set(
                replay_key,
                "1",
                nx=True,
                ex=self._max_clock_skew_seconds * 2 + 1,
            )
        except Exception as error:
            raise HTTPException(
                status_code=503,
                detail="BFF replay protection is unavailable",
            ) from error
        if not accepted:
            raise _unauthorized("Replayed BFF request")
        return user_id


def sign_bff_request(
    secret: bytes,
    *,
    method: str,
    raw_path: str,
    raw_query: str,
    user_id: str,
    timestamp: int,
    nonce: str,
    body: bytes,
) -> str:
    canonical = canonical_bff_request(
        method=method,
        raw_path=raw_path,
        raw_query=raw_query,
        user_id=user_id,
        timestamp=timestamp,
        nonce=nonce,
        body_sha256=hashlib.sha256(body).hexdigest(),
    )
    digest = hmac.new(secret, canonical, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode().rstrip("=")


def canonical_bff_request(
    *,
    method: str,
    raw_path: str,
    raw_query: str,
    user_id: str,
    timestamp: int,
    nonce: str,
    body_sha256: str,
) -> bytes:
    return "\n".join(
        (
            "v1",
            method.upper(),
            raw_path,
            raw_query,
            user_id,
            str(timestamp),
            nonce,
            body_sha256,
        )
    ).encode()


def _decode_keys(keys_json: str) -> dict[str, bytes]:
    try:
        values = json.loads(keys_json)
    except json.JSONDecodeError as error:
        raise ValueError("BFF HMAC keys must be a JSON object") from error
    if not isinstance(values, dict) or not values:
        raise ValueError("At least one BFF HMAC key is required")
    keys: dict[str, bytes] = {}
    for key_id, encoded in values.items():
        if not isinstance(key_id, str) or not _KEY_ID.fullmatch(key_id):
            raise ValueError("Invalid BFF HMAC key ID")
        if not isinstance(encoded, str):
            raise ValueError("BFF HMAC key must be base64url text")
        try:
            padding = "=" * (-len(encoded) % 4)
            secret = base64.b64decode(
                encoded + padding,
                altchars=b"-_",
                validate=True,
            )
        except (binascii.Error, ValueError) as error:
            raise ValueError("Invalid base64url BFF HMAC key") from error
        if len(secret) < 32:
            raise ValueError("BFF HMAC keys must contain at least 32 bytes")
        keys[key_id] = secret
    return keys


def _required_user(value: str | None) -> str:
    if not value or len(value) > 255 or "\n" in value or "\r" in value:
        raise _unauthorized("X-User-ID header is required")
    return value


def _raw_path(request: Request) -> str:
    value = request.scope.get("raw_path")
    if isinstance(value, bytes):
        return value.decode("ascii")
    return request.url.path


def _raw_query(request: Request) -> str:
    value = request.scope.get("query_string", b"")
    return value.decode("ascii") if isinstance(value, bytes) else str(value)


def _replay_key(prefix: str, key_id: str, nonce: str) -> str:
    digest = hashlib.sha256(f"{key_id}\0{nonce}".encode()).hexdigest()
    return f"{prefix}:{digest}"


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(status_code=401, detail=detail)


ForwardedUserId = Annotated[str | None, Header(alias="X-User-ID")]
BffSignatureVersion = Annotated[
    str | None,
    Header(alias="X-BFF-Signature-Version"),
]
BffKeyId = Annotated[str | None, Header(alias="X-BFF-Key-ID")]
BffTimestamp = Annotated[str | None, Header(alias="X-BFF-Timestamp")]
BffNonce = Annotated[str | None, Header(alias="X-BFF-Nonce")]
BffSignature = Annotated[str | None, Header(alias="X-BFF-Signature")]

__all__ = [
    "BffKeyId",
    "BffNonce",
    "BffSignature",
    "BffSignatureVersion",
    "BffTimestamp",
    "ForwardedUserId",
    "IdentityHeaders",
    "IdentityProvider",
    "SignedBffIdentityProvider",
    "TrustedHeaderIdentityProvider",
    "canonical_bff_request",
    "sign_bff_request",
]
