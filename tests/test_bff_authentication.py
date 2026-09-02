import base64
import json
import os
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi import Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import BaseModel
from redis.asyncio import Redis
from starlette.requests import Request

from ex_agent.api.container import api_container, current_user
from ex_agent.api.identity import (
    IdentityHeaders,
    SignedBffIdentityProvider,
    sign_bff_request,
)
from ex_agent.config import Settings

SECRET_OLD = b"old-signing-key-material-32-bytes!!"
SECRET_NEW = b"new-signing-key-material-32-bytes!!"
NOW = 1_800_000_000


class FakeRedis:
    def __init__(self, *, fail: bool = False) -> None:
        self.values: set[str] = set()
        self.fail = fail

    async def set(self, key, value, *, nx, ex):
        assert value == "1" and nx is True and ex == 601
        if self.fail:
            raise ConnectionError("redis unavailable")
        if key in self.values:
            return False
        self.values.add(key)
        return True


class Body(BaseModel):
    value: str


def encoded(secret: bytes) -> str:
    return base64.urlsafe_b64encode(secret).decode().rstrip("=")


def provider(redis=None, *, keys=None) -> SignedBffIdentityProvider:
    values = keys or {"old": encoded(SECRET_OLD)}
    return SignedBffIdentityProvider(
        cast(Any, redis or FakeRedis()),
        json.dumps(values),
        clock=lambda: NOW,
    )


def app_for(identity) -> TestClient:
    app = FastAPI()

    @app.post("/signed")
    async def signed(
        body: Body,
        user_id: str = Depends(current_user),
    ) -> dict[str, str]:
        return {"user_id": user_id, "value": body.value}

    app.dependency_overrides[api_container] = lambda: SimpleNamespace(
        identity=identity
    )
    return TestClient(app)


def headers(
    body: bytes,
    *,
    secret: bytes = SECRET_OLD,
    key_id: str = "old",
    nonce: str = "nonce-0000000001",
    timestamp: int = NOW,
) -> dict[str, str]:
    signature = sign_bff_request(
        secret,
        method="POST",
        raw_path="/signed",
        raw_query="",
        user_id="user-1",
        timestamp=timestamp,
        nonce=nonce,
        body=body,
    )
    return {
        "Content-Type": "application/json",
        "X-User-ID": "user-1",
        "X-BFF-Signature-Version": "v1",
        "X-BFF-Key-ID": key_id,
        "X-BFF-Timestamp": str(timestamp),
        "X-BFF-Nonce": nonce,
        "X-BFF-Signature": signature,
    }


def test_signed_body_is_verified_and_remains_available_to_fastapi() -> None:
    body = b'{"value":"verified"}'
    client = app_for(provider())

    response = client.post("/signed", content=body, headers=headers(body))

    assert response.status_code == 200
    assert response.json() == {"user_id": "user-1", "value": "verified"}


def test_nonce_replay_is_rejected_separately_from_app_idempotency() -> None:
    body = b'{"value":"once"}'
    signed = headers(body)
    client = app_for(provider())

    first = client.post("/signed", content=body, headers=signed)
    replay = client.post("/signed", content=body, headers=signed)

    assert first.status_code == 200
    assert replay.status_code == 401
    assert replay.json()["detail"] == "Replayed BFF request"


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ({"body": b'{"value":"tampered"}'}, "Invalid BFF signature"),
        ({"timestamp": NOW - 301}, "Expired BFF signature"),
        ({"key_id": "missing"}, "Unknown BFF key ID"),
    ],
)
def test_tampering_expiry_and_unknown_key_are_rejected(
    mutation,
    expected,
) -> None:
    original = b'{"value":"original"}'
    sent = mutation.get("body", original)
    signed = headers(
        original,
        key_id=mutation.get("key_id", "old"),
        timestamp=mutation.get("timestamp", NOW),
    )
    client = app_for(provider())

    response = client.post("/signed", content=sent, headers=signed)

    assert response.status_code == 401
    assert response.json()["detail"] == expected


def test_key_rotation_accepts_old_and_new_key_ids() -> None:
    body = b'{"value":"rotation"}'
    identity = provider(
        keys={
            "old": encoded(SECRET_OLD),
            "new": encoded(SECRET_NEW),
        }
    )
    client = app_for(identity)

    old = client.post(
        "/signed",
        content=body,
        headers=headers(body, nonce="nonce-old-0000001"),
    )
    new = client.post(
        "/signed",
        content=body,
        headers=headers(
            body,
            secret=SECRET_NEW,
            key_id="new",
            nonce="nonce-new-0000001",
        ),
    )

    assert old.status_code == new.status_code == 200


def test_replay_store_outage_fails_closed() -> None:
    body = b'{"value":"closed"}'
    client = app_for(provider(FakeRedis(fail=True)))

    response = client.post("/signed", content=body, headers=headers(body))

    assert response.status_code == 503
    assert response.json()["detail"] == (
        "BFF replay protection is unavailable"
    )


def test_hmac_key_material_and_production_mode_are_validated() -> None:
    with pytest.raises(ValueError, match="at least 32 bytes"):
        provider(keys={"short": encoded(b"short")})
    with pytest.raises(ValueError, match="JSON object"):
        SignedBffIdentityProvider(cast(Any, FakeRedis()), "")
    with pytest.raises(ValueError, match="production requires"):
        Settings(app_env="production")
    settings = Settings(app_env="production", bff_auth_mode="hmac")
    assert settings.bff_auth_mode == "hmac"


def test_non_decimal_timestamp_is_rejected_before_signature_check() -> None:
    body = b'{"value":"timestamp"}'
    signed = headers(body)
    signed["X-BFF-Timestamp"] = f"+{NOW}"
    client = app_for(provider())

    response = client.post("/signed", content=body, headers=signed)

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid BFF timestamp"


@pytest.mark.redis
@pytest.mark.skipif(
    "TEST_REDIS_URL" not in os.environ,
    reason="Requires isolated Redis",
)
async def test_real_redis_atomically_rejects_replayed_nonce() -> None:
    redis = Redis.from_url(
        os.environ["TEST_REDIS_URL"],
        decode_responses=True,
    )
    prefix = "test:bff-auth:nonce"
    identity = SignedBffIdentityProvider(
        redis,
        json.dumps({"old": encoded(SECRET_OLD)}),
        nonce_prefix=prefix,
        clock=lambda: NOW,
    )
    body = b'{"value":"redis"}'
    envelope = IdentityHeaders(
        user_id="user-1",
        signature_version="v1",
        key_id="old",
        timestamp=str(NOW),
        nonce="nonce-redis-00001",
        signature=sign_bff_request(
            SECRET_OLD,
            method="POST",
            raw_path="/signed",
            raw_query="",
            user_id="user-1",
            timestamp=NOW,
            nonce="nonce-redis-00001",
            body=body,
        ),
    )

    def request() -> Request:
        sent = False

        async def receive():
            nonlocal sent
            if sent:
                return {"type": "http.disconnect"}
            sent = True
            return {
                "type": "http.request",
                "body": body,
                "more_body": False,
            }

        return Request(
            {
                "type": "http",
                "method": "POST",
                "scheme": "http",
                "path": "/signed",
                "raw_path": b"/signed",
                "query_string": b"",
                "headers": [],
                "server": ("test", 80),
                "client": ("test", 1),
            },
            receive,
        )

    try:
        assert await identity.user_id(request(), envelope) == "user-1"
        with pytest.raises(HTTPException) as replay:
            await identity.user_id(request(), envelope)
        assert getattr(replay.value, "status_code", None) == 401
    finally:
        keys = [key async for key in redis.scan_iter(f"{prefix}:*")]
        if keys:
            await redis.delete(*keys)
        await redis.aclose()
