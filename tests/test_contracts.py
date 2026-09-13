"""Pure contract, configuration and template-boundary tests."""

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI, Request
from pydantic import ValidationError

from agent_service.application.cursors import CursorCodec
from agent_service.bootstrap.configuration import load_settings
from agent_service.domain.management import DomainError, Page
from agent_service.settings import Settings
from api_service.factory import install_management_api
from api_service.schemas import ProjectCreate, ProjectUpdate, SessionCreate
from api_service.security import HeaderIdentityProvider


def test_page_generic_on_python_311():
    assert Page[int](items=[1], next_cursor=None, has_more=False).items == [1]


def test_development_database_name(monkeypatch):
    monkeypatch.setenv("SERVICE_ENV", "development")
    for name in (
        "SERVICE_CONFIG",
        "MANAGEMENT_DATABASE_URL",
        "MANAGEMENT_CURSOR_SECRET",
    ):
        monkeypatch.delenv(name, raising=False)
    settings = load_settings(Path(__file__).resolve().parents[1])
    assert settings.database_url.get_secret_value().endswith("/chatapp")


def test_cursor_roundtrip_and_scope():
    codec = CursorCodec("secret" * 8)
    timestamp, item_id = datetime.now(UTC), uuid4()
    token = codec.encode("owner:project", timestamp, item_id)
    assert codec.decode("owner:project", token) == (timestamp, item_id)
    with pytest.raises(DomainError, match="커서"):
        codec.decode("another-owner", token)
    assert codec.decode("owner:project", None) is None


@pytest.mark.parametrize("cursor", ["", "?", "a" * 5000, "e30="])
def test_invalid_cursor(cursor):
    with pytest.raises(DomainError):
        CursorCodec("secret" * 8).decode("scope", cursor)


def test_cursor_tampering():
    codec = CursorCodec("secret" * 8)
    token = codec.encode("scope", datetime.now(UTC), uuid4())
    changed = ("A" if token[0] != "A" else "B") + token[1:]
    with pytest.raises(DomainError):
        codec.decode("scope", changed)


@pytest.mark.parametrize(
    "payload",
    [
        {"version": 1},
        {"version": 1, "name": None},
        {"version": 0, "name": "x"},
        {"version": True, "name": "x"},
        {"version": 1, "name": " "},
        {"version": 1, "name": "x", "user_uuid": str(uuid4())},
    ],
)
def test_patch_rejects_invalid(payload):
    with pytest.raises(ValidationError):
        ProjectUpdate.model_validate(payload)


def test_patch_nullable_description():
    patch = ProjectUpdate(version=1, description=None)
    assert patch.model_dump(exclude_unset=True, exclude={"version"}) == {
        "description": None
    }
    assert ProjectCreate(name=" test ").name == "test"
    assert SessionCreate(project_id=uuid4()).title == "새 대화"


def test_production_rejects_development_auth(settings):
    with pytest.raises(ValidationError):
        Settings.model_validate(
            {
                **settings.model_dump(),
                "environment": "production",
            }
        )


def test_production_config_never_falls_back(monkeypatch):
    monkeypatch.setenv("SERVICE_ENV", "production")
    monkeypatch.delenv("SERVICE_CONFIG", raising=False)
    monkeypatch.delenv("MANAGEMENT_DATABASE_URL", raising=False)
    monkeypatch.delenv("MANAGEMENT_CURSOR_SECRET", raising=False)
    root = Path(__file__).resolve().parents[1]
    with pytest.raises(RuntimeError, match="Invalid management"):
        load_settings(root)


def request(headers, host="127.0.0.1"):
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [
                (key.encode(), value.encode()) for key, value in headers
            ],
            "client": (host, 1234),
        }
    )


async def test_header_identity_rejects_missing_and_duplicate(settings):
    provider = HeaderIdentityProvider(settings)
    for headers in [[], [("x-user-id", "a"), ("x-user-id", "b")]]:
        with pytest.raises(DomainError):
            await provider.employee_id(request(headers))


async def test_header_mode_never_ignores_token(settings):
    provider = HeaderIdentityProvider(settings)
    with pytest.raises(DomainError) as caught:
        await provider.employee_id(
            request(
                [
                    ("authorization", "Bearer invalid"),
                    ("x-user-id", "employee"),
                ]
            )
        )
    assert caught.value.code == "AUTH_MODE_MISMATCH"


async def test_trusted_gateway_peer_check(settings):
    configured = Settings.model_validate(
        {
            **settings.model_dump(),
            "auth_mode": "trusted_header",
            "trusted_proxy_cidrs": ["10.0.0.0/24"],
        }
    )
    provider = HeaderIdentityProvider(configured)
    headers = [("x-user-id", "employee")]
    assert (
        await provider.employee_id(request(headers, "10.0.0.2")) == "employee"
    )
    with pytest.raises(DomainError):
        await provider.employee_id(
            request([*headers, ("x-forwarded-for", "10.0.0.2")], "192.0.2.1")
        )


async def test_host_routes_unchanged(settings):
    host = FastAPI()

    @host.get("/host/{number}")
    async def existing(number: int):
        return {"number": number}

    install_management_api(host, settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=host), base_url="http://test"
    ) as client:
        response = await client.get("/host/1")
        assert response.json() == {"number": 1}
        assert "X-Request-Id" not in response.headers
        invalid = await client.get("/host/not-a-number")
        assert invalid.status_code == 422
        assert "detail" in invalid.json()


def test_duplicate_install_rejected(settings):
    app = FastAPI()
    install_management_api(app, settings)
    with pytest.raises(RuntimeError, match="already installed"):
        install_management_api(app, settings)
