"""Tests never truncate or migrate a caller's existing database."""

import os
from collections.abc import AsyncIterator
from uuid import uuid4

import httpx
import pytest
from psycopg import AsyncConnection
from psycopg.conninfo import conninfo_to_dict

from agent_service.settings import Settings
from api_service.factory import create_app


@pytest.fixture
def settings() -> Settings:
    return Settings.model_validate(
        {
            "database_url": "postgresql://unused:unused@localhost/chatapp",
            "cursor_secret": "test-cursor-key-0123456789-0123456789",
        }
    )


@pytest.fixture
async def client(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    url = os.environ.get("MANAGEMENT_TEST_DATABASE_URL")
    if not url:
        raise pytest.skip.Exception(
            "Set MANAGEMENT_TEST_DATABASE_URL for PostgreSQL tests"
        )
    if conninfo_to_dict(url).get("dbname") != "chatapp":
        raise RuntimeError(
            "Integration tests require an isolated chatapp database"
        )
    configured = Settings.model_validate(
        {
            **settings.model_dump(),
            "database_url": url,
        }
    )
    app = create_app(configured)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as test_client:
            yield test_client


@pytest.fixture
async def db(client) -> AsyncIterator[AsyncConnection]:
    # The client fixture validates the explicit test database name first.
    url = os.environ["MANAGEMENT_TEST_DATABASE_URL"]
    async with await AsyncConnection.connect(
        url, autocommit=True
    ) as connection:
        yield connection


@pytest.fixture
async def home(client):
    response = await client.post(
        "/api/v1/me", headers={"X-User-Id": f"test-{uuid4()}"}
    )
    assert response.status_code == 200, response.text
    return response.json()


@pytest.fixture
def identity(home):
    return {"X-User-UUID": home["user"]["user_uuid"]}
