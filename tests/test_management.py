"""Real PostgreSQL transactions and all eleven management endpoints."""

import asyncio
from uuid import UUID, uuid4

import pytest

from agent_service.domain.management import DomainError
from agent_service.infrastructure.database.management import Repository

pytestmark = pytest.mark.postgres


async def create_project(client, identity, **fields):
    response = await client.post(
        "/api/v1/projects",
        headers={**identity, "Idempotency-Key": str(uuid4())},
        json={"name": "분석 프로젝트", **fields},
    )
    assert response.status_code == 201, response.text
    return response.json()


async def test_bootstrap_concurrent_and_audited(client, db):
    employee = f"concurrent-{uuid4()}"
    responses = await asyncio.gather(
        *[
            client.post("/api/v1/me", headers={"X-User-Id": employee})
            for _ in range(8)
        ]
    )
    assert all(response.status_code == 200 for response in responses)
    first = responses[0].json()
    assert all(response.json() == first for response in responses)
    user = first["user"]
    project = first["default_project"]
    assert user["user_id"] == project["user_id"] == employee
    assert project["user_uuid"] == user["user_uuid"]
    for item in [user, project]:
        assert item["created_at"] == item["updated_at"]
        assert item["created_by"] == item["updated_by"] == user["user_uuid"]
    cursor = await db.execute(
        "SELECT count(*) FROM management.sessions WHERE project_id = %s",
        (project["project_id"],),
    )
    assert await cursor.fetchone() == (0,)


async def test_bootstrap_rolls_back(client, db, monkeypatch):
    employee = f"rollback-{uuid4()}"

    async def fail(self, owner):
        raise DomainError("INJECTED", "test rollback")

    monkeypatch.setattr(Repository, "default_project", fail)
    response = await client.post("/api/v1/me", headers={"X-User-Id": employee})
    assert response.status_code == 409
    cursor = await db.execute(
        "SELECT count(*) FROM management.users WHERE user_id = %s",
        (employee,),
    )
    assert await cursor.fetchone() == (0,)


async def test_default_project_cannot_be_deleted(client, home, identity):
    project_id = home["default_project"]["project_id"]
    response = await client.delete(
        f"/api/v1/projects/{project_id}", headers=identity
    )
    assert response.status_code == 409
    assert response.json()["code"] == "DEFAULT_PROJECT_DELETE_FORBIDDEN"


async def test_project_crud_and_idempotency(client, home, identity):
    headers = {**identity, "Idempotency-Key": str(uuid4())}
    payload = {"name": " original ", "description": "keep"}
    responses = await asyncio.gather(
        *[
            client.post("/api/v1/projects", headers=headers, json=payload)
            for _ in range(6)
        ]
    )
    assert all(response.status_code == 201 for response in responses)
    original = responses[0].json()
    assert all(response.json() == original for response in responses)
    assert original["name"] == "original"
    assert original["user_id"] == home["user"]["user_id"]
    path = f"/api/v1/projects/{original['project_id']}"
    conflict = await client.post(
        "/api/v1/projects", headers=headers, json={"name": "different"}
    )
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "IDEMPOTENCY_CONFLICT"
    assert (await client.get(path, headers=identity)).json() == original
    updated = await client.patch(
        path, headers=identity, json={"version": 1, "name": "renamed"}
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["description"] == "keep"
    assert updated.json()["version"] == 2
    assert updated.json()["created_at"] == original["created_at"]
    assert updated.json()["updated_by"] == identity["X-User-UUID"]
    stale = await client.patch(
        path, headers=identity, json={"version": 1, "name": "stale"}
    )
    assert stale.status_code == 409
    cleared = await client.patch(
        path, headers=identity, json={"version": 2, "description": None}
    )
    assert cleared.status_code == 200
    assert cleared.json()["description"] is None
    replay = await client.post(
        "/api/v1/projects", headers=headers, json=payload
    )
    assert replay.json() == original
    for _ in range(2):
        deleted = await client.delete(path, headers=identity)
        assert deleted.status_code == 204 and deleted.content == b""
    assert (await client.get(path, headers=identity)).status_code == 404
    assert (
        await client.post("/api/v1/projects", headers=headers, json=payload)
    ).status_code == 404


async def test_concurrent_patch_one_wins(client, identity):
    project = await create_project(client, identity)
    path = f"/api/v1/projects/{project['project_id']}"
    results = await asyncio.gather(
        *[
            client.patch(
                path, headers=identity, json={"version": 1, "name": name}
            )
            for name in ["first", "second"]
        ]
    )
    assert sorted(item.status_code for item in results) == [200, 409]


async def test_session_crud_and_parent_delete(client, identity):
    project = await create_project(client, identity)
    headers = {**identity, "Idempotency-Key": str(uuid4())}
    body = {"project_id": project["project_id"]}
    created = await client.post("/api/v1/sessions", headers=headers, json=body)
    assert created.status_code == 201, created.text
    session = created.json()
    assert session["title"] == "새 대화"
    assert session["created_at"] == session["updated_at"]
    replay = await client.post("/api/v1/sessions", headers=headers, json=body)
    assert replay.json() == session
    path = f"/api/v1/sessions/{session['session_id']}"
    assert (await client.get(path, headers=identity)).json() == session
    listed = await client.get(
        "/api/v1/sessions", headers=identity, params=body
    )
    assert listed.json()["items"] == [session]
    edited = await client.patch(
        path, headers=identity, json={"version": 1, "title": "EDA"}
    )
    assert edited.status_code == 200
    assert edited.json()["version"] == 2
    assert (
        await client.patch(
            path, headers=identity, json={"version": 1, "title": "stale"}
        )
    ).status_code == 409
    for _ in range(2):
        assert (await client.delete(path, headers=identity)).status_code == 204
    assert (await client.get(path, headers=identity)).status_code == 404
    headers["Idempotency-Key"] = str(uuid4())
    new = await client.post("/api/v1/sessions", headers=headers, json=body)
    assert new.status_code == 201
    deleted = await client.delete(
        f"/api/v1/projects/{project['project_id']}", headers=identity
    )
    assert deleted.status_code == 204
    path = f"/api/v1/sessions/{new.json()['session_id']}"
    assert (await client.get(path, headers=identity)).status_code == 404
    assert (
        await client.get("/api/v1/sessions", headers=identity, params=body)
    ).status_code == 404
    assert (
        await client.post("/api/v1/sessions", headers=headers, json=body)
    ).status_code == 404


async def test_ownership_isolation(client, identity):
    project = await create_project(client, identity)
    another = await client.post(
        "/api/v1/me", headers={"X-User-Id": str(uuid4())}
    )
    foreign = {"X-User-UUID": another.json()["user"]["user_uuid"]}
    path = f"/api/v1/projects/{project['project_id']}"
    assert (await client.get(path, headers=foreign)).status_code == 404
    assert (
        await client.patch(
            path, headers=foreign, json={"version": 1, "name": "intrusion"}
        )
    ).status_code == 404
    assert (await client.delete(path, headers=foreign)).status_code == 404
    body = {"project_id": project["project_id"]}
    assert (
        await client.get("/api/v1/sessions", headers=foreign, params=body)
    ).status_code == 404
    key = str(uuid4())
    assert (
        await client.post(
            "/api/v1/sessions",
            headers={**foreign, "Idempotency-Key": key},
            json=body,
        )
    ).status_code == 404
    own_project = another.json()["default_project"]["project_id"]
    # Failed create rolls back the reserved idempotency key.
    retry = await client.post(
        "/api/v1/sessions",
        headers={**foreign, "Idempotency-Key": key},
        json={"project_id": own_project},
    )
    assert retry.status_code == 201


async def test_project_keyset_pagination(client, identity, db):
    for _ in range(4):
        await create_project(client, identity)
    await db.execute(
        "UPDATE management.projects SET created_at = %s "
        "WHERE owner_user_uuid = %s",
        ("2026-01-01T00:00:00+00:00", identity["X-User-UUID"]),
    )
    ids = []
    cursor = None
    while True:
        params = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        page = (
            await client.get(
                "/api/v1/projects", headers=identity, params=params
            )
        ).json()
        ids.extend(item["project_id"] for item in page["items"])
        cursor = page["next_cursor"]
        if not page["has_more"]:
            assert cursor is None
            break
    assert len(ids) == len(set(ids)) == 5
    assert ids == sorted(ids, key=UUID, reverse=True)


async def test_identity_errors_and_disabled(client, home, identity, db):
    assert (await client.get("/api/v1/projects")).status_code == 401
    assert (
        await client.get(
            "/api/v1/projects", headers={"X-User-UUID": str(uuid4())}
        )
    ).status_code == 403
    response = await client.post(
        "/api/v1/projects", headers=identity, json={"name": "missing key"}
    )
    assert response.status_code == 422
    assert "input" not in str(response.json())
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["X-Request-Id"] == response.json()["request_id"]
    await db.execute(
        "UPDATE management.users SET status = 'disabled' WHERE user_uuid = %s",
        (identity["X-User-UUID"],),
    )
    assert (
        await client.get("/api/v1/projects", headers=identity)
    ).status_code == 403
    assert (
        await client.post(
            "/api/v1/me", headers={"X-User-Id": home["user"]["user_id"]}
        )
    ).status_code == 403
