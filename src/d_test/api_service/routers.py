"""Management endpoints; no graph imports or background work."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query, Response

from d_test.agent_service.domain.management import Home, Page, Project, Session

from .dependencies import CurrentUser, EmployeeId, IdempotencyKey, Service
from .schemas import (
    ProjectCreate,
    ProjectUpdate,
    SessionCreate,
    SessionUpdate,
)

router = APIRouter(prefix="/api/v1")
Limit = Annotated[int, Query(ge=1, le=100)]
Cursor = Annotated[str | None, Query(min_length=1, max_length=4096)]


@router.post("/me", response_model=Home, tags=["user"])
async def me(employee: EmployeeId, management: Service) -> Home:
    return await management.me(employee)


@router.get("/me", response_model=Home, tags=["user"])
async def get_me(user: CurrentUser, management: Service) -> Home:
    return await management.get_me(user.user_uuid)


@router.post(
    "/projects", response_model=Project, status_code=201, tags=["projects"]
)
async def create_project(
    payload: ProjectCreate,
    user: CurrentUser,
    management: Service,
    key: IdempotencyKey,
) -> Project:
    return await management.create_project(
        user.user_uuid, key, payload.name, payload.description
    )


@router.get("/projects", response_model=Page[Project], tags=["projects"])
async def list_projects(
    user: CurrentUser,
    management: Service,
    limit: Limit = 20,
    cursor: Cursor = None,
) -> Page[Project]:
    return await management.projects(user.user_uuid, limit, cursor)


@router.get(
    "/projects/{project_id}", response_model=Project, tags=["projects"]
)
async def get_project(
    project_id: UUID, user: CurrentUser, management: Service
) -> Project:
    return await management.project(user.user_uuid, project_id)


@router.patch(
    "/projects/{project_id}", response_model=Project, tags=["projects"]
)
async def update_project(
    project_id: UUID,
    payload: ProjectUpdate,
    user: CurrentUser,
    management: Service,
) -> Project:
    changes = payload.model_dump(exclude_unset=True, exclude={"version"})
    return await management.update_project(
        user.user_uuid, project_id, payload.version, changes
    )


@router.delete("/projects/{project_id}", status_code=204, tags=["projects"])
async def delete_project(
    project_id: UUID, user: CurrentUser, management: Service
) -> Response:
    await management.delete_project(user.user_uuid, project_id)
    return Response(status_code=204)


@router.post(
    "/sessions", response_model=Session, status_code=201, tags=["sessions"]
)
async def create_session(
    payload: SessionCreate,
    user: CurrentUser,
    management: Service,
    key: IdempotencyKey,
) -> Session:
    return await management.create_session(
        user.user_uuid, key, payload.project_id, payload.title
    )


@router.get("/sessions", response_model=Page[Session], tags=["sessions"])
async def list_sessions(
    project_id: UUID,
    user: CurrentUser,
    management: Service,
    limit: Limit = 20,
    cursor: Cursor = None,
) -> Page[Session]:
    return await management.sessions(user.user_uuid, project_id, limit, cursor)


@router.get(
    "/sessions/{session_id}", response_model=Session, tags=["sessions"]
)
async def get_session(
    session_id: UUID, user: CurrentUser, management: Service
) -> Session:
    return await management.session(user.user_uuid, session_id)


@router.patch(
    "/sessions/{session_id}", response_model=Session, tags=["sessions"]
)
async def update_session(
    session_id: UUID,
    payload: SessionUpdate,
    user: CurrentUser,
    management: Service,
) -> Session:
    return await management.update_session(
        user.user_uuid, session_id, payload.version, payload.title
    )


@router.delete("/sessions/{session_id}", status_code=204, tags=["sessions"])
async def delete_session(
    session_id: UUID, user: CurrentUser, management: Service
) -> Response:
    await management.delete_session(user.user_uuid, session_id)
    return Response(status_code=204)
