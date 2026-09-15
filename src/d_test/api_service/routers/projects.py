"""Project HTTP endpoints."""

from uuid import UUID

from fastapi import APIRouter, Response

from ..dependencies import CurrentUser, IdempotencyKey, Service
from ..dependencies.parameters import Cursor, Limit
from ..schemas.common import Page
from ..schemas.projects import Project, ProjectCreate, ProjectUpdate

router = APIRouter(prefix="/api/v1")


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
