"""Session HTTP endpoints."""

from uuid import UUID

from fastapi import APIRouter, Response

from ..dependencies import CurrentUser, IdempotencyKey, Service
from ..dependencies.parameters import Cursor, Limit
from ..schemas.common import Page
from ..schemas.sessions import Session, SessionCreate, SessionUpdate

router = APIRouter(prefix="/api/v1")


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
