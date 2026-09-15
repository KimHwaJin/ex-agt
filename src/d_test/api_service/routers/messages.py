"""Read persisted session messages."""

from uuid import UUID

from fastapi import APIRouter, Request

from ..dependencies import CurrentUser
from ..dependencies.parameters import Cursor, Limit
from ..dependencies.services import run_service as service
from ..schemas.common import Page
from ..schemas.runs import Message

router = APIRouter(prefix="/api/v1", tags=["agent"])


@router.get("/sessions/{session_id}/messages", response_model=Page[Message])
async def list_messages(
    session_id: UUID,
    request: Request,
    user: CurrentUser,
    limit: Limit = 20,
    cursor: Cursor = None,
):
    return await service(request).listing(
        user.user_uuid, session_id, limit, cursor, messages=True
    )
