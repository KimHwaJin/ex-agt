"""Current user provisioning and profile endpoints."""

from fastapi import APIRouter

from ..dependencies import CurrentUser, EmployeeId, Service
from ..schemas.users import Home

router = APIRouter(prefix="/api/v1")


@router.post("/me", response_model=Home, tags=["user"])
async def me(employee: EmployeeId, management: Service) -> Home:
    return await management.me(employee)


@router.get("/me", response_model=Home, tags=["user"])
async def get_me(user: CurrentUser, management: Service) -> Home:
    return await management.get_me(user.user_uuid)
