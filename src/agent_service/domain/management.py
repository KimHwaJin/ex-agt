"""Typed management entities; no HTTP or authentication framework imports."""

from datetime import datetime
from typing import Generic, TypeVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class DomainError(Exception):
    def __init__(self, code: str, message: str, status: int = 409):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


class Audit(BaseModel):
    model_config = ConfigDict(extra="ignore")
    created_at: datetime
    created_by: UUID
    updated_at: datetime
    updated_by: UUID


class User(Audit):
    user_uuid: UUID
    user_id: str
    status: str


class Project(Audit):
    project_id: UUID
    user_uuid: UUID
    user_id: str
    name: str
    description: str | None
    is_default: bool
    version: int


class Session(Audit):
    session_id: UUID
    project_id: UUID
    title: str
    version: int


class Home(BaseModel):
    user: User
    default_project: Project


T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    items: list[T]
    next_cursor: str | None
    has_more: bool


def not_found() -> DomainError:
    return DomainError("NOT_FOUND", "리소스를 찾을 수 없습니다.", 404)
