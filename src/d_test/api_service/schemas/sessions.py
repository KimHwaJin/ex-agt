"""Session request contracts and shared response models."""

from typing import Annotated
from uuid import UUID

from pydantic import Field, StringConstraints

from d_test.agent_service.domain.management import Session as Session

from .common import WriteRequest

SessionTitle = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)
]


class SessionCreate(WriteRequest):
    project_id: UUID
    title: SessionTitle = "새 대화"


class SessionUpdate(WriteRequest):
    version: int = Field(ge=1, strict=True)
    title: SessionTitle
