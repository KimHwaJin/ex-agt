"""HTTP write contracts: callers cannot set ownership or audit fields."""

from typing import Annotated
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)

ProjectName = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)
]
SessionTitle = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)
]
Description = Annotated[str, StringConstraints(max_length=2000)]


class WriteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ProjectCreate(WriteRequest):
    name: ProjectName
    description: Description | None = None


class ProjectUpdate(WriteRequest):
    version: int = Field(ge=1, strict=True)
    name: ProjectName | None = None
    description: Description | None = None

    @model_validator(mode="after")
    def validate_patch(self) -> "ProjectUpdate":
        if not self.model_fields_set.intersection({"name", "description"}):
            raise ValueError("수정할 필드를 하나 이상 전달해 주세요.")
        if "name" in self.model_fields_set and self.name is None:
            raise ValueError("이름은 null일 수 없습니다.")
        return self


class SessionCreate(WriteRequest):
    project_id: UUID
    title: SessionTitle = "새 대화"


class SessionUpdate(WriteRequest):
    version: int = Field(ge=1, strict=True)
    title: SessionTitle
