"""Project request contracts and shared response models."""

from typing import Annotated

from pydantic import Field, StringConstraints, model_validator

from d_test.agent_service.domain.management import Project as Project

from .common import WriteRequest

ProjectName = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)
]

Description = Annotated[str, StringConstraints(max_length=2000)]


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
