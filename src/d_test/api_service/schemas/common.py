"""Common HTTP schema building blocks."""

from pydantic import BaseModel, ConfigDict

from d_test.agent_service.domain.management import Page as Page


class WriteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
