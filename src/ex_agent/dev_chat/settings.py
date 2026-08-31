from pydantic import Field, HttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ChatSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CHAT_UI_",
        env_file=".env.chat-ui",
        extra="ignore",
    )

    api_url: HttpUrl = HttpUrl("http://127.0.0.1:8010")
    user_id: str = Field(
        default="chat-ui-test-user",
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$",
    )
    project_id: str = Field(
        default="chat-ui-test-project",
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$",
    )
    watch_seconds: float = Field(
        default=30,
        gt=0,
        le=300,
        description="Automatic SSE reconciliation interval; never a HITL stop",
    )
    request_timeout_seconds: float = Field(default=10, gt=0, le=60)

    @field_validator("project_id")
    @classmethod
    def validate_project(cls, value: str) -> str:
        if value == "unscoped":
            raise ValueError("unscoped is reserved by Executor")
        return value
