from __future__ import annotations

from uuid import uuid4

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="EW_", extra="ignore")

    database_url: str
    redis_url: str
    namespace: str = Field(default="executor-worker", min_length=1)
    executor_base_url: str = "http://localhost:8000/api/v1"
    executor_event_stream: str = "executor.events"
    command_stream_name: str | None = None
    event_group_name: str | None = None
    command_group_name: str | None = None
    instance_id: str = Field(default_factory=lambda: str(uuid4()))
    concurrency: int = Field(default=4, ge=1)
    ingress_concurrency: int | None = Field(default=None, ge=1)
    dispatch_concurrency: int | None = Field(default=None, ge=1)
    pool_size: int = Field(default=8, ge=2)
    batch_size: int = Field(default=100, ge=1, le=500)
    poll_seconds: float = Field(default=0.2, gt=0)
    idle_poll_seconds: float = Field(default=2, gt=0)
    claim_idle_milliseconds: int = Field(default=30000, ge=2001)
    lease_ttl_seconds: int = Field(default=60, ge=3)
    lease_renew_seconds: int = Field(default=1, ge=1)
    publish_lease_seconds: int = Field(default=30, ge=1)
    max_handler_attempts: int = Field(default=5, ge=1)
    shutdown_seconds: float = Field(default=25, ge=0)
    request_timeout_seconds: float = Field(default=10, gt=0)
    health_port: int = Field(default=8011, ge=0, le=65535)

    @property
    def command_stream(self) -> str:
        return self.command_stream_name or f"{self.namespace}:commands"

    @property
    def event_group(self) -> str:
        return self.event_group_name or f"{self.namespace}:ingress"

    @property
    def command_group(self) -> str:
        return self.command_group_name or f"{self.namespace}:dispatch"

    @property
    def ingress_workers(self) -> int:
        return self.ingress_concurrency or self.concurrency

    @property
    def dispatch_workers(self) -> int:
        return self.dispatch_concurrency or self.concurrency
