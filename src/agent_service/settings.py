"""Validated settings loaded from an explicitly selected YAML profile."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    environment: Literal["development", "production"] = "development"
    database_url: SecretStr
    cursor_secret: SecretStr
    auth_mode: Literal["development_header", "trusted_header", "external"] = (
        "development_header"
    )
    trusted_proxy_cidrs: list[str] = Field(default_factory=list)
    identity_provider_factory: str | None = None
    logging_mode: Literal["standard", "host", "preconfigured"] = "standard"
    logging_initializer: str | None = None
    logging_yaml: str | None = None
    pool_min_size: int = Field(default=1, ge=1)
    pool_max_size: int = Field(default=10, ge=1)
    pool_timeout: float = Field(default=10, gt=0)
    bind_host: str = "127.0.0.1"
    bind_port: int = Field(default=8020, ge=1, le=65535)
    agent_backend: Literal["disabled", "demo", "langgraph"] = "disabled"
    embedded_run_worker: bool = False
    demo_scenario: Literal["reply", "analysis", "failure"] = "analysis"
    demo_step_seconds: float = Field(default=0.5, ge=0.05, le=10)
    stream_poll_seconds: float = Field(default=0.5, ge=0.05, le=10)
    stream_max_seconds: float = Field(default=60, ge=1, le=300)
    # None이면 management DB URL을 사용하되 테이블은 별도 스키마에 저장.
    checkpoint_database_url: SecretStr | None = None
    checkpoint_schema: str = Field(
        default="agent_checkpoints", pattern=r"^[a-z][a-z0-9_]{0,62}$"
    )
    checkpoint_pool_max_size: int = Field(default=5, ge=2, le=100)
    worker_concurrency: int = Field(default=2, ge=1, le=32)
    worker_poll_seconds: float = Field(default=0.25, ge=0.05, le=10)
    worker_reschedule_seconds: float = Field(default=5, ge=1, le=60)
    run_timeout_seconds: float = Field(default=180, ge=1, le=1800)
    recovery_max_attempts: int = Field(default=3, ge=1, le=10)
    model_name: str | None = None
    # 동일 endpoint에서 선택 가능한 이름. 기본 모델은 자동 포함된다.
    allowed_model_names: list[str] = Field(default_factory=list, max_length=31)
    model_provider: str = "openai"
    model_base_url: str | None = None
    model_api_key: SecretStr | None = None
    model_timeout_seconds: float = Field(default=60, ge=1, le=600)
    model_max_retries: int = Field(default=1, ge=0, le=5)
    model_max_tokens: int = Field(default=2048, ge=1, le=16384)
    # 함수 원문을 포함하는 코드 계획에만 적용되는 출력 상한.
    code_plan_max_tokens: int = Field(default=8192, ge=512, le=16384)
    model_extra_body: dict = Field(default_factory=dict)
    context_message_limit: int = Field(default=40, ge=2, le=200)
    output_flush_chars: int = Field(default=256, ge=1, le=4096)
    output_flush_seconds: float = Field(default=0.2, ge=0.05, le=2)
    output_max_chars: int = Field(default=64000, ge=100, le=256000)

    @model_validator(mode="after")
    def validate_runtime(self) -> "Settings":
        url = self.database_url.get_secret_value()
        if not url.startswith(("postgresql://", "postgres://")):
            raise ValueError("database_url must be a PostgreSQL URI")
        if len(self.cursor_secret.get_secret_value()) < 32:
            raise ValueError("cursor_secret must have at least 32 characters")
        if self.pool_min_size > self.pool_max_size:
            raise ValueError("pool_min_size exceeds pool_max_size")
        if self.auth_mode == "trusted_header":
            if not self.trusted_proxy_cidrs:
                raise ValueError("trusted proxies must be explicitly set")
            from ipaddress import ip_network

            for network in self.trusted_proxy_cidrs:
                ip_network(network)
        if self.auth_mode == "external":
            if not self.identity_provider_factory:
                raise ValueError("external identity provider is required")
        if self.environment == "production":
            if self.agent_backend == "demo":
                raise ValueError("demo agent is forbidden in production")
            if self.auth_mode == "development_header":
                raise ValueError("development identity is not production auth")
            if self.logging_mode not in {"host", "preconfigured"}:
                raise ValueError("production requires host logging")
            if self.cursor_secret.get_secret_value().startswith("development"):
                raise ValueError("replace the development cursor secret")
        if self.embedded_run_worker and self.agent_backend == "disabled":
            raise ValueError("embedded worker requires an enabled backend")
        if self.checkpoint_database_url and not (
            self.checkpoint_database_url.get_secret_value().startswith(
                ("postgresql://", "postgres://")
            )
        ):
            raise ValueError("checkpoint database must be PostgreSQL")
        if self.agent_backend == "langgraph":
            if not self.model_name:
                raise ValueError("langgraph backend requires model_name")
            if self.checkpoint_pool_max_size <= self.worker_concurrency:
                raise ValueError("checkpoint pool needs an extra health slot")
        if any(not name.strip() for name in self.allowed_model_names):
            raise ValueError("allowed model names cannot be blank")
        if self.logging_mode == "host":
            if not self.logging_initializer or not self.logging_yaml:
                raise ValueError("host logging initializer and YAML required")
        return self

    @property
    def selectable_models(self) -> tuple[str, ...]:
        names = ([self.model_name] if self.model_name else []) + (
            self.allowed_model_names
        )
        return tuple(dict.fromkeys(names))
