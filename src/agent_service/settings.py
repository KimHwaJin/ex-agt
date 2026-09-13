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
    logging_mode: Literal["standard", "host"] = "standard"
    logging_initializer: str | None = None
    logging_yaml: str | None = None
    pool_min_size: int = Field(default=1, ge=1)
    pool_max_size: int = Field(default=10, ge=1)
    pool_timeout: float = Field(default=10, gt=0)
    bind_host: str = "127.0.0.1"
    bind_port: int = Field(default=8020, ge=1, le=65535)

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
            if self.auth_mode == "development_header":
                raise ValueError("development identity is not production auth")
            if self.logging_mode != "host":
                raise ValueError("production requires host logging")
            if self.cursor_secret.get_secret_value().startswith("development"):
                raise ValueError("replace the development cursor secret")
        if self.logging_mode == "host":
            if not self.logging_initializer or not self.logging_yaml:
                raise ValueError("host logging initializer and YAML required")
        return self
