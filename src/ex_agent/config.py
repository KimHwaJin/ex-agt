from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded exclusively from the environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = "local"
    log_level: str = "INFO"
    host: str = "0.0.0.0"
    port: int = 8010

    agent_database_url: str = (
        "postgresql+psycopg://agent:agent@127.0.0.1:55432/agent"
    )
    agent_checkpoint_database_url: str = (
        "postgresql://agent:agent@127.0.0.1:55432/agent"
    )
    agent_redis_url: str = "redis://127.0.0.1:56379/0"
    agent_command_stream: str = "agent.commands"
    agent_command_consumer_group: str = "agent-workflow-workers-v1"
    agent_command_dead_letter_stream: str = "agent.commands.dlq"
    agent_product_event_stream: str = "agent.product-events"
    agent_product_event_channel_prefix: str = "agent.task-events"
    executor_event_stream: str = "executor.events"
    executor_event_consumer_group: str = "agent-executor-events-v1"
    executor_event_dead_letter_stream: str = "executor.events.agent-dlq"

    executor_base_url: str = "http://127.0.0.1:8000/api/v1"
    executor_shared_storage_root: Path = Path("./shared_dir")
    executor_source_mode: Literal["PATH"] = "PATH"
    executor_runtime_profile: str = "basic"
    executor_request_timeout_seconds: float = Field(default=30, gt=0)
    executor_operation_wait_timeout_seconds: int = Field(
        default=3600,
        ge=30,
    )
    executor_result_context_max_chars: int = Field(
        default=20000,
        ge=1000,
        le=100000,
    )
    executor_result_manifest_max_bytes: int = Field(
        default=2097152,
        ge=1024,
        le=16777216,
    )
    executor_failure_cleanup_timeout_seconds: float = Field(
        default=60,
        gt=0,
    )
    executor_failure_cleanup_poll_seconds: float = Field(
        default=0.5,
        gt=0,
    )

    agent_model: str = "qwen38-27b-fp8"
    agent_model_provider: str = "openai"
    agent_model_base_url: str = "http://model.frodo.com/v1"
    agent_model_api_key: str = "EMPTY"
    agent_model_temperature: float = 0
    agent_model_timeout_seconds: float = Field(default=120, gt=0)
    agent_model_max_retries: int = Field(default=2, ge=0, le=10)
    agent_model_max_tokens: int = Field(default=4096, ge=1)
    agent_model_enable_thinking: bool = False
    agent_embedding_provider: Literal["dummy", "openai"] = "dummy"
    agent_embedding_model: str = "dummy-hash-v1"
    agent_embedding_base_url: str = "http://model.frodo.com/v1"
    agent_embedding_api_key: str = "EMPTY"
    agent_embedding_dimensions: int = Field(default=1024, ge=1)
    planner_timeout_seconds: float = Field(default=120, gt=0)
    planner_context_max_chars: int = Field(default=50000, ge=1000)
    planner_max_model_calls: int = Field(default=4, ge=1, le=20)
    correction_limit: int = Field(default=3, ge=0, le=10)
    agent_skill_root: Path = Path("./skills")

    command_block_milliseconds: int = Field(default=5000, ge=100)
    command_claim_idle_milliseconds: int = Field(default=30000, ge=1000)
    stream_claim_batch_size: int = Field(default=10, ge=1, le=1000)
    command_max_retry_attempts: int = Field(default=5, ge=1, le=10000)
    executor_event_max_retry_attempts: int = Field(
        default=100,
        ge=1,
        le=10000,
    )
    stream_retry_state_ttl_seconds: int = Field(
        default=604800,
        ge=60,
    )
    consumer_gc_idle_milliseconds: int = Field(
        default=86400000,
        ge=60000,
    )
    worker_instance_id: str | None = Field(default=None, max_length=128)
    worker_command_concurrency: int = Field(default=4, ge=1, le=64)
    worker_executor_event_concurrency: int = Field(default=8, ge=1, le=64)
    checkpoint_pool_min_size: int = Field(default=1, ge=1, le=64)
    checkpoint_pool_max_size: int = Field(default=8, ge=1, le=64)
    task_lock_ttl_seconds: int = Field(default=60, ge=30)
    task_lock_renew_interval_seconds: int = Field(default=10, ge=5)
    executor_event_claim_idle_milliseconds: int = Field(
        default=30000,
        ge=1000,
    )
    executor_event_lock_ttl_seconds: int = Field(default=60, ge=30)
    executor_event_lock_renew_interval_seconds: int = Field(
        default=10,
        ge=5,
    )
    outbox_poll_milliseconds: int = Field(default=100, ge=10)
    outbox_idle_max_milliseconds: int = Field(default=1000, ge=100)
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_claim_timeout_seconds: int = Field(default=30, ge=5)
    product_event_stream_maxlen: int = Field(default=100000, ge=1000)
    sse_heartbeat_seconds: float = Field(default=15, ge=1)
    worker_retry_initial_seconds: float = Field(default=0.5, ge=0.1)
    worker_retry_max_seconds: float = Field(default=30, ge=1)
    worker_shutdown_grace_seconds: float = Field(
        default=25,
        ge=0,
        le=300,
    )
    worker_metrics_enabled: bool = True
    worker_metrics_host: str = "0.0.0.0"
    worker_metrics_port: int = Field(default=8011, ge=1, le=65535)
    worker_metrics_refresh_seconds: float = Field(default=10, ge=1)
    readiness_probe_timeout_seconds: float = Field(default=2, ge=0.1)
    worker_readiness_stale_seconds: float = Field(default=30, ge=2)

    @model_validator(mode="after")
    def validate_concurrency_settings(self) -> "Settings":
        if self.checkpoint_pool_min_size > self.checkpoint_pool_max_size:
            raise ValueError(
                "checkpoint_pool_min_size cannot exceed "
                "checkpoint_pool_max_size"
            )
        if self.worker_command_concurrency > self.checkpoint_pool_max_size:
            raise ValueError(
                "worker_command_concurrency cannot exceed "
                "checkpoint_pool_max_size"
            )
        if self.task_lock_renew_interval_seconds >= (
            self.task_lock_ttl_seconds
        ):
            raise ValueError(
                "task_lock_renew_interval_seconds must be less than "
                "task_lock_ttl_seconds"
            )
        if self.task_lock_renew_interval_seconds * 1000 >= (
            self.command_claim_idle_milliseconds
        ):
            raise ValueError(
                "task lock renewal must be faster than command claim idle"
            )
        if self.executor_event_lock_renew_interval_seconds >= (
            self.executor_event_lock_ttl_seconds
        ):
            raise ValueError(
                "executor_event_lock_renew_interval_seconds must be less "
                "than executor_event_lock_ttl_seconds"
            )
        if self.executor_event_lock_renew_interval_seconds * 1000 >= (
            self.executor_event_claim_idle_milliseconds
        ):
            raise ValueError(
                "executor event lock renewal must be faster than event "
                "claim idle"
            )
        if self.worker_retry_initial_seconds > self.worker_retry_max_seconds:
            raise ValueError(
                "worker_retry_initial_seconds cannot exceed "
                "worker_retry_max_seconds"
            )
        retry_windows = {
            "command": (
                self.command_claim_idle_milliseconds
                * self.command_max_retry_attempts
            ),
            "executor event": (
                self.executor_event_claim_idle_milliseconds
                * self.executor_event_max_retry_attempts
            ),
        }
        for name, window_milliseconds in retry_windows.items():
            if self.stream_retry_state_ttl_seconds * 1000 <= (
                window_milliseconds
            ):
                raise ValueError(
                    "stream_retry_state_ttl_seconds must exceed the "
                    f"{name} retry window"
                )
        if self.outbox_poll_milliseconds > self.outbox_idle_max_milliseconds:
            raise ValueError(
                "outbox_poll_milliseconds cannot exceed "
                "outbox_idle_max_milliseconds"
            )
        if self.worker_readiness_stale_seconds <= (
            self.worker_metrics_refresh_seconds
        ):
            raise ValueError(
                "worker_readiness_stale_seconds must exceed "
                "worker_metrics_refresh_seconds"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
