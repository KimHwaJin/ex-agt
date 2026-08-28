from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
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
    agent_product_event_stream: str = "agent.product-events"
    executor_event_stream: str = "executor.events"
    executor_event_consumer_group: str = "agent-executor-events-v1"

    executor_base_url: str = "http://127.0.0.1:8000/api/v1"
    executor_shared_storage_root: Path = Path("./shared_dir")
    executor_source_mode: Literal["PATH"] = "PATH"
    executor_runtime_profile: str = "basic"
    executor_request_timeout_seconds: float = Field(default=30, gt=0)
    executor_operation_wait_timeout_seconds: int = Field(
        default=3600,
        ge=30,
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

    command_block_milliseconds: int = Field(default=5000, ge=100)
    command_claim_idle_milliseconds: int = Field(default=30000, ge=1000)
    command_claim_batch_size: int = Field(default=20, ge=1, le=1000)


@lru_cache
def get_settings() -> Settings:
    return Settings()
