from __future__ import annotations

from uuid import uuid4

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="EW_", extra="ignore")

    # Worker의 Inbox/Outbox, 실행 연결 정보를 저장할 PostgreSQL URL이다.
    database_url: str

    # Executor 원본 이벤트와 내부 command Stream을 사용할 Redis URL이다.
    redis_url: str

    # DB 행, Redis key와 기본 Stream 이름을 서비스별로 구분하는 값이다.
    namespace: str = Field(default="executor-worker", min_length=1)

    # 이벤트 순번 누락 시 실행 이력을 조회할 Executor REST API 주소다.
    executor_base_url: str = "http://localhost:8000/api/v1"

    # Executor가 원본 실행 이벤트를 발행하는 Redis Stream 이름이다.
    executor_event_stream: str = "executor.events"

    # Inbox에서 변환한 command를 발행할 내부 Stream 이름이다.
    # 지정하지 않으면 ``{namespace}:commands``를 사용한다.
    command_stream_name: str | None = None

    # 원본 Executor event Stream을 읽는 consumer group 이름이다.
    # 지정하지 않으면 ``{namespace}:ingress``를 사용한다.
    event_group_name: str | None = None

    # 내부 command Stream을 읽는 consumer group 이름이다.
    # 지정하지 않으면 ``{namespace}:dispatch``를 사용한다.
    command_group_name: str | None = None

    # Redis consumer를 구분하는 Worker replica 식별자다.
    # 지정하지 않으면 프로세스를 시작할 때 UUID를 생성한다.
    instance_id: str = Field(default_factory=lambda: str(uuid4()))

    # ingress/dispatch 전용 동시성 값이 없을 때 사용하는 공통 기본값이다.
    concurrency: int = Field(default=4, ge=1)

    # Executor 원본 이벤트를 동시에 수집하는 consumer 수다.
    # 지정하지 않으면 ``concurrency`` 값을 사용한다.
    ingress_concurrency: int | None = Field(default=None, ge=1)

    # 내부 command handler를 동시에 실행하는 consumer 수다.
    # 지정하지 않으면 ``concurrency`` 값을 사용한다.
    dispatch_concurrency: int | None = Field(default=None, ge=1)

    # Worker가 사용하는 PostgreSQL 비동기 연결 풀의 최대 크기다.
    pool_size: int = Field(default=8, ge=2)

    # 이력 보충, Inbox routing, Outbox 발행을 한 번에 처리할 최대 개수다.
    batch_size: int = Field(default=100, ge=1, le=500)

    # Router와 Outbox에 처리할 데이터가 있을 때의 반복 간격(초)이다.
    poll_seconds: float = Field(default=0.2, gt=0)

    # 처리할 데이터가 없을 때 지수 backoff가 증가할 최대 간격(초)이다.
    idle_poll_seconds: float = Field(default=2, gt=0)

    # 다른 consumer가 멈춘 pending 메시지를 회수하기 전 대기시간(ms)이다.
    claim_idle_milliseconds: int = Field(default=30000, ge=2001)

    # 세션 잠금과 메시지 처리 lease가 만료되는 시간(초)이다.
    lease_ttl_seconds: int = Field(default=60, ge=3)

    # 처리 중인 세션 잠금과 메시지 lease를 갱신하는 간격(초)이다.
    lease_renew_seconds: int = Field(default=1, ge=1)

    # 한 Worker가 DB Outbox 행의 발행 권한을 점유하는 시간(초)이다.
    publish_lease_seconds: int = Field(default=30, ge=1)

    # 업무 handler 실패를 최종 실패와 DLQ로 보내기 전 최대 시도 횟수다.
    max_handler_attempts: int = Field(default=5, ge=1)

    # 종료 신호 후 실행 중인 consumer와 handler를 기다릴 최대 시간(초)이다.
    shutdown_seconds: float = Field(default=25, ge=0)

    # Executor HTTP 요청과 Redis 연결에 적용하는 timeout(초)이다.
    request_timeout_seconds: float = Field(default=10, gt=0)

    # liveness, readiness, metrics HTTP 서버 포트다. 0이면 비활성화한다.
    health_port: int = Field(default=8011, ge=0, le=65535)

    @property
    def command_stream(self) -> str:
        """실제로 사용할 내부 command Stream 이름을 반환한다."""

        return self.command_stream_name or f"{self.namespace}:commands"

    @property
    def event_group(self) -> str:
        """실제로 사용할 Executor event consumer group을 반환한다."""

        return self.event_group_name or f"{self.namespace}:ingress"

    @property
    def command_group(self) -> str:
        """실제로 사용할 내부 command consumer group을 반환한다."""

        return self.command_group_name or f"{self.namespace}:dispatch"

    @property
    def ingress_workers(self) -> int:
        """원본 Executor event를 소비할 실제 동시성 값을 반환한다."""

        return self.ingress_concurrency or self.concurrency

    @property
    def dispatch_workers(self) -> int:
        """내부 command handler를 실행할 실제 동시성 값을 반환한다."""

        return self.dispatch_concurrency or self.concurrency
