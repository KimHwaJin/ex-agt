# 공통 Worker — 정식 소스

공통 실행 기반은 [src/worker](.)다. 기존 standalone_worker의
소비·Inbox/Outbox 구현을 이동했으며 별도 소스 복사본은 유지하지 않는다.
이 README가 문서 진입점이며 기능·연결·DB 초기화·과거 검증 문서는 docs/에 있다.

현재 전체 통합의 **1단계**다. 기존 ex_agent API/Worker는 여전히 기존 경로로
실행된다. 새 agent.worker_main의 실제 Agent 연결은 다음 단계에서 구현한다.
미구현 factory는 이벤트 소비 전에 실패하며 데모로 대신 실행하지 않는다.

## 위치와 책임

| 위치 | 책임 |
|---|---|
| src/worker | Redis 소비, Inbox/Outbox, 순서·중복·재시도, guard, telemetry |
| src/worker/docs | 기능 설명·Agent 연결·DB 초기화·과거 검증 이력 |
| src/agent/integrations | Agent State에 종속된 LangGraph 어댑터와 핸들러 연결 |
| src/agent/worker_main.py | 공통 Worker와 Agent 그래프 조립·시작·종료 |
| tests/worker | 공통 워커 및 그래프 연결 회귀 테스트 |
| examples/worker | State·interrupt·API 연결·실패 보상 참조 예제 |
| worker_migrations | 독립 ew_* 테이블의 Alembic migration |
| deploy/worker | 격리 Compose 검증, 환경변수·K8s 배포 템플릿 |

worker는 agent/api/ex_agent/LangGraph/FastAPI를 import하지 않는다.
다른 서비스에는 src/worker 전체와 필요한 의존성·migration만 전달할 수 있다.
공통 모듈의 이벤트 처리는 등록한 async 핸들러에 위임한다.

## 설치와 검증

모든 명령의 작업 디렉터리는 **저장소 루트**다. 별도 uv.lock이나 하위 .venv를
사용하지 않는다. 기존 langgraph dev 환경을 함께 쓰면 --group chat-ui도 추가한다.

```bash
uv sync --frozen --no-editable --group chat-ui
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync ty check
uv run --no-sync python -m pytest -q tests/worker
docker compose -f deploy/worker/compose.test.yaml run --build --rm test
docker compose -f deploy/worker/compose.test.yaml stop postgres redis
```

Compose는 root Dockerfile의 test stage와 파일 기반 설치를 사용한다.
ex-agent-worker-foundation-test라는 별도 프로젝트의 임시 PostgreSQL·Redis를
사용하며 운영 DB나 기존 API/Worker를 시작하지 않는다. 원래 서비스의 전체
회귀도 루트 tests에서 함께 실행할 수 있다.

## 초기화·실행

배포 전 한 번씩 필요한 migration을 Job으로 수행한다. 시작 시 자동 DDL은 없다.

```bash
uv run --no-sync --env-file .env \
  alembic -c worker_migrations/alembic.ini upgrade head
```

EW_DATABASE_URL은 psycopg PostgreSQL URL, EW_REDIS_URL은 Redis URL이다.
환경 예시는 [deploy/worker/.env.example](../../deploy/worker/.env.example)에 있다.
LangGraph checkpoint 테이블은 별도 Agent 배포 절차에서 setup한다.

Agent 연결을 구현한 뒤 새 Worker를 시작하는 명령:

```bash
uv run --no-sync --env-file .env python -m agent.worker_main
```

아직 이 명령을 기존 ex-agent-worker의 배포 대체로 사용하지 않는다.
[연결 가이드](docs/agent-integration.md)와 [전환 계획](../../docs/worker-centered-refactor.md)의
완료 조건을 확인한다. root Docker runtime의 기본 CMD도 아직 기존 API다.

## 전달과 복구 계약

Executor Stream → Inbox 저장·ACK → binding 확인·순번 복구 →
Command/Outbox 원자 저장 → 내부 Stream → 세션 guard → 핸들러 → DONE·ACK.

원본 이벤트의 event_id/execution_id/event_sequence와 payload를 보존한다.
binding은 execution_id → session_id/task_id 연결이고 thread_id는 session_id다.
등록 전 이벤트는 DB에서 대기하며, 미등록 타입은 IGNORED로 기록한다.
원본 누락 순번은 Executor history REST API로 보충하고 임의로 건너뛰지 않는다.

Outbox는 발행 복구, Redis pending은 핸들러 재전달을 담당한다.
업무 실패만 DB 재시도 횟수를 차감하며 DeferEvent/잠금 충돌은 대기한다.
같은 Execution의 Command는 순서를 지키고 같은 세션의 실행은 직렬화한다.
호스트의 외부 부수 효과는 command_id + 안정적인 작업명으로 멱등하게 처리한다.

Redis lease와 checkpoint가 외부 API까지 exactly-once로 묶어 주지는 않는다.
장기 채팅 금지·성공 리포트·취소 확인·요청 접수는 Agent/API가 맡는다.

## 운영·인수인계

기본 포트 8011의 /health/live, /health/ready, /metrics를 제공한다.
SIGTERM/SIGINT 수신 시 신규 소비 중단 후 설정된 시간 동안 핸들러를 drain한다.
Worker 종료는 Executor 실행 취소가 아니다.

사용자가 삭제한 DLQ replay/discard·trim CLI는 복원하지 않았다.
소비기의 DLQ 발행과 Store의 Command retry/skip·audit는 유지한다.
관리 API 및 Stream 정리는 별도 후속 작업이다.

- [기능별 설명](docs/features.md)
- [Agent 개발자 연결 지점](docs/agent-integration.md)
- [DB 초기화와 전환 주의사항](docs/migrations.md)
- [통합 전환·검증 상태](../../docs/worker-centered-refactor.md)
- [이동 이전 검증 이력](docs/validation-history.md)
