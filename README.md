# ex-agent

LangGraph 상태 머신과 LangChain `create_agent()` middleware를 사용하는
데이터 분석/코드 실행 Agent BFF다. 승인된 코드는 Executor REST API로 제출하고
Executor Redis event에서 workflow를 재개한다.

## API+Agent / Worker 구조

공통 워커는 [src/worker](src/worker)로 편입했고, 그래프 연결·시작 코드는
src/agent로 이동했다. [현재 워커 안내](src/worker/README.md)와
[전환 계획·검증 상태](docs/worker-centered-refactor.md)를 참고한다.
FastAPI는 요청을 세션 그래프에 직접 접수·invoke하고, Worker는 Executor 이벤트를
Inbox/Outbox로 내구성 있게 전달해 같은 그래프를 resume한다. 두 프로세스는
`session_id = thread_id`인 PostgreSQL checkpoint와 Redis SessionGuard를 공유한다.

## 개발 명령

API/Worker 컨테이너와 **Agent Chat UI를 함께 테스트**하려면
[Agent Chat UI Testing](docs/agent-chat-ui-testing.md)을 참고한다.
실제 API → Agent → Executor → Jupyter → Worker 경로는
[Live Executor E2E](docs/live-executor-e2e.md)를 참고한다.
로컬 `langgraph dev`는 UI 연결 그래프를 제공하고, 업무 START/RESUME은 API가,
Executor 이벤트 resume은 Worker가 처리한다.

```bash
uv sync --frozen --group dev --no-editable --reinstall-package ex-agent
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync ty check
uv run --no-sync python -m pytest
```

실제 PostgreSQL/pgvector와 Redis 통합 테스트:

```bash
docker compose --profile test build
docker compose --profile test run --rm test
```

`test` profile은 app container와 격리된 임시 `test-postgres`와 `test-redis`를
사용한다. API/worker가 실행 중이어도 같은 outbox나 consumer group을 경쟁하지
않으며 test migration도 별도 DB에 적용된다.

API 부하 테스트와 Prometheus 지표 설명은
[Performance Testing](docs/performance-testing.md)을 참고한다. API는 `/metrics`,
Worker는 기본적으로 `8011` 포트에서 metrics를 제공한다.
Liveness/readiness 계약과 Prometheus 경보 기준은
[Readiness and Alerting](docs/operations-readiness.md)을 참고한다.
실제 Kubernetes Worker 정상·강제 재시작 복구는
[Kubernetes Worker restart E2E](deploy/rolling-e2e/README.md)로 재현한다.
별도 Agent에서도 사용할 수 있는 Redis Stream 소비기 계약과 예시는
[Reusable Redis Stream Consumer](docs/redis-stream-consumer.md)를 참고한다.
외부 이벤트를 내구성 있는 커맨드로 바꾸어 LangGraph를 재개하는 이식용
참조 구현은
[Durable event to LangGraph](examples/durable_event_to_langgraph/README.md)를
참고한다.
기존 Agent 개발자에게 Worker를 전달할 때는
[독립 Worker 모듈](src/worker/README.md)을 먼저 읽는다.
`src/worker/`에 공통 소스와 문서가 함께 있다. 실행하려면 루트 의존성과
`worker_migrations/`도 필요하며, Agent 연결 예제와 테스트·배포 자료의 위치는
워커 README에서 안내한다. 별도 standalone_worker 폴더는 유지하지 않는다.
이전 Task 기반 연결 예제 설명은
[Worker 인수인계 가이드](docs/worker-handoff-guide.md)에 보존했다.
이전 Task 기반 [연결 예제](examples/api_agent_worker/README.md)와
[동일 Pod 배포 템플릿](deploy/handoff/README.md)도 참조용으로 보존한다.
이전 Worker 중심 구현은
[과거 서비스 참조](docs/worker-reference-implementation.md)에 별도 보존했다.
현재 모듈 경계와 허용 import 방향은
[Project Structure](docs/project-structure.md)를 참고한다.
공통 audit 필드, cursor pagination과 OpenAPI 규칙은
[API Conventions](docs/api-conventions.md)를 참고한다.

결정론적 전체 수명주기 benchmark 예시:

```bash
uv run --no-sync python scripts/lifecycle_benchmark.py \
  --scenario multi_analysis --requests 20 --concurrency 4
```

API와 worker 실행:

```bash
cp .env.example .env
docker compose up --build
```

Kubernetes 운영 배포는 루트 Dockerfile로 이미지 하나를 만든 뒤 같은 Pod에서
`ex-agent-api`와 `ex-agent-worker`를 별도 컨테이너로 실행한다. 배포 전에 같은
이미지의 `ex-agent-migrate` Job을 완료해야 한다. 정식 manifest와 환경별 치환
항목은 [Kubernetes 배포](deploy/k8s/README.md)를 따른다.

기본 Compose는 `migrate`, `api`, `worker`만 실행하고 Executor 쪽
PostgreSQL(`5432`)과 Redis(`6379`)에 연결한다. PostgreSQL 서버는 공유하되
Agent의 DB/role은 `agent`로 분리하며 Executor DB에 Agent migration을
실행하지 않는다. 최초 DB 생성과 기존 데이터 전환 주의사항은
[Shared Executor Infrastructure](docs/shared-executor-infrastructure.md)를
참고한다. `.env`의 두 DB URL과 Redis URL이 실제 접속 정보와 일치해야 한다.

Executor는 별도로 실행하고 `EXECUTOR_BASE_URL`을 설정한다. Agent와 Executor가
PATH source를 교환하려면 `EXECUTOR_SHARED_DIR`이 Executor의 `shared_dir`을
가리켜야 한다. `EXECUTOR_SOURCE_MODE=PATH`만 허용되며, 실행 코드와 성공
리포트는 공유 입력 파일의 상대경로와 SHA-256으로만 제출된다.

API와 worker는 반드시 같은 `AGENT_REDIS_URL`을 사용해야 한다.
Executor의 `executor.events`를 같은 Redis에서 소비하는 배치라면
`.env`에 `redis://host.docker.internal:6379/0` 같은 실제 공유 Redis URL을
저장한다. 일회성 shell 변수로만 주입하면 후속 `docker compose up`
시 worker가 기본 로컬 Redis로 복귀할 수 있다.

## API 흐름

- `POST /api/v1/projects/{project_id}/sessions/{session_id}/tasks`
- `GET /api/v1/tasks/{task_id}`
- `POST /api/v1/tasks/{task_id}/resume`
- `POST /api/v1/tasks/{task_id}/cancel`
- `GET /api/v1/tasks/{task_id}/events` (`Last-Event-ID` 기반 SSE)
- `GET /api/v1/tasks/{task_id}/workflow-promotion-draft`
- `POST /api/v1/tasks/{task_id}/workflow-promotions`
- `POST /api/v1/workflows/{workflow_id}/versions`
- `GET /api/v1/workflows/{workflow_id}`
- `GET /api/v1/workflows/{workflow_id}/versions`
- `GET /api/v1/workflows/{workflow_id}/versions/{version_id}`
- `GET /api/v1/workflows/{workflow_id}/lifecycle-actions`
- `POST /api/v1/workflows/{workflow_id}/versions/{version_id}/reviews`
- `POST /api/v1/workflows/{workflow_id}/versions/{version_id}/activate`
- `POST /api/v1/workflows/{workflow_id}/status`

BFF는 인증한 사용자의 `X-User-ID`를 전달한다. production에서는 method,
path/query, user ID, timestamp, nonce와 body hash를 HMAC으로 함께 서명하며 상세
계약은 [BFF 요청 서명](docs/bff-request-signing.md)을 따른다. Task 생성 시 BFF가
채번한 `task_id`와 `input_message_id`를 body에 넣는다. API는 Task와 요청 원장을
한 트랜잭션으로 저장한 뒤 같은 세션 guard에서 Graph를 직접 invoke한다. 승인·수정·
취소도 현재 interrupt ID와 함께 접수하며, 응답 유실이나 API 종료 시 요청 복구
loop가 checkpoint 증거를 확인해 이어 간다. Worker는 Executor 이벤트만 받아
Graph를 resume한다.

Task의 비최종 상태와 interrupt ID는 checkpoint에서 멱등 projection한다. SSE의
재연결·누락 복구 원본은 PostgreSQL event history이며, 공통 runtime의 제품 이벤트
outbox relay가 Task별 Redis Pub/Sub을 깨운다.

기본 LLM은 내부 vLLM OpenAI 호환 endpoint
`http://model.frodo.com/v1`의 `qwen38-27b-nvfp4`이다. Compose는
`model.frodo.com:10.250.110.99`를 각 Agent 컨테이너의 `/etc/hosts`에
추가한다. IP가 바뀌면 `MODEL_HOST_IP`, 모델이 바뀌면 `AGENT_MODEL`로
덮어쓴다. 기본값은 현재 vLLM에 등록된 모델 ID와 동일하게 유지한다.

현재 Workflow 검색은 외부 모델 없이 `dummy-hash-v1` 결정적 임베딩을 사용한다.
동일한 1024차원 구현을 인덱싱과 질의에 함께 사용해 pgvector 흐름을 개발할 수
있지만, 의미 검색 품질을 보장하지는 않는다. 실제 임베딩 모델이 확보되면
`AGENT_EMBEDDING_PROVIDER=openai`와 모델 endpoint를 설정해 교체한다.
Embedding 생성 자체가 실패하거나 차원이 다르면 `workflow.search_degraded` Task
event를 남기고 동적 MULTI 계획으로 전환한다.

승인 시점부터 성공 리포트 완료, Executor 실패 확인 또는 취소 완료까지
Session lock을 유지한다. 성공 리포트는 Executor REPORT Artifact API로
Notebook의 Markdown cell과 함께 생성한다. 리포트 본문도 INLINE API
payload가 아니라 공유 입력 루트의 Markdown 파일로 전달한다.

상세 설계는 [LangGraph Workflow Design](docs/langgraph-design.md)을 참고한다.
Workflow version 운영 API는
[Workflow Operations API](docs/workflow-operations.md)를 참고한다.
