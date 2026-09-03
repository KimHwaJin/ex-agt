# 공통 Executor Worker 실전 인수인계 가이드

이 문서는 `src/worker`를 오늘 다른 Agent 서비스에 이관하기 위한 단일 진입점이다.
기준 소스는 현재 저장소의 `worker` 패키지다.

## 1. 지금 따로 떼어 쓸 수 있는가

가능하다. `src/worker`는 `agent`, `ex_agent`, FastAPI, LangChain, LangGraph를 import하지
않는다. Worker core는 Executor Redis event를 안전하게 받아 PostgreSQL
Inbox/Outbox를 거쳐 등록된 비동기 handler에 전달한다. 받는 서비스의 LangGraph
State, 노드, API와 업무 정책은 handler 바깥에 둔다.

다만 `src/worker`만 복사하면 자동으로 Agent가 완성되는 것은 아니다. 받는 개발자는
이 문서의 **호스트가 채울 부분**을 자신의 코드에 구현해야 한다.

### 반드시 전달할 파일

| 경로 | 용도 |
|---|---|
| `src/worker/` | 독립 Worker core와 이 문서 |
| `worker_migrations/` | `ew_*` Inbox/Outbox/binding/audit 테이블 |
| `deploy/worker/.env.example` | 독립 `EW_*` 환경변수 예시 |

### 구현 참고용으로 같이 전달할 파일

| 경로 | 개발자가 가져가서 수정할 부분 |
|---|---|
| `src/agent/worker_main.py` | Worker 프로세스 시작·종료 골격 |
| `src/agent/integrations/worker_hooks.py` | Executor event type별 handler registry |
| `src/agent/integrations/langgraph_adapter.py` | EventContext → LangGraph resume 변환 예제 |
| `src/agent/runtime/bridge.py` | API가 Worker DB binding과 SessionGuard를 공유하는 예제 |
| `examples/worker/session_graph.py` | wait → 수락 checkpoint → 적용/receipt 예제 |
| `examples/worker/api_integration.py` | execution binding 등록 예제 |
| `examples/worker/failure_cleanup.py` | 선택적인 Executor 취소 확인 예제 |

배포 예제가 필요하면 `deploy/worker/deployment.yaml.example`과
`deploy/worker/migrate-job.yaml.example`도 같이 전달한다. 전자는 받는 서비스의
image와 API/Worker entrypoint에 맞춰 수정하는 골격이고, 후자는 `ew_*` 테이블을
초기화하는 Job 예제다.

`src/agent`는 현재 ex-agent 업무에 특화되어 있으므로 통째로 공통 패키지라고 주장하지
않는다. 위 파일은 받는 서비스가 자기 패키지 아래로 복사·수정할 참고 구현이다.

## 2. 설치 의존성

Worker core의 최소 런타임은 Python 3.12 이상과 다음 패키지다.

```toml
dependencies = [
    "httpx>=0.28,<1",
    "prometheus-client>=0.22,<1",
    "psycopg[binary,pool]>=3.2,<4",
    "pydantic>=2,<3",
    "pydantic-settings>=2,<3",
    "redis>=6.4,<7",
]
```

`worker_migrations`를 독립 Alembic chain으로 실행하면 다음도 필요하다.

```toml
"alembic>=1.16,<2"
"sqlalchemy[asyncio]>=2,<3"
```

LangGraph로 resume할 호스트에는 별도로 `langgraph`와 운영용 PostgreSQL
checkpointer가 필요하다. Worker core 자체의 의존성은 아니다.

## 3. 전체 처리 흐름

```text
Executor Redis Stream
  → ingress consumer
  → 원본 event를 ew_inbox에 저장
  → 원본 Redis message ACK
  → execution binding 기준으로 연속된 sequence만 advance
  → ew_commands + ew_outbox를 한 DB transaction으로 생성
  → outbox relay가 내부 command Stream에 발행
  → dispatch consumer
  → DB에서 EventContext 복원
  → session_id 기반 SessionGuard 획득
  → event_type handler 호출
  → DONE/IGNORED/FAILED 기록
  → 내부 Redis message ACK 또는 pending 유지/DLQ
```

원본 이벤트는 DB에 저장된 뒤 ACK된다. Outbox 발행과 handler 호출은 at-least-once일
수 있으므로 handler의 외부 부수 효과는 `command_id + 고정된 작업명`으로 멱등하게
만든다.

## 4. 받는 개발자가 채울 코드

### 4.1 Worker main

자기 서비스에 `your_agent/worker_main.py`를 만들고 다음 자원을 조립한다.

1. `worker.Settings` 또는 호스트 설정에서 변환한 값을 만든다.
2. event type별 handler registry를 만든다.
3. `ExecutorWorker(settings, handlers)`를 연다.
4. 운영용 PostgreSQL checkpointer를 연다.
5. API와 같은 버전·State 계약의 graph를 compile한다.
6. handler가 사용할 graph adapter를 연결한다.
7. SIGTERM/SIGINT에서 `worker.request_stop()`을 호출한다.
8. `await worker.run()`으로 foreground 실행한다.

현재 동작하는 골격은
[agent/worker_main.py](../../agent/worker_main.py)에 있다. 받는 서비스의 실행 명령은
다음처럼 별도로 둔다.

```text
python -m your_agent.worker_main
```

API 컨테이너에서 이 main을 실행하지 않는다. 같은 image를 사용하더라도 Worker
컨테이너의 `args`만 Worker main으로 교체한다.

### 4.2 event type registry

`handlers`의 key는 Executor 원본 `event_type`, value는
`async def handler(context: EventContext) -> None` 함수다.

```python
handlers = {
    "execution.operation_completed": resume_graph,
    "execution.completed": resume_graph,
    # 구현을 완료한 뒤에만 등록한다.
    # "execution.step_completed": persist_progress,
}
```

자기 그래프가 실제로 기다리는 이벤트만 등록한다. 같은 Redis consumer group을
공유하는 모든 replica는 동일한 registry를 사용해야 한다. 등록하지 않은 타입은
Executor 순번을 막지 않고 `IGNORED`로 기록된다.

등록한 handler가 정상 return하면 Worker는 Command를 `DONE`으로 확정한다. 빈
`pass`나 로그만 수행하는 미완성 handler를 등록하면 이벤트를 영구 처리한 것으로
기록하므로 금지한다.

### 4.3 handler 결과 계약

```python
from worker import DeferEvent, IgnoreEvent, RejectEvent
```

| handler 결과 | Worker 처리 |
|---|---|
| 정상 return | `DONE`, ACK |
| `DeferEvent` | ACK하지 않고 대기, 업무 실패 횟수 미차감 |
| `IgnoreEvent` | `IGNORED`, ACK |
| `RejectEvent` | 즉시 `FAILED`, DLQ |
| 그 외 예외 | 업무 실패 횟수 증가, 한도 전 pending 재처리, 소진 시 DLQ |

DB·Redis 일시 장애와 SessionGuard 충돌은 Worker가 `DEFER`로 처리한다. handler에서
네트워크 일시 장애를 `RejectEvent`로 바꾸지 않는다.

### 4.4 LangGraph adapter와 State

`EventContext`는 LangGraph State가 아니다. 다음 식별 정보와 Executor 원본을 담은
Worker → 호스트 경계 객체다.

```text
namespace
session_id
task_id
execution_id
command_id
event
```

이 프로젝트의 adapter는 다음 State 필드를 사용한다.

| State 필드 | 의미 |
|---|---|
| `active_task_id` | 현재 세션에서 실행 중인 Task |
| `execution_id` | 현재 Executor Execution |
| `ew_pending` | checkpoint가 수락한 Worker action |
| `ew_receipts` | `command_id → event_id` 처리 영수증 |
| `ew_sequences` | `execution_id → 마지막 적용 sequence` |

받는 서비스의 State가 다르면 `src/worker`를 수정하지 말고 자기
`langgraph_adapter.py`에서 매핑한다. API와 Worker는 반드시 같은 graph definition,
PostgreSQL checkpointer와 다음 thread 설정을 사용한다.

```python
config = {"configurable": {"thread_id": context.session_id}}
```

즉 현재 인수 계약은 `session_id = LangGraph thread_id`다. `task_id`는 현재 작업의
소유권 검증, `execution_id`는 Executor 실행 연결, `command_id`는 Worker 전달의
멱등성에 사용한다. 서로 대체하지 않는다.

Executor 대기 노드는 자신의 대기 종류를 명시한다.

```python
action = interrupt(
    {
        "kind": "EXECUTOR_EVENT",
        "task_id": state["active_task_id"],
        "execution_id": state["execution_id"],
    }
)
return {"ew_pending": action}
```

Worker adapter는 정확히 이 interrupt만 `Command(resume=...)`으로 재개해야 한다.
사용자 승인 interrupt에는 Executor event를 넣지 않는다. 수락 node와 실제 적용
node를 분리해 action이 먼저 checkpoint되게 하고, 적용 완료 node가 receipt와
sequence를 기록하게 한다. `ainvoke(..., durability="sync")`로 다음 전달 확정 전에
checkpoint 저장을 기다린다.

### 4.5 API의 execution binding 등록

Worker가 event를 session과 Task로 연결하려면 Executor 제출 응답의
`execution_id`를 반드시 등록해야 한다.

```python
await bindings.register(
    execution_id=execution_id,
    session_id=session_id,
    task_id=task_id,
)
```

등록은 불변이다. 같은 `execution_id`를 다른 session이나 Task로 다시 등록하면
실패한다. API와 Worker는 같은 Worker PostgreSQL DB와 `namespace`를 사용해야 한다.

권장 순서는 다음과 같다.

1. API가 `SessionGuard(session_id)`를 획득한다.
2. 안정적인 업무 idempotency key로 Executor에 제출하거나 기존 제출을 복원한다.
3. 반환된 `execution_id` binding을 등록한다.
4. 같은 guard 안에서 graph가 Executor 대기 checkpoint까지 저장됐는지 확인한다.
5. guard를 해제한다.

이벤트가 binding보다 먼저 와도 Inbox에는 보존되지만 API는 제출 응답 유실 시 같은
idempotency key로 `execution_id`를 복원하고 binding 등록을 완료해야 한다.

### 4.6 API와 SessionGuard 공유

API가 graph를 직접 invoke/resume한다면 Worker와 같은 Redis, `namespace`의
`SessionGuard`를 사용한다. 현재 참고 구현은
[runtime/bridge.py](../../agent/runtime/bridge.py)다. API에서는
`ExecutorWorker.run()`을 시작하지 않고 Store/binding/guard 연결만 연다.

Dispatcher가 handler 호출 전에 이미 SessionGuard를 획득하므로 event handler에서
같은 guard를 다시 잡지 않는다. 이 guard는 짧은 graph invocation 충돌 방지용이다.
며칠짜리 코드 실행 동안 채팅을 막는 장기 Session lock은 호스트 서비스가 별도로
저장·관리한다.

## 5. Executor event 계약

Executor 원본 Redis Stream entry는 다음 field를 제공해야 한다.

| field | 형식 |
|---|---|
| `event_id` | UUID 문자열, 이벤트의 전역 멱등 식별자 |
| `execution_id` | UUID 문자열 |
| `event_type` | 비어 있지 않은 문자열 |
| `event_sequence` | 1 이상 정수 문자열, Execution별 연속 순번 |
| `schema_version` | 문자열 `1.0` |
| `occurred_at` | 발생 시각 문자열 |
| `payload` | JSON object를 직렬화한 문자열 |

순번이 뒤집히거나 누락되면 Worker는 다음 REST API로 이력을 보충한다.

```http
GET {EW_EXECUTOR_BASE_URL}/executions/{execution_id}/events
  ?after_sequence={last_sequence}&limit={batch_size}
```

응답은 최소한 다음 형태여야 한다.

```json
{
  "items": [
    {
      "event_id": "00000000-0000-4000-8000-000000000001",
      "execution_id": "00000000-0000-4000-8000-000000000002",
      "event_type": "execution.completed",
      "event_sequence": 2,
      "schema_version": "1.0",
      "occurred_at": "2026-09-03T00:00:00Z",
      "payload": {}
    }
  ],
  "has_more": false
}
```

내부 command Stream의 `schema_version`은 `1`이다. 이것은 Executor event의 `1.0`과
서로 다른 envelope 버전이므로 혼동하지 않는다.

## 6. 전체 설정값

`worker.Settings()`를 직접 사용하면 모든 환경변수에 `EW_` 접두사가 붙는다.
호스트가 이미 Settings 체계를 갖고 있다면 환경변수를 중복 정의하지 말고
`WorkerSettings(...)`를 명시적으로 생성하는 방식을 권장한다. 현재 변환 예제는
`src/agent/runtime/config.py`다.

설정의 source of truth와 예시는 다음 위치에 있다.

| 위치 | 역할 |
|---|---|
| `src/worker/config.py` | 공통 Worker가 실제로 읽고 검증하는 전체 설정 |
| `deploy/worker/.env.example` | `EW_*` 직접 주입 예시 |
| `src/agent/runtime/config.py` | 기존 서비스 설정을 `WorkerSettings`로 변환하는 예시 |
| `deploy/worker/deployment.yaml.example` | Pod UID와 Secret을 넣는 Kubernetes 예시 |

`.env.example`은 문서용 템플릿이며 `worker.Settings()`가 파일을 자동으로 읽지는
않는다. 컨테이너 환경변수로 주입하거나 실행기가 `.env`를 명시적으로 로드해야 한다.

| 환경변수 | 필수/기본값 | 의미 |
|---|---|---|
| `EW_DATABASE_URL` | 필수 | `ew_*` 테이블을 둘 PostgreSQL psycopg URL |
| `EW_REDIS_URL` | 필수 | Executor event와 내부 command를 사용할 Redis URL |
| `EW_NAMESPACE` | `executor-worker` | DB 행·Redis key·기본 Stream을 구분하는 서비스 단위 이름 |
| `EW_EXECUTOR_BASE_URL` | `http://localhost:8000/api/v1` | 순번 누락 시 Executor history 조회 base URL |
| `EW_EXECUTOR_EVENT_STREAM` | `executor.events` | Executor가 발행하는 원본 Stream |
| `EW_COMMAND_STREAM_NAME` | `{namespace}:commands` | Worker 내부 Outbox 발행 Stream |
| `EW_EVENT_GROUP_NAME` | `{namespace}:ingress` | 원본 Stream consumer group |
| `EW_COMMAND_GROUP_NAME` | `{namespace}:dispatch` | 내부 Stream consumer group |
| `EW_INSTANCE_ID` | 시작 시 UUID | replica별 Redis consumer 이름 prefix |
| `EW_CONCURRENCY` | `4` | ingress/dispatch 공통 fallback 슬롯 수 |
| `EW_INGRESS_CONCURRENCY` | `EW_CONCURRENCY` | 원본 이벤트 소비 슬롯 수 |
| `EW_DISPATCH_CONCURRENCY` | `EW_CONCURRENCY` | 업무 handler 실행 슬롯 수 |
| `EW_POOL_SIZE` | `8` | Worker PostgreSQL connection pool 크기 |
| `EW_BATCH_SIZE` | `100` | history, routing, outbox 처리 묶음 크기 |
| `EW_POLL_SECONDS` | `0.2` | Router/Outbox 활성 poll 간격 |
| `EW_IDLE_POLL_SECONDS` | `2` | 일이 없을 때 최대 poll 간격 |
| `EW_CLAIM_IDLE_MILLISECONDS` | `30000` | 다른 consumer의 pending 회수 대기시간 |
| `EW_LEASE_TTL_SECONDS` | `60` | SessionGuard/처리 lease TTL |
| `EW_LEASE_RENEW_SECONDS` | `1` | lease heartbeat 간격 |
| `EW_PUBLISH_LEASE_SECONDS` | `30` | DB Outbox claim TTL |
| `EW_MAX_HANDLER_ATTEMPTS` | `5` | 업무 handler 최종 실패 전 시도 상한 |
| `EW_SHUTDOWN_SECONDS` | `25` | 종료 시 진행 handler drain 상한 |
| `EW_REQUEST_TIMEOUT_SECONDS` | `10` | Redis 연결과 Executor HTTP timeout |
| `EW_HEALTH_PORT` | `8011` | health/metrics HTTP 포트, `0`이면 비활성 |

설정 규칙:

- `EW_NAMESPACE`는 환경·서비스별로 안정적으로 고정하고 Pod, 사용자, session별로
  바꾸지 않는다.
- 같은 서비스 replica는 Stream/group/namespace를 공유하고
  `EW_INSTANCE_ID`만 고유해야 한다. Kubernetes에서는 Pod UID를 사용한다.
- 서로 다른 서비스가 같은 Executor Stream을 읽으면 서로 다른 event group과
  namespace를 사용한다.
- `EW_LEASE_RENEW_SECONDS * 2 < EW_LEASE_TTL_SECONDS`여야 한다.
- lease 갱신 간격은 `EW_CLAIM_IDLE_MILLISECONDS`보다 충분히 짧아야 한다.
- pool size는 동시에 수행할 ingress/dispatch와 호스트 DB 사용량을 포함해 산정한다.

### LangGraph 호스트가 별도로 정할 설정

아래 값은 Worker core의 `EW_*` 설정이 아니며 받는 Agent의 설정 체계에 둔다.

| 설정 | 요구사항 |
|---|---|
| LangGraph checkpoint PostgreSQL URL | API와 Worker가 동일한 DB를 사용한다. |
| graph/runtime factory | API와 Worker가 같은 State·node 버전을 compile한다. |
| Worker entrypoint | `python -m your_agent.worker_main` 형태로 고정한다. |
| Executor 제출 idempotency key | API가 응답 유실 후 같은 execution을 복원할 수 있어야 한다. |
| 장기 session lock 저장소 | 코드 실행 중 채팅 차단 상태를 제품 DB 등에 보존한다. |
| product event/SSE 저장소 | 진행·완료 상태를 BFF와 프론트에 전달한다. |

LLM 모델, Skill, 분석 함수, 파일 저장소 설정은 event handler가 실제 Agent graph를
재개한 뒤 사용하는 호스트 업무 설정이다. Worker core가 직접 읽지 않는다.

현재 core 내부에 고정된 transport 값도 있다. Ingress/dispatch DLQ 이름은 각각
`{namespace}:ingress:dlq`, `{namespace}:dispatch:dlq`이고, 전송 오류 재시도 상태는
7일, pending이 없는 consumer 정리 기준은 1일이다. 이를 환경별로 바꿔야 한다면
`src/worker/runtime.py`에서 하드코딩하지 말고 새 `Settings` 필드로 승격해야 한다.

## 7. DB migration

Worker 시작은 DDL을 실행하지 않는다. 배포 전에 다음을 한 번 실행한다.

```bash
EW_DATABASE_URL='postgresql://user:password@host:5432/database' \
  alembic -c worker_migrations/alembic.ini upgrade head
```

독립 chain은 `ew_alembic_version`을 사용하며 다음 테이블을 만든다.

```text
ew_bindings
ew_inbox
ew_commands
ew_outbox
ew_audit
```

받는 서비스가 기존 Alembic chain 하나만 허용하면
`worker_migrations/versions/0001_worker_tables.py`의 operation을 자기 신규 revision에
옮길 수 있다. 기존 테이블을 검증 없이 `stamp`하지 않는다. LangGraph PostgreSQL
checkpointer의 `setup()`도 Worker 시작이 아니라 별도 migration/release 단계에서
실행한다.

상세 절차는 [migrations.md](migrations.md)에 있다.

## 8. 배포 계약

권장 배포는 같은 image, 같은 Pod의 두 컨테이너다.

```text
api-agent container → 수신 서비스의 API main
worker container    → python -m your_agent.worker_main
```

Worker에는 다음이 필요하다.

- PostgreSQL/Redis/Executor URL Secret 또는 ConfigMap
- Worker DB migration 완료
- API와 동일한 graph 코드와 PostgreSQL checkpoint 연결
- Worker health port `8011`의 `/health/live`, `/health/ready`, `/metrics`
- Worker drain보다 긴 Pod `terminationGracePeriodSeconds`
- `WORKER_INSTANCE_ID` 또는 `EW_INSTANCE_ID`에 Pod UID

API와 Worker가 `PATH` 방식 Executor 파일을 함께 사용한다면 같은 공유 PVC도
mount한다. BFF 인증키처럼 API에만 필요한 Secret은 Worker에 주입하지 않는다.

## 9. Worker가 제공하지 않는 기능

다음은 받는 Agent 서비스가 구현한다.

- 사용자 인증·인가, 채팅 API, session/task 생성
- 분석 계획, Skill/Tool 선택, 코드 생성과 Executor 제출
- Executor 제출 idempotency와 응답 유실 복구
- 사용자 승인·수정·거절, 장기 Session lock
- `execution.completed`의 성공/실패/취소 판정과 결과 조회
- 성공 리포트 작성, 실패 안내, 취소 후 terminal 확인
- 진행 상황을 제품 DB/SSE로 전달하는 projection
- FAILED Command 재시도/skip을 위한 관리자 API와 권한
- Redis Stream·Inbox·receipt 보존 및 정리 정책

Worker는 메시지 전달을 exactly-once로 만들지 않는다. DB Inbox/Outbox, pending
reclaim과 receipt를 조합해 중복에도 같은 결과로 수렴할 기반을 제공한다.

## 10. 오늘 전달 전 체크리스트

### 코드·설정

- [ ] `src/worker`, `worker_migrations`, 이 문서를 전달했다.
- [ ] 최소 의존성을 받는 프로젝트의 lockfile에 추가했다.
- [ ] 모든 `EW_*` 또는 명시적 `WorkerSettings` 값을 환경별로 정했다.
- [ ] 서비스별 고유 namespace와 consumer group을 정했다.
- [ ] 모든 Worker replica가 동일한 handler registry를 사용한다.

### Agent 연결

- [ ] `session_id = thread_id`로 API와 Worker가 같은 checkpointer를 사용한다.
- [ ] Executor 제출 후 execution/session/task binding을 반드시 등록한다.
- [ ] Executor wait interrupt와 사용자 승인 interrupt를 구분한다.
- [ ] action 수락 checkpoint와 실제 적용 node를 분리했다.
- [ ] `command_id → event_id` receipt와 execution sequence를 저장한다.
- [ ] 외부 부수 효과가 command ID 기반으로 멱등하다.

### 운영 검증

- [ ] Alembic과 LangGraph checkpoint migration을 Worker 시작 전에 완료했다.
- [ ] event가 binding보다 먼저 도착해도 나중에 처리되는지 확인했다.
- [ ] 중복·역순·누락 event를 주입해 최종 순서를 확인했다.
- [ ] handler 도중 Worker를 종료하고 pending reclaim을 확인했다.
- [ ] `/health/live`, `/health/ready`, `/metrics`를 확인했다.
- [ ] Worker 종료 유예 후 미완료 command/outbox/lock이 수렴하는지 확인했다.

현재 저장소의 격리 회귀는 `tests/worker`와
`deploy/worker/compose.test.yaml`에서 실행한다.

```bash
docker compose -f deploy/worker/compose.test.yaml run --build --rm test
```

더 상세한 내부 기능은 [features.md](features.md), 현재 Agent 연결은
[agent-integration.md](agent-integration.md), 과거 검증 근거는
[validation-history.md](validation-history.md)를 참고한다.
