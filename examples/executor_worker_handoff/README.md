# Executor Worker 전달 패키지

이 디렉터리만 복사해 다른 LangGraph Agent 저장소에 전달할 수 있다.
원본 `ex-agent`, `agent.runtime`, `ex_agent.config`는 import하지 않는다.

이 브랜치의 패키지는 **Redis 6.0.8 호환판**이다. 사용법과 Agent 계약은 유지하며
Worker core와 Redis 의존성만 보완했다. 재전달 파일, 격리 Compose 테스트,
지표 변경, 적용 범위는 [Redis 6.0.8 가이드](docs/redis-6.0.8.md)를 확인한다.
원본 Agent 서비스 및 Executor의 Redis 호환 수정까지 포함한 것은 아니다.

이 패키지가 제공하는 것은 Agent가 아니라 다음 경계다.

```text
Executor Redis event
  -> durable Worker (Inbox / Outbox / retry / ordering / session guard)
  -> EventContext
  -> receiving Agent's LangGraph resume
```

수령자는 자기 Agent, API, Executor 제출 코드와 업무 노드를 계속 소유한다.

## 디렉터리

```text
executor_worker_handoff/
├── src/worker/                    # 수정하지 않는 Worker core
├── src/agent/                     # 가장 작은 동작 예제, 교체 대상
│   └── graph/
│       ├── state.py               # Agent State 상속 예제
│       ├── nodes.py               # 제출·이벤트 적용 예제
│       └── builder.py             # 경계 노드 연결 예제
├── src/agent_worker/
│   ├── api_bridge.py              # API에서 binding/guard만 여는 도구
│   ├── graph_boundary.py          # Agent graph에 붙일 최소 State/노드
│   ├── graph_provider.py          # 수령자가 자기 graph를 연결할 파일
│   ├── langgraph_adapter.py       # EventContext -> graph resume
│   ├── worker_hooks.py            # Executor event_type registry
│   └── worker_main.py             # 그대로 사용하는 Worker entrypoint
├── migrations/                    # ew_* Worker 테이블
├── alembic.ini
├── .env.example
├── Dockerfile
└── pyproject.toml
```

`src/agent`는 Worker 자체가 아니다. 수령자가 자기 Agent 패키지를 올리기 전에 전체
연결을 빠르게 이해할 수 있도록 넣은 최소 샘플이다. LLM을 호출하지 않으며
`submit_sample_execution`도 실제 Executor API를 호출하지 않고 가짜 `execution_id`를
만든다. 운영 코드에서는 `src/agent`를 수령자의 실제 Agent 패키지로 교체한다.

## 수령자가 구현할 곳

### 1. 자기 Agent State에 경계 State 추가

자기 State를 다음처럼 확장한다. API의 최초 Agent 입력에는 `messages` 같은
기존 입력만 넣어도 된다. `task_id`와 `execution_id`는 코드 실행을 시작하는 Agent
노드가 만든다.

```python
from typing import Annotated, Any, TypedDict

from langgraph.graph.message import add_messages

from agent_worker.graph_boundary import ExecutorBoundaryState


class AgentInput(TypedDict):
    messages: list[Any]


class AgentState(ExecutorBoundaryState, total=False):
    messages: Annotated[list[Any], add_messages]
    # 수령자의 나머지 State 필드
```

LangGraph 호출 시 `session_id`는 State에 중복 저장하지 않고 다음 위치에 넣는다.

```python
config = {"configurable": {"thread_id": session_id}}
```

### 2. 자기 graph에 Executor 경계 노드 연결

수령자의 기존 노드를 그대로 유지한다. Executor 제출과 업무 처리 노드 전후에
다음 세 공통 Boundary 노드를 연결한다.

```python
from agent_worker.graph_boundary import ExecutorBoundaryNodes

boundary = ExecutorBoundaryNodes(bindings)

builder.add_node("register_execution", boundary.register_execution)
builder.add_node("wait_executor_event", boundary.wait_executor_event)
builder.add_node(
    "record_executor_receipt",
    boundary.record_executor_receipt,
)

# submit_execution은 {"task_id": ..., "execution_id": ...}를 반환한다.
builder.add_edge("submit_execution", "register_execution")
builder.add_edge("register_execution", "wait_executor_event")
builder.add_edge("wait_executor_event", "handle_executor_event")
builder.add_edge("handle_executor_event", "record_executor_receipt")
```

`handle_executor_event`는 수령자가 구현하는 업무 로직이다. 이벤트 상태를 해석하고
Agent State를 갱신하되 Worker receipt를 직접 기록하지 않는다.

```python
async def handle_executor_event(state: AgentState) -> dict[str, Any]:
    action = state["ew_pending"]
    event = action["event"]

    # 수령자의 상태 변경, 다음 실행 제출, 최종 결과 처리 등을 수행한다.
    # 외부 API 호출은 action["command_id"]에 고정 suffix를 붙여 멱등화한다.
    await apply_business_logic(
        event,
        idempotency_key=f"{action['command_id']}:apply-event",
    )

    return {
        # 수령자의 State update
        "execution_status": event["payload"]["status"],
    }
```

`record_executor_receipt`는 공통 Boundary 노드다. 앞선
`handle_executor_event`가 성공한 경우에만 실행되어 `command_id → event_id`와
마지막 `event_sequence`를 LangGraph checkpoint에 기록한다. 이 receipt가 없으면
Worker는 이벤트 적용 완료를 확인할 수 없어 해당 command를 재시도한다.

```text
register_execution          공통 Boundary
  → wait_executor_event     공통 Boundary
  → handle_executor_event   수령자 Agent 업무 로직
  → record_executor_receipt 공통 Boundary
```

Executor 실행 결과가 `FAILED`나 `CANCELLED`여도 Agent가 해당 이벤트를 정상적으로
반영했다면 receipt를 기록한다. receipt는 Executor 실행 성공이 아니라 **Agent의
이벤트 처리 완료**를 뜻한다.

### 3. `graph_provider.py`에서 자기 graph builder 호출

기본 상태에서는 [graph_provider.py](src/agent_worker/graph_provider.py)가 함께 제공된
샘플 `agent.graph`를 import한다. 수령자의 Agent를 추가한 뒤 import 경로만 교체한다.

```python
from your_agent.graph import build_graph


def build_agent_graph(*, bindings, checkpointer):
    return build_graph(
        bindings=bindings,
        checkpointer=checkpointer,
    )
```

API 프로세스와 Worker 프로세스는 반드시 같은 버전의 graph definition과 State
계약을 사용해야 한다.

### 4. `worker_hooks.py`에서 event type 선택

[worker_hooks.py](src/agent_worker/worker_hooks.py)의 key는 내부 Outbox 타입이
아니라 **Executor가 발행한 원본 `event_type`**이다.

```python
return {
    "execution.operation_completed": resume_graph,
    "execution.completed": resume_graph,
}
```

자기 graph가 실제로 기다리는 이벤트만 등록한다. 같은 Redis consumer group을
공유하는 모든 Worker replica는 같은 registry를 사용해야 한다.

## API 프로세스에서 해야 할 일

API+Agent 프로세스는 `ExecutorWorker.run()`을 실행하지 않는다. FastAPI lifespan에서
`ApiWorkerBridge`만 열고, 자기 graph를 만들 때 `bridge.bindings`를 주입한다.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from agent_worker import ApiWorkerBridge
from agent_worker.graph_provider import build_agent_graph
from worker import Settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with ApiWorkerBridge(Settings()) as bridge:
        async with AsyncPostgresSaver.from_conn_string(
            checkpoint_database_url
        ) as checkpointer:
            app.state.graph = build_agent_graph(
                bindings=bridge.bindings,
                checkpointer=checkpointer,
            )
            app.state.worker_guard = bridge.guard
            yield
```

Executor 제출 결과의 `execution_id`는 graph의 `register_execution` 노드에서
`execution_id -> session_id + task_id`로 저장된다. API와 Worker는 같은
`EW_DATABASE_URL`, `EW_REDIS_URL`, `EW_NAMESPACE`를 사용해야 한다.

`SessionGuard`는 API와 Worker가 동시에 같은 LangGraph thread를 invoke하는 것을
막는 짧은 실행 잠금이다. 며칠간 채팅을 막는 장기 세션 잠금은 Agent 서비스가
별도로 구현한다.

## 설치와 DB 초기화

```bash
uv sync --frozen --all-groups --no-editable
export EW_DATABASE_URL='postgresql://...'
uv run --no-sync alembic -c alembic.ini upgrade head
```

위 migration은 `ew_bindings`, `ew_inbox`, `ew_commands`, `ew_outbox`,
`ew_audit`만 만든다. LangGraph checkpoint 테이블은 수령자의 기존 배포 migration
또는 별도 1회성 Job에서 준비한다. Worker 시작 때 `checkpointer.setup()`을 호출하지
않는다.

## Worker 실행

`.env.example`의 값을 Kubernetes Secret/ConfigMap 환경변수로 주입한 후 실행한다.

```bash
uv run --no-sync executor-event-worker
```

또는 다음 모듈을 직접 실행한다.

```bash
uv run --no-sync python -m agent_worker.worker_main
```

하나의 image를 API와 Worker 컨테이너가 공유해도 된다. 컨테이너 명령만 다르게 둔다.

```text
API container:    uv run --no-sync uvicorn your_agent.api:app --host 0.0.0.0
Worker container: executor-event-worker
```

Worker health endpoint의 기본 포트는 `8011`이다.

```text
GET /health/live
GET /health/ready
GET /metrics
```

## Handler 결과 계약

직접 event handler를 추가할 때는 `async def`로 구현한다.

| 결과 | Worker 처리 |
|---|---|
| 정상 return | `DONE`, ACK |
| `DeferEvent` | pending 유지, 실패 횟수 미차감 |
| `IgnoreEvent` | `IGNORED`, ACK |
| `RejectEvent` | `FAILED`, DLQ |
| 그 외 예외 | 설정된 횟수만큼 재시도 후 DLQ |

Worker가 Handler를 호출할 때 전달하는 `EventContext`에는 다음 값이 들어 있다.

```text
namespace
session_id
task_id
execution_id
command_id
event                 # Executor 원본 event와 payload
```

Inbox/Outbox와 내부 command Stream은 Worker 내부 구현이다. Agent 개발자는
`context.event.event_type`, `context.event.payload`와 위 식별자만 사용하면 된다.

## Executor event 계약

Executor Redis Stream entry는 다음 field를 제공해야 한다.

```text
event_id          UUID
execution_id      UUID
event_type        non-empty string
event_sequence    integer >= 1
schema_version    "1.0"
occurred_at       timestamp string
payload           JSON object string
```

이벤트 순번이 빠지거나 뒤집혀 도착하면 Worker가 다음 API로 누락 이력을 조회한다.

```http
GET {EW_EXECUTOR_BASE_URL}/executions/{execution_id}/events
  ?after_sequence={last_sequence}&limit={batch_size}
```

## 전달 전 확인

```bash
uv sync --frozen --all-groups --no-editable
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync ty check
uv run --no-sync pytest
```

외부 서비스가 없으면 통합 테스트는 skip된다. 실제 Redis/DB 호환 검증은
[Compose 테스트](docs/redis-6.0.8.md#검증-실행)로 실행한다.

수령자에게는 이 `executor_worker_handoff` 디렉터리 하나만 전달한다. 기존 저장소의
`src/agent`, `src/ex_agent`, 루트 `worker_main.py`는 함께 전달하지 않는다.

여기서 전달하지 않는 `src/agent`는 원본 `ex-agent` 저장소의 Agent를 뜻한다.
이 디렉터리 내부의 `src/agent`는 의존성 없는 최소 샘플이므로 전달 대상이다.
